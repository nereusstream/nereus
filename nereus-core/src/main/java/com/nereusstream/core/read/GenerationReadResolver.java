/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.read;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PublicationId;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.ResolvedRange;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamState;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.core.physical.ObjectReadLease;
import com.nereusstream.core.physical.ObjectReadPinManager;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.metadata.oxia.F4MetadataConditionFailedException;
import com.nereusstream.metadata.oxia.F4ScanToken;
import com.nereusstream.metadata.oxia.GenerationIndexValidator;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationScanPage;
import com.nereusstream.metadata.oxia.OffsetIndexEntry;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.metadata.oxia.VersionedGenerationZeroIndex;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

/** Authoritative view-scoped generation resolver with durable object pinning and same-view fallback. */
public final class GenerationReadResolver {
    public static final long MAX_GENERATION_RANGE_RECORDS = 1_048_576L;
    public static final int GENERATION_SCAN_PAGE_SIZE = 512;
    public static final int MAX_GENERATION_CANDIDATES_PER_RESOLVE = 4_096;

    private static final Comparator<GenerationReadCandidate> GENERATION_DESCENDING = Comparator
            .comparingLong((GenerationReadCandidate value) -> value.resolvedRange().generation())
            .reversed();

    private final String cluster;
    private final OxiaMetadataStore l0Store;
    private final GenerationMetadataStore generationStore;
    private final GenerationIndexValidator indexValidator;
    private final ReadTargetReaderRegistry readers;
    private final PhysicalObjectIdentityResolver identityResolver;
    private final ObjectReadPinManager pinManager;
    private final GenerationIndexRepairer repairer;
    private final Clock clock;
    private final Executor callbackExecutor;

    public GenerationReadResolver(
            String cluster,
            OxiaMetadataStore l0Store,
            GenerationMetadataStore generationStore,
            GenerationIndexValidator indexValidator,
            ReadTargetReaderRegistry readers,
            PhysicalObjectIdentityResolver identityResolver,
            ObjectReadPinManager pinManager,
            int maxRepairCommits,
            Clock clock,
            Executor callbackExecutor) {
        this(
                cluster,
                l0Store,
                generationStore,
                indexValidator,
                readers,
                identityResolver,
                pinManager,
                new MetadataGenerationIndexRepairer(
                        cluster, l0Store, maxRepairCommits),
                clock,
                callbackExecutor);
    }

    public GenerationReadResolver(
            String cluster,
            OxiaMetadataStore l0Store,
            GenerationMetadataStore generationStore,
            GenerationIndexValidator indexValidator,
            ReadTargetReaderRegistry readers,
            PhysicalObjectIdentityResolver identityResolver,
            ObjectReadPinManager pinManager,
            GenerationIndexRepairer repairer,
            Clock clock,
            Executor callbackExecutor) {
        this.cluster = requireText(cluster, "cluster");
        this.l0Store = Objects.requireNonNull(l0Store, "l0Store");
        this.generationStore = Objects.requireNonNull(generationStore, "generationStore");
        this.indexValidator = Objects.requireNonNull(indexValidator, "indexValidator");
        this.readers = Objects.requireNonNull(readers, "readers");
        this.identityResolver = Objects.requireNonNull(identityResolver, "identityResolver");
        this.pinManager = Objects.requireNonNull(pinManager, "pinManager");
        this.repairer = Objects.requireNonNull(repairer, "repairer");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.callbackExecutor = Objects.requireNonNull(callbackExecutor, "callbackExecutor");
    }

    public CompletableFuture<Optional<PinnedResolvedRange>> resolve(
            StreamId streamId,
            long offset,
            ReadView view,
            Duration timeout) {
        return resolve(
                Objects.requireNonNull(streamId, "streamId"),
                offset,
                Objects.requireNonNull(view, "view"),
                new ReadOperationDeadline(timeout),
                true,
                Set.of());
    }

    CompletableFuture<Optional<PinnedResolvedRange>> resolve(
            StreamId streamId,
            long offset,
            ReadView view,
            ReadOperationDeadline deadline,
            boolean allowRepair) {
        return resolve(streamId, offset, view, deadline, allowRepair, Set.of());
    }

    CompletableFuture<Optional<PinnedResolvedRange>> resolve(
            StreamId streamId,
            long offset,
            ReadView view,
            ReadOperationDeadline deadline,
            boolean allowRepair,
            Set<GenerationReadCandidate> excludedCandidates) {
        Set<GenerationReadCandidate> exclusions = Set.copyOf(
                Objects.requireNonNull(excludedCandidates, "excludedCandidates"));
        if (offset < 0) {
            return NereusException.failedFuture(
                    ErrorCode.INVALID_ARGUMENT, false, "read offset must be non-negative");
        }
        return loadSnapshot(streamId, deadline).thenComposeAsync(snapshot -> {
            StorageProfile profile = validateReadable(streamId, offset, snapshot);
            if (offset >= snapshot.committedEnd().committedEndOffset()) {
                return CompletableFuture.completedFuture(Optional.empty());
            }
            long maximumEnd = Math.min(
                    snapshot.committedEnd().committedEndOffset(),
                    saturatingAdd(offset, MAX_GENERATION_RANGE_RECORDS));
            return scanAll(
                            streamId,
                            view,
                            Math.addExact(offset, 1),
                            maximumEnd,
                            Optional.empty(),
                            new ArrayList<>(),
                            deadline)
                    .thenCompose(wrappers -> candidates(
                            streamId,
                            offset,
                            view,
                            snapshot,
                            wrappers,
                            profile.objectMaterializationEnabled()))
                    .thenCompose(candidates -> {
                        List<GenerationReadCandidate> admitted = candidates.stream()
                                .filter(candidate -> !exclusions.contains(candidate))
                                .toList();
                        if (!admitted.isEmpty()) {
                            long maximumReadDeadlineMillis = maximumReadDeadlineMillis(deadline);
                            return pinNext(
                                    streamId,
                                    offset,
                                    view,
                                    admitted,
                                    0,
                                    maximumReadDeadlineMillis,
                                    deadline,
                                    null);
                        }
                        if (!candidates.isEmpty() && !exclusions.isEmpty()) {
                            return CompletableFuture.failedFuture(new NereusException(
                                    ErrorCode.READ_RESOLUTION_FAILED,
                                    true,
                                    "all same-view generation candidates failed during this read"));
                        }
                        if (allowRepair && view == ReadView.COMMITTED) {
                            return deadline.bound(
                                            () -> repairer.repair(
                                                    streamId,
                                                    offset,
                                                    deadline.remaining()),
                                            "repair committed generation index")
                                    .thenCompose(result -> {
                                        requireRepairResult(streamId, offset, result);
                                        if (result.source()
                                                == GenerationIndexRepairSource.TRIMMED) {
                                            return CompletableFuture.failedFuture(
                                                    new NereusException(
                                                            ErrorCode.OFFSET_TRIMMED,
                                                            false,
                                                            "requested offset was trimmed during index repair"));
                                        }
                                        return resolve(
                                                streamId,
                                                offset,
                                                view,
                                                deadline,
                                                false,
                                                exclusions);
                                    });
                        }
                        return CompletableFuture.failedFuture(new NereusException(
                                ErrorCode.READ_RESOLUTION_FAILED,
                                true,
                                "no committed generation covers a committed offset"));
                    });
        }, callbackExecutor);
    }

    private CompletableFuture<StreamMetadataSnapshot> loadSnapshot(
            StreamId streamId,
            ReadOperationDeadline deadline) {
        return deadline.bound(
                () -> l0Store.getStreamSnapshot(cluster, streamId),
                "load stream head for generation resolve");
    }

    private CompletableFuture<List<VersionedGenerationCandidate>> scanAll(
            StreamId streamId,
            ReadView view,
            long minimumEnd,
            long maximumEnd,
            Optional<F4ScanToken> continuation,
            List<VersionedGenerationCandidate> accumulated,
            ReadOperationDeadline deadline) {
        int remaining = MAX_GENERATION_CANDIDATES_PER_RESOLVE - accumulated.size();
        int pageSize = Math.min(GENERATION_SCAN_PAGE_SIZE, Math.max(1, remaining));
        return deadline.bound(
                        () -> generationStore.scanIndex(
                                cluster,
                                streamId,
                                view,
                                minimumEnd,
                                maximumEnd,
                                continuation,
                                pageSize),
                        "scan generation index")
                .thenCompose(page -> appendPage(
                        streamId,
                        view,
                        minimumEnd,
                        maximumEnd,
                        accumulated,
                        page,
                        deadline));
    }

    private CompletableFuture<List<VersionedGenerationCandidate>> appendPage(
            StreamId streamId,
            ReadView view,
            long minimumEnd,
            long maximumEnd,
            List<VersionedGenerationCandidate> accumulated,
            GenerationScanPage page,
            ReadOperationDeadline deadline) {
        if (accumulated.size() + page.values().size() > MAX_GENERATION_CANDIDATES_PER_RESOLVE) {
            return metadataLimit();
        }
        accumulated.addAll(page.values());
        if (page.continuation().isEmpty()) {
            return CompletableFuture.completedFuture(List.copyOf(accumulated));
        }
        if (accumulated.size() == MAX_GENERATION_CANDIDATES_PER_RESOLVE) {
            return deadline.bound(
                            () -> generationStore.scanIndex(
                                    cluster,
                                    streamId,
                                    view,
                                    minimumEnd,
                                    maximumEnd,
                                    page.continuation(),
                                    1),
                            "probe generation candidate limit")
                    .thenCompose(probe -> probe.values().isEmpty()
                            ? CompletableFuture.completedFuture(List.copyOf(accumulated))
                            : metadataLimit());
        }
        return scanAll(
                streamId,
                view,
                minimumEnd,
                maximumEnd,
                page.continuation(),
                accumulated,
                deadline);
    }

    private CompletableFuture<List<GenerationReadCandidate>> candidates(
            StreamId streamId,
            long offset,
            ReadView view,
            StreamMetadataSnapshot snapshot,
            List<VersionedGenerationCandidate> wrappers,
            boolean higherGenerationsAllowed) {
        Map<Long, String> positiveGenerationKeys = new HashMap<>();
        List<GenerationReadCandidate> result = new ArrayList<>();
        for (VersionedGenerationCandidate wrapper : wrappers) {
            if (wrapper instanceof VersionedGenerationIndex higher) {
                if (!higherGenerationsAllowed) {
                    return CompletableFuture.failedFuture(invariant(
                            "a primary-WAL-only profile contains a higher generation",
                            null));
                }
                String previous = positiveGenerationKeys.putIfAbsent(
                        higher.value().generation(), higher.key());
                if (previous != null && !previous.equals(higher.key())) {
                    return CompletableFuture.failedFuture(invariant(
                            "one stream/view contains duplicate positive generation numbers", null));
                }
                if (higher.value().lifecycle() != GenerationLifecycle.COMMITTED) {
                    continue;
                }
                OffsetIndexEntry entry = indexValidator.requireCommitted(
                        higher,
                        streamId,
                        view,
                        snapshot.committedEnd().committedEndOffset(),
                        snapshot.committedEnd().commitVersion());
                if (covers(entry, offset)) {
                    result.add(toCandidate(
                            view,
                            entry,
                            higher.key(),
                            higher.durableValueSha256(),
                            Optional.of(new PublicationId(higher.value().publicationId()))));
                }
            } else if (wrapper instanceof VersionedGenerationZeroIndex zero) {
                if (view != ReadView.COMMITTED) {
                    return CompletableFuture.failedFuture(invariant(
                            "generation zero appeared outside COMMITTED view", null));
                }
                OffsetIndexEntry entry = zero.value();
                if (!entry.tombstoned()
                        && entry.commitVersion() <= snapshot.committedEnd().commitVersion()
                        && entry.offsetEnd() <= snapshot.committedEnd().committedEndOffset()
                        && covers(entry, offset)) {
                    result.add(toCandidate(
                            view,
                            entry,
                            zero.key(),
                            zero.durableValueSha256(),
                            Optional.empty()));
                }
            } else {
                return CompletableFuture.failedFuture(invariant(
                        "generation scan returned an unknown candidate wrapper", null));
            }
        }
        result.sort(GENERATION_DESCENDING);
        return CompletableFuture.completedFuture(List.copyOf(result));
    }

    private CompletableFuture<Optional<PinnedResolvedRange>> pinNext(
            StreamId streamId,
            long offset,
            ReadView view,
            List<GenerationReadCandidate> candidates,
            int index,
            long maximumReadDeadlineMillis,
            ReadOperationDeadline deadline,
            Throwable lastFallbackFailure) {
        if (index >= candidates.size()) {
            NereusException unavailable = new NereusException(
                    ErrorCode.READ_RESOLUTION_FAILED,
                    true,
                    "all same-view generation candidates lost pin revalidation",
                    lastFallbackFailure);
            return CompletableFuture.failedFuture(unavailable);
        }
        GenerationReadCandidate candidate = candidates.get(index);
        try {
            readers.require(candidate.resolvedRange().readTarget());
        } catch (Throwable unsupported) {
            return CompletableFuture.failedFuture(unsupported);
        }
        if (!(candidate.resolvedRange().readTarget() instanceof ObjectSliceReadTarget objectTarget)) {
            if (!candidate.generationZero()) {
                return CompletableFuture.failedFuture(new NereusException(
                        ErrorCode.UNSUPPORTED_READ_TARGET,
                        false,
                        "higher generations require an object-slice target"));
            }
            return CompletableFuture.completedFuture(Optional.of(new PinnedResolvedRange(candidate)));
        }
        return deadline.bound(
                        () -> identityResolver.resolve(objectTarget, view),
                        "resolve physical object identity")
                .thenCompose(identity -> acquire(
                        streamId,
                        offset,
                        candidate,
                        identity,
                        maximumReadDeadlineMillis,
                        deadline))
                .handle((pinned, failure) -> {
                    if (failure == null) {
                        return CompletableFuture.completedFuture(Optional.of(pinned));
                    }
                    Throwable cause = unwrap(failure);
                    if (!isFallbackAdmissionFailure(cause)) {
                        return CompletableFuture.<Optional<PinnedResolvedRange>>failedFuture(cause);
                    }
                    return pinNext(
                            streamId,
                            offset,
                            view,
                            candidates,
                            index + 1,
                            maximumReadDeadlineMillis,
                            deadline,
                            cause);
                })
                .thenCompose(value -> value);
    }

    private CompletableFuture<PinnedResolvedRange> acquire(
            StreamId streamId,
            long offset,
            GenerationReadCandidate candidate,
            PhysicalObjectIdentity identity,
            long maximumReadDeadlineMillis,
            ReadOperationDeadline deadline) {
        return deadline.bound(
                        () -> pinManager.acquire(
                                identity,
                                maximumReadDeadlineMillis,
                                () -> revalidate(streamId, offset, candidate, deadline)),
                        "acquire durable generation reader pin")
                .thenCompose(lease -> wrapPinned(candidate, lease));
    }

    private CompletableFuture<PinnedResolvedRange> wrapPinned(
            GenerationReadCandidate candidate,
            ObjectReadLease lease) {
        try {
            return CompletableFuture.completedFuture(new PinnedResolvedRange(candidate, lease));
        } catch (Throwable failure) {
            return lease.release().handle((ignored, releaseFailure) -> {
                if (releaseFailure != null) {
                    failure.addSuppressed(unwrap(releaseFailure));
                }
                throw new CompletionException(failure);
            });
        }
    }

    private CompletableFuture<Void> revalidate(
            StreamId streamId,
            long offset,
            GenerationReadCandidate expected,
            ReadOperationDeadline deadline) {
        long offsetEnd = expected.resolvedRange().offsetRange().endOffset();
        long generation = expected.resolvedRange().generation();
        return deadline.bound(
                        () -> generationStore.getCandidate(
                                cluster, streamId, expected.view(), offsetEnd, generation),
                        "reload exact generation index after reader pin")
                .thenCompose(optional -> {
                    VersionedGenerationCandidate actual = optional.orElseThrow(() -> condition(
                            "selected generation index disappeared during reader pin"));
                    if (!sameWrapperIdentity(expected, actual)) {
                        return CompletableFuture.failedFuture(condition(
                                "selected generation index changed during reader pin"));
                    }
                    return loadSnapshot(streamId, deadline).thenApply(snapshot -> {
                        validateReadable(streamId, offset, snapshot);
                        if (expected.resolvedRange().offsetRange().endOffset()
                                        > snapshot.committedEnd().committedEndOffset()
                                || expected.resolvedRange().commitVersion()
                                        > snapshot.committedEnd().commitVersion()) {
                            throw condition("selected generation no longer fits committed head truth");
                        }
                        readers.require(expected.resolvedRange().readTarget());
                        return null;
                    });
                });
    }

    private static boolean sameWrapperIdentity(
            GenerationReadCandidate expected,
            VersionedGenerationCandidate actual) {
        if (!actual.key().equals(expected.indexKey())
                || actual.metadataVersion() != expected.indexMetadataVersion()
                || !actual.durableValueSha256().equals(expected.indexRecordSha256())) {
            return false;
        }
        if (actual instanceof VersionedGenerationZeroIndex zero) {
            return expected.generationZero()
                    && zero.value().generation() == 0
                    && !zero.value().tombstoned();
        }
        if (actual instanceof VersionedGenerationIndex higher) {
            return !expected.generationZero()
                    && higher.value().lifecycle() == GenerationLifecycle.COMMITTED
                    && higher.value().generation() == expected.resolvedRange().generation()
                    && expected.publicationId().orElseThrow().value()
                            .equals(higher.value().publicationId());
        }
        return false;
    }

    private static void requireRepairResult(
            StreamId streamId,
            long offset,
            GenerationIndexRepairResult result) {
        if (!result.streamId().equals(streamId)
                || result.targetOffset() != offset) {
            throw invariant(
                    "generation index repair returned another target", null);
        }
    }

    private static GenerationReadCandidate toCandidate(
            ReadView view,
            OffsetIndexEntry entry,
            String key,
            com.nereusstream.api.Checksum durableSha256,
            Optional<PublicationId> publicationId) {
        ResolvedRange resolved = new ResolvedRange(
                entry.range(),
                entry.generation(),
                entry.readTarget(),
                entry.payloadFormat(),
                entry.recordCount(),
                entry.entryCount(),
                entry.logicalBytes(),
                entry.schemaRefs(),
                entry.projectionRef(),
                entry.commitVersion());
        return new GenerationReadCandidate(
                view,
                resolved,
                key,
                entry.metadataVersion(),
                durableSha256,
                entry.generation() == 0,
                publicationId);
    }

    private static boolean covers(OffsetIndexEntry entry, long offset) {
        return entry.offsetStart() <= offset && offset < entry.offsetEnd();
    }

    private static StorageProfile validateReadable(
            StreamId streamId,
            long offset,
            StreamMetadataSnapshot snapshot) {
        if (!snapshot.metadata().streamId().equals(streamId.value())) {
            throw invariant("stream snapshot belongs to another stream", null);
        }
        StreamState state;
        StorageProfile profile;
        try {
            state = StreamState.valueOf(snapshot.metadata().state());
            profile = StorageProfile.valueOf(snapshot.metadata().profile()).canonical();
        } catch (IllegalArgumentException failure) {
            throw invariant("stream snapshot contains an unknown state or profile", failure);
        }
        if (state != StreamState.ACTIVE && state != StreamState.SEALED) {
            throw new NereusException(
                    state == StreamState.DELETED ? ErrorCode.STREAM_NOT_FOUND : ErrorCode.STREAM_NOT_ACTIVE,
                    state == StreamState.CREATING,
                    "stream state does not admit generation reads");
        }
        if (offset < snapshot.trim().trimOffset()) {
            throw new NereusException(
                    ErrorCode.OFFSET_TRIMMED,
                    false,
                    "requested offset is below the stream trim offset");
        }
        return profile;
    }

    private long maximumReadDeadlineMillis(ReadOperationDeadline deadline) {
        long remainingMillis = Math.max(1, deadline.remaining().toMillis());
        try {
            return Math.addExact(clock.millis(), remainingMillis);
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    private static long saturatingAdd(long value, long positiveDelta) {
        if (value < 0 || positiveDelta <= 0) {
            throw new IllegalArgumentException("saturatingAdd requires non-negative value and positive delta");
        }
        return value > Long.MAX_VALUE - positiveDelta ? Long.MAX_VALUE : value + positiveDelta;
    }

    private static boolean isFallbackAdmissionFailure(Throwable failure) {
        if (failure instanceof F4MetadataConditionFailedException) {
            return true;
        }
        return failure instanceof NereusException nereus
                && (nereus.code() == ErrorCode.METADATA_CONDITION_FAILED
                        || nereus.code() == ErrorCode.OBJECT_NOT_FOUND);
    }

    private static CompletableFuture<List<VersionedGenerationCandidate>> metadataLimit() {
        return CompletableFuture.failedFuture(new NereusException(
                ErrorCode.METADATA_LIMIT_EXCEEDED,
                false,
                "generation candidate count exceeds the hard resolver limit"));
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static NereusException condition(String message) {
        return new NereusException(ErrorCode.METADATA_CONDITION_FAILED, true, message);
    }

    private static NereusException invariant(String message, Throwable cause) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message, cause);
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }
}
