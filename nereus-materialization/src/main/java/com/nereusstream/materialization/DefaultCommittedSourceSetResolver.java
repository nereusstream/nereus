/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.ProjectionRef;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamState;
import com.nereusstream.metadata.oxia.F4ScanToken;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationScanPage;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.ProjectionIdentity;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.metadata.oxia.VersionedMaterializationStreamRegistration;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Bounded generation-index resolver for one exact COMMITTED coverage.
 *
 * <p>The index scan is not treated as a snapshot transaction. The selected path is reread by exact
 * generation identity, and the current stream/registration authority is checked after the scan.
 */
public final class DefaultCommittedSourceSetResolver implements CommittedSourceSetResolver {
    public static final int MAX_CANDIDATE_EDGES = 4_096;
    public static final int MAX_PATH_STATES = 4_096;

    private static final Comparator<SourceGeneration> EDGE_ORDER =
            Comparator.comparingLong((SourceGeneration source) -> source.range().startOffset())
                    .thenComparingLong(source -> source.range().endOffset())
                    .thenComparing(
                            Comparator.comparingLong(SourceGeneration::generation).reversed())
                    .thenComparing(
                            SourceGeneration::indexKey,
                            DefaultCommittedSourceSetResolver::compareUtf8);

    private final String cluster;
    private final OxiaMetadataStore l0Metadata;
    private final GenerationMetadataStore generations;
    private final int scanPageSize;
    private final MaterializationStreamAuthorityMode authorityMode;

    public DefaultCommittedSourceSetResolver(
            String cluster,
            OxiaMetadataStore l0Metadata,
            GenerationMetadataStore generations,
            int scanPageSize) {
        this(
                cluster,
                l0Metadata,
                generations,
                scanPageSize,
                MaterializationStreamAuthorityMode.PROJECTION_REQUIRED);
    }

    public DefaultCommittedSourceSetResolver(
            String cluster,
            OxiaMetadataStore l0Metadata,
            GenerationMetadataStore generations,
            int scanPageSize,
            MaterializationStreamAuthorityMode authorityMode) {
        this.cluster = requireText(cluster, "cluster");
        this.l0Metadata = Objects.requireNonNull(l0Metadata, "l0Metadata");
        this.generations = Objects.requireNonNull(generations, "generations");
        if (scanPageSize <= 0 || scanPageSize > 1_000) {
            throw new IllegalArgumentException("scanPageSize must be in [1, 1000]");
        }
        this.scanPageSize = scanPageSize;
        this.authorityMode = Objects.requireNonNull(authorityMode, "authorityMode");
    }

    @Override
    public CompletableFuture<CommittedSourceSetResolution> resolve(
            StreamId streamId, OffsetRange coverage) {
        try {
            StreamId exactStream = Objects.requireNonNull(streamId, "streamId");
            OffsetRange exactCoverage = Objects.requireNonNull(coverage, "coverage");
            if (exactCoverage.isEmpty()) {
                throw new IllegalArgumentException(
                        "authoritative COMMITTED source coverage cannot be empty");
            }
            long minimumOffsetEnd = Math.addExact(exactCoverage.startOffset(), 1);
            return scanCandidates(
                            exactStream,
                            minimumOffsetEnd,
                            exactCoverage.endOffset(),
                            Optional.empty(),
                            new ArrayList<>())
                    .thenCompose(
                            candidates ->
                                    loadAuthority(exactStream)
                                            .thenCompose(
                                                    authority -> {
                                                        requireCoverageAuthority(
                                                                exactStream,
                                                                exactCoverage,
                                                                authority);
                                                        ExactSourceSet sourceSet =
                                                                selectExactSourceSet(
                                                                        exactStream,
                                                                        exactCoverage,
                                                                        candidates,
                                                                        authority);
                                                        CommittedSourceSetResolution resolution =
                                                                new CommittedSourceSetResolution(
                                                                        exactStream,
                                                                        sourceSet,
                                                                        authority.snapshot(),
                                                                        authority.registration());
                                                        return revalidate(resolution)
                                                                .thenApply(ignored -> resolution);
                                                    }));
        } catch (RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    @Override
    public CompletableFuture<Void> revalidate(CommittedSourceSetResolution expected) {
        try {
            CommittedSourceSetResolution exact = Objects.requireNonNull(expected, "expected");
            CompletableFuture<Authority> authority = loadAuthority(exact.streamId());
            List<CompletableFuture<Void>> sourceChecks =
                    new ArrayList<>(exact.sourceSet().sources().size());
            for (SourceGeneration source : exact.sourceSet().sources()) {
                sourceChecks.add(
                        generations
                                .getCandidate(
                                        cluster,
                                        exact.streamId(),
                                        ReadView.COMMITTED,
                                        source.range().endOffset(),
                                        source.generation())
                                .thenAccept(
                                        candidate -> {
                                            if (candidate.isEmpty()
                                                    || !MaterializationSourceMapper
                                                            .matchesExactSource(
                                                                    candidate.orElseThrow(),
                                                                    exact.streamId(),
                                                                    source)) {
                                                throw condition(
                                                        "authoritative COMMITTED source changed"
                                                            + " during resolution");
                                            }
                                        }));
            }
            return CompletableFuture.allOf(sourceChecks.toArray(CompletableFuture[]::new))
                    .thenCombine(
                            authority,
                            (ignored, current) -> {
                                requireCoverageAuthority(
                                        exact.streamId(), exact.sourceSet().coverage(), current);
                                requireStableAuthority(exact, current);
                                return null;
                            });
        } catch (RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private CompletableFuture<List<VersionedGenerationCandidate>> scanCandidates(
            StreamId streamId,
            long minimumOffsetEnd,
            long maximumOffsetEnd,
            Optional<F4ScanToken> continuation,
            ArrayList<VersionedGenerationCandidate> values) {
        return generations
                .scanIndex(
                        cluster,
                        streamId,
                        ReadView.COMMITTED,
                        minimumOffsetEnd,
                        maximumOffsetEnd,
                        continuation,
                        scanPageSize)
                .thenCompose(
                        page ->
                                appendCandidatePage(
                                        streamId,
                                        minimumOffsetEnd,
                                        maximumOffsetEnd,
                                        values,
                                        page));
    }

    private CompletableFuture<List<VersionedGenerationCandidate>> appendCandidatePage(
            StreamId streamId,
            long minimumOffsetEnd,
            long maximumOffsetEnd,
            ArrayList<VersionedGenerationCandidate> values,
            GenerationScanPage page) {
        values.addAll(Objects.requireNonNull(page, "generation scan page").values());
        if (values.size() > MAX_CANDIDATE_EDGES) {
            return CompletableFuture.failedFuture(
                    limit("authoritative COMMITTED source scan exceeds 4096 candidates"));
        }
        if (page.continuation().isPresent()) {
            return scanCandidates(
                    streamId, minimumOffsetEnd, maximumOffsetEnd, page.continuation(), values);
        }
        return CompletableFuture.completedFuture(List.copyOf(values));
    }

    private CompletableFuture<Authority> loadAuthority(StreamId streamId) {
        return l0Metadata
                .getStreamSnapshot(cluster, streamId)
                .thenCombine(
                        generations.getStreamRegistration(cluster, streamId),
                        (snapshot, registration) ->
                                new Authority(
                                        Objects.requireNonNull(snapshot, "stream snapshot"),
                                        registration.orElseThrow(
                                                () ->
                                                        condition(
                                                                "materialization stream"
                                                                    + " registration is absent"))));
    }

    private ExactSourceSet selectExactSourceSet(
            StreamId streamId,
            OffsetRange coverage,
            List<VersionedGenerationCandidate> candidates,
            Authority authority) {
        requireUniquePositiveGenerations(candidates);
        List<SourceGeneration> edges =
                candidates.stream()
                        .map(
                                candidate ->
                                        MaterializationSourceMapper.committedSource(
                                                candidate,
                                                streamId,
                                                ReadView.COMMITTED,
                                                authority
                                                        .snapshot()
                                                        .committedEnd()
                                                        .committedEndOffset(),
                                                authority.snapshot().committedEnd().commitVersion(),
                                                authority.effectiveProjection()))
                        .flatMap(Optional::stream)
                        .filter(source -> source.range().startOffset() >= coverage.startOffset())
                        .filter(source -> source.range().endOffset() <= coverage.endOffset())
                        .sorted(EDGE_ORDER)
                        .toList();
        Map<Long, Map<PathState, Path>> pathsByEnd = new HashMap<>();
        int pathStates = 0;
        for (SourceGeneration edge : edges) {
            List<Path> candidatesAtEdge = new ArrayList<>();
            if (edge.range().startOffset() == coverage.startOffset()) {
                candidatesAtEdge.add(Path.first(edge));
            }
            Map<PathState, Path> predecessors = pathsByEnd.get(edge.range().startOffset());
            if (predecessors != null) {
                for (Path predecessor : predecessors.values()) {
                    if (predecessor.canAppend(edge)) {
                        candidatesAtEdge.add(predecessor.append(edge));
                    }
                }
            }
            for (Path candidate : candidatesAtEdge) {
                Map<PathState, Path> atEnd =
                        pathsByEnd.computeIfAbsent(
                                candidate.endOffset(), ignored -> new HashMap<>());
                PathState state = candidate.state();
                Path current = atEnd.get(state);
                if (current == null) {
                    if (++pathStates > MAX_PATH_STATES) {
                        throw limit(
                                "authoritative COMMITTED source path search exceeds 4096 states");
                    }
                    atEnd.put(state, candidate);
                } else if (comparePath(candidate, current) < 0) {
                    atEnd.put(state, candidate);
                }
            }
        }
        Map<PathState, Path> complete = pathsByEnd.get(coverage.endOffset());
        if (complete == null || complete.isEmpty()) {
            throw condition(
                    "no exact gap-free authoritative COMMITTED source path covers the requested"
                        + " range");
        }
        Path selected =
                complete.values().stream()
                        .min(DefaultCommittedSourceSetResolver::comparePath)
                        .orElseThrow();
        return ExactSourceSet.create(ReadView.COMMITTED, coverage, selected.sources());
    }

    private static void requireUniquePositiveGenerations(
            List<VersionedGenerationCandidate> candidates) {
        Map<Long, String> keysByGeneration = new HashMap<>();
        for (VersionedGenerationCandidate candidate : candidates) {
            if (!(candidate instanceof VersionedGenerationIndex higher)) {
                continue;
            }
            String previous =
                    keysByGeneration.putIfAbsent(higher.value().generation(), higher.key());
            if (previous != null && !previous.equals(higher.key())) {
                throw invariant(
                        "one COMMITTED source view contains duplicate positive generations", null);
            }
        }
    }

    private void requireCoverageAuthority(
            StreamId streamId, OffsetRange coverage, Authority authority) {
        StreamMetadataSnapshot snapshot = authority.snapshot();
        if (!snapshot.metadata().streamId().equals(streamId.value())
                || !authority.registration().value().streamId().equals(streamId.value())) {
            throw invariant("COMMITTED source authority belongs to another stream", null);
        }
        StreamState state;
        StorageProfile profile;
        try {
            state = StreamState.valueOf(snapshot.metadata().state());
            profile = StorageProfile.valueOf(snapshot.metadata().profile()).canonical();
        } catch (RuntimeException failure) {
            throw invariant(
                    "COMMITTED source authority contains an unsupported state/profile", failure);
        }
        if (state != StreamState.ACTIVE && state != StreamState.SEALED) {
            throw condition("stream state does not admit COMMITTED source resolution");
        }
        if (!profile.objectMaterializationEnabled()
                || !authority.registration().value().storageProfile().equals(profile.name())
                || !matchesAuthorityMode(streamId, profile, authority)) {
            throw condition(
                    "materialization registration no longer matches stream profile/projection");
        }
        if (coverage.startOffset() < snapshot.trim().trimOffset()
                || coverage.endOffset() > snapshot.committedEnd().committedEndOffset()
                || snapshot.committedEnd().commitVersion() <= 0) {
            throw condition(
                    "requested COMMITTED source range is outside authoritative retained bounds");
        }
    }

    private boolean matchesAuthorityMode(
            StreamId streamId, StorageProfile profile, Authority authority) {
        if (authorityMode == MaterializationStreamAuthorityMode.PROJECTION_REQUIRED) {
            return authority.effectiveProjection().isPresent();
        }
        return authority.effectiveProjection().isEmpty()
                && authority
                        .registration()
                        .value()
                        .projectionRef()
                        .equals(DirectMaterializationStreamAuthority.encodedProjectionRef())
                && authority
                        .registration()
                        .value()
                        .projectionIdentitySha256()
                        .equals(
                                DirectMaterializationStreamAuthority.identitySha256(
                                                streamId, profile)
                                        .value());
    }

    private static void requireStableAuthority(
            CommittedSourceSetResolution expected, Authority current) {
        StreamMetadataSnapshot previous = expected.streamSnapshot();
        var previousRegistration = expected.registration().value();
        var currentRegistration = current.registration().value();
        long sourceCommitVersion =
                expected.sourceSet()
                        .sources()
                        .get(expected.sourceSet().sources().size() - 1)
                        .commitVersion();
        if (!previous.metadata().profile().equals(current.snapshot().metadata().profile())
                || previous.metadata().policyVersion()
                        != current.snapshot().metadata().policyVersion()
                || !previousRegistration.streamId().equals(currentRegistration.streamId())
                || !previousRegistration.projectionRef().equals(currentRegistration.projectionRef())
                || !previousRegistration
                        .projectionIdentitySha256()
                        .equals(currentRegistration.projectionIdentitySha256())
                || !previousRegistration
                        .storageProfile()
                        .equals(currentRegistration.storageProfile())
                || current.snapshot().committedEnd().commitVersion() < sourceCommitVersion) {
            throw condition(
                    "COMMITTED source stream or registration authority changed during resolution");
        }
    }

    private static int comparePath(Path left, Path right) {
        int count = Integer.compare(left.sources().size(), right.sources().size());
        if (count != 0) {
            return count;
        }
        for (int index = 0; index < left.sources().size(); index++) {
            SourceGeneration leftEdge = left.sources().get(index);
            SourceGeneration rightEdge = right.sources().get(index);
            int generation = Long.compare(rightEdge.generation(), leftEdge.generation());
            if (generation != 0) {
                return generation;
            }
            int edgeEnd = Long.compare(rightEdge.range().endOffset(), leftEdge.range().endOffset());
            if (edgeEnd != 0) {
                return edgeEnd;
            }
            int key = compareUtf8(leftEdge.indexKey(), rightEdge.indexKey());
            if (key != 0) {
                return key;
            }
        }
        return Long.compare(left.cumulativeSizeAtStart(), right.cumulativeSizeAtStart());
    }

    private static int compareUtf8(String left, String right) {
        byte[] leftBytes = left.getBytes(StandardCharsets.UTF_8);
        byte[] rightBytes = right.getBytes(StandardCharsets.UTF_8);
        int length = Math.min(leftBytes.length, rightBytes.length);
        for (int index = 0; index < length; index++) {
            int compared = Integer.compare(leftBytes[index] & 0xff, rightBytes[index] & 0xff);
            if (compared != 0) {
                return compared;
            }
        }
        return Integer.compare(leftBytes.length, rightBytes.length);
    }

    private static NereusException condition(String message) {
        return new NereusException(ErrorCode.METADATA_CONDITION_FAILED, true, message);
    }

    private static NereusException limit(String message) {
        return new NereusException(ErrorCode.METADATA_LIMIT_EXCEEDED, false, message);
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

    private record Authority(
            StreamMetadataSnapshot snapshot,
            VersionedMaterializationStreamRegistration registration,
            Optional<ProjectionRef> effectiveProjection) {
        private Authority(
                StreamMetadataSnapshot snapshot,
                VersionedMaterializationStreamRegistration registration) {
            this(snapshot, registration, decodeProjection(registration.value().projectionRef()));
        }

        private static Optional<ProjectionRef> decodeProjection(String encoded) {
            try {
                return ProjectionIdentity.decode(encoded);
            } catch (RuntimeException failure) {
                throw invariant(
                        "materialization registration contains an unsupported projection", failure);
            }
        }
    }

    private record PathState(
            long cumulativeSizeAtStart, long cumulativeSizeAtEnd, long lastCommitVersion) {}

    private record Path(
            long endOffset,
            long cumulativeSizeAtStart,
            long cumulativeSizeAtEnd,
            long lastCommitVersion,
            List<SourceGeneration> sources) {
        private static Path first(SourceGeneration source) {
            return new Path(
                    source.range().endOffset(),
                    source.cumulativeSizeAtStart(),
                    source.cumulativeSizeAtEnd(),
                    source.commitVersion(),
                    List.of(source));
        }

        private boolean canAppend(SourceGeneration source) {
            return endOffset == source.range().startOffset()
                    && cumulativeSizeAtEnd == source.cumulativeSizeAtStart()
                    && source.commitVersion() >= lastCommitVersion;
        }

        private Path append(SourceGeneration source) {
            ArrayList<SourceGeneration> combined = new ArrayList<>(sources.size() + 1);
            combined.addAll(sources);
            combined.add(source);
            return new Path(
                    source.range().endOffset(),
                    cumulativeSizeAtStart,
                    source.cumulativeSizeAtEnd(),
                    source.commitVersion(),
                    List.copyOf(combined));
        }

        private PathState state() {
            return new PathState(cumulativeSizeAtStart, cumulativeSizeAtEnd, lastCommitVersion);
        }
    }
}
