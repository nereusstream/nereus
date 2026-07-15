/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.recovery;

import com.nereusstream.api.AppendOutcome;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.core.physical.ObjectReadLease;
import com.nereusstream.core.physical.ObjectReadPinManager;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.metadata.oxia.AppendRecoveryCommit;
import com.nereusstream.metadata.oxia.AppendReplayRecords;
import com.nereusstream.metadata.oxia.CommitAppendRequest;
import com.nereusstream.metadata.oxia.CommittedAppend;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.ReachableCommittedAppend;
import com.nereusstream.metadata.oxia.VersionedRecoveryCheckpointRoot;
import com.nereusstream.metadata.oxia.codec.MetadataRecordCodecFactory;
import com.nereusstream.metadata.oxia.records.RecoveryCheckpointReferenceRecord;
import com.nereusstream.metadata.oxia.records.StreamCommitTargetRecord;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointCodecV1;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointEntry;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointObject;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

/** Root-double-read exact append replay across the live tail and current NRC1 prefix. */
public final class CheckpointAppendReplayReader implements AppendRecoverySearcher {
    private static final int MAX_ROOT_RESTARTS = 8;

    private final String cluster;
    private final GenerationMetadataStore generationStore;
    private final AnchorAwareCommitWalker walker;
    private final RecoveryCheckpointCodecV1 codec;
    private final ObjectReadPinManager pinManager;
    private final Clock clock;

    public CheckpointAppendReplayReader(
            String cluster,
            GenerationMetadataStore generationStore,
            AnchorAwareCommitWalker walker,
            RecoveryCheckpointCodecV1 codec,
            ObjectReadPinManager pinManager,
            Clock clock) {
        this.cluster = requireText(cluster, "cluster");
        this.generationStore = Objects.requireNonNull(generationStore, "generationStore");
        this.walker = Objects.requireNonNull(walker, "walker");
        this.codec = Objects.requireNonNull(codec, "codec");
        this.pinManager = Objects.requireNonNull(pinManager, "pinManager");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public CompletableFuture<AppendReplayResolution> search(
            CommitAppendRequest request,
            int maximumLiveCommits,
            int pageSize,
            Duration timeout) {
        Objects.requireNonNull(request, "request");
        requireBounds(maximumLiveCommits, pageSize, timeout);
        return searchAttempt(
                request,
                maximumLiveCommits,
                pageSize,
                new ReplayDeadline(timeout, clock),
                0);
    }

    private CompletableFuture<AppendReplayResolution> searchAttempt(
            CommitAppendRequest request,
            int maximumLiveCommits,
            int pageSize,
            ReplayDeadline deadline,
            int rootRestarts) {
        CompletableFuture<AppendReplayResolution> attempt = walker.walk(
                        request.streamId(), maximumLiveCommits, pageSize)
                .thenCompose(walk -> searchStableWalk(request, walk, deadline));
        return attempt.handle((value, failure) -> {
            if (failure == null) {
                return CompletableFuture.completedFuture(value);
            }
            Throwable exact = unwrap(failure);
            if (exact instanceof RecoveryRootChangedException
                    && rootRestarts + 1 < MAX_ROOT_RESTARTS) {
                return searchAttempt(
                        request,
                        maximumLiveCommits,
                        pageSize,
                        deadline,
                        rootRestarts + 1);
            }
            if (exact instanceof RecoveryRootChangedException) {
                return CompletableFuture.<AppendReplayResolution>failedFuture(
                        condition("recovery root changed throughout append replay"));
            }
            return CompletableFuture.<AppendReplayResolution>failedFuture(exact);
        }).thenCompose(Function.identity());
    }

    private CompletableFuture<AppendReplayResolution> searchStableWalk(
            CommitAppendRequest request,
            AnchorAwareCommitWalk walk,
            ReplayDeadline deadline) {
        deadline.requireRemaining("walk append recovery tail");
        if (!walk.anchorReached()) {
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.METADATA_UNAVAILABLE,
                    true,
                    "append replay live tail exceeds its bounded scan",
                    AppendOutcome.MAY_HAVE_COMMITTED));
        }
        AppendReplayResolution live = searchLiveTail(request, walk);
        if (live != null) {
            return CompletableFuture.completedFuture(live);
        }
        Optional<VersionedRecoveryCheckpointRoot> optionalRoot = walk.recoveryRoot();
        if (optionalRoot.isEmpty()
                || optionalRoot.orElseThrow().value().checkpoints().isEmpty()) {
            return CompletableFuture.completedFuture(
                    AppendReplayResolution.notCommitted(
                            walk.commitsNewestFirst().size()));
        }
        VersionedRecoveryCheckpointRoot root = optionalRoot.orElseThrow();
        long expectedStart = request.expectedStartOffset();
        if (expectedStart < root.value().coveredStartOffset()) {
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.METADATA_UNAVAILABLE,
                    false,
                    "append replay evidence precedes the retained recovery checkpoint prefix",
                    AppendOutcome.MAY_HAVE_COMMITTED));
        }
        if (expectedStart >= root.value().coveredEndOffset()) {
            return CompletableFuture.failedFuture(invariant(
                    "live tail did not decide an append at or after the recovery anchor"));
        }
        RecoveryCheckpointReferenceRecord reference = referenceCovering(root, expectedStart);
        PhysicalObjectIdentity object = checkpointIdentity(reference);
        long readDeadlineMillis = deadline.maximumReadDeadlineMillis();
        CompletableFuture<ObjectReadLease> acquired = pinManager.acquire(
                object,
                readDeadlineMillis,
                () -> requireExactRoot(root));
        return withLease(acquired, lease -> openAndSearch(
                        request,
                        walk,
                        root,
                        reference,
                        deadline))
                .thenCompose(result -> requireExactRoot(root).thenApply(ignored -> result));
    }

    private AppendReplayResolution searchLiveTail(
            CommitAppendRequest request,
            AnchorAwareCommitWalk walk) {
        int scanned = 0;
        for (AppendRecoveryCommit evidence : walk.commitsNewestFirst()) {
            StreamCommitTargetRecord record = evidence.canonicalCommit();
            scanned++;
            if (record.commitId().equals(request.commitId())) {
                CommittedAppend append = AppendReplayRecords.validateAndHydrate(
                        request, record, AppendOutcome.KNOWN_COMMITTED);
                return AppendReplayResolution.found(
                        reachable(append, walk),
                        AppendReplayEvidenceSource.LIVE_COMMIT,
                        scanned);
            }
            if (record.offsetStart() <= request.expectedStartOffset()) {
                return AppendReplayResolution.notCommitted(scanned);
            }
        }
        return null;
    }

    private CompletableFuture<AppendReplayResolution> openAndSearch(
            CommitAppendRequest request,
            AnchorAwareCommitWalk walk,
            VersionedRecoveryCheckpointRoot root,
            RecoveryCheckpointReferenceRecord reference,
            ReplayDeadline deadline) {
        return codec.openAndVerify(
                        new ObjectKey(reference.objectKey()),
                        reference.objectLength(),
                        new Checksum(ChecksumType.SHA256, reference.contentSha256()),
                        deadline.remaining("open recovery checkpoint for append replay"))
                .thenCompose(checkpoint -> {
                    requireExactReference(root, reference, checkpoint);
                    return codec.findCommitCoveringOffset(
                            checkpoint,
                            request.expectedStartOffset(),
                            deadline.remaining("find append replay checkpoint entry"));
                })
                .thenApply(optional -> decideCheckpointEntry(
                        request,
                        walk,
                        optional.orElseThrow(() -> invariant(
                                "recovery checkpoint has no entry for a covered offset"))));
    }

    private AppendReplayResolution decideCheckpointEntry(
            CommitAppendRequest request,
            AnchorAwareCommitWalk walk,
            RecoveryCheckpointEntry entry) {
        int scanned = walk.commitsNewestFirst().size();
        if (entry.range().startOffset() != request.expectedStartOffset()
                || !entry.commitId().equals(request.commitId())) {
            return AppendReplayResolution.notCommitted(scanned);
        }
        byte[] canonical = bytes(entry.canonicalCommitRecord());
        StreamCommitTargetRecord record;
        try {
            record = MetadataRecordCodecFactory.decodeEnvelope(
                    canonical, StreamCommitTargetRecord.class);
        } catch (RuntimeException failure) {
            throw invariant("cannot decode NRC1 append replay record", failure);
        }
        if (record.metadataVersion() != 0
                || !Arrays.equals(
                        canonical,
                        MetadataRecordCodecFactory.encodeEnvelope(
                                record, StreamCommitTargetRecord.class))) {
            throw invariant("NRC1 append replay record is not canonical");
        }
        CommittedAppend append = AppendReplayRecords.validateAndHydrate(
                request, record, AppendOutcome.KNOWN_COMMITTED);
        return AppendReplayResolution.found(
                reachable(append, walk),
                AppendReplayEvidenceSource.RECOVERY_CHECKPOINT,
                scanned);
    }

    private CompletableFuture<Void> requireExactRoot(
            VersionedRecoveryCheckpointRoot expected) {
        return generationStore.getRecoveryRoot(
                        cluster, new com.nereusstream.api.StreamId(
                                expected.value().streamId()))
                .thenAccept(actual -> {
                    if (!actual.equals(Optional.of(expected))) {
                        throw new RecoveryRootChangedException();
                    }
                });
    }

    private static ReachableCommittedAppend reachable(
            CommittedAppend append,
            AnchorAwareCommitWalk walk) {
        var head = walk.observedHead();
        return ReachableCommittedAppend.verified(
                append,
                head.lastCommitId(),
                head.offsetEnd(),
                head.cumulativeSize(),
                head.commitVersion());
    }

    private static RecoveryCheckpointReferenceRecord referenceCovering(
            VersionedRecoveryCheckpointRoot root,
            long offset) {
        for (RecoveryCheckpointReferenceRecord reference : root.value().checkpoints()) {
            if (reference.coveredStartOffset() <= offset
                    && offset < reference.coveredEndOffset()) {
                return reference;
            }
        }
        throw invariant("recovery root has a gap at the append replay offset");
    }

    private static PhysicalObjectIdentity checkpointIdentity(
            RecoveryCheckpointReferenceRecord reference) {
        PhysicalObjectIdentity identity = PhysicalObjectIdentity.create(
                new ObjectKey(reference.objectKey()),
                Optional.of(new ObjectId(reference.objectId())),
                PhysicalObjectKind.RECOVERY_CHECKPOINT,
                reference.objectLength(),
                new Checksum(ChecksumType.CRC32C, reference.storageCrc32c()),
                Optional.of(new Checksum(
                        ChecksumType.SHA256, reference.contentSha256())),
                Optional.empty());
        if (!identity.objectKeyHash().equals(
                new ObjectKeyHash(reference.objectKeyHash()))) {
            throw invariant("recovery checkpoint reference has a wrong object-key hash");
        }
        return identity;
    }

    private void requireExactReference(
            VersionedRecoveryCheckpointRoot root,
            RecoveryCheckpointReferenceRecord reference,
            RecoveryCheckpointObject object) {
        var header = object.header();
        if (!header.cluster().equals(cluster)
                || !header.streamId().value().equals(root.value().streamId())
                || header.checkpointSequence() != reference.checkpointSequence()
                || !header.checkpointAttemptId().equals(
                        reference.checkpointAttemptId())
                || header.coverage().startOffset()
                        != reference.coveredStartOffset()
                || header.coverage().endOffset() != reference.coveredEndOffset()
                || header.firstCommitVersion() != reference.firstCommitVersion()
                || header.lastCommitVersion() != reference.lastCommitVersion()
                || header.cumulativeSizeAtStart()
                        != reference.cumulativeSizeAtStart()
                || header.cumulativeSizeAtEnd()
                        != reference.cumulativeSizeAtEnd()
                || !header.firstCommitId().equals(reference.firstCommitId())
                || !header.lastCommitId().equals(reference.lastCommitId())
                || !header.sourceHeadCommitId().equals(
                        reference.sourceHeadCommitId())
                || header.sourceHeadCommitVersion()
                        != reference.sourceHeadCommitVersion()
                || !header.projectionIdentitySha256().value().equals(
                        reference.projectionIdentitySha256())
                || header.expectedEntryCount() != reference.commitEntryCount()
                || header.expectedPublicationCount() != reference.publicationCount()
                || !object.objectId().value().equals(reference.objectId())
                || !object.objectKey().value().equals(reference.objectKey())
                || object.objectLength() != reference.objectLength()
                || !object.contentSha256().value().equals(
                        reference.contentSha256())) {
            throw invariant("verified NRC1 object differs from recovery-root reference");
        }
    }

    private static <T> CompletableFuture<T> withLease(
            CompletableFuture<ObjectReadLease> acquired,
            Function<ObjectReadLease, CompletableFuture<T>> operation) {
        return acquired.thenCompose(lease -> {
            CompletableFuture<T> source;
            try {
                source = Objects.requireNonNull(
                        operation.apply(lease), "checkpoint replay operation");
            } catch (Throwable failure) {
                source = CompletableFuture.failedFuture(failure);
            }
            CompletableFuture<T> result = new CompletableFuture<>();
            source.whenComplete((value, failure) -> lease.release()
                    .whenComplete((ignored, releaseFailure) -> {
                        if (failure == null && releaseFailure == null) {
                            result.complete(value);
                            return;
                        }
                        Throwable exact = failure == null
                                ? unwrap(releaseFailure)
                                : unwrap(failure);
                        if (failure != null && releaseFailure != null) {
                            exact.addSuppressed(unwrap(releaseFailure));
                        }
                        result.completeExceptionally(exact);
                    }));
            return result;
        });
    }

    private static byte[] bytes(ByteBuffer value) {
        ByteBuffer copy = value.asReadOnlyBuffer();
        byte[] result = new byte[copy.remaining()];
        copy.get(result);
        return result;
    }

    private static void requireBounds(
            int maximumLiveCommits,
            int pageSize,
            Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        if (maximumLiveCommits <= 0
                || pageSize <= 0
                || pageSize > maximumLiveCommits
                || timeout.isZero()
                || timeout.isNegative()) {
            throw new IllegalArgumentException("checkpoint append replay bounds are invalid");
        }
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = Objects.requireNonNull(failure, "failure");
        while ((current instanceof CompletionException
                        || current instanceof java.util.concurrent.ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static NereusException invariant(String message) {
        return invariant(message, null);
    }

    private static NereusException invariant(String message, Throwable cause) {
        return new NereusException(
                ErrorCode.METADATA_INVARIANT_VIOLATION,
                false,
                message,
                cause,
                AppendOutcome.MAY_HAVE_COMMITTED);
    }

    private static NereusException condition(String message) {
        return new NereusException(
                ErrorCode.METADATA_CONDITION_FAILED,
                true,
                message,
                AppendOutcome.MAY_HAVE_COMMITTED);
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }

    private static final class RecoveryRootChangedException
            extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }

    private static final class ReplayDeadline {
        private final long expiresAtNanos;
        private final Clock clock;

        private ReplayDeadline(Duration timeout, Clock clock) {
            this.clock = clock;
            long timeoutNanos;
            try {
                timeoutNanos = timeout.toNanos();
            } catch (ArithmeticException overflow) {
                timeoutNanos = Long.MAX_VALUE;
            }
            long now = System.nanoTime();
            expiresAtNanos = timeoutNanos >= Long.MAX_VALUE - now
                    ? Long.MAX_VALUE
                    : now + timeoutNanos;
        }

        private Duration remaining(String action) {
            long remaining = expiresAtNanos - System.nanoTime();
            if (remaining <= 0) {
                throw new NereusException(
                        ErrorCode.TIMEOUT,
                        true,
                        action + " exceeded the append recovery deadline",
                        AppendOutcome.MAY_HAVE_COMMITTED);
            }
            return Duration.ofNanos(remaining);
        }

        private void requireRemaining(String action) {
            remaining(action);
        }

        private long maximumReadDeadlineMillis() {
            long remainingMillis = Math.max(
                    1L, remaining("acquire recovery checkpoint read pin").toMillis());
            try {
                return Math.addExact(clock.millis(), remainingMillis);
            } catch (ArithmeticException overflow) {
                throw new NereusException(
                        ErrorCode.INVALID_ARGUMENT,
                        false,
                        "append replay read deadline overflows",
                        overflow,
                        AppendOutcome.MAY_HAVE_COMMITTED);
            }
        }
    }
}
