/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.core.recovery;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamState;
import com.nereusstream.core.append.GenerationZeroPhysicalReferencePublisher;
import com.nereusstream.metadata.oxia.AppendRecoveryCommit;
import com.nereusstream.metadata.oxia.AppendReplayRecords;
import com.nereusstream.metadata.oxia.MaterializedGenerationZero;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.ProjectionIdentity;
import com.nereusstream.metadata.oxia.ReachableCommittedAppend;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * Repairs generation-zero index and physical-protection gaps from a root-stable live append tail.
 *
 * <p>The scanner never reconstructs checkpoint-retired generation-zero targets. Offsets owned by an NRC1 prefix are
 * routed to {@link CheckpointDerivedIndexRepairer} by the composite read-repair path.
 */
public final class GenerationZeroRepairScanner {
    private final String cluster;
    private final OxiaMetadataStore metadataStore;
    private final AnchorAwareCommitWalker walker;
    private final GenerationZeroPhysicalReferencePublisher physicalReferences;
    private final int maxCommits;
    private final int pageSize;

    public GenerationZeroRepairScanner(
            String cluster,
            OxiaMetadataStore metadataStore,
            AnchorAwareCommitWalker walker,
            GenerationZeroPhysicalReferencePublisher physicalReferences,
            int maxCommits,
            int pageSize) {
        this.cluster = requireText(cluster, "cluster");
        this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
        this.walker = Objects.requireNonNull(walker, "walker");
        this.physicalReferences = Objects.requireNonNull(
                physicalReferences, "physicalReferences");
        if (maxCommits <= 0 || pageSize <= 0 || pageSize > maxCommits || pageSize > 1_000) {
            throw new IllegalArgumentException("generation-zero repair scan bounds are invalid");
        }
        this.maxCommits = maxCommits;
        this.pageSize = pageSize;
    }

    /** Repairs every untrimmed commit returned by one bounded, root-stable live-tail walk. */
    public CompletableFuture<ScanResult> repairAll(
            StreamId streamId, Duration timeout) {
        Objects.requireNonNull(streamId, "streamId");
        RepairDeadline deadline = new RepairDeadline(timeout);
        return loadSnapshot(streamId, deadline)
                .thenCompose(snapshot -> deadline.bound(
                        () -> walker.walk(streamId, maxCommits, pageSize),
                        "walk live tail for generation-zero repair")
                        .thenCompose(walk -> {
                            requireWalkAtOrAfterSnapshot(streamId, snapshot, walk);
                            List<AppendRecoveryCommit> repairOrder =
                                    new ArrayList<>(walk.commitsNewestFirst());
                            Collections.reverse(repairOrder);
                            List<AppendRecoveryCommit> untrimmed = repairOrder.stream()
                                    .filter(commit -> commit.canonicalCommit().offsetEnd()
                                            > snapshot.trim().trimOffset())
                                    .toList();
                            return repairSequentially(
                                            untrimmed,
                                            walk,
                                            0,
                                            0,
                                            deadline)
                                    .thenCompose(repaired -> loadSnapshot(streamId, deadline)
                                            .thenApply(reloaded -> {
                                                requireSnapshotStream(streamId, reloaded);
                                                requireHeadNotRegressed(walk, reloaded);
                                                return new ScanResult(
                                                        streamId,
                                                        walk.commitsNewestFirst().size(),
                                                        repaired,
                                                        walk.anchorReached(),
                                                        walk.observedHead().commitVersion());
                                            }));
                        }));
    }

    /** Repairs the exact live commit covering one committed offset, including its VISIBLE_GENERATION protection. */
    public CompletableFuture<TargetRepairResult> repairCovering(
            StreamId streamId,
            long targetOffset,
            Duration timeout) {
        Objects.requireNonNull(streamId, "streamId");
        if (targetOffset < 0) {
            throw new IllegalArgumentException("targetOffset must be non-negative");
        }
        RepairDeadline deadline = new RepairDeadline(timeout);
        return loadSnapshot(streamId, deadline)
                .thenCompose(snapshot -> {
                    if (targetOffset < snapshot.trim().trimOffset()) {
                        return CompletableFuture.completedFuture(
                                TargetRepairResult.trimmed(streamId, targetOffset));
                    }
                    if (targetOffset >= snapshot.committedEnd().committedEndOffset()) {
                        return CompletableFuture.failedFuture(new NereusException(
                                ErrorCode.READ_RESOLUTION_FAILED,
                                true,
                                "generation-zero repair target is not a committed offset"));
                    }
                    return deadline.bound(
                                    () -> walker.walk(streamId, maxCommits, pageSize),
                                    "walk live tail for read-after-commit repair")
                            .thenCompose(walk -> repairCovering(
                                    streamId,
                                    targetOffset,
                                    snapshot,
                                    walk,
                                    deadline));
                });
    }

    private CompletableFuture<TargetRepairResult> repairCovering(
            StreamId streamId,
            long targetOffset,
            StreamMetadataSnapshot snapshot,
            AnchorAwareCommitWalk walk,
            RepairDeadline deadline) {
        requireWalkAtOrAfterSnapshot(streamId, snapshot, walk);
        Optional<AppendRecoveryCommit> candidate = walk.commitsNewestFirst().stream()
                .filter(commit -> commit.canonicalCommit().offsetStart() <= targetOffset
                        && targetOffset < commit.canonicalCommit().offsetEnd())
                .findFirst();
        if (candidate.isEmpty()) {
            if (walk.recoveryRoot().isPresent()
                    && !walk.recoveryRoot().orElseThrow().value().checkpoints().isEmpty()
                    && targetOffset
                            < walk.recoveryRoot().orElseThrow().value().coveredEndOffset()) {
                return CompletableFuture.failedFuture(new NereusException(
                        ErrorCode.READ_RESOLUTION_FAILED,
                        true,
                        "repair target belongs to recovery-checkpoint evidence"));
            }
            if (!walk.anchorReached()) {
                return CompletableFuture.failedFuture(new NereusException(
                        ErrorCode.READ_RESOLUTION_FAILED,
                        true,
                        "generation-zero repair live tail exceeds its bounded scan"));
            }
            return CompletableFuture.failedFuture(invariant(
                    "root-stable live tail does not contain a committed repair target",
                    null));
        }
        return repairCommit(candidate.orElseThrow(), walk, deadline)
                .thenCompose(materialized -> loadSnapshot(streamId, deadline)
                        .thenApply(reloaded -> {
                            requireSnapshotStream(streamId, reloaded);
                            requireHeadNotRegressed(walk, reloaded);
                            if (targetOffset < reloaded.trim().trimOffset()) {
                                return TargetRepairResult.trimmed(
                                        streamId, targetOffset);
                            }
                            return TargetRepairResult.repaired(
                                    streamId,
                                    targetOffset,
                                    walk.commitsNewestFirst().size(),
                                    materialized);
                        }));
    }

    private CompletableFuture<Integer> repairSequentially(
            List<AppendRecoveryCommit> commits,
            AnchorAwareCommitWalk walk,
            int index,
            int repaired,
            RepairDeadline deadline) {
        if (index == commits.size()) {
            return CompletableFuture.completedFuture(repaired);
        }
        return repairCommit(commits.get(index), walk, deadline)
                .thenCompose(ignored -> repairSequentially(
                        commits,
                        walk,
                        index + 1,
                        Math.addExact(repaired, 1),
                        deadline));
    }

    private CompletableFuture<MaterializedGenerationZero> repairCommit(
            AppendRecoveryCommit evidence,
            AnchorAwareCommitWalk walk,
            RepairDeadline deadline) {
        ReachableCommittedAppend reachable = hydrateReachable(evidence, walk);
        return deadline.bound(
                        () -> metadataStore.materializeGenerationZero(
                                cluster, reachable),
                        "materialize repaired generation-zero index")
                .thenCompose(materialized -> deadline.bound(
                                () -> physicalReferences.protectVisibleIndex(
                                        materialized, deadline.remaining()),
                                "protect repaired generation-zero index")
                        .thenApply(protectedIndex -> {
                            if (!protectedIndex.materialized().equals(materialized)) {
                                throw invariant(
                                        "generation-zero protection returned another materialization",
                                        null);
                            }
                            return materialized;
                        }));
    }

    private static ReachableCommittedAppend hydrateReachable(
            AppendRecoveryCommit evidence,
            AnchorAwareCommitWalk walk) {
        try {
            var committed = AppendReplayRecords.hydrate(
                    evidence.canonicalCommit(),
                    ProjectionIdentity.decode(
                            evidence.canonicalCommit().projectionRef()));
            return ReachableCommittedAppend.verified(
                    committed,
                    walk.observedHead().lastCommitId(),
                    walk.observedHead().offsetEnd(),
                    walk.observedHead().cumulativeSize(),
                    walk.observedHead().commitVersion());
        } catch (RuntimeException failure) {
            throw invariant(
                    "live append evidence cannot be hydrated for generation-zero repair",
                    failure);
        }
    }

    private CompletableFuture<StreamMetadataSnapshot> loadSnapshot(
            StreamId streamId, RepairDeadline deadline) {
        return deadline.bound(
                        () -> metadataStore.getStreamSnapshot(cluster, streamId),
                        "load stream snapshot for generation-zero repair")
                .thenApply(snapshot -> {
                    requireSnapshotStream(streamId, snapshot);
                    requireObjectWalReadable(snapshot);
                    return snapshot;
                });
    }

    private static void requireSnapshotStream(
            StreamId streamId, StreamMetadataSnapshot snapshot) {
        if (!snapshot.metadata().streamId().equals(streamId.value())) {
            throw invariant("stream snapshot belongs to another stream", null);
        }
    }

    private static void requireObjectWalReadable(StreamMetadataSnapshot snapshot) {
        StreamState state;
        StorageProfile profile;
        try {
            state = StreamState.valueOf(snapshot.metadata().state());
            profile = StorageProfile.valueOf(snapshot.metadata().profile())
                    .canonical();
        } catch (IllegalArgumentException failure) {
            throw invariant(
                    "stream snapshot contains an unknown state or profile",
                    failure);
        }
        if (state != StreamState.ACTIVE && state != StreamState.SEALED) {
            throw new NereusException(
                    state == StreamState.DELETED
                            ? ErrorCode.STREAM_NOT_FOUND
                            : ErrorCode.STREAM_NOT_ACTIVE,
                    state == StreamState.CREATING,
                    "stream state does not admit generation-zero repair");
        }
        if (!profile.usesObjectWal()) {
            throw new NereusException(
                    ErrorCode.UNSUPPORTED_STORAGE_PROFILE,
                    false,
                    "generation-zero repair requires an Object-WAL profile");
        }
    }

    private static void requireWalkAtOrAfterSnapshot(
            StreamId streamId,
            StreamMetadataSnapshot snapshot,
            AnchorAwareCommitWalk walk) {
        if (!walk.observedHead().streamId().equals(streamId)
                || walk.observedHead().offsetEnd()
                        < snapshot.committedEnd().committedEndOffset()
                || walk.observedHead().cumulativeSize()
                        < snapshot.committedEnd().cumulativeSize()
                || walk.observedHead().commitVersion()
                        < snapshot.committedEnd().commitVersion()) {
            throw invariant(
                    "root-stable live-tail head regressed behind the stream snapshot",
                    null);
        }
    }

    private static void requireHeadNotRegressed(
            AnchorAwareCommitWalk walk, StreamMetadataSnapshot snapshot) {
        if (snapshot.committedEnd().committedEndOffset()
                        < walk.observedHead().offsetEnd()
                || snapshot.committedEnd().cumulativeSize()
                        < walk.observedHead().cumulativeSize()
                || snapshot.committedEnd().commitVersion()
                        < walk.observedHead().commitVersion()) {
            throw invariant(
                    "stream head regressed after generation-zero repair",
                    null);
        }
    }

    public record ScanResult(
            StreamId streamId,
            int scannedCommits,
            int protectedIndexes,
            boolean anchorReached,
            long observedHeadCommitVersion) {
        public ScanResult {
            Objects.requireNonNull(streamId, "streamId");
            if (scannedCommits < 0
                    || protectedIndexes < 0
                    || protectedIndexes > scannedCommits
                    || observedHeadCommitVersion < 0) {
                throw new IllegalArgumentException(
                        "generation-zero scan result is invalid");
            }
        }
    }

    public record TargetRepairResult(
            StreamId streamId,
            long targetOffset,
            int scannedCommits,
            boolean trimmed,
            Optional<MaterializedGenerationZero> materialized) {
        public TargetRepairResult {
            Objects.requireNonNull(streamId, "streamId");
            materialized = Objects.requireNonNull(materialized, "materialized");
            if (targetOffset < 0
                    || scannedCommits < 0
                    || trimmed == materialized.isPresent()) {
                throw new IllegalArgumentException(
                        "generation-zero target repair result is invalid");
            }
            materialized.ifPresent(value -> {
                if (!value.committedAppend().streamId().equals(streamId)
                        || value.committedAppend().range().startOffset()
                                > targetOffset
                        || targetOffset
                                >= value.committedAppend().range().endOffset()) {
                    throw new IllegalArgumentException(
                            "materialized generation zero does not cover target");
                }
            });
        }

        static TargetRepairResult trimmed(
                StreamId streamId, long targetOffset) {
            return new TargetRepairResult(
                    streamId,
                    targetOffset,
                    0,
                    true,
                    Optional.empty());
        }

        static TargetRepairResult repaired(
                StreamId streamId,
                long targetOffset,
                int scannedCommits,
                MaterializedGenerationZero materialized) {
            return new TargetRepairResult(
                    streamId,
                    targetOffset,
                    scannedCommits,
                    false,
                    Optional.of(materialized));
        }
    }

    private static NereusException invariant(
            String message, Throwable cause) {
        return new NereusException(
                ErrorCode.METADATA_INVARIANT_VIOLATION,
                false,
                message,
                cause);
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static final class RepairDeadline {
        private final long expiresAtNanos;

        private RepairDeadline(Duration timeout) {
            Objects.requireNonNull(timeout, "timeout");
            if (timeout.isZero() || timeout.isNegative()) {
                throw new IllegalArgumentException("timeout must be positive");
            }
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

        private Duration remaining() {
            return Duration.ofNanos(Math.max(
                    0, expiresAtNanos - System.nanoTime()));
        }

        private <T> CompletableFuture<T> bound(
                Supplier<CompletableFuture<T>> operation, String action) {
            long remaining = expiresAtNanos - System.nanoTime();
            if (remaining <= 0) {
                return CompletableFuture.failedFuture(timeout(action));
            }
            CompletableFuture<T> source;
            try {
                source = Objects.requireNonNull(
                        operation.get(), "operation future");
            } catch (Throwable failure) {
                return CompletableFuture.failedFuture(failure);
            }
            return source.orTimeout(remaining, TimeUnit.NANOSECONDS)
                    .handle((value, failure) -> {
                        if (failure == null) {
                            return value;
                        }
                        Throwable exact = unwrap(failure);
                        if (exact instanceof TimeoutException) {
                            throw timeout(action);
                        }
                        if (exact instanceof RuntimeException runtime) {
                            throw runtime;
                        }
                        throw new CompletionException(exact);
                    });
        }

        private static NereusException timeout(String action) {
            return new NereusException(
                    ErrorCode.TIMEOUT,
                    true,
                    action + " exceeded its deadline");
        }
    }
}
