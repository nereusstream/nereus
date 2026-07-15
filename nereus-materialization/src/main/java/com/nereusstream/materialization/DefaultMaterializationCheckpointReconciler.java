/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamState;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.metadata.oxia.F4MetadataConditionFailedException;
import com.nereusstream.metadata.oxia.F4ScanToken;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationScanPage;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.metadata.oxia.VersionedMaterializationCheckpoint;
import com.nereusstream.metadata.oxia.VersionedMaterializationStreamRegistration;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import com.nereusstream.metadata.oxia.records.MaterializationCheckpointRecord;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;

/** Monotonic checkpoint CAS loop with bounded full-prefix validation and response-loss recovery. */
public final class DefaultMaterializationCheckpointReconciler
        implements MaterializationCheckpointReconciler {
    public static final int MAX_CANDIDATES = 4_096;
    private static final int MAX_CAS_ATTEMPTS = 8;
    private static final Comparator<VersionedGenerationIndex> COVER_ORDER = Comparator
            .comparingLong((VersionedGenerationIndex value) -> value.value().offsetEnd())
            .reversed()
            .thenComparing(Comparator.comparingLong(
                    (VersionedGenerationIndex value) -> value.value().generation()).reversed())
            .thenComparing(VersionedGenerationIndex::key, DefaultMaterializationCheckpointReconciler::compareUtf8);

    private final String cluster;
    private final OxiaMetadataStore l0Metadata;
    private final GenerationMetadataStore generations;
    private final int pageSize;
    private final Duration operationTimeout;
    private final ScheduledExecutorService scheduler;
    private final Clock clock;
    private final ReadTargetCodecRegistry targetCodecs = ReadTargetCodecRegistry.phase15();

    public DefaultMaterializationCheckpointReconciler(
            String cluster,
            OxiaMetadataStore l0Metadata,
            GenerationMetadataStore generations,
            int pageSize,
            Duration operationTimeout,
            ScheduledExecutorService scheduler,
            Clock clock) {
        this.cluster = requireText(cluster, "cluster");
        this.l0Metadata = Objects.requireNonNull(l0Metadata, "l0Metadata");
        this.generations = Objects.requireNonNull(generations, "generations");
        if (pageSize <= 0 || pageSize > MaterializationConfig.MAX_PAGE_SIZE) {
            throw new IllegalArgumentException("pageSize must be in [1, 1000]");
        }
        this.pageSize = pageSize;
        this.operationTimeout = requirePositive(operationTimeout, "operationTimeout");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public CompletableFuture<VersionedMaterializationCheckpoint> reconcile(
            StreamId streamId,
            MaterializationPolicy policy,
            MaterializationTaskMutationGuard mutationGuard) {
        try {
            Operation operation = new Operation(
                    Objects.requireNonNull(streamId, "streamId"),
                    requirePolicy(policy),
                    Objects.requireNonNull(mutationGuard, "mutationGuard"));
            return operation.run().whenComplete((ignored, failure) -> operation.close());
        } catch (RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private final class Operation implements AutoCloseable {
        private final StreamId streamId;
        private final MaterializationPolicy policy;
        private final MaterializationTaskMutationGuard mutationGuard;
        private final MaterializationDeadline deadline = new MaterializationDeadline(
                operationTimeout, scheduler);

        private Operation(
                StreamId streamId,
                MaterializationPolicy policy,
                MaterializationTaskMutationGuard mutationGuard) {
            this.streamId = streamId;
            this.policy = policy;
            this.mutationGuard = mutationGuard;
        }

        private CompletableFuture<VersionedMaterializationCheckpoint> run() {
            return attempt(0);
        }

        private CompletableFuture<VersionedMaterializationCheckpoint> attempt(int attempt) {
            if (attempt >= MAX_CAS_ATTEMPTS) {
                return CompletableFuture.failedFuture(condition(
                        "materialization checkpoint CAS recovery exhausted"));
            }
            return deadline.bound(
                            () -> l0Metadata.getStreamSnapshot(cluster, streamId),
                            "load stream snapshot for materialization checkpoint")
                    .thenCompose(snapshot -> deadline.bound(
                                    () -> generations.getStreamRegistration(cluster, streamId),
                                    "load registration for materialization checkpoint")
                            .thenCompose(registration -> loadCheckpoint().thenCompose(checkpoint ->
                                    reconcileSnapshot(
                                            snapshot,
                                            registration.orElseThrow(() -> condition(
                                                    "materialization registration is absent")),
                                            checkpoint,
                                            attempt))));
        }

        private CompletableFuture<VersionedMaterializationCheckpoint> loadCheckpoint() {
            return deadline.bound(
                            () -> generations.getMaterializationCheckpoint(
                                    cluster,
                                    streamId,
                                    policy.policyId(),
                                    policy.policyVersion()),
                            "load materialization checkpoint")
                    .thenCompose(optional -> {
                        if (optional.isPresent()) {
                            return CompletableFuture.completedFuture(optional.orElseThrow());
                        }
                        return deadline.bound(
                                        mutationGuard::revalidate,
                                        "revalidate activation before checkpoint create")
                                .thenCompose(ignored -> deadline.bound(
                                        () -> generations.getOrCreateMaterializationCheckpoint(
                                                cluster,
                                                streamId,
                                                policy.policyId(),
                                                policy.policyVersion(),
                                                policy.digestSha256()),
                                        "create materialization checkpoint"));
                    });
        }

        private CompletableFuture<VersionedMaterializationCheckpoint> reconcileSnapshot(
                StreamMetadataSnapshot snapshot,
                VersionedMaterializationStreamRegistration registration,
                VersionedMaterializationCheckpoint checkpoint,
                int attempt) {
            Bounds bounds = validateSnapshot(snapshot, registration, checkpoint);
            if (bounds.trimOffset() >= bounds.headOffset()) {
                return update(checkpoint, Coverage.at(bounds.trimOffset()), attempt);
            }
            long minimumOffsetEnd = Math.addExact(bounds.trimOffset(), 1);
            return scan(
                            minimumOffsetEnd,
                            bounds.headOffset(),
                            Optional.empty(),
                            new ArrayList<>())
                    .thenCompose(candidates -> update(
                            checkpoint,
                            coverage(bounds, registration.value().projectionRef(), candidates),
                            attempt));
        }

        private Bounds validateSnapshot(
                StreamMetadataSnapshot snapshot,
                VersionedMaterializationStreamRegistration registration,
                VersionedMaterializationCheckpoint checkpoint) {
            if (!snapshot.metadata().streamId().equals(streamId.value())
                    || !registration.value().streamId().equals(streamId.value())
                    || !checkpoint.value().streamId().equals(streamId.value())) {
                throw invariant("checkpoint input belongs to another stream", null);
            }
            StreamState state;
            StorageProfile profile;
            StorageProfile registeredProfile;
            try {
                state = StreamState.valueOf(snapshot.metadata().state());
                profile = StorageProfile.valueOf(snapshot.metadata().profile()).canonical();
                registeredProfile = StorageProfile.valueOf(
                        registration.value().storageProfile()).canonical();
            } catch (RuntimeException failure) {
                throw invariant("checkpoint input contains an unsupported state/profile", failure);
            }
            if ((state != StreamState.ACTIVE && state != StreamState.SEALED)
                    || profile != registeredProfile
                    || !profile.objectMaterializationEnabled()) {
                throw condition("stream no longer admits materialization checkpoint mutation");
            }
            MaterializationCheckpointRecord value = checkpoint.value();
            if (!value.policyId().equals(policy.policyId())
                    || value.policyVersion() != policy.policyVersion()
                    || !value.policySha256().equals(policy.digestSha256().value())) {
                throw invariant("materialization checkpoint policy identity mismatch", null);
            }
            long trim = snapshot.trim().trimOffset();
            long head = snapshot.committedEnd().committedEndOffset();
            if (trim > head || value.contiguousCoveredOffset() > head) {
                throw invariant("materialization checkpoint is ahead of authoritative stream bounds", null);
            }
            return new Bounds(trim, head);
        }

        private CompletableFuture<List<VersionedGenerationCandidate>> scan(
                long minimumOffsetEnd,
                long maximumOffsetEnd,
                Optional<F4ScanToken> continuation,
                ArrayList<VersionedGenerationCandidate> values) {
            return deadline.bound(
                            () -> generations.scanIndex(
                                    cluster,
                                    streamId,
                                    policy.view(),
                                    minimumOffsetEnd,
                                    maximumOffsetEnd,
                                    continuation,
                                    pageSize),
                            "scan committed generations for materialization checkpoint")
                    .thenCompose(page -> appendPage(
                            minimumOffsetEnd, maximumOffsetEnd, values, page));
        }

        private CompletableFuture<List<VersionedGenerationCandidate>> appendPage(
                long minimumOffsetEnd,
                long maximumOffsetEnd,
                ArrayList<VersionedGenerationCandidate> values,
                GenerationScanPage page) {
            values.addAll(page.values());
            if (values.size() > MAX_CANDIDATES) {
                return CompletableFuture.failedFuture(new NereusException(
                        ErrorCode.METADATA_LIMIT_EXCEEDED,
                        false,
                        "materialization checkpoint candidate count exceeds 4096"));
            }
            if (page.continuation().isPresent()) {
                return scan(
                        minimumOffsetEnd,
                        maximumOffsetEnd,
                        page.continuation(),
                        values);
            }
            return CompletableFuture.completedFuture(List.copyOf(values));
        }

        private Coverage coverage(
                Bounds bounds,
                String projectionRef,
                List<VersionedGenerationCandidate> candidates) {
            List<VersionedGenerationIndex> healthy = candidates.stream()
                    .filter(VersionedGenerationIndex.class::isInstance)
                    .map(VersionedGenerationIndex.class::cast)
                    .filter(index -> satisfiesPolicy(index, projectionRef, bounds))
                    .toList();
            long cursor = bounds.trimOffset();
            long observedCommitVersion = 0;
            long lastTaskSequence = 0;
            String lastTaskId = "";
            int steps = 0;
            while (cursor < bounds.headOffset()) {
                final long current = cursor;
                VersionedGenerationIndex selected = healthy.stream()
                        .filter(index -> index.value().offsetStart() <= current
                                && index.value().offsetEnd() > current)
                        .sorted(COVER_ORDER)
                        .findFirst()
                        .orElse(null);
                if (selected == null) {
                    break;
                }
                if (++steps > healthy.size()) {
                    throw invariant("checkpoint coverage selection did not converge", null);
                }
                GenerationIndexRecord value = selected.value();
                cursor = value.offsetEnd();
                observedCommitVersion = Math.max(observedCommitVersion, value.lastCommitVersion());
                if (value.lastCommitVersion() > lastTaskSequence
                        || value.lastCommitVersion() == lastTaskSequence
                                && compareUtf8(value.taskId(), lastTaskId) < 0) {
                    lastTaskSequence = value.lastCommitVersion();
                    lastTaskId = value.taskId();
                }
            }
            return new Coverage(cursor, observedCommitVersion, lastTaskSequence, lastTaskId);
        }

        private boolean satisfiesPolicy(
                VersionedGenerationIndex candidate,
                String projectionRef,
                Bounds bounds) {
            GenerationIndexRecord value = candidate.value();
            if (!value.streamId().equals(streamId.value())
                    || value.readViewId() != policy.view().wireId()
                    || value.offsetEnd() > bounds.headOffset()) {
                throw invariant("generation scan returned an out-of-scope checkpoint candidate", null);
            }
            if (value.lifecycle() != GenerationLifecycle.COMMITTED
                    || !value.policySha256().equals(policy.digestSha256().value())
                    || !value.materializationPolicySha256().equals(policy.digestSha256().value())
                    || !value.projectionRef().equals(projectionRef)) {
                return false;
            }
            try {
                if (!(targetCodecs.decode(value.readTarget()) instanceof ObjectSliceReadTarget target)
                        || target.objectType() != ObjectType.STREAM_COMPACTED_OBJECT
                        || !target.physicalFormat().equals(policy.targetPhysicalFormat())) {
                    throw invariant("current-policy generation has an incompatible physical target", null);
                }
            } catch (NereusException failure) {
                throw failure;
            } catch (RuntimeException failure) {
                throw invariant("current-policy generation target cannot be decoded", failure);
            }
            return true;
        }

        private CompletableFuture<VersionedMaterializationCheckpoint> update(
                VersionedMaterializationCheckpoint current,
                Coverage coverage,
                int attempt) {
            MaterializationCheckpointRecord value = current.value();
            if (coverage.contiguousCoveredOffset() < value.contiguousCoveredOffset()) {
                return CompletableFuture.failedFuture(invariant(
                        "materialization checkpoint is ahead of verified committed coverage", null));
            }
            long covered = Math.max(value.contiguousCoveredOffset(), coverage.contiguousCoveredOffset());
            long observed = Math.max(value.observedCommitVersion(), coverage.observedCommitVersion());
            long taskSequence = value.lastTaskSequence();
            String taskId = value.lastTaskId();
            if (coverage.lastTaskSequence() > taskSequence) {
                taskSequence = coverage.lastTaskSequence();
                taskId = coverage.lastTaskId();
            }
            if (covered == value.contiguousCoveredOffset()
                    && observed == value.observedCommitVersion()
                    && taskSequence == value.lastTaskSequence()
                    && taskId.equals(value.lastTaskId())) {
                return CompletableFuture.completedFuture(current);
            }
            MaterializationCheckpointRecord replacement = new MaterializationCheckpointRecord(
                    value.schemaVersion(),
                    value.streamId(),
                    value.policyId(),
                    value.policyVersion(),
                    value.policySha256(),
                    covered,
                    observed,
                    taskSequence,
                    taskId,
                    Math.max(clock.millis(), value.updatedAtMillis()),
                    0);
            return deadline.bound(
                            mutationGuard::revalidate,
                            "revalidate activation before checkpoint CAS")
                    .thenCompose(ignored -> deadline.bound(
                            () -> generations.compareAndSetMaterializationCheckpoint(
                                    cluster, replacement, current.metadataVersion()),
                            "advance materialization checkpoint"))
                    .handle((updated, failure) -> {
                        if (failure == null) {
                            return CompletableFuture.completedFuture(updated);
                        }
                        Throwable exact = unwrap(failure);
                        return deadline.bound(
                                        () -> generations.getMaterializationCheckpoint(
                                                cluster,
                                                streamId,
                                                policy.policyId(),
                                                policy.policyVersion()),
                                        "reload materialization checkpoint after CAS failure")
                                .thenCompose(optional -> {
                                    if (optional.isPresent()
                                            && exactReplacement(
                                                    replacement, optional.orElseThrow())) {
                                        return CompletableFuture.completedFuture(optional.orElseThrow());
                                    }
                                    if (isConditionFailure(exact)) {
                                        return attempt(attempt + 1);
                                    }
                                    return CompletableFuture.failedFuture(exact);
                                });
                    })
                    .thenCompose(valueFuture -> valueFuture);
        }

        @Override
        public void close() {
            deadline.close();
        }
    }

    private static boolean exactReplacement(
            MaterializationCheckpointRecord expected,
            VersionedMaterializationCheckpoint actual) {
        return expected.withMetadataVersion(actual.metadataVersion()).equals(actual.value());
    }

    private static boolean isConditionFailure(Throwable failure) {
        return failure instanceof F4MetadataConditionFailedException
                || failure instanceof NereusException nereus
                        && nereus.code() == ErrorCode.METADATA_CONDITION_FAILED;
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static MaterializationPolicy requirePolicy(MaterializationPolicy policy) {
        Objects.requireNonNull(policy, "policy");
        if (policy.view() != ReadView.COMMITTED
                || policy.taskKind() != TaskKind.LOSSLESS_REWRITE
                || !policy.targetPhysicalFormat().equals(MaterializationPolicy.COMMITTED_FORMAT)) {
            throw new IllegalArgumentException(
                    "checkpoint reconciler admits only lossless COMMITTED NCP1 policy");
        }
        return policy;
    }

    private static Duration requirePositive(Duration value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(field + " must be positive");
        }
        return value;
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

    private record Bounds(long trimOffset, long headOffset) {
    }

    private record Coverage(
            long contiguousCoveredOffset,
            long observedCommitVersion,
            long lastTaskSequence,
            String lastTaskId) {
        private Coverage {
            if (contiguousCoveredOffset < 0
                    || observedCommitVersion < 0
                    || lastTaskSequence < 0
                    || (lastTaskSequence == 0) != lastTaskId.isEmpty()) {
                throw new IllegalArgumentException("invalid materialization checkpoint coverage");
            }
        }

        private static Coverage at(long offset) {
            return new Coverage(offset, 0, 0, "");
        }
    }
}
