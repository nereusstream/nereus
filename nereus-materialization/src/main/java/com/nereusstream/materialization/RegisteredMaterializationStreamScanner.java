/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.ProjectionRef;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamState;
import com.nereusstream.core.capability.GenerationActivationProof;
import com.nereusstream.core.capability.GenerationOperation;
import com.nereusstream.core.capability.GenerationProtocolActivationGuard;
import com.nereusstream.core.capability.LiveProjectionSubject;
import com.nereusstream.materialization.recovery.RecoveryCheckpointPublisher;
import com.nereusstream.metadata.oxia.F4Keyspace;
import com.nereusstream.metadata.oxia.F4ScanToken;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.ProjectionIdentity;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.StreamRegistrationScanPage;
import com.nereusstream.metadata.oxia.VersionedMaterializationStreamRegistration;
import com.nereusstream.metadata.oxia.VersionedMaterializationTask;
import com.nereusstream.metadata.oxia.records.MaterializationStreamRegistrationRecord;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Process-wide 64-shard liveness scanner; registration triggers work but never becomes stream/index truth. */
public final class RegisteredMaterializationStreamScanner {
    private final String cluster;
    private final OxiaMetadataStore l0Metadata;
    private final GenerationMetadataStore generations;
    private final GenerationProtocolActivationGuard activationGuard;
    private final MaterializationSourceRepairer sourceRepairer;
    private final MaterializationPlanner planner;
    private final MaterializationTaskStore tasks;
    private final MaterializationTaskRecovery recovery;
    private final TaskRecoveryScanner recoveryScanner;
    private final RecoveryCheckpointPublisher recoveryCheckpoints;
    private final MaterializationCheckpointReconciler checkpoints;
    private final TerminalWorkflowMetadataRetirer metadataRetirer;
    private final MaterializationPolicy policy;
    private final int registryPageSize;
    private final int maxTasksPerPlan;
    private final F4Keyspace keyspace;

    public RegisteredMaterializationStreamScanner(
            String cluster,
            OxiaMetadataStore l0Metadata,
            GenerationMetadataStore generations,
            GenerationProtocolActivationGuard activationGuard,
            MaterializationPlanner planner,
            MaterializationTaskStore tasks,
            MaterializationTaskRecovery recovery,
            TaskRecoveryScanner recoveryScanner,
            RecoveryCheckpointPublisher recoveryCheckpoints,
            MaterializationCheckpointReconciler checkpoints,
            TerminalWorkflowMetadataRetirer metadataRetirer,
            MaterializationPolicy policy,
            int registryPageSize,
            int maxTasksPerPlan) {
        this(
                cluster,
                l0Metadata,
                generations,
                activationGuard,
                MaterializationSourceRepairer.noOp(),
                planner,
                tasks,
                recovery,
                recoveryScanner,
                recoveryCheckpoints,
                checkpoints,
                metadataRetirer,
                policy,
                registryPageSize,
                maxTasksPerPlan);
    }

    public RegisteredMaterializationStreamScanner(
            String cluster,
            OxiaMetadataStore l0Metadata,
            GenerationMetadataStore generations,
            GenerationProtocolActivationGuard activationGuard,
            MaterializationSourceRepairer sourceRepairer,
            MaterializationPlanner planner,
            MaterializationTaskStore tasks,
            MaterializationTaskRecovery recovery,
            TaskRecoveryScanner recoveryScanner,
            RecoveryCheckpointPublisher recoveryCheckpoints,
            MaterializationCheckpointReconciler checkpoints,
            TerminalWorkflowMetadataRetirer metadataRetirer,
            MaterializationPolicy policy,
            int registryPageSize,
            int maxTasksPerPlan) {
        this.cluster = requireText(cluster, "cluster");
        this.l0Metadata = Objects.requireNonNull(l0Metadata, "l0Metadata");
        this.generations = Objects.requireNonNull(generations, "generations");
        this.activationGuard = Objects.requireNonNull(activationGuard, "activationGuard");
        this.sourceRepairer = Objects.requireNonNull(
                sourceRepairer, "sourceRepairer");
        this.planner = Objects.requireNonNull(planner, "planner");
        this.tasks = Objects.requireNonNull(tasks, "tasks");
        this.recovery = Objects.requireNonNull(recovery, "recovery");
        this.recoveryScanner = Objects.requireNonNull(recoveryScanner, "recoveryScanner");
        this.recoveryCheckpoints = Objects.requireNonNull(
                recoveryCheckpoints, "recoveryCheckpoints");
        this.checkpoints = Objects.requireNonNull(checkpoints, "checkpoints");
        this.metadataRetirer = Objects.requireNonNull(metadataRetirer, "metadataRetirer");
        this.policy = Objects.requireNonNull(policy, "policy");
        if (registryPageSize <= 0 || registryPageSize > 1_000) {
            throw new IllegalArgumentException("registryPageSize must be in [1, 1000]");
        }
        if (maxTasksPerPlan <= 0 || maxTasksPerPlan > 1_000) {
            throw new IllegalArgumentException("maxTasksPerPlan must be in [1, 1000]");
        }
        this.registryPageSize = registryPageSize;
        this.maxTasksPerPlan = maxTasksPerPlan;
        this.keyspace = new F4Keyspace(cluster);
    }

    public CompletableFuture<RegisteredMaterializationScanResult> scanOnce() {
        try {
            return scanShard(0, new Accumulator());
        } catch (RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private CompletableFuture<RegisteredMaterializationScanResult> scanShard(
            int shard,
            Accumulator accumulator) {
        if (shard == F4Keyspace.MATERIALIZATION_REGISTRY_SHARDS) {
            return CompletableFuture.completedFuture(accumulator.result());
        }
        return scanRegistryPage(shard, Optional.empty(), accumulator).thenCompose(ignored -> {
            accumulator.shardsScanned++;
            return scanShard(shard + 1, accumulator);
        });
    }

    private CompletableFuture<Void> scanRegistryPage(
            int shard,
            Optional<F4ScanToken> continuation,
            Accumulator accumulator) {
        return generations.scanStreamRegistrations(
                        cluster, shard, continuation, registryPageSize)
                .thenCompose(page -> processRegistrationPage(page, 0, accumulator).thenCompose(ignored -> {
                    if (page.continuation().isPresent()) {
                        return scanRegistryPage(shard, page.continuation(), accumulator);
                    }
                    return CompletableFuture.completedFuture(null);
                }));
    }

    private CompletableFuture<Void> processRegistrationPage(
            StreamRegistrationScanPage page,
            int index,
            Accumulator accumulator) {
        if (index == page.values().size()) {
            return CompletableFuture.completedFuture(null);
        }
        VersionedMaterializationStreamRegistration registration = page.values().get(index);
        accumulator.registrationsScanned++;
        return processRegistration(registration, accumulator).thenCompose(ignored ->
                processRegistrationPage(page, index + 1, accumulator));
    }

    private CompletableFuture<Void> processRegistration(
            VersionedMaterializationStreamRegistration registration,
            Accumulator accumulator) {
        MaterializationStreamRegistrationRecord value = registration.value();
        StreamId streamId = new StreamId(value.streamId());
        if (!registration.key().equals(keyspace.materializationRegistryKey(streamId))) {
            return CompletableFuture.failedFuture(invariant(
                    "registered materialization stream key/value identity mismatch", null));
        }
        return l0Metadata.getStreamSnapshot(cluster, streamId).thenCompose(snapshot -> {
            Optional<LiveProjectionSubject> subject = validateRegistration(value, streamId, snapshot);
            if (subject.isEmpty()) {
                accumulator.registrationsSkipped++;
                return CompletableFuture.completedFuture(null);
            }
            return activationGuard.requireReady(
                            policy.taskKind() == TaskKind.TOPIC_KEY_COMPACTION
                                    ? GenerationOperation.TOPIC_COMPACTED_PUBLISH
                                    : GenerationOperation.GENERATION_PUBLISH,
                            subject.orElseThrow(),
                            true)
                    .thenCompose(proof -> processAdmittedStream(
                            streamId, snapshot, proof, accumulator));
        });
    }

    private Optional<LiveProjectionSubject> validateRegistration(
            MaterializationStreamRegistrationRecord registration,
            StreamId streamId,
            StreamMetadataSnapshot snapshot) {
        if (!snapshot.metadata().streamId().equals(streamId.value())) {
            throw invariant("registered stream snapshot belongs to another stream", null);
        }
        StreamState state;
        StorageProfile registeredProfile;
        StorageProfile actualProfile;
        Optional<ProjectionRef> projection;
        try {
            state = StreamState.valueOf(snapshot.metadata().state());
            registeredProfile = StorageProfile.valueOf(registration.storageProfile()).canonical();
            actualProfile = StorageProfile.valueOf(snapshot.metadata().profile()).canonical();
            projection = ProjectionIdentity.decode(registration.projectionRef());
        } catch (RuntimeException failure) {
            throw invariant("registered stream carries an unsupported state/profile/projection", failure);
        }
        if ((state != StreamState.ACTIVE && state != StreamState.SEALED)
                || registeredProfile != actualProfile
                || !actualProfile.objectMaterializationEnabled()
                || projection.isEmpty()
                || registration.lastHintCommitVersion() > snapshot.committedEnd().commitVersion()) {
            return Optional.empty();
        }
        return Optional.of(new LiveProjectionSubject(
                streamId,
                projection.orElseThrow(),
                new Checksum(ChecksumType.SHA256, registration.projectionIdentitySha256())));
    }

    private CompletableFuture<Void> processAdmittedStream(
            StreamId streamId,
            StreamMetadataSnapshot snapshot,
            GenerationActivationProof proof,
            Accumulator accumulator) {
        accumulator.registrationsAdmitted++;
        MaterializationTaskMutationGuard mutationGuard = () -> activationGuard.revalidate(proof);
        return sourceRepairer.repair(streamId)
                .thenCompose(ignored -> recoveryScanner.scan(
                        streamId, mutationGuard))
                .thenCompose(recovered -> {
            accumulator.existingTasksRecovered = Math.addExact(
                    accumulator.existingTasksRecovered, recovered.scannedTasks());
            long trim = snapshot.trim().trimOffset();
            long head = snapshot.committedEnd().committedEndOffset();
            CompletableFuture<Void> planned;
            if (trim >= head) {
                planned = CompletableFuture.completedFuture(null);
            } else {
                // The advisory checkpoint is intentionally not a skip boundary. Scanning authoritative trim..head
                // keeps a stale/ahead checkpoint incapable of hiding a missing task or committed index.
                planned = planner.plan(
                                streamId,
                                new OffsetRange(trim, head),
                                policy,
                                maxTasksPerPlan)
                        .thenCompose(tasks -> createAndRecover(
                                tasks, 0, mutationGuard, accumulator));
            }
            return planned.thenCompose(ignored -> recoveryCheckpoints.checkpoint(streamId))
                    .thenCompose(ignored -> checkpoints.reconcile(
                            streamId, policy, mutationGuard))
                    .thenCompose(ignored -> metadataRetirer.retire(
                            streamId, policy, trim, mutationGuard))
                    .thenAccept(retired -> accumulator.workflowMetadataRetired = Math.addExact(
                            accumulator.workflowMetadataRetired,
                            Math.addExact(
                                    retired.tasksRetired(),
                                    Math.addExact(
                                            retired.retentionStatsRetired(),
                                            retired.checkpointsRetired()))));
                });
    }

    private CompletableFuture<Void> createAndRecover(
            List<MaterializationTask> planned,
            int index,
            MaterializationTaskMutationGuard mutationGuard,
            Accumulator accumulator) {
        if (index == planned.size()) {
            return CompletableFuture.completedFuture(null);
        }
        return tasks.create(planned.get(index), mutationGuard).thenCompose(durable -> {
            accumulator.plannedTasksConverged++;
            return recoverCreated(durable, mutationGuard).thenCompose(ignored ->
                    createAndRecover(planned, index + 1, mutationGuard, accumulator));
        });
    }

    private CompletableFuture<MaterializationTaskRecoveryAction> recoverCreated(
            VersionedMaterializationTask durable,
            MaterializationTaskMutationGuard mutationGuard) {
        return recovery.recover(durable, mutationGuard);
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

    private static final class Accumulator {
        private int shardsScanned;
        private int registrationsScanned;
        private int registrationsAdmitted;
        private int registrationsSkipped;
        private int existingTasksRecovered;
        private int plannedTasksConverged;
        private int workflowMetadataRetired;

        private RegisteredMaterializationScanResult result() {
            return new RegisteredMaterializationScanResult(
                    shardsScanned,
                    registrationsScanned,
                    registrationsAdmitted,
                    registrationsSkipped,
                    existingTasksRecovered,
                    plannedTasksConverged,
                    workflowMetadataRetired);
        }
    }
}
