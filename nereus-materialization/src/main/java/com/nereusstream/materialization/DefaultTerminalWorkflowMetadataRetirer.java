/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.core.physical.ObjectProtectionOwner;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.metadata.oxia.F4ScanToken;
import com.nereusstream.metadata.oxia.GenerationIndexIdentity;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationScanPage;
import com.nereusstream.metadata.oxia.MaterializationCheckpointScanPage;
import com.nereusstream.metadata.oxia.ObjectProtectionIdentity;
import com.nereusstream.metadata.oxia.ObjectProtectionScanPage;
import com.nereusstream.metadata.oxia.PhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.TaskScanPage;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.metadata.oxia.VersionedMaterializationCheckpoint;
import com.nereusstream.metadata.oxia.VersionedMaterializationTask;
import com.nereusstream.metadata.oxia.VersionedObjectProtection;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.VersionedRangeRetentionStats;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import com.nereusstream.metadata.oxia.records.MaterializationCheckpointRecord;
import com.nereusstream.metadata.oxia.records.MaterializationTaskRecord;
import com.nereusstream.metadata.oxia.records.ObjectProtectionRecord;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.records.RangeRetentionStatsRecord;
import com.nereusstream.metadata.oxia.records.TaskLifecycle;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/** Exact task/index/checkpoint/root/protection proof before terminal task metadata deletion. */
public final class DefaultTerminalWorkflowMetadataRetirer
        implements TerminalWorkflowMetadataRetirer {
    private static final int MAX_SCAN_ENTRIES = 4_096;
    private static final int MAX_DELETE_RECOVERY_ATTEMPTS = 8;

    private final String cluster;
    private final MaterializationTaskStore tasks;
    private final GenerationMetadataStore generations;
    private final PhysicalObjectMetadataStore physical;
    private final long metadataAuditGraceMillis;
    private final int pageSize;
    private final Duration operationTimeout;
    private final ScheduledExecutorService scheduler;
    private final Clock clock;

    public DefaultTerminalWorkflowMetadataRetirer(
            String cluster,
            MaterializationTaskStore tasks,
            GenerationMetadataStore generations,
            PhysicalObjectMetadataStore physical,
            Duration metadataAuditGrace,
            int pageSize,
            Duration operationTimeout,
            ScheduledExecutorService scheduler,
            Clock clock) {
        this.cluster = requireText(cluster, "cluster");
        this.tasks = Objects.requireNonNull(tasks, "tasks");
        this.generations = Objects.requireNonNull(generations, "generations");
        this.physical = Objects.requireNonNull(physical, "physical");
        this.metadataAuditGraceMillis = requirePositiveMillis(
                metadataAuditGrace, "metadataAuditGrace");
        if (pageSize <= 0 || pageSize > 1_000) {
            throw new IllegalArgumentException("pageSize must be in [1, 1000]");
        }
        this.pageSize = pageSize;
        this.operationTimeout = requirePositive(operationTimeout, "operationTimeout");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public CompletableFuture<TerminalWorkflowMetadataRetirementResult> retire(
            StreamId streamId,
            MaterializationPolicy currentPolicy,
            long stableCompletedTrimOffset,
            MaterializationTaskMutationGuard mutationGuard) {
        try {
            if (stableCompletedTrimOffset < 0) {
                throw new IllegalArgumentException(
                        "stableCompletedTrimOffset must be non-negative");
            }
            Operation operation = new Operation(
                    Objects.requireNonNull(streamId, "streamId"),
                    Objects.requireNonNull(currentPolicy, "currentPolicy"),
                    stableCompletedTrimOffset,
                    Objects.requireNonNull(mutationGuard, "mutationGuard"));
            CompletableFuture<TerminalWorkflowMetadataRetirementResult> result = operation.run();
            result.whenComplete((ignored, failure) -> operation.close());
            return result;
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private final class Operation implements AutoCloseable {
        private final StreamId streamId;
        private final MaterializationPolicy currentPolicy;
        private final long stableCompletedTrimOffset;
        private final MaterializationTaskMutationGuard mutationGuard;
        private final MaterializationDeadline deadline;
        private final Accumulator accumulator = new Accumulator();
        private final List<VersionedMaterializationTask> scannedTasks = new ArrayList<>();
        private final List<VersionedRangeRetentionStats> scannedStats = new ArrayList<>();
        private final List<VersionedMaterializationCheckpoint> scannedCheckpoints = new ArrayList<>();

        private Operation(
                StreamId streamId,
                MaterializationPolicy currentPolicy,
                long stableCompletedTrimOffset,
                MaterializationTaskMutationGuard mutationGuard) {
            this.streamId = streamId;
            this.currentPolicy = currentPolicy;
            this.stableCompletedTrimOffset = stableCompletedTrimOffset;
            this.mutationGuard = mutationGuard;
            this.deadline = new MaterializationDeadline(operationTimeout, scheduler);
        }

        private CompletableFuture<TerminalWorkflowMetadataRetirementResult> run() {
            return scanTaskPage(Optional.empty())
                    .thenCompose(ignored -> retireTaskAt(0))
                    .thenCompose(ignored -> scanStatsPage(Optional.empty()))
                    .thenCompose(ignored -> retireStatsAt(0))
                    .thenCompose(ignored -> scanCheckpointPage(Optional.empty()))
                    .thenCompose(ignored -> retireCheckpointAt(0))
                    .thenApply(ignored -> accumulator.result());
        }

        private CompletableFuture<Void> scanTaskPage(Optional<F4ScanToken> continuation) {
            return deadline.bound(
                            () -> generations.scanTasks(
                                    cluster, streamId, continuation, pageSize),
                            "scan terminal materialization tasks")
                    .thenCompose(page -> {
                        addTaskPage(page);
                        return page.continuation().isPresent()
                                ? scanTaskPage(page.continuation())
                                : CompletableFuture.completedFuture(null);
                    });
        }

        private void addTaskPage(TaskScanPage page) {
            if (scannedTasks.size() > MAX_SCAN_ENTRIES - page.values().size()) {
                throw invariant("terminal task audit exceeded its hard scan bound", null);
            }
            scannedTasks.addAll(page.values());
            accumulator.tasksScanned = scannedTasks.size();
        }

        private CompletableFuture<Void> retireTaskAt(int index) {
            if (index == scannedTasks.size()) {
                return CompletableFuture.completedFuture(null);
            }
            return retireTask(scannedTasks.get(index), 0).thenCompose(retired -> {
                if (retired.eligible()) {
                    accumulator.tasksEligible++;
                }
                if (retired.retired()) {
                    accumulator.tasksRetired++;
                    accumulator.protectionsReleased = Math.addExact(
                            accumulator.protectionsReleased, retired.protectionsReleased());
                }
                return retireTaskAt(index + 1);
            });
        }

        private CompletableFuture<Void> scanStatsPage(Optional<F4ScanToken> continuation) {
            return deadline.bound(
                            () -> generations.scanRangeRetentionStats(
                                    cluster,
                                    streamId,
                                    0,
                                    Long.MAX_VALUE,
                                    continuation,
                                    pageSize),
                            "scan range-retention stats for terminal metadata retirement")
                    .thenCompose(page -> {
                        if (scannedStats.size() > MAX_SCAN_ENTRIES - page.values().size()) {
                            throw invariant(
                                    "retention-stats retirement exceeded its hard scan bound", null);
                        }
                        scannedStats.addAll(page.values());
                        accumulator.retentionStatsScanned = scannedStats.size();
                        return page.continuation().isPresent()
                                ? scanStatsPage(page.continuation())
                                : CompletableFuture.completedFuture(null);
                    });
        }

        private CompletableFuture<Void> retireStatsAt(int index) {
            if (index == scannedStats.size()) {
                return CompletableFuture.completedFuture(null);
            }
            return retireStats(scannedStats.get(index), 0).thenCompose(retired -> {
                if (retired) {
                    accumulator.retentionStatsRetired++;
                }
                return retireStatsAt(index + 1);
            });
        }

        private CompletableFuture<Boolean> retireStats(
                VersionedRangeRetentionStats expected,
                int attempt) {
            if (attempt >= MAX_DELETE_RECOVERY_ATTEMPTS) {
                return CompletableFuture.failedFuture(invariant(
                        "retention-stats delete recovery exhausted", null));
            }
            return staleStats(expected.value()).thenCompose(stale -> {
                if (!stale) {
                    return CompletableFuture.completedFuture(false);
                }
                return deadline.bound(
                                mutationGuard::revalidate,
                                "revalidate retention-stats retirement activation")
                        .thenCompose(ignored -> deadline.bound(
                                () -> generations.getRangeRetentionStats(
                                        cluster,
                                        streamId,
                                        expected.value().offsetEnd(),
                                        expected.value().commitVersion()),
                                "reload exact range-retention stats before delete"))
                        .thenCompose(reloaded -> {
                            if (reloaded.isEmpty()
                                    || !sameStats(expected, reloaded.orElseThrow())) {
                                return CompletableFuture.completedFuture(false);
                            }
                            return staleStats(reloaded.orElseThrow().value())
                                    .thenCompose(stillStale -> stillStale
                                            ? deleteStats(reloaded.orElseThrow(), attempt)
                                            : CompletableFuture.completedFuture(false));
                        });
            });
        }

        private CompletableFuture<Boolean> deleteStats(
                VersionedRangeRetentionStats expected,
                int attempt) {
            return deadline.bound(
                            () -> generations.deleteRangeRetentionStats(
                                    cluster,
                                    streamId,
                                    expected.value().offsetEnd(),
                                    expected.value().commitVersion(),
                                    expected.metadataVersion()),
                            "delete stale range-retention stats")
                    .handle((ignored, failure) -> {
                        if (failure == null) {
                            return CompletableFuture.completedFuture(true);
                        }
                        Throwable original = unwrap(failure);
                        return deadline.bound(
                                        () -> generations.getRangeRetentionStats(
                                                cluster,
                                                streamId,
                                                expected.value().offsetEnd(),
                                                expected.value().commitVersion()),
                                        "reload range-retention stats after delete response loss")
                                .thenCompose(reloaded -> {
                                    if (reloaded.isEmpty()) {
                                        return CompletableFuture.completedFuture(true);
                                    }
                                    if (!sameStats(expected, reloaded.orElseThrow())) {
                                        return CompletableFuture.failedFuture(original);
                                    }
                                    return retireStats(reloaded.orElseThrow(), attempt + 1);
                                });
                    })
                    .thenCompose(value -> value);
        }

        private CompletableFuture<Boolean> staleStats(RangeRetentionStatsRecord stats) {
            if (stats.offsetEnd() <= stableCompletedTrimOffset) {
                return CompletableFuture.completedFuture(true);
            }
            return deadline.bound(
                            () -> generations.getCandidateByKey(
                                    cluster,
                                    streamId,
                                    ReadView.COMMITTED,
                                    stats.sourceIndexKey()),
                            "reload retention-stats source index")
                    .thenApply(source -> source.isEmpty()
                            || !matchesStatsSource(stats, source.orElseThrow()));
        }

        private CompletableFuture<Void> scanCheckpointPage(
                Optional<F4ScanToken> continuation) {
            return deadline.bound(
                            () -> generations.scanMaterializationCheckpoints(
                                    cluster, streamId, continuation, pageSize),
                            "scan advisory materialization checkpoints for retirement")
                    .thenCompose(page -> {
                        addCheckpointPage(page);
                        return page.continuation().isPresent()
                                ? scanCheckpointPage(page.continuation())
                                : CompletableFuture.completedFuture(null);
                    });
        }

        private void addCheckpointPage(MaterializationCheckpointScanPage page) {
            if (scannedCheckpoints.size() > MAX_SCAN_ENTRIES - page.values().size()) {
                throw invariant(
                        "checkpoint retirement exceeded its hard scan bound", null);
            }
            scannedCheckpoints.addAll(page.values());
            accumulator.checkpointsScanned = scannedCheckpoints.size();
        }

        private CompletableFuture<Void> retireCheckpointAt(int index) {
            if (index == scannedCheckpoints.size()) {
                return CompletableFuture.completedFuture(null);
            }
            return retireCheckpoint(scannedCheckpoints.get(index), 0).thenCompose(retired -> {
                if (retired) {
                    accumulator.checkpointsRetired++;
                }
                return retireCheckpointAt(index + 1);
            });
        }

        private CompletableFuture<Boolean> retireCheckpoint(
                VersionedMaterializationCheckpoint expected,
                int attempt) {
            if (attempt >= MAX_DELETE_RECOVERY_ATTEMPTS) {
                return CompletableFuture.failedFuture(invariant(
                        "old-policy checkpoint delete recovery exhausted", null));
            }
            if (isCurrentPolicy(expected.value())
                    || !auditGraceElapsed(expected.value().updatedAtMillis())) {
                return CompletableFuture.completedFuture(false);
            }
            return oldPolicySnapshot(expected.value()).thenCompose(first -> {
                if (!first.retirable()) {
                    return CompletableFuture.completedFuture(false);
                }
                return deadline.bound(
                                mutationGuard::revalidate,
                                "revalidate old-policy checkpoint retirement activation")
                        .thenCompose(ignored -> oldPolicySnapshot(expected.value()))
                        .thenCompose(second -> {
                            if (!second.retirable() || !first.equals(second)) {
                                return CompletableFuture.completedFuture(false);
                            }
                            return deadline.bound(
                                            () -> generations.getMaterializationCheckpoint(
                                                    cluster,
                                                    streamId,
                                                    expected.value().policyId(),
                                                    expected.value().policyVersion()),
                                            "reload old-policy checkpoint before delete")
                                    .thenCompose(reloaded -> {
                                        if (reloaded.isEmpty()
                                                || !sameCheckpoint(
                                                        expected, reloaded.orElseThrow())) {
                                            return CompletableFuture.completedFuture(false);
                                        }
                                        return deleteCheckpoint(
                                                reloaded.orElseThrow(), attempt);
                                    });
                        });
            });
        }

        private CompletableFuture<Boolean> deleteCheckpoint(
                VersionedMaterializationCheckpoint expected,
                int attempt) {
            return deadline.bound(
                            () -> generations.deleteMaterializationCheckpoint(
                                    cluster,
                                    streamId,
                                    expected.value().policyId(),
                                    expected.value().policyVersion(),
                                    expected.metadataVersion()),
                            "delete old-policy materialization checkpoint")
                    .handle((ignored, failure) -> {
                        if (failure == null) {
                            return CompletableFuture.completedFuture(true);
                        }
                        Throwable original = unwrap(failure);
                        return deadline.bound(
                                        () -> generations.getMaterializationCheckpoint(
                                                cluster,
                                                streamId,
                                                expected.value().policyId(),
                                                expected.value().policyVersion()),
                                        "reload old-policy checkpoint after delete response loss")
                                .thenCompose(reloaded -> {
                                    if (reloaded.isEmpty()) {
                                        return CompletableFuture.completedFuture(true);
                                    }
                                    if (!sameCheckpoint(expected, reloaded.orElseThrow())) {
                                        return CompletableFuture.failedFuture(original);
                                    }
                                    return retireCheckpoint(
                                            reloaded.orElseThrow(), attempt + 1);
                                });
                    })
                    .thenCompose(value -> value);
        }

        private CompletableFuture<OldPolicySnapshot> oldPolicySnapshot(
                MaterializationCheckpointRecord checkpoint) {
            List<VersionedMaterializationTask> tasks = new ArrayList<>();
            List<IndexFingerprint> indexes = new ArrayList<>();
            return scanFreshTasks(Optional.empty(), tasks)
                    .thenCompose(ignored -> scanPolicyIndexes(
                            checkpoint, ReadView.COMMITTED, Optional.empty(), indexes))
                    .thenCompose(ignored -> scanPolicyIndexes(
                            checkpoint, ReadView.TOPIC_COMPACTED, Optional.empty(), indexes))
                    .thenApply(ignored -> {
                        boolean taskReference = tasks.stream().anyMatch(task ->
                                task.value().policyId().equals(checkpoint.policyId())
                                        && task.value().policyVersion()
                                                == checkpoint.policyVersion());
                        boolean preparedPublication = indexes.stream().anyMatch(index ->
                                index.lifecycle() == GenerationLifecycle.PREPARED);
                        return new OldPolicySnapshot(
                                !taskReference && !preparedPublication,
                                tasks.stream().map(TaskFingerprint::from).toList(),
                                List.copyOf(indexes));
                    });
        }

        private CompletableFuture<Void> scanFreshTasks(
                Optional<F4ScanToken> continuation,
                List<VersionedMaterializationTask> values) {
            return deadline.bound(
                            () -> generations.scanTasks(
                                    cluster, streamId, continuation, pageSize),
                            "rescan task prefix for old-policy checkpoint retirement")
                    .thenCompose(page -> {
                        if (values.size() > MAX_SCAN_ENTRIES - page.values().size()) {
                            throw invariant(
                                    "old-policy task rescan exceeded its hard bound", null);
                        }
                        values.addAll(page.values());
                        return page.continuation().isPresent()
                                ? scanFreshTasks(page.continuation(), values)
                                : CompletableFuture.completedFuture(null);
                    });
        }

        private CompletableFuture<Void> scanPolicyIndexes(
                MaterializationCheckpointRecord checkpoint,
                ReadView view,
                Optional<F4ScanToken> continuation,
                List<IndexFingerprint> values) {
            return deadline.bound(
                            () -> generations.scanIndex(
                                    cluster,
                                    streamId,
                                    view,
                                    0,
                                    Long.MAX_VALUE,
                                    continuation,
                                    pageSize),
                            "rescan generation-index prefix for old-policy checkpoint retirement")
                    .thenCompose(page -> {
                        for (VersionedGenerationCandidate candidate : page.values()) {
                            if (candidate instanceof VersionedGenerationIndex higher
                                    && higher.value().policySha256().equals(
                                            checkpoint.policySha256())) {
                                if (values.size() == MAX_SCAN_ENTRIES) {
                                    throw invariant(
                                            "old-policy index rescan exceeded its hard bound", null);
                                }
                                values.add(IndexFingerprint.from(higher));
                            }
                        }
                        return page.continuation().isPresent()
                                ? scanPolicyIndexes(
                                        checkpoint,
                                        view,
                                        page.continuation(),
                                        values)
                                : CompletableFuture.completedFuture(null);
                    });
        }

        private boolean isCurrentPolicy(MaterializationCheckpointRecord checkpoint) {
            boolean sameIdentity = checkpoint.policyId().equals(currentPolicy.policyId())
                    && checkpoint.policyVersion() == currentPolicy.policyVersion();
            if (sameIdentity
                    && !checkpoint.policySha256().equals(
                            currentPolicy.digestSha256().value())) {
                throw invariant(
                        "current policy checkpoint identity collided with another digest", null);
            }
            return sameIdentity;
        }

        private CompletableFuture<TaskRetirement> retireTask(
                VersionedMaterializationTask expected,
                int deleteAttempt) {
            if (!isTerminal(expected.value().lifecycle()) || !auditGraceElapsed(expected.value())) {
                return CompletableFuture.completedFuture(TaskRetirement.NOT_ELIGIBLE);
            }
            return prove(expected, false).thenCompose(initialOptional -> {
                if (initialOptional.isEmpty()) {
                    return CompletableFuture.completedFuture(TaskRetirement.NOT_ELIGIBLE);
                }
                TaskProof initial = initialOptional.orElseThrow();
                return releaseAt(initial, 0).thenCompose(released ->
                        prove(expected, true).thenCompose(finalOptional -> {
                            if (finalOptional.isEmpty()) {
                                return CompletableFuture.completedFuture(
                                        new TaskRetirement(true, false, released));
                            }
                            TaskProof exact = finalOptional.orElseThrow();
                            if (!sameRoots(initial.roots(), exact.roots())) {
                                return CompletableFuture.completedFuture(
                                        new TaskRetirement(true, false, released));
                            }
                            return deadline.bound(
                                            mutationGuard::revalidate,
                                            "revalidate terminal metadata retirement activation")
                                    .thenCompose(ignored -> deleteTask(
                                            exact.task(), deleteAttempt, released));
                        }));
            });
        }

        private CompletableFuture<TaskRetirement> deleteTask(
                VersionedMaterializationTask expected,
                int attempt,
                int released) {
            if (attempt >= MAX_DELETE_RECOVERY_ATTEMPTS) {
                return CompletableFuture.failedFuture(invariant(
                        "terminal task delete recovery exhausted", null));
            }
            return deadline.bound(
                            () -> generations.deleteTask(
                                    cluster,
                                    streamId,
                                    expected.value().taskId(),
                                    expected.metadataVersion()),
                            "delete terminal materialization task")
                    .handle((ignored, failure) -> {
                        if (failure == null) {
                            return CompletableFuture.completedFuture(
                                    new TaskRetirement(true, true, released));
                        }
                        Throwable original = unwrap(failure);
                        return deadline.bound(
                                        () -> generations.getTask(
                                                cluster,
                                                streamId,
                                                expected.value().taskId()),
                                        "reload terminal task after delete response loss")
                                .thenCompose(reloaded -> {
                                    if (reloaded.isEmpty()) {
                                        return CompletableFuture.completedFuture(
                                                new TaskRetirement(true, true, released));
                                    }
                                    VersionedMaterializationTask actual = reloaded.orElseThrow();
                                    if (!sameTask(expected, actual)) {
                                        return CompletableFuture.failedFuture(original);
                                    }
                                    return retireTask(actual, attempt + 1)
                                            .thenApply(retry -> new TaskRetirement(
                                                    true,
                                                    retry.retired(),
                                                    Math.addExact(
                                                            released,
                                                            retry.protectionsReleased())));
                                });
                    })
                    .thenCompose(value -> value);
        }

        private CompletableFuture<Optional<TaskProof>> prove(
                VersionedMaterializationTask expected,
                boolean requireNoTemporaryProtections) {
            return deadline.bound(
                            () -> generations.getTask(
                                    cluster, streamId, expected.value().taskId()),
                            "reload terminal materialization task")
                    .thenCompose(actualOptional -> {
                        if (actualOptional.isEmpty()
                                || !sameTask(expected, actualOptional.orElseThrow())) {
                            return CompletableFuture.completedFuture(Optional.empty());
                        }
                        VersionedMaterializationTask actual = actualOptional.orElseThrow();
                        if (!isTerminal(actual.value().lifecycle())
                                || !auditGraceElapsed(actual.value())) {
                            return CompletableFuture.completedFuture(Optional.empty());
                        }
                        MaterializationTask task = tasks.requireTask(actual);
                        Optional<MaterializationOutput> output = actual.value().output()
                                .map(value -> MaterializationRecordMapper.domainOutput(task, value));
                        List<ObjectExpectation> expectations = expectations(task, output);
                        return loadRoots(expectations, 0, new LinkedHashMap<>())
                                .thenCompose(rootsOptional -> {
                                    if (rootsOptional.isEmpty()) {
                                        return CompletableFuture.completedFuture(Optional.empty());
                                    }
                                    Map<ObjectKeyHash, VersionedPhysicalObjectRoot> roots =
                                            rootsOptional.orElseThrow();
                                    if (actual.value().lifecycle() == TaskLifecycle.PUBLISHED) {
                                        return provePublished(
                                                actual,
                                                task,
                                                output.orElseThrow(),
                                                roots,
                                                requireNoTemporaryProtections);
                                    }
                                    return proveFailedTerminal(
                                            actual,
                                            task,
                                            output,
                                            roots,
                                            requireNoTemporaryProtections);
                                });
                    });
        }

        private CompletableFuture<Optional<TaskProof>> provePublished(
                VersionedMaterializationTask durable,
                MaterializationTask task,
                MaterializationOutput output,
                Map<ObjectKeyHash, VersionedPhysicalObjectRoot> roots,
                boolean requireNoTemporaryProtections) {
            long generation = durable.value().allocatedGeneration().orElseThrow();
            GenerationIndexIdentity identity = new GenerationIndexIdentity(
                    task.streamId(), task.view(), task.coverage().endOffset(), generation);
            CompletableFuture<Optional<VersionedGenerationIndex>> indexFuture = deadline.bound(
                    () -> generations.getIndex(cluster, identity),
                    "reload terminal task generation index");
            CompletableFuture<Optional<VersionedMaterializationCheckpoint>> checkpointFuture = deadline.bound(
                    () -> generations.getMaterializationCheckpoint(
                            cluster,
                            task.streamId(),
                            task.policy().policyId(),
                            task.policy().policyVersion()),
                    "reload terminal task materialization checkpoint");
            return indexFuture.thenCombine(checkpointFuture, Pair::new).thenCompose(pair -> {
                if (pair.left().isEmpty() || pair.right().isEmpty()) {
                    return CompletableFuture.completedFuture(Optional.empty());
                }
                VersionedGenerationIndex index = pair.left().orElseThrow();
                VersionedMaterializationCheckpoint checkpoint = pair.right().orElseThrow();
                if (!matchesPublishedIndex(durable, task, output, index)
                        || !checkpointCovers(task, checkpoint.value())) {
                    return CompletableFuture.completedFuture(Optional.empty());
                }
                return auditProtections(
                                durable,
                                task,
                                Optional.of(output),
                                roots,
                                Optional.of(index))
                        .thenApply(audit -> {
                            if (!audit.valid()
                                    || requireNoTemporaryProtections
                                            && !audit.temporary().isEmpty()) {
                                return Optional.empty();
                            }
                            return Optional.of(new TaskProof(
                                    durable,
                                    Optional.of(index),
                                    Optional.of(checkpoint),
                                    roots,
                                    audit.temporary()));
                        });
            });
        }

        private CompletableFuture<Optional<TaskProof>> proveFailedTerminal(
                VersionedMaterializationTask durable,
                MaterializationTask task,
                Optional<MaterializationOutput> output,
                Map<ObjectKeyHash, VersionedPhysicalObjectRoot> roots,
                boolean requireNoTemporaryProtections) {
            return scanTaskIndexes(task, Optional.empty(), new ArrayList<>())
                    .thenCompose(indexes -> {
                        if (indexes.stream().anyMatch(index -> referencesTerminalTask(
                                durable, output, index))) {
                            return CompletableFuture.completedFuture(Optional.empty());
                        }
                        return auditProtections(
                                        durable,
                                        task,
                                        output,
                                        roots,
                                        Optional.empty())
                                .thenApply(audit -> {
                                    if (!audit.valid()
                                            || requireNoTemporaryProtections
                                                    && !audit.temporary().isEmpty()) {
                                        return Optional.empty();
                                    }
                                    return Optional.of(new TaskProof(
                                            durable,
                                            Optional.empty(),
                                            Optional.empty(),
                                            roots,
                                            audit.temporary()));
                                });
                    });
        }

        private CompletableFuture<List<VersionedGenerationIndex>> scanTaskIndexes(
                MaterializationTask task,
                Optional<F4ScanToken> continuation,
                List<VersionedGenerationIndex> values) {
            return deadline.bound(
                            () -> generations.scanIndex(
                                    cluster,
                                    task.streamId(),
                                    task.view(),
                                    task.coverage().endOffset(),
                                    task.coverage().endOffset(),
                                    continuation,
                                    pageSize),
                            "scan terminal task publication indexes")
                    .thenCompose(page -> {
                        addHigherIndexes(page, values);
                        return page.continuation().isPresent()
                                ? scanTaskIndexes(task, page.continuation(), values)
                                : CompletableFuture.completedFuture(values);
                    });
        }

        private void addHigherIndexes(
                GenerationScanPage page,
                List<VersionedGenerationIndex> values) {
            for (var candidate : page.values()) {
                if (candidate instanceof VersionedGenerationIndex higher) {
                    if (values.size() == MAX_SCAN_ENTRIES) {
                        throw invariant(
                                "terminal task index audit exceeded its hard scan bound", null);
                    }
                    values.add(higher);
                }
            }
        }

        private CompletableFuture<ProtectionAudit> auditProtections(
                VersionedMaterializationTask durable,
                MaterializationTask task,
                Optional<MaterializationOutput> output,
                Map<ObjectKeyHash, VersionedPhysicalObjectRoot> roots,
                Optional<VersionedGenerationIndex> visibleIndex) {
            Map<ProtectionKey, ProtectionRole> expected = expectedProtections(task, output, visibleIndex);
            List<VersionedObjectProtection> all = new ArrayList<>();
            List<ObjectKeyHash> objects = List.copyOf(roots.keySet());
            return scanObjectProtections(objects, 0, Optional.empty(), all)
                    .thenApply(ignored -> classifyProtections(
                            durable, roots, expected, visibleIndex, all));
        }

        private CompletableFuture<Void> scanObjectProtections(
                List<ObjectKeyHash> objects,
                int objectIndex,
                Optional<F4ScanToken> continuation,
                List<VersionedObjectProtection> values) {
            if (objectIndex == objects.size()) {
                return CompletableFuture.completedFuture(null);
            }
            ObjectKeyHash object = objects.get(objectIndex);
            return deadline.bound(
                            () -> physical.scanProtections(
                                    cluster, object, continuation, pageSize),
                            "scan terminal task object protections")
                    .thenCompose(page -> {
                        if (values.size() > MAX_SCAN_ENTRIES - page.values().size()) {
                            throw invariant(
                                    "terminal protection audit exceeded its hard scan bound", null);
                        }
                        values.addAll(page.values());
                        if (page.continuation().isPresent()) {
                            return scanObjectProtections(
                                    objects,
                                    objectIndex,
                                    page.continuation(),
                                    values);
                        }
                        return scanObjectProtections(
                                objects, objectIndex + 1, Optional.empty(), values);
                    });
        }

        private ProtectionAudit classifyProtections(
                VersionedMaterializationTask durable,
                Map<ObjectKeyHash, VersionedPhysicalObjectRoot> roots,
                Map<ProtectionKey, ProtectionRole> expected,
                Optional<VersionedGenerationIndex> visibleIndex,
                List<VersionedObjectProtection> values) {
            boolean visibleFound = visibleIndex.isEmpty();
            List<VersionedObjectProtection> temporary = new ArrayList<>();
            for (VersionedObjectProtection protection : values) {
                ObjectProtectionRecord record = protection.value();
                ObjectKeyHash object = new ObjectKeyHash(record.objectKeyHash());
                VersionedPhysicalObjectRoot root = roots.get(object);
                if (root == null) {
                    throw invariant("protection scan returned another physical object", null);
                }
                ProtectionKey key = protectionKey(record);
                ProtectionRole role = expected.get(key);
                boolean taskOwned = record.ownerKey().equals(durable.key());
                if (role == ProtectionRole.VISIBLE) {
                    ObjectProtectionOwner owner = MaterializationProtectionIdentities.indexOwner(
                            visibleIndex.orElseThrow());
                    if (!matchesOwner(record, owner)
                            || record.rootLifecycleEpoch() != root.value().lifecycleEpoch()) {
                        return ProtectionAudit.INVALID;
                    }
                    visibleFound = true;
                    continue;
                }
                if (role == ProtectionRole.TEMPORARY) {
                    if (!taskOwned
                            || record.ownerMetadataVersion() > durable.metadataVersion()
                            || record.rootLifecycleEpoch() != root.value().lifecycleEpoch()) {
                        return ProtectionAudit.INVALID;
                    }
                    temporary.add(protection);
                    continue;
                }
                if (taskOwned) {
                    throw invariant(
                            "terminal task owns an unexpected object protection", null);
                }
            }
            return visibleFound
                    ? new ProtectionAudit(true, List.copyOf(temporary))
                    : ProtectionAudit.INVALID;
        }

        private CompletableFuture<Integer> releaseAt(TaskProof proof, int index) {
            if (index == proof.temporaryProtections().size()) {
                return CompletableFuture.completedFuture(index);
            }
            VersionedObjectProtection protection = proof.temporaryProtections().get(index);
            return revalidateTaskAndRoot(proof.task(), proof.roots(), protection)
                    .thenCompose(ignored -> deleteProtection(protection, 0))
                    .thenCompose(ignored -> releaseAt(proof, index + 1));
        }

        private CompletableFuture<Void> revalidateTaskAndRoot(
                VersionedMaterializationTask expected,
                Map<ObjectKeyHash, VersionedPhysicalObjectRoot> roots,
                VersionedObjectProtection protection) {
            ObjectKeyHash object = new ObjectKeyHash(protection.value().objectKeyHash());
            VersionedPhysicalObjectRoot expectedRoot = roots.get(object);
            CompletableFuture<Optional<VersionedMaterializationTask>> taskFuture = deadline.bound(
                    () -> generations.getTask(
                            cluster, streamId, expected.value().taskId()),
                    "revalidate terminal task before protection release");
            CompletableFuture<Optional<VersionedPhysicalObjectRoot>> rootFuture = deadline.bound(
                    () -> physical.getRoot(cluster, object),
                    "revalidate terminal task physical root before protection release");
            return taskFuture.thenCombine(rootFuture, Pair::new).thenAccept(pair -> {
                if (pair.left().isEmpty()
                        || !sameTask(expected, pair.left().orElseThrow())
                        || pair.right().isEmpty()
                        || !sameRoot(expectedRoot, pair.right().orElseThrow())) {
                    throw condition(
                            "terminal task/root changed before protection release", null);
                }
            });
        }

        private CompletableFuture<Void> deleteProtection(
                VersionedObjectProtection expected,
                int attempt) {
            if (attempt >= MAX_DELETE_RECOVERY_ATTEMPTS) {
                return CompletableFuture.failedFuture(invariant(
                        "terminal protection delete recovery exhausted", null));
            }
            ObjectProtectionIdentity identity = protectionIdentity(expected.value());
            return deadline.bound(
                            () -> physical.deleteProtection(
                                    cluster, identity, expected.metadataVersion()),
                            "delete terminal task-owned protection")
                    .handle((ignored, failure) -> {
                        if (failure == null) {
                            return CompletableFuture.<Void>completedFuture(null);
                        }
                        Throwable original = unwrap(failure);
                        return findProtection(identity, Optional.empty())
                                .thenCompose(actual -> {
                                    if (actual.isEmpty()) {
                                        return CompletableFuture.completedFuture(null);
                                    }
                                    if (!sameProtection(expected, actual.orElseThrow())) {
                                        return CompletableFuture.failedFuture(original);
                                    }
                                    return deleteProtection(actual.orElseThrow(), attempt + 1);
                                });
                    })
                    .thenCompose(value -> value);
        }

        private CompletableFuture<Optional<VersionedObjectProtection>> findProtection(
                ObjectProtectionIdentity identity,
                Optional<F4ScanToken> continuation) {
            return deadline.bound(
                            () -> physical.scanProtections(
                                    cluster, identity.object(), continuation, pageSize),
                            "reload terminal task-owned protection")
                    .thenCompose(page -> {
                        Optional<VersionedObjectProtection> found = page.values().stream()
                                .filter(value -> protectionIdentity(value.value()).equals(identity))
                                .findFirst();
                        if (found.isPresent() || page.continuation().isEmpty()) {
                            return CompletableFuture.completedFuture(found);
                        }
                        return findProtection(identity, page.continuation());
                    });
        }

        private CompletableFuture<Optional<Map<ObjectKeyHash, VersionedPhysicalObjectRoot>>> loadRoots(
                List<ObjectExpectation> expectations,
                int index,
                LinkedHashMap<ObjectKeyHash, VersionedPhysicalObjectRoot> roots) {
            if (index == expectations.size()) {
                return CompletableFuture.completedFuture(Optional.of(Map.copyOf(roots)));
            }
            ObjectExpectation expectation = expectations.get(index);
            VersionedPhysicalObjectRoot existing = roots.get(expectation.object());
            if (existing != null) {
                if (!expectation.matches(existing)) {
                    throw invariant("one task carries conflicting physical object expectations", null);
                }
                return loadRoots(expectations, index + 1, roots);
            }
            return deadline.bound(
                            () -> physical.getRoot(cluster, expectation.object()),
                            "reload terminal task physical root")
                    .thenCompose(optional -> {
                        if (optional.isEmpty()) {
                            return CompletableFuture.completedFuture(Optional.empty());
                        }
                        VersionedPhysicalObjectRoot root = optional.orElseThrow();
                        if (root.value().lifecycle() != PhysicalObjectLifecycle.ACTIVE) {
                            return CompletableFuture.completedFuture(Optional.empty());
                        }
                        if (!expectation.matches(root)) {
                            throw invariant(
                                    "terminal task target conflicts with physical root", null);
                        }
                        roots.put(expectation.object(), root);
                        return loadRoots(expectations, index + 1, roots);
                    });
        }

        @Override
        public void close() {
            deadline.close();
        }
    }

    private static List<ObjectExpectation> expectations(
            MaterializationTask task,
            Optional<MaterializationOutput> output) {
        List<ObjectExpectation> values = new ArrayList<>();
        for (SourceGeneration source : task.sources()) {
            if (!(source.readTarget() instanceof ObjectSliceReadTarget target)) {
                throw new NereusException(
                        ErrorCode.UNSUPPORTED_READ_TARGET,
                        false,
                        "terminal materialization source is not an object slice");
            }
            long requiredEnd;
            try {
                requiredEnd = Math.addExact(target.objectOffset(), target.objectLength());
            } catch (ArithmeticException overflow) {
                throw invariant("terminal materialization source range overflows", overflow);
            }
            PhysicalObjectKind kind = target.objectType() == ObjectType.MULTI_STREAM_WAL_OBJECT
                    ? PhysicalObjectKind.OBJECT_WAL
                    : source.view() == ReadView.COMMITTED
                            ? PhysicalObjectKind.COMMITTED_COMPACTED
                            : PhysicalObjectKind.TOPIC_COMPACTED;
            values.add(ObjectExpectation.source(
                    target.objectKey(), target.objectId(), kind, requiredEnd));
        }
        output.ifPresent(value -> values.add(ObjectExpectation.exact(
                MaterializationRecordMapper.physicalIdentity(value))));
        return List.copyOf(values);
    }

    private Map<ProtectionKey, ProtectionRole> expectedProtections(
            MaterializationTask task,
            Optional<MaterializationOutput> output,
            Optional<VersionedGenerationIndex> visibleIndex) {
        Map<ProtectionKey, ProtectionRole> result = new LinkedHashMap<>();
        for (SourceGeneration source : task.sources()) {
            ObjectSliceReadTarget target = (ObjectSliceReadTarget) source.readTarget();
            putExpected(
                    result,
                    new ProtectionKey(
                            ObjectKeyHash.from(target.objectKey()),
                            ObjectProtectionType.MATERIALIZATION_SOURCE,
                            MaterializationProtectionIdentities.sourceReferenceId(
                                    cluster, task, source)),
                    ProtectionRole.TEMPORARY);
        }
        if (output.isPresent()) {
            MaterializationOutput value = output.orElseThrow();
            putExpected(
                    result,
                    new ProtectionKey(
                            value.objectKeyHash(),
                            ObjectProtectionType.MATERIALIZATION_OUTPUT,
                            MaterializationProtectionIdentities.outputReferenceId(
                                    cluster, task, value)),
                    ProtectionRole.TEMPORARY);
        }
        if (visibleIndex.isPresent()) {
            VersionedGenerationIndex index = visibleIndex.orElseThrow();
            MaterializationOutput value = output.orElseThrow();
            putExpected(
                    result,
                    new ProtectionKey(
                            value.objectKeyHash(),
                            ObjectProtectionType.VISIBLE_GENERATION,
                            MaterializationProtectionIdentities.visibleReferenceId(
                                    cluster,
                                    task,
                                    index.value().generation(),
                                    index.value().publicationId())),
                    ProtectionRole.VISIBLE);
        }
        return Map.copyOf(result);
    }

    private static void putExpected(
            Map<ProtectionKey, ProtectionRole> values,
            ProtectionKey key,
            ProtectionRole role) {
        ProtectionRole existing = values.putIfAbsent(key, role);
        if (existing != null && existing != role) {
            throw invariant("one protection identity has conflicting terminal roles", null);
        }
    }

    private static boolean matchesPublishedIndex(
            VersionedMaterializationTask durable,
            MaterializationTask task,
            MaterializationOutput output,
            VersionedGenerationIndex index) {
        GenerationIndexRecord value = index.value();
        MaterializationTaskRecord taskValue = durable.value();
        var encodedTarget = ReadTargetCodecRegistry.phase15().encode(output.readTarget());
        return value.lifecycle() == GenerationLifecycle.COMMITTED
                && value.streamId().equals(task.streamId().value())
                && value.readViewId() == task.view().wireId()
                && value.offsetStart() == task.coverage().startOffset()
                && value.offsetEnd() == task.coverage().endOffset()
                && value.generation() == taskValue.allocatedGeneration().orElseThrow()
                && value.publicationId().equals(taskValue.publicationId())
                && value.taskId().equals(task.taskId())
                && value.sourceSetSha256().equals(task.sourceSetSha256().value())
                && value.policySha256().equals(task.policyDigestSha256().value())
                && value.readTarget().equals(encodedTarget)
                && value.targetIdentitySha256().equals(output.targetIdentitySha256().value())
                && value.materializationPolicySha256().equals(task.policyDigestSha256().value())
                && value.payloadFormat().equals(output.logicalFormat())
                && value.sourceRecordCount() == output.sourceRecordCount()
                && value.outputRecordCount() == output.outputRecordCount()
                && value.entryCount() == output.entryCount()
                && value.logicalBytes() == output.logicalBytes()
                && value.cumulativeSizeAtStart() == output.cumulativeSizeAtStart()
                && value.cumulativeSizeAtEnd() == output.cumulativeSizeAtEnd()
                && value.firstCommitVersion() == task.sources().get(0).commitVersion()
                && value.lastCommitVersion() == task.taskSequence()
                && value.schemaRefs().equals(output.schemaRefs())
                && value.projectionRef().equals(
                        MaterializationRecordMapper.projectionIdentity(output.projectionRef()));
    }

    private static boolean checkpointCovers(
            MaterializationTask task,
            MaterializationCheckpointRecord checkpoint) {
        return checkpoint.streamId().equals(task.streamId().value())
                && checkpoint.policyId().equals(task.policy().policyId())
                && checkpoint.policyVersion() == task.policy().policyVersion()
                && checkpoint.policySha256().equals(task.policyDigestSha256().value())
                && checkpoint.contiguousCoveredOffset() >= task.coverage().endOffset()
                && checkpoint.observedCommitVersion() >= task.taskSequence()
                && checkpoint.lastTaskSequence() >= task.taskSequence();
    }

    private static boolean referencesTerminalTask(
            VersionedMaterializationTask task,
            Optional<MaterializationOutput> output,
            VersionedGenerationIndex index) {
        GenerationIndexRecord value = index.value();
        if (value.lifecycle() != GenerationLifecycle.PREPARED
                && value.lifecycle() != GenerationLifecycle.COMMITTED) {
            return false;
        }
        return value.taskId().equals(task.value().taskId())
                || output.isPresent()
                        && (value.targetIdentitySha256().equals(
                                        output.orElseThrow().targetIdentitySha256().value())
                                || value.readTarget().equals(ReadTargetCodecRegistry.phase15()
                                        .encode(output.orElseThrow().readTarget())));
    }

    private static boolean matchesStatsSource(
            RangeRetentionStatsRecord stats,
            VersionedGenerationCandidate source) {
        return source.key().equals(stats.sourceIndexKey())
                && source.metadataVersion() == stats.sourceIndexMetadataVersion()
                && source.durableValueSha256().value().equals(
                        stats.sourceIndexIdentitySha256());
    }

    private static boolean matchesOwner(
            ObjectProtectionRecord record,
            ObjectProtectionOwner owner) {
        return record.ownerKey().equals(owner.ownerKey())
                && record.ownerMetadataVersion() == owner.metadataVersion()
                && record.ownerIdentitySha256().equals(owner.identitySha256().value());
    }

    private boolean auditGraceElapsed(MaterializationTaskRecord task) {
        return auditGraceElapsed(task.updatedAtMillis());
    }

    private boolean auditGraceElapsed(long updatedAtMillis) {
        long deadlineMillis;
        try {
            deadlineMillis = Math.addExact(updatedAtMillis, metadataAuditGraceMillis);
        } catch (ArithmeticException overflow) {
            return false;
        }
        return deadlineMillis < clock.millis();
    }

    private static boolean isTerminal(TaskLifecycle lifecycle) {
        return lifecycle == TaskLifecycle.PUBLISHED
                || lifecycle == TaskLifecycle.CANCELLED
                || lifecycle == TaskLifecycle.TERMINAL_FAILED;
    }

    private static boolean sameTask(
            VersionedMaterializationTask left,
            VersionedMaterializationTask right) {
        return left.key().equals(right.key())
                && left.metadataVersion() == right.metadataVersion()
                && left.durableValueSha256().equals(right.durableValueSha256())
                && left.value().equals(right.value());
    }

    private static boolean sameRoots(
            Map<ObjectKeyHash, VersionedPhysicalObjectRoot> left,
            Map<ObjectKeyHash, VersionedPhysicalObjectRoot> right) {
        return left.size() == right.size()
                && left.entrySet().stream().allMatch(entry -> {
                    VersionedPhysicalObjectRoot other = right.get(entry.getKey());
                    return other != null && sameRoot(entry.getValue(), other);
                });
    }

    private static boolean sameRoot(
            VersionedPhysicalObjectRoot left,
            VersionedPhysicalObjectRoot right) {
        return left.key().equals(right.key())
                && left.metadataVersion() == right.metadataVersion()
                && left.durableValueSha256().equals(right.durableValueSha256())
                && left.value().equals(right.value());
    }

    private static boolean sameProtection(
            VersionedObjectProtection left,
            VersionedObjectProtection right) {
        return left.key().equals(right.key())
                && left.metadataVersion() == right.metadataVersion()
                && left.durableValueSha256().equals(right.durableValueSha256())
                && left.value().equals(right.value());
    }

    private static boolean sameStats(
            VersionedRangeRetentionStats left,
            VersionedRangeRetentionStats right) {
        return left.key().equals(right.key())
                && left.metadataVersion() == right.metadataVersion()
                && left.durableValueSha256().equals(right.durableValueSha256())
                && left.value().equals(right.value());
    }

    private static boolean sameCheckpoint(
            VersionedMaterializationCheckpoint left,
            VersionedMaterializationCheckpoint right) {
        return left.key().equals(right.key())
                && left.metadataVersion() == right.metadataVersion()
                && left.durableValueSha256().equals(right.durableValueSha256())
                && left.value().equals(right.value());
    }

    private static ProtectionKey protectionKey(ObjectProtectionRecord value) {
        return new ProtectionKey(
                new ObjectKeyHash(value.objectKeyHash()),
                ObjectProtectionType.fromWireId(value.protectionTypeId()),
                value.referenceId());
    }

    private static ObjectProtectionIdentity protectionIdentity(ObjectProtectionRecord value) {
        ProtectionKey key = protectionKey(value);
        return new ObjectProtectionIdentity(key.object(), key.type(), key.referenceId());
    }

    private static Duration requirePositive(Duration value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(field + " must be positive");
        }
        return value;
    }

    private static long requirePositiveMillis(Duration value, String field) {
        requirePositive(value, field);
        try {
            long millis = value.toMillis();
            if (millis <= 0) {
                throw new IllegalArgumentException(field + " must be at least one millisecond");
            }
            return millis;
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException(field + " is too large", overflow);
        }
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
        while ((current instanceof java.util.concurrent.CompletionException
                        || current instanceof java.util.concurrent.ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static NereusException invariant(String message, Throwable cause) {
        return new NereusException(
                ErrorCode.METADATA_INVARIANT_VIOLATION, false, message, cause);
    }

    private static NereusException condition(String message, Throwable cause) {
        return new NereusException(ErrorCode.METADATA_CONDITION_FAILED, true, message, cause);
    }

    private record Pair<L, R>(L left, R right) {
    }

    private record OldPolicySnapshot(
            boolean retirable,
            List<TaskFingerprint> tasks,
            List<IndexFingerprint> indexes) {
        private OldPolicySnapshot {
            tasks = List.copyOf(Objects.requireNonNull(tasks, "tasks"));
            indexes = List.copyOf(Objects.requireNonNull(indexes, "indexes"));
        }
    }

    private record TaskFingerprint(
            String key,
            long metadataVersion,
            String durableValueSha256) {
        private static TaskFingerprint from(VersionedMaterializationTask task) {
            return new TaskFingerprint(
                    task.key(), task.metadataVersion(), task.durableValueSha256().value());
        }
    }

    private record IndexFingerprint(
            String key,
            long metadataVersion,
            String durableValueSha256,
            GenerationLifecycle lifecycle) {
        private static IndexFingerprint from(VersionedGenerationIndex index) {
            return new IndexFingerprint(
                    index.key(),
                    index.metadataVersion(),
                    index.durableValueSha256().value(),
                    index.value().lifecycle());
        }
    }

    private record TaskProof(
            VersionedMaterializationTask task,
            Optional<VersionedGenerationIndex> index,
            Optional<VersionedMaterializationCheckpoint> checkpoint,
            Map<ObjectKeyHash, VersionedPhysicalObjectRoot> roots,
            List<VersionedObjectProtection> temporaryProtections) {
        private TaskProof {
            Objects.requireNonNull(task, "task");
            index = Objects.requireNonNull(index, "index");
            checkpoint = Objects.requireNonNull(checkpoint, "checkpoint");
            roots = Map.copyOf(Objects.requireNonNull(roots, "roots"));
            temporaryProtections = List.copyOf(Objects.requireNonNull(
                    temporaryProtections, "temporaryProtections"));
        }
    }

    private record TaskRetirement(boolean eligible, boolean retired, int protectionsReleased) {
        private static final TaskRetirement NOT_ELIGIBLE = new TaskRetirement(false, false, 0);

        private TaskRetirement {
            if (protectionsReleased < 0 || retired && !eligible) {
                throw new IllegalArgumentException("task retirement result is invalid");
            }
        }
    }

    private record ProtectionAudit(
            boolean valid,
            List<VersionedObjectProtection> temporary) {
        private static final ProtectionAudit INVALID = new ProtectionAudit(false, List.of());

        private ProtectionAudit {
            temporary = List.copyOf(Objects.requireNonNull(temporary, "temporary"));
            if (!valid && !temporary.isEmpty()) {
                throw new IllegalArgumentException(
                        "invalid protection audit cannot carry cleanup handles");
            }
        }
    }

    private enum ProtectionRole {
        TEMPORARY,
        VISIBLE
    }

    private record ProtectionKey(
            ObjectKeyHash object,
            ObjectProtectionType type,
            String referenceId) {
        private ProtectionKey {
            Objects.requireNonNull(object, "object");
            Objects.requireNonNull(type, "type");
            referenceId = requireText(referenceId, "referenceId");
        }
    }

    private record ObjectExpectation(
            ObjectKeyHash object,
            ObjectKey objectKey,
            ObjectId objectId,
            PhysicalObjectKind kind,
            long requiredEnd,
            Optional<PhysicalObjectIdentity> exact) {
        private ObjectExpectation {
            Objects.requireNonNull(object, "object");
            Objects.requireNonNull(objectKey, "objectKey");
            Objects.requireNonNull(objectId, "objectId");
            Objects.requireNonNull(kind, "kind");
            if (requiredEnd <= 0) {
                throw new IllegalArgumentException("requiredEnd must be positive");
            }
            exact = Objects.requireNonNull(exact, "exact");
        }

        private static ObjectExpectation source(
                ObjectKey key,
                ObjectId id,
                PhysicalObjectKind kind,
                long requiredEnd) {
            return new ObjectExpectation(
                    ObjectKeyHash.from(key), key, id, kind, requiredEnd, Optional.empty());
        }

        private static ObjectExpectation exact(PhysicalObjectIdentity identity) {
            return new ObjectExpectation(
                    identity.objectKeyHash(),
                    identity.objectKey(),
                    identity.objectId().orElseThrow(),
                    identity.kind(),
                    identity.objectLength(),
                    Optional.of(identity));
        }

        private boolean matches(VersionedPhysicalObjectRoot root) {
            PhysicalObjectIdentity actual = PhysicalObjectIdentity.from(root.value());
            if (exact.isPresent()) {
                return actual.equals(exact.orElseThrow());
            }
            return actual.objectKeyHash().equals(object)
                    && actual.objectKey().equals(objectKey)
                    && actual.objectId().isPresent()
                    && actual.objectId().orElseThrow().equals(objectId)
                    && actual.kind() == kind
                    && actual.objectLength() >= requiredEnd;
        }
    }

    private static final class Accumulator {
        private int tasksScanned;
        private int tasksEligible;
        private int tasksRetired;
        private int protectionsReleased;
        private int retentionStatsScanned;
        private int retentionStatsRetired;
        private int checkpointsScanned;
        private int checkpointsRetired;

        private TerminalWorkflowMetadataRetirementResult result() {
            return new TerminalWorkflowMetadataRetirementResult(
                    tasksScanned,
                    tasksEligible,
                    tasksRetired,
                    protectionsReleased,
                    retentionStatsScanned,
                    retentionStatsRetired,
                    checkpointsScanned,
                    checkpointsRetired);
        }
    }
}
