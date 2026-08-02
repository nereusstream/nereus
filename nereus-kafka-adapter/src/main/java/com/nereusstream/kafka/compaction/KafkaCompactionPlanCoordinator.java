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

package com.nereusstream.kafka.compaction;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.kafka.compaction.KafkaCompactionPassOneCollector.Snapshot;
import com.nereusstream.kafka.compaction.KafkaCompactionPlanner.Candidate;
import com.nereusstream.materialization.ExactSourceSet;
import com.nereusstream.materialization.MaterializationTask;
import com.nereusstream.materialization.MaterializationTaskMutationGuard;
import com.nereusstream.materialization.MaterializationTaskStore;
import com.nereusstream.metadata.oxia.KafkaCompactionPlanMetadataStore;
import com.nereusstream.metadata.oxia.KafkaPartitionId;
import com.nereusstream.metadata.oxia.VersionedKafkaCompactionPlan;
import com.nereusstream.metadata.oxia.records.KafkaCompactionPlanRecord;
import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Converges the immutable KCP1 child before admitting its linked generic materialization task.
 *
 * <p>The two roots can live on different Oxia partitions, so this class deliberately avoids an
 * atomicity claim. Plan-first ordering makes an orphan plan harmless, while a visible task always
 * passed an immediate reread of its exact plan child.
 */
public final class KafkaCompactionPlanCoordinator {
    private final KafkaCompactionPlanMetadataStore plans;
    private final TaskRoots tasks;
    private final KafkaCompactionPlanRecordMapper mapper;
    private final Clock clock;

    public KafkaCompactionPlanCoordinator(
            KafkaCompactionPlanMetadataStore plans, MaterializationTaskStore tasks, Clock clock) {
        this(
                plans,
                (task, guard) -> Objects.requireNonNull(tasks, "tasks")
                        .create(task, guard)
                        .thenApply(tasks::requireTask),
                new KafkaCompactionPlanRecordMapper(),
                clock);
    }

    KafkaCompactionPlanCoordinator(
            KafkaCompactionPlanMetadataStore plans,
            TaskRoots tasks,
            KafkaCompactionPlanRecordMapper mapper,
            Clock clock) {
        this.plans = Objects.requireNonNull(plans, "plans");
        this.tasks = Objects.requireNonNull(tasks, "tasks");
        this.mapper = Objects.requireNonNull(mapper, "mapper");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    public CompletableFuture<Converged> converge(
            KafkaPartitionId partition,
            MaterializationTask outputTask,
            long bindingMetadataVersion,
            long lastStableOffset,
            long highWatermark,
            Candidate candidate,
            ExactSourceSet decisionSources,
            Snapshot passOneSnapshot,
            MaterializationTaskMutationGuard authorityGuard) {
        try {
            KafkaPartitionId exactPartition = Objects.requireNonNull(partition, "partition");
            MaterializationTask exactTask = Objects.requireNonNull(outputTask, "outputTask");
            MaterializationTaskMutationGuard exactGuard = Objects.requireNonNull(authorityGuard, "authorityGuard");
            KafkaCompactionPlan plan = KafkaCompactionPlan.create(
                    exactTask,
                    bindingMetadataVersion,
                    lastStableOffset,
                    highWatermark,
                    candidate,
                    decisionSources,
                    passOneSnapshot);
            KafkaCompactionPlanRecord requested = mapper.toRecord(exactPartition, plan, clock.millis());
            return revalidate(exactGuard)
                    .thenCompose(ignored -> plans.putCompactionPlanIfAbsent(requested))
                    .thenCompose(durablePlan -> tasks.create(exactTask, () -> revalidate(exactGuard)
                                    .thenCompose(ignored -> requirePersistedPlan(
                                            exactPartition, requested,
                                            plan, durablePlan)))
                            .thenApply(durableTask -> {
                                if (!durableTask.equals(exactTask)) {
                                    throw invariant("Kafka compaction task create returned another " + "task");
                                }
                                return new Converged(plan, durablePlan, durableTask);
                            }));
        } catch (RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    public CompletableFuture<Recovered> recover(KafkaPartitionId partition, MaterializationTask outputTask) {
        try {
            KafkaPartitionId exactPartition = Objects.requireNonNull(partition, "partition");
            MaterializationTask exactTask = Objects.requireNonNull(outputTask, "outputTask");
            return plans.getCompactionPlan(exactPartition, exactTask.taskId()).thenApply(optional -> {
                VersionedKafkaCompactionPlan durable = optional.orElseThrow(() -> new NereusException(
                        ErrorCode.METADATA_CONDITION_FAILED,
                        true,
                        "Kafka compaction task has no durable KCP1 " + "attachment"));
                KafkaCompactionPlan plan = mapper.fromRecord(durable.value());
                plan.requireMaterializationTask(exactTask);
                return new Recovered(plan, durable, exactTask);
            });
        } catch (RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private CompletableFuture<Void> requirePersistedPlan(
            KafkaPartitionId partition,
            KafkaCompactionPlanRecord requested,
            KafkaCompactionPlan expectedPlan,
            VersionedKafkaCompactionPlan expectedDurable) {
        return plans.getCompactionPlan(partition, requested.materializationTaskId())
                .thenAccept(optional -> {
                    VersionedKafkaCompactionPlan current = optional.orElseThrow(
                            () -> invariant("Kafka compaction plan disappeared before task creation"));
                    if (!current.equals(expectedDurable)
                            || !current.value().withMetadataVersion(0).equals(requested)
                            || !mapper.fromRecord(current.value()).equals(expectedPlan)) {
                        throw invariant("Kafka compaction plan changed before task creation");
                    }
                });
    }

    private static CompletableFuture<Void> revalidate(MaterializationTaskMutationGuard guard) {
        try {
            return Objects.requireNonNull(guard.revalidate(), "Kafka compaction authority guard future");
        } catch (RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private static NereusException invariant(String message) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }

    @FunctionalInterface
    interface TaskRoots {
        CompletableFuture<MaterializationTask> create(MaterializationTask task, MaterializationTaskMutationGuard guard);
    }

    public record Converged(
            KafkaCompactionPlan plan, VersionedKafkaCompactionPlan durablePlan, MaterializationTask outputTask) {
        public Converged {
            Objects.requireNonNull(plan, "plan");
            Objects.requireNonNull(durablePlan, "durablePlan");
            Objects.requireNonNull(outputTask, "outputTask");
            plan.requireMaterializationTask(outputTask);
        }
    }

    public record Recovered(
            KafkaCompactionPlan plan, VersionedKafkaCompactionPlan durablePlan, MaterializationTask outputTask) {
        public Recovered {
            Objects.requireNonNull(plan, "plan");
            Objects.requireNonNull(durablePlan, "durablePlan");
            Objects.requireNonNull(outputTask, "outputTask");
            plan.requireMaterializationTask(outputTask);
        }
    }
}
