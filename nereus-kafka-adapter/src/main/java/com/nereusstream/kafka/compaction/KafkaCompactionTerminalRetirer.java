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
import com.nereusstream.api.StreamId;
import com.nereusstream.materialization.MaterializationTask;
import com.nereusstream.materialization.MaterializationTaskMutationGuard;
import com.nereusstream.materialization.MaterializationTaskStore;
import com.nereusstream.metadata.oxia.KafkaCompactionPlanMetadataStore;
import com.nereusstream.metadata.oxia.KafkaPartitionId;
import com.nereusstream.metadata.oxia.VersionedKafkaCompactionPlan;
import com.nereusstream.metadata.oxia.VersionedMaterializationTask;
import com.nereusstream.metadata.oxia.records.TaskLifecycle;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Retires one terminal materialization task and its immutable KCP1 attachment.
 *
 * <p>The task root is always deleted first. Only after an exact reload proves the task absent does
 * this class conditionally delete KCP1. Therefore every state exposed during cleanup is either both
 * roots present or a harmless plan-only orphan; it never deliberately exposes a task without its
 * restart image. The supplied guard must represent a stable authority fence that also rejects
 * concurrent task admission for this partition.
 */
public final class KafkaCompactionTerminalRetirer {
  private static final int MAX_DELETE_RECOVERY_ATTEMPTS = 8;

  private final KafkaCompactionPlanMetadataStore plans;
  private final TaskRoots tasks;
  private final KafkaCompactionPlanRecordMapper mapper;

  public KafkaCompactionTerminalRetirer(
      KafkaCompactionPlanMetadataStore plans, MaterializationTaskStore tasks) {
    this(
        plans,
        new TaskRoots() {
          private final MaterializationTaskStore delegate = Objects.requireNonNull(tasks, "tasks");

          @Override
          public CompletableFuture<Optional<VersionedMaterializationTask>> get(
              StreamId streamId, String taskId) {
            return delegate.get(streamId, taskId);
          }

          @Override
          public CompletableFuture<Void> delete(VersionedMaterializationTask expected) {
            return delegate.delete(expected);
          }

          @Override
          public MaterializationTask requireTask(VersionedMaterializationTask durable) {
            return delegate.requireTask(durable);
          }
        },
        new KafkaCompactionPlanRecordMapper());
  }

  KafkaCompactionTerminalRetirer(
      KafkaCompactionPlanMetadataStore plans,
      TaskRoots tasks,
      KafkaCompactionPlanRecordMapper mapper) {
    this.plans = Objects.requireNonNull(plans, "plans");
    this.tasks = Objects.requireNonNull(tasks, "tasks");
    this.mapper = Objects.requireNonNull(mapper, "mapper");
  }

  public CompletableFuture<RetirementResult> retire(
      KafkaPartitionId partition,
      VersionedMaterializationTask expectedTask,
      VersionedKafkaCompactionPlan expectedPlan,
      MaterializationTaskMutationGuard authorityGuard) {
    try {
      KafkaPartitionId exactPartition = Objects.requireNonNull(partition, "partition");
      VersionedMaterializationTask exactTaskRoot =
          Objects.requireNonNull(expectedTask, "expectedTask");
      MaterializationTask exactTask = requireExpectedTask(exactTaskRoot);
      VersionedKafkaCompactionPlan exactPlan = Objects.requireNonNull(expectedPlan, "expectedPlan");
      MaterializationTaskMutationGuard exactGuard =
          Objects.requireNonNull(authorityGuard, "authorityGuard");
      requireExpectedRoots(exactPartition, exactTask, exactPlan);
      return revalidate(exactGuard)
          .thenCompose(
              ignored ->
                  loadPlan(exactPartition, exactTask, exactPlan)
                      .thenCompose(
                          currentPlan ->
                              currentPlan.isEmpty()
                                  ? requireBothAbsent(exactTask)
                                  : retireTask(
                                      exactPartition,
                                      exactTask,
                                      exactTaskRoot,
                                      exactPlan,
                                      currentPlan.orElseThrow(),
                                      exactGuard)));
    } catch (Throwable failure) {
      return CompletableFuture.failedFuture(failure);
    }
  }

  private CompletableFuture<RetirementResult> requireBothAbsent(MaterializationTask expectedTask) {
    return getTask(expectedTask)
        .thenApply(
            task -> {
              if (task.isPresent()) {
                throw invariant(
                    "Kafka compaction task remains after its KCP1 attachment disappeared", null);
              }
              return new RetirementResult(false, false, 0, 0);
            });
  }

  private CompletableFuture<RetirementResult> retireTask(
      KafkaPartitionId partition,
      MaterializationTask expectedTask,
      VersionedMaterializationTask expectedTaskRoot,
      VersionedKafkaCompactionPlan expectedPlan,
      VersionedKafkaCompactionPlan currentPlan,
      MaterializationTaskMutationGuard authorityGuard) {
    requireSamePlan(expectedPlan, currentPlan);
    return getTask(expectedTask)
        .thenCompose(
            currentTask -> {
              if (currentTask.isEmpty()) {
                return retirePlan(
                    partition, expectedTask, expectedPlan, authorityGuard, false, 0, 0);
              }
              VersionedMaterializationTask exactTask = currentTask.orElseThrow();
              if (!exactTask.equals(expectedTaskRoot)) {
                throw invariant("Kafka compaction terminal task changed before retirement", null);
              }
              return deleteTask(expectedTask, exactTask, 0)
                  .thenCompose(
                      attempts ->
                          retirePlan(
                              partition,
                              expectedTask,
                              expectedPlan,
                              authorityGuard,
                              true,
                              attempts,
                              0));
            });
  }

  private CompletableFuture<Integer> deleteTask(
      MaterializationTask expectedTask, VersionedMaterializationTask expected, int attempt) {
    if (attempt >= MAX_DELETE_RECOVERY_ATTEMPTS) {
      return CompletableFuture.failedFuture(
          invariant("Kafka compaction terminal task delete recovery exhausted", null));
    }
    return deleteTaskRoot(expected)
        .handle(
            (ignored, failure) -> {
              if (failure == null) {
                return CompletableFuture.completedFuture(attempt + 1);
              }
              Throwable original = unwrap(failure);
              return getTask(expectedTask)
                  .thenCompose(
                      reloaded -> {
                        if (reloaded.isEmpty()) {
                          return CompletableFuture.completedFuture(attempt + 1);
                        }
                        VersionedMaterializationTask actual = reloaded.orElseThrow();
                        if (!actual.equals(expected)) {
                          return CompletableFuture.failedFuture(
                              invariant(
                                  "Kafka compaction terminal task changed after uncertain delete",
                                  original));
                        }
                        return deleteTask(expectedTask, actual, attempt + 1);
                      });
            })
        .thenCompose(value -> value);
  }

  private CompletableFuture<RetirementResult> retirePlan(
      KafkaPartitionId partition,
      MaterializationTask expectedTask,
      VersionedKafkaCompactionPlan expectedPlan,
      MaterializationTaskMutationGuard authorityGuard,
      boolean taskWasPresent,
      int taskDeleteAttempts,
      int planDeleteAttempt) {
    if (planDeleteAttempt >= MAX_DELETE_RECOVERY_ATTEMPTS) {
      return CompletableFuture.failedFuture(
          invariant("Kafka compaction plan delete recovery exhausted", null));
    }
    return revalidate(authorityGuard)
        .thenCompose(ignored -> requireTaskAbsent(expectedTask))
        .thenCompose(ignored -> loadPlan(partition, expectedTask, expectedPlan))
        .thenCompose(
            current -> {
              if (current.isEmpty()) {
                return CompletableFuture.completedFuture(
                    new RetirementResult(
                        taskWasPresent, true, taskDeleteAttempts, planDeleteAttempt));
              }
              VersionedKafkaCompactionPlan exact = current.orElseThrow();
              return deletePlanRoot(exact)
                  .handle(
                      (ignored, failure) -> {
                        if (failure == null) {
                          return CompletableFuture.completedFuture(
                              new RetirementResult(
                                  taskWasPresent, true, taskDeleteAttempts, planDeleteAttempt + 1));
                        }
                        Throwable original = unwrap(failure);
                        return plans
                            .getCompactionPlan(partition, expectedTask.taskId())
                            .thenCompose(
                                reloaded -> {
                                  if (reloaded.isEmpty()) {
                                    return CompletableFuture.completedFuture(
                                        new RetirementResult(
                                            taskWasPresent,
                                            true,
                                            taskDeleteAttempts,
                                            planDeleteAttempt + 1));
                                  }
                                  VersionedKafkaCompactionPlan actual = reloaded.orElseThrow();
                                  if (!actual.equals(exact)) {
                                    return CompletableFuture.failedFuture(
                                        invariant(
                                            "Kafka compaction plan changed after uncertain delete",
                                            original));
                                  }
                                  return retirePlan(
                                      partition,
                                      expectedTask,
                                      expectedPlan,
                                      authorityGuard,
                                      taskWasPresent,
                                      taskDeleteAttempts,
                                      planDeleteAttempt + 1);
                                });
                      })
                  .thenCompose(value -> value);
            });
  }

  private CompletableFuture<Void> requireTaskAbsent(MaterializationTask expectedTask) {
    return getTask(expectedTask)
        .thenAccept(
            current -> {
              if (current.isPresent()) {
                throw invariant("Kafka compaction task reappeared before KCP1 retirement", null);
              }
            });
  }

  private CompletableFuture<Optional<VersionedKafkaCompactionPlan>> loadPlan(
      KafkaPartitionId partition,
      MaterializationTask expectedTask,
      VersionedKafkaCompactionPlan expectedPlan) {
    return plans
        .getCompactionPlan(partition, expectedTask.taskId())
        .thenApply(
            current -> {
              current.ifPresent(value -> requireSamePlan(expectedPlan, value));
              return current;
            });
  }

  private CompletableFuture<Optional<VersionedMaterializationTask>> getTask(
      MaterializationTask expectedTask) {
    try {
      return Objects.requireNonNull(
          tasks.get(expectedTask.streamId(), expectedTask.taskId()),
          "Kafka compaction task get future");
    } catch (Throwable failure) {
      return CompletableFuture.failedFuture(failure);
    }
  }

  private CompletableFuture<Void> deleteTaskRoot(VersionedMaterializationTask expected) {
    try {
      return Objects.requireNonNull(tasks.delete(expected), "Kafka compaction task delete future");
    } catch (Throwable failure) {
      return CompletableFuture.failedFuture(failure);
    }
  }

  private CompletableFuture<Void> deletePlanRoot(VersionedKafkaCompactionPlan expected) {
    try {
      return Objects.requireNonNull(
          plans.deleteCompactionPlan(expected), "Kafka compaction plan delete future");
    } catch (Throwable failure) {
      return CompletableFuture.failedFuture(failure);
    }
  }

  private void requireExpectedRoots(
      KafkaPartitionId partition,
      MaterializationTask expectedTask,
      VersionedKafkaCompactionPlan expectedPlan) {
    if (!expectedPlan.value().identity().equals(partition)
        || expectedPlan.value().metadataVersion() != expectedPlan.metadataVersion()) {
      throw new IllegalArgumentException(
          "expected Kafka compaction plan does not match its partition/version wrapper");
    }
    KafkaCompactionPlan decoded = mapper.fromRecord(expectedPlan.value());
    decoded.requireMaterializationTask(expectedTask);
  }

  private MaterializationTask requireExpectedTask(VersionedMaterializationTask durable) {
    MaterializationTask task;
    try {
      task = tasks.requireTask(durable);
    } catch (RuntimeException failure) {
      throw invariant("Kafka compaction terminal task cannot be decoded", failure);
    }
    if (!isTerminal(durable.value().lifecycle())) {
      throw new NereusException(
          ErrorCode.METADATA_CONDITION_FAILED, false, "Kafka compaction task is not terminal");
    }
    return task;
  }

  private static boolean isTerminal(TaskLifecycle lifecycle) {
    return lifecycle == TaskLifecycle.PUBLISHED
        || lifecycle == TaskLifecycle.CANCELLED
        || lifecycle == TaskLifecycle.TERMINAL_FAILED;
  }

  private static void requireSamePlan(
      VersionedKafkaCompactionPlan expected, VersionedKafkaCompactionPlan actual) {
    if (!actual.equals(expected)) {
      throw invariant("Kafka compaction plan changed before terminal retirement", null);
    }
  }

  private static CompletableFuture<Void> revalidate(MaterializationTaskMutationGuard guard) {
    try {
      return Objects.requireNonNull(
          guard.revalidate(), "Kafka compaction terminal authority guard future");
    } catch (Throwable failure) {
      return CompletableFuture.failedFuture(failure);
    }
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
    return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message, cause);
  }

  interface TaskRoots {
    CompletableFuture<Optional<VersionedMaterializationTask>> get(StreamId streamId, String taskId);

    CompletableFuture<Void> delete(VersionedMaterializationTask expected);

    MaterializationTask requireTask(VersionedMaterializationTask durable);
  }

  /** Bounded convergence evidence for one exact dual-root retirement. */
  public record RetirementResult(
      boolean taskWasPresent,
      boolean planWasPresent,
      int taskDeleteAttempts,
      int planDeleteAttempts) {
    public RetirementResult {
      if (taskDeleteAttempts < 0
          || taskDeleteAttempts > MAX_DELETE_RECOVERY_ATTEMPTS
          || planDeleteAttempts < 0
          || planDeleteAttempts > MAX_DELETE_RECOVERY_ATTEMPTS
          || (!taskWasPresent && taskDeleteAttempts != 0)
          || (!planWasPresent && planDeleteAttempts != 0)) {
        throw new IllegalArgumentException("invalid Kafka compaction retirement accounting");
      }
    }
  }
}
