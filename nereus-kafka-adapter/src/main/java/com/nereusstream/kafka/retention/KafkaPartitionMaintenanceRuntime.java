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

package com.nereusstream.kafka.retention;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.kafka.compaction.KafkaCompactionScheduler;
import com.nereusstream.kafka.partition.KafkaPartitionIdentity;
import com.nereusstream.kafka.partition.KafkaPartitionState;
import com.nereusstream.kafka.partition.KafkaPartitionStorage;
import com.nereusstream.kafka.partition.KafkaPartitionStorageManager;
import com.nereusstream.kafka.runtime.KafkaRuntimeBackgroundService;
import com.nereusstream.metadata.oxia.KafkaPartitionId;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Process owner that periodically runs checkpoint-before-trim retention for current Kafka leaders.
 *
 * <p>The fork snapshots its current leader set and supplies authority-fenced hooks. This runtime
 * validates that each registration still matches the process-current product storage immediately
 * before launch, bounds cross-partition concurrency, and drains all accepted operations before the
 * partition manager can close.
 */
public final class KafkaPartitionMaintenanceRuntime implements KafkaRuntimeBackgroundService {
  private static final Comparator<OwnedPartition> PARTITION_ORDER =
      Comparator.comparing((OwnedPartition partition) -> partition.identity().kafkaClusterId())
          .thenComparing(partition -> partition.identity().topicId())
          .thenComparingInt(partition -> partition.identity().partition())
          .thenComparing(partition -> partition.identity().observedTopicName());

  private final KafkaPartitionStorageManager partitions;
  private final OwnedPartitionSource ownedPartitions;
  private final int maxConcurrentPartitions;
  private final int maxPartitionsPerPass;
  private final Executor callbackExecutor;
  private final KafkaCompactionScheduler scheduler;

  public KafkaPartitionMaintenanceRuntime(
      KafkaPartitionStorageManager partitions,
      OwnedPartitionSource ownedPartitions,
      Duration interval,
      int maxConcurrentPartitions,
      int maxPartitionsPerPass,
      ScheduledExecutorService scheduler,
      Executor callbackExecutor) {
    this.partitions = Objects.requireNonNull(partitions, "partitions");
    this.ownedPartitions = Objects.requireNonNull(ownedPartitions, "ownedPartitions");
    this.maxConcurrentPartitions =
        bounded(maxConcurrentPartitions, 1, 256, "maxConcurrentPartitions");
    this.maxPartitionsPerPass =
        bounded(maxPartitionsPerPass, 1, 100_000, "maxPartitionsPerPass");
    if (maxPartitionsPerPass < maxConcurrentPartitions) {
      throw new IllegalArgumentException(
          "maxPartitionsPerPass must be at least maxConcurrentPartitions");
    }
    this.callbackExecutor = Objects.requireNonNull(callbackExecutor, "callbackExecutor");
    this.scheduler =
        new KafkaCompactionScheduler(
            ignored -> runProcessPass(),
            Objects.requireNonNull(interval, "interval"),
            Objects.requireNonNull(scheduler, "scheduler"),
            callbackExecutor);
  }

  @Override
  public CompletableFuture<Void> start() {
    return scheduler.start();
  }

  /** Requests one coalesced administrative pass; primarily used by lifecycle and deterministic tests. */
  public CompletableFuture<Void> trigger() {
    return scheduler.trigger(KafkaCompactionScheduler.Trigger.ADMIN);
  }

  public boolean isRunning() {
    return scheduler.isRunning();
  }

  @Override
  public CompletableFuture<Void> closeAsync() {
    return scheduler.closeAsync();
  }

  private CompletableFuture<Void> runProcessPass() {
    NonCancellableFuture<Void> result = new NonCancellableFuture<>();
    CompletableFuture<List<OwnedPartition>> snapshot;
    try {
      snapshot =
          toFuture(
              Objects.requireNonNull(
                  ownedPartitions.snapshot(maxPartitionsPerPass),
                  "Kafka maintenance owned-partition snapshot future"));
    } catch (Throwable failure) {
      result.completeExceptionally(unwrap(failure));
      return result;
    }
    snapshot.whenComplete(
        (registrations, failure) ->
            executeCallback(
                () -> {
                  if (failure != null) {
                    result.completeExceptionally(unwrap(failure));
                    return;
                  }
                  List<OwnedPartition> ordered;
                  try {
                    ordered = validateAndOrder(registrations);
                  } catch (Throwable invalid) {
                    result.completeExceptionally(unwrap(invalid));
                    return;
                  }
                  new ProcessPass(ordered, result).start();
                }));
    return result;
  }

  private List<OwnedPartition> validateAndOrder(List<OwnedPartition> registrations) {
    List<OwnedPartition> exact =
        new ArrayList<>(Objects.requireNonNull(registrations, "owned partition snapshot"));
    if (exact.size() > maxPartitionsPerPass) {
      throw new NereusException(
          ErrorCode.METADATA_LIMIT_EXCEEDED,
          false,
          "Kafka maintenance owned-partition snapshot exceeded its pass bound");
    }
    Set<KafkaPartitionId> identities = new HashSet<>();
    for (OwnedPartition partition : exact) {
      OwnedPartition registration = Objects.requireNonNull(partition, "owned partition");
      if (!identities.add(registration.identity().durableId())) {
        throw invariant("Kafka maintenance snapshot contains a duplicate partition identity");
      }
    }
    exact.sort(PARTITION_ORDER);
    return List.copyOf(exact);
  }

  private CompletableFuture<KafkaRetentionCoordinator.RunResult> launch(
      OwnedPartition registration) {
    Optional<KafkaPartitionStorage> current;
    try {
      current = partitions.current(registration.identity());
    } catch (Throwable failure) {
      return CompletableFuture.failedFuture(unwrap(failure));
    }
    if (current.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }
    KafkaPartitionStorage storage = current.orElseThrow();
    if (!storage.identity().equals(registration.identity())) {
      return CompletableFuture.failedFuture(
          invariant("Kafka partition manager returned a mismatched maintenance identity"));
    }
    if (storage.leaderEpoch() != registration.leaderEpoch()
        || storage.state() != KafkaPartitionState.LEADER_WRITABLE) {
      return CompletableFuture.completedFuture(null);
    }
    KafkaPartitionMaintenance maintenance =
        storage
            .maintenance()
            .orElseThrow(
                () ->
                    invariant(
                        "Writable Kafka partition has no configured maintenance state machine"));
    try {
      return Objects.requireNonNull(
          maintenance.runRetention(registration.hooks()),
          "Kafka partition retention future");
    } catch (Throwable failure) {
      return CompletableFuture.failedFuture(unwrap(failure));
    }
  }

  private void executeCallback(Runnable callback) {
    try {
      callbackExecutor.execute(callback);
    } catch (RejectedExecutionException rejected) {
      callback.run();
    }
  }

  private static <T> CompletableFuture<T> toFuture(
      java.util.concurrent.CompletionStage<T> stage) {
    CompletableFuture<T> result = new CompletableFuture<>();
    stage.whenComplete(
        (value, failure) -> {
          if (failure == null) {
            result.complete(value);
          } else {
            result.completeExceptionally(unwrap(failure));
          }
        });
    return result;
  }

  private static int bounded(int value, int minimum, int maximum, String field) {
    if (value < minimum || value > maximum) {
      throw new IllegalArgumentException(
          field + " must be in [" + minimum + ", " + maximum + "]");
    }
    return value;
  }

  private static NereusException invariant(String message) {
    return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
  }

  private static Throwable unwrap(Throwable failure) {
    Throwable current = Objects.requireNonNull(failure, "failure");
    while (current instanceof CompletionException && current.getCause() != null) {
      current = current.getCause();
    }
    return current;
  }

  @FunctionalInterface
  public interface OwnedPartitionSource {
    java.util.concurrent.CompletionStage<List<OwnedPartition>> snapshot(int maximumPartitions);
  }

  /** Immutable fork registration for one current leader and its partition-lock hooks. */
  public record OwnedPartition(
      KafkaPartitionIdentity identity, int leaderEpoch, KafkaPartitionMaintenance.Hooks hooks) {
    public OwnedPartition {
      Objects.requireNonNull(identity, "identity");
      Objects.requireNonNull(hooks, "hooks");
      if (leaderEpoch < 0) {
        throw new IllegalArgumentException("leaderEpoch must be non-negative");
      }
    }
  }

  private final class ProcessPass {
    private final List<OwnedPartition> registrations;
    private final NonCancellableFuture<Void> result;
    private final ConcurrentLinkedQueue<PartitionCompletion> completions =
        new ConcurrentLinkedQueue<>();
    private final AtomicInteger workInProgress = new AtomicInteger();
    private final List<Throwable> failures;
    private int nextIndex;
    private int active;
    private boolean completed;

    private ProcessPass(List<OwnedPartition> registrations, NonCancellableFuture<Void> result) {
      this.registrations = registrations;
      this.result = result;
      this.failures = new ArrayList<>(registrations.size());
      for (int index = 0; index < registrations.size(); index++) {
        failures.add(null);
      }
    }

    private void start() {
      drain();
    }

    private void complete(int index, Throwable failure) {
      completions.add(new PartitionCompletion(index, failure));
      drain();
    }

    private void drain() {
      if (workInProgress.getAndIncrement() != 0) {
        return;
      }
      int missed = 1;
      do {
        PartitionCompletion completion;
        while ((completion = completions.poll()) != null) {
          active--;
          if (completion.failure() != null) {
            failures.set(completion.index(), unwrap(completion.failure()));
          }
        }
        while (!completed
            && active < maxConcurrentPartitions
            && nextIndex < registrations.size()) {
          int index = nextIndex++;
          OwnedPartition registration = registrations.get(index);
          active++;
          CompletableFuture<KafkaRetentionCoordinator.RunResult> operation =
              launch(registration);
          operation.whenComplete(
              (ignored, failure) -> executeCallback(() -> complete(index, failure)));
        }
        if (!completed && nextIndex == registrations.size() && active == 0) {
          completed = true;
          completeResult();
        }
        missed = workInProgress.addAndGet(-missed);
      } while (missed != 0);
    }

    private void completeResult() {
      List<Throwable> exactFailures = failures.stream().filter(Objects::nonNull).toList();
      if (exactFailures.isEmpty()) {
        result.complete(null);
        return;
      }
      NereusException aggregate =
          new NereusException(
              ErrorCode.METADATA_UNAVAILABLE,
              true,
              "Kafka maintenance pass failed for "
                  + exactFailures.size()
                  + " owned partition(s)",
              exactFailures.get(0));
      for (int index = 1; index < exactFailures.size(); index++) {
        aggregate.addSuppressed(exactFailures.get(index));
      }
      result.completeExceptionally(aggregate);
    }
  }

  private record PartitionCompletion(int index, Throwable failure) {}

  private static final class NonCancellableFuture<T> extends CompletableFuture<T> {
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return false;
    }
  }
}
