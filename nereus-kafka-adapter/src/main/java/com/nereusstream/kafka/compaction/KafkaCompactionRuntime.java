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
 * Runtime bridge from a fork-owned partition snapshot to recoverable product partition passes.
 *
 * <p>The Kafka fork remains the authority for partition-lock/KRaft/local-log capture. This bridge
 * accepts only bounded immutable registrations, orders internal topics first, and rechecks each
 * registration against the process-current product storage immediately before creating its pass.
 * Leader churn is a normal skip; a pass that already acquired its capture still performs its own
 * durable authority revalidation.
 */
public final class KafkaCompactionRuntime implements KafkaRuntimeBackgroundService {
    private static final Comparator<OwnedPartition> PARTITION_ORDER = Comparator.comparing(OwnedPartition::workClass)
            .thenComparing(partition -> partition.identity().kafkaClusterId())
            .thenComparing(partition -> partition.identity().topicId())
            .thenComparingInt(partition -> partition.identity().partition())
            .thenComparing(partition -> partition.identity().observedTopicName());

    private final KafkaPartitionStorageManager partitions;
    private final OwnedPartitionSource ownedPartitions;
    private final PartitionPassFactory passFactory;
    private final int maxConcurrentPartitions;
    private final int maxPartitionsPerPass;
    private final Executor callbackExecutor;
    private final KafkaCompactionScheduler scheduler;

    public KafkaCompactionRuntime(
            KafkaPartitionStorageManager partitions,
            OwnedPartitionSource ownedPartitions,
            PartitionPassFactory passFactory,
            Duration interval,
            int maxConcurrentPartitions,
            int maxPartitionsPerPass,
            ScheduledExecutorService scheduler,
            Executor callbackExecutor) {
        this.partitions = Objects.requireNonNull(partitions, "partitions");
        this.ownedPartitions = Objects.requireNonNull(ownedPartitions, "ownedPartitions");
        this.passFactory = Objects.requireNonNull(passFactory, "passFactory");
        this.maxConcurrentPartitions = requireBounded(maxConcurrentPartitions, 1, 256, "maxConcurrentPartitions");
        this.maxPartitionsPerPass = requireBounded(maxPartitionsPerPass, 1, 100_000, "maxPartitionsPerPass");
        this.callbackExecutor = Objects.requireNonNull(callbackExecutor, "callbackExecutor");
        this.scheduler = new KafkaCompactionScheduler(
                this::runProcessPass,
                Objects.requireNonNull(interval, "interval"),
                Objects.requireNonNull(scheduler, "scheduler"),
                callbackExecutor);
    }

    @Override
    public CompletableFuture<Void> start() {
        return scheduler.start();
    }

    public CompletableFuture<Void> trigger(KafkaCompactionScheduler.Trigger trigger) {
        return scheduler.trigger(Objects.requireNonNull(trigger, "trigger"));
    }

    public boolean isRunning() {
        return scheduler.isRunning();
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return scheduler.closeAsync();
    }

    private CompletableFuture<Void> runProcessPass(KafkaCompactionScheduler.TriggerBatch triggers) {
        NonCancellableFuture<Void> result = new NonCancellableFuture<>();
        CompletableFuture<List<OwnedPartition>> snapshot;
        try {
            snapshot = toFuture(Objects.requireNonNull(
                    ownedPartitions.snapshot(triggers, maxPartitionsPerPass), "Kafka owned-partition snapshot future"));
        } catch (Throwable failure) {
            result.completeExceptionally(unwrap(failure));
            return result;
        }
        snapshot.whenComplete((registrations, failure) -> executeCallback(() -> {
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
                new ArrayList<>(Objects.requireNonNull(registrations, "Kafka owned-partition snapshot"));
        if (exact.size() > maxPartitionsPerPass) {
            throw new NereusException(
                    ErrorCode.METADATA_LIMIT_EXCEEDED,
                    false,
                    "Kafka owned-partition snapshot exceeded the configured pass bound");
        }
        Set<KafkaPartitionId> identities = new HashSet<>();
        for (OwnedPartition partition : exact) {
            OwnedPartition registration = Objects.requireNonNull(partition, "Kafka owned-partition registration");
            if (!identities.add(registration.identity().durableId())) {
                throw new NereusException(
                        ErrorCode.METADATA_INVARIANT_VIOLATION,
                        false,
                        "Kafka owned-partition snapshot contains a duplicate identity");
            }
        }
        exact.sort(PARTITION_ORDER);
        return List.copyOf(exact);
    }

    private CompletableFuture<KafkaCompactionPartitionPass.RunResult> launch(OwnedPartition registration) {
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
                    invariant("Kafka partition manager returned a mismatched current identity"));
        }
        if (storage.leaderEpoch() != registration.leaderEpoch()
                || storage.state() != KafkaPartitionState.LEADER_WRITABLE) {
            return CompletableFuture.completedFuture(null);
        }
        try {
            PartitionPass pass = Objects.requireNonNull(
                    passFactory.create(registration), "Kafka partition-pass factory returned null");
            return Objects.requireNonNull(
                    pass.runOnce(registration.identity().durableId()), "Kafka compaction partition pass returned null");
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

    private static <T> CompletableFuture<T> toFuture(java.util.concurrent.CompletionStage<T> stage) {
        CompletableFuture<T> result = new CompletableFuture<>();
        stage.whenComplete((value, failure) -> {
            if (failure == null) {
                result.complete(value);
            } else {
                result.completeExceptionally(unwrap(failure));
            }
        });
        return result;
    }

    private static int requireBounded(int value, int minimum, int maximum, String field) {
        if (value < minimum || value > maximum) {
            throw new IllegalArgumentException(field + " must be in [" + minimum + ", " + maximum + "]");
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
        java.util.concurrent.CompletionStage<List<OwnedPartition>> snapshot(
                KafkaCompactionScheduler.TriggerBatch triggers, int maximumPartitions);
    }

    @FunctionalInterface
    public interface PartitionPassFactory {
        PartitionPass create(OwnedPartition partition);
    }

    @FunctionalInterface
    public interface PartitionPass {
        CompletableFuture<KafkaCompactionPartitionPass.RunResult> runOnce(KafkaPartitionId partition);
    }

    /**
     * Immutable fork-to-product registration captured while the fork still owns its partition.
     */
    public record OwnedPartition(
            KafkaPartitionIdentity identity,
            int leaderEpoch,
            WorkClass workClass,
            KafkaCompactionPartitionPass.CaptureProvider captureProvider) {
        public OwnedPartition {
            Objects.requireNonNull(identity, "identity");
            Objects.requireNonNull(workClass, "workClass");
            Objects.requireNonNull(captureProvider, "captureProvider");
            if (leaderEpoch < 0) {
                throw new IllegalArgumentException("leaderEpoch must be non-negative");
            }
        }
    }

    /**
     * Internal topics sort first but remain subject to the same global concurrency bound.
     */
    public enum WorkClass {
        INTERNAL,
        USER
    }

    private final class ProcessPass {
        private final List<OwnedPartition> registrations;
        private final NonCancellableFuture<Void> result;
        private final ConcurrentLinkedQueue<PartitionCompletion> completions = new ConcurrentLinkedQueue<>();
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
                while (!completed && active < maxConcurrentPartitions && nextIndex < registrations.size()) {
                    int index = nextIndex++;
                    OwnedPartition registration = registrations.get(index);
                    active++;
                    CompletableFuture<KafkaCompactionPartitionPass.RunResult> operation = launch(registration);
                    operation.whenComplete((ignored, failure) -> executeCallback(() -> complete(index, failure)));
                }
                if (!completed && nextIndex == registrations.size() && active == 0) {
                    completed = true;
                    completeResult();
                }
                missed = workInProgress.addAndGet(-missed);
            } while (missed != 0);
        }

        private void completeResult() {
            List<Throwable> exactFailures =
                    failures.stream().filter(Objects::nonNull).toList();
            if (exactFailures.isEmpty()) {
                result.complete(null);
                return;
            }
            NereusException aggregate = new NereusException(
                    ErrorCode.METADATA_UNAVAILABLE,
                    true,
                    "Kafka compaction process pass failed for " + exactFailures.size() + " owned partition(s)",
                    exactFailures.get(0));
            for (int index = 1; index < exactFailures.size(); index++) {
                aggregate.addSuppressed(exactFailures.get(index));
            }
            result.completeExceptionally(aggregate);
        }
    }

    private record PartitionCompletion(int index, Throwable failure) {}

    /**
     * Scheduler cancellation starts drain but cannot report completion before accepted work exits.
     */
    private static final class NonCancellableFuture<T> extends CompletableFuture<T> {
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }
    }
}
