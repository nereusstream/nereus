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
import com.nereusstream.materialization.MaterializationTaskMutationGuard;
import com.nereusstream.materialization.MaterializationTaskStore;
import com.nereusstream.metadata.oxia.KafkaCompactionPlanMetadataStore;
import com.nereusstream.metadata.oxia.KafkaCompactionPlanScanPage;
import com.nereusstream.metadata.oxia.KafkaCompactionPlanScanToken;
import com.nereusstream.metadata.oxia.KafkaPartitionId;
import com.nereusstream.metadata.oxia.VersionedKafkaCompactionPlan;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Bounded scanner that removes KCP1 children left before materialization-task admission.
 *
 * <p>The supplied authority guard must be acquired from the same partition-serialized admission
 * domain as {@link KafkaCompactionPlanCoordinator}. It must remain valid for the whole returned
 * future and reject concurrent task admission. The grace period alone is not treated as a
 * synchronization mechanism.
 */
public final class KafkaCompactionPlanOrphanScanner {
    private final KafkaCompactionPlanMetadataStore plans;
    private final TaskRoots tasks;
    private final Clock clock;
    private final Duration orphanGrace;
    private final int pageSize;
    private final int maxPlansPerScan;

    public KafkaCompactionPlanOrphanScanner(
            KafkaCompactionPlanMetadataStore plans,
            MaterializationTaskStore tasks,
            Clock clock,
            Duration orphanGrace,
            int pageSize,
            int maxPlansPerScan) {
        this(plans, taskRoots(tasks), clock, orphanGrace, pageSize, maxPlansPerScan);
    }

    KafkaCompactionPlanOrphanScanner(
            KafkaCompactionPlanMetadataStore plans,
            TaskRoots tasks,
            Clock clock,
            Duration orphanGrace,
            int pageSize,
            int maxPlansPerScan) {
        this.plans = Objects.requireNonNull(plans, "plans");
        this.tasks = Objects.requireNonNull(tasks, "tasks");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.orphanGrace = requirePositive(orphanGrace, "orphanGrace");
        if (pageSize <= 0 || pageSize > 1_000) {
            throw new IllegalArgumentException("pageSize must be in [1, 1000]");
        }
        if (maxPlansPerScan <= 0 || maxPlansPerScan < pageSize) {
            throw new IllegalArgumentException("maxPlansPerScan must be at least pageSize");
        }
        this.pageSize = pageSize;
        this.maxPlansPerScan = maxPlansPerScan;
    }

    public CompletableFuture<ScanResult> scan(
            KafkaPartitionId partition,
            Optional<KafkaCompactionPlanScanToken> continuation,
            MaterializationTaskMutationGuard authorityGuard) {
        try {
            KafkaPartitionId exactPartition = Objects.requireNonNull(partition, "partition");
            Optional<KafkaCompactionPlanScanToken> exactContinuation =
                    Objects.requireNonNull(continuation, "continuation");
            MaterializationTaskMutationGuard exactGuard = Objects.requireNonNull(authorityGuard, "authorityGuard");
            Accumulator accumulator = new Accumulator(exactPartition, exactGuard, clock.millis(), exactContinuation);
            return revalidate(exactGuard)
                    .thenCompose(ignored -> scanPage(accumulator))
                    .thenApply(ignored -> accumulator.result());
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private CompletableFuture<Void> scanPage(Accumulator accumulator) {
        int remaining = maxPlansPerScan - accumulator.scanned;
        if (remaining == 0) {
            return CompletableFuture.completedFuture(null);
        }
        int limit = Math.min(pageSize, remaining);
        return scanPlans(accumulator.partition, accumulator.continuation, limit).thenCompose(page -> {
            accumulator.continuation = page.continuation();
            return processPlans(accumulator, page.plans(), 0)
                    .thenCompose(
                            ignored -> accumulator.continuation.isPresent() && accumulator.scanned < maxPlansPerScan
                                    ? scanPage(accumulator)
                                    : CompletableFuture.completedFuture(null));
        });
    }

    private CompletableFuture<Void> processPlans(
            Accumulator accumulator, List<VersionedKafkaCompactionPlan> page, int index) {
        if (index == page.size()) {
            return CompletableFuture.completedFuture(null);
        }
        return processPlan(accumulator, page.get(index))
                .thenCompose(ignored -> processPlans(accumulator, page, index + 1));
    }

    private CompletableFuture<Void> processPlan(Accumulator accumulator, VersionedKafkaCompactionPlan expected) {
        if (!expected.value().identity().equals(accumulator.partition)) {
            return CompletableFuture.failedFuture(invariant("Kafka compaction plan scan returned another partition"));
        }
        accumulator.scanned++;
        if (!eligibleByAge(expected, accumulator.scanNowMillis)) {
            accumulator.youngerThanGrace++;
            return CompletableFuture.completedFuture(null);
        }
        StreamId streamId = new StreamId(expected.value().streamId());
        String taskId = expected.value().materializationTaskId();
        return taskExists(streamId, taskId).thenCompose(exists -> {
            if (exists) {
                accumulator.taskRootPresent++;
                return CompletableFuture.completedFuture(null);
            }
            return revalidate(accumulator.authorityGuard).thenCompose(ignored -> getPlan(accumulator.partition, taskId)
                    .thenCompose(current -> {
                        if (current.isEmpty()) {
                            accumulator.disappeared++;
                            return CompletableFuture.completedFuture(null);
                        }
                        VersionedKafkaCompactionPlan exact = current.orElseThrow();
                        requireExact(expected, exact);
                        return taskExists(streamId, taskId).thenCompose(appeared -> {
                            if (appeared) {
                                accumulator.taskRootPresent++;
                                return CompletableFuture.completedFuture(null);
                            }
                            return revalidate(accumulator.authorityGuard)
                                    .thenCompose(ignoredAgain -> deleteAndReconcile(accumulator, exact));
                        });
                    }));
        });
    }

    private CompletableFuture<Void> deleteAndReconcile(Accumulator accumulator, VersionedKafkaCompactionPlan expected) {
        CompletableFuture<Void> deleted;
        try {
            deleted = Objects.requireNonNull(
                    plans.deleteCompactionPlan(expected), "Kafka compaction plan delete returned a null future");
        } catch (Throwable failure) {
            deleted = CompletableFuture.failedFuture(failure);
        }
        return deleted.<CompletableFuture<Void>>handle((ignored, failure) -> {
                    if (failure == null) {
                        accumulator.deleted++;
                        return CompletableFuture.<Void>completedFuture(null);
                    }
                    Throwable original = unwrap(failure);
                    return getPlan(expected.value().identity(), expected.value().materializationTaskId())
                            .thenCompose(current -> {
                                if (current.isEmpty()) {
                                    accumulator.deleted++;
                                    accumulator.reconciledDeleteResponses++;
                                    return CompletableFuture.completedFuture(null);
                                }
                                requireExact(expected, current.orElseThrow());
                                return CompletableFuture.failedFuture(original);
                            });
                })
                .thenCompose(value -> value);
    }

    private CompletableFuture<KafkaCompactionPlanScanPage> scanPlans(
            KafkaPartitionId partition, Optional<KafkaCompactionPlanScanToken> continuation, int limit) {
        try {
            return Objects.requireNonNull(
                    plans.scanCompactionPlans(partition, continuation, limit),
                    "Kafka compaction plan scan returned a null future");
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private CompletableFuture<Optional<VersionedKafkaCompactionPlan>> getPlan(
            KafkaPartitionId partition, String taskId) {
        try {
            return Objects.requireNonNull(
                    plans.getCompactionPlan(partition, taskId), "Kafka compaction plan get returned a null future");
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private CompletableFuture<Boolean> taskExists(StreamId streamId, String taskId) {
        try {
            return Objects.requireNonNull(
                    tasks.exists(streamId, taskId), "Kafka compaction task lookup returned a null future");
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private boolean eligibleByAge(VersionedKafkaCompactionPlan plan, long scanNowMillis) {
        long createdAtMillis = plan.value().createdAtMillis();
        long graceMillis = orphanGrace.toMillis();
        return createdAtMillis <= scanNowMillis && scanNowMillis - createdAtMillis >= graceMillis;
    }

    private static CompletableFuture<Void> revalidate(MaterializationTaskMutationGuard guard) {
        try {
            return Objects.requireNonNull(
                    guard.revalidate(), "Kafka compaction orphan authority guard returned a null future");
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private static void requireExact(VersionedKafkaCompactionPlan expected, VersionedKafkaCompactionPlan current) {
        if (!expected.equals(current)) {
            throw invariant("Kafka compaction plan changed during orphan retirement");
        }
    }

    private static Duration requirePositive(Duration value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isZero() || value.isNegative() || value.toMillis() == 0) {
            throw new IllegalArgumentException(field + " must be at least one millisecond");
        }
        return value;
    }

    private static TaskRoots taskRoots(MaterializationTaskStore tasks) {
        MaterializationTaskStore exact = Objects.requireNonNull(tasks, "tasks");
        return (streamId, taskId) -> exact.get(streamId, taskId).thenApply(Optional::isPresent);
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static NereusException invariant(String message) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }

    @FunctionalInterface
    interface TaskRoots {
        CompletableFuture<Boolean> exists(StreamId streamId, String taskId);
    }

    public record ScanResult(
            int scanned,
            int youngerThanGrace,
            int taskRootPresent,
            int disappeared,
            int deleted,
            int reconciledDeleteResponses,
            Optional<KafkaCompactionPlanScanToken> continuation) {
        public ScanResult {
            if (scanned < 0
                    || youngerThanGrace < 0
                    || taskRootPresent < 0
                    || disappeared < 0
                    || deleted < 0
                    || reconciledDeleteResponses < 0
                    || youngerThanGrace + taskRootPresent + disappeared + deleted != scanned
                    || reconciledDeleteResponses > deleted) {
                throw new IllegalArgumentException("invalid Kafka compaction orphan scan accounting");
            }
            continuation = Objects.requireNonNull(continuation, "continuation");
        }

        public boolean exhausted() {
            return continuation.isEmpty();
        }
    }

    private static final class Accumulator {
        private final KafkaPartitionId partition;
        private final MaterializationTaskMutationGuard authorityGuard;
        private final long scanNowMillis;
        private int scanned;
        private int youngerThanGrace;
        private int taskRootPresent;
        private int disappeared;
        private int deleted;
        private int reconciledDeleteResponses;
        private Optional<KafkaCompactionPlanScanToken> continuation;

        private Accumulator(
                KafkaPartitionId partition,
                MaterializationTaskMutationGuard authorityGuard,
                long scanNowMillis,
                Optional<KafkaCompactionPlanScanToken> continuation) {
            this.partition = partition;
            this.authorityGuard = authorityGuard;
            this.scanNowMillis = scanNowMillis;
            this.continuation = continuation;
        }

        private ScanResult result() {
            return new ScanResult(
                    scanned,
                    youngerThanGrace,
                    taskRootPresent,
                    disappeared,
                    deleted,
                    reconciledDeleteResponses,
                    continuation);
        }
    }
}
