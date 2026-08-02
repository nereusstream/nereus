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

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * One-partition retention orchestration with coalesced concurrent scheduler triggers.
 */
public final class KafkaRetentionCoordinator {
    private final KafkaTrimBarrier.SnapshotLoader snapshots;
    private final KafkaRetentionPlanner planner;
    private final KafkaTrimBarrier barrier;
    private final AtomicReference<CompletableFuture<RunResult>> inFlight = new AtomicReference<>();

    public KafkaRetentionCoordinator(
            KafkaTrimBarrier.SnapshotLoader snapshots, KafkaRetentionPlanner planner, KafkaTrimBarrier barrier) {
        this.snapshots = Objects.requireNonNull(snapshots, "snapshots");
        this.planner = Objects.requireNonNull(planner, "planner");
        this.barrier = Objects.requireNonNull(barrier, "barrier");
    }

    public CompletableFuture<RunResult> runOnce() {
        for (; ; ) {
            CompletableFuture<RunResult> existing = inFlight.get();
            if (existing != null) {
                return view(existing);
            }
            CompletableFuture<RunResult> owner = new CompletableFuture<>();
            if (!inFlight.compareAndSet(null, owner)) {
                continue;
            }
            execute().whenComplete((value, failure) -> {
                if (failure == null) {
                    owner.complete(value);
                } else {
                    owner.completeExceptionally(unwrap(failure));
                }
                inFlight.compareAndSet(owner, null);
            });
            return view(owner);
        }
    }

    private CompletableFuture<RunResult> execute() {
        CompletableFuture<KafkaTrimBarrier.Snapshot> loaded;
        try {
            loaded = Objects.requireNonNull(
                    snapshots.loadCurrent(), "Kafka retention snapshot loader returned a null future");
        } catch (Throwable failure) {
            loaded = CompletableFuture.failedFuture(failure);
        }
        return loaded.thenCompose(snapshot -> {
            KafkaRetentionPlanner.Plan plan = planner.plan(snapshot.retention());
            if (!plan.shouldTrim()) {
                return CompletableFuture.completedFuture(new RunResult(plan, Optional.empty()));
            }
            return barrier.advance(snapshot, plan).thenApply(result -> new RunResult(plan, Optional.of(result)));
        });
    }

    private static CompletableFuture<RunResult> view(CompletableFuture<RunResult> operation) {
        return operation.thenApply(result -> result);
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    public record RunResult(KafkaRetentionPlanner.Plan plan, Optional<KafkaTrimBarrier.Result> trimResult) {
        public RunResult {
            Objects.requireNonNull(plan, "plan");
            trimResult = Objects.requireNonNull(trimResult, "trimResult");
            if (plan.shouldTrim() != trimResult.isPresent()) {
                throw new IllegalArgumentException("Kafka retention run result does not match its plan");
            }
        }
    }
}
