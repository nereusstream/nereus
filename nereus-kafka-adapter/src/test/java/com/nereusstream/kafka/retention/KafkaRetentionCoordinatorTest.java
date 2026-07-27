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

import static com.nereusstream.kafka.retention.KafkaRetentionTestFixtures.binding;
import static com.nereusstream.kafka.retention.KafkaRetentionTestFixtures.checkpoint;
import static com.nereusstream.kafka.retention.KafkaRetentionTestFixtures.head;
import static com.nereusstream.kafka.retention.KafkaRetentionTestFixtures.retentionSnapshot;
import static com.nereusstream.kafka.retention.KafkaRetentionTestFixtures.snapshot;
import static com.nereusstream.kafka.retention.KafkaRetentionTestFixtures.verified;
import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.StreamStorage;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class KafkaRetentionCoordinatorTest {
  private final KafkaRetentionPlanner planner = new KafkaRetentionPlanner();

  @Test
  void returnsNoopWithoutEnteringCheckpointOrTrimBoundary() {
    AtomicInteger checkpointCalls = new AtomicInteger();
    var captured = snapshot(binding(), head(0), retentionSnapshot(-1, -1, 20, 5_000));
    KafkaTrimBarrier barrier =
        new KafkaTrimBarrier(
            planner,
            () -> CompletableFuture.failedFuture(new AssertionError("must not reload")),
            (snapshot, target) -> {
              checkpointCalls.incrementAndGet();
              return CompletableFuture.failedFuture(new AssertionError("must not checkpoint"));
            },
            storage(
                new AtomicReference<>(head(0)), target -> CompletableFuture.completedFuture(null)),
            Duration.ofSeconds(5),
            (snapshot, offset, checkpoint) -> CompletableFuture.completedFuture(null));
    KafkaRetentionCoordinator coordinator =
        new KafkaRetentionCoordinator(
            () -> CompletableFuture.completedFuture(captured), planner, barrier);

    KafkaRetentionCoordinator.RunResult result = coordinator.runOnce().join();

    assertThat(result.plan().shouldTrim()).isFalse();
    assertThat(result.trimResult()).isEmpty();
    assertThat(checkpointCalls).hasValue(0);
  }

  @Test
  void coalescesConcurrentTriggersAndCallerCancellationDoesNotCancelSharedRun() {
    CompletableFuture<KafkaTrimBarrier.Snapshot> load = new CompletableFuture<>();
    AtomicInteger loads = new AtomicInteger();
    KafkaTrimBarrier barrier =
        new KafkaTrimBarrier(
            planner,
            () -> CompletableFuture.failedFuture(new AssertionError("must not reload")),
            (snapshot, target) ->
                CompletableFuture.failedFuture(new AssertionError("must not checkpoint")),
            storage(
                new AtomicReference<>(head(0)), target -> CompletableFuture.completedFuture(null)),
            Duration.ofSeconds(5),
            (snapshot, offset, checkpoint) -> CompletableFuture.completedFuture(null));
    KafkaRetentionCoordinator coordinator =
        new KafkaRetentionCoordinator(
            () -> {
              loads.incrementAndGet();
              return load;
            },
            planner,
            barrier);

    CompletableFuture<KafkaRetentionCoordinator.RunResult> cancelled = coordinator.runOnce();
    CompletableFuture<KafkaRetentionCoordinator.RunResult> survivor = coordinator.runOnce();
    assertThat(cancelled.cancel(false)).isTrue();
    load.complete(snapshot(binding(), head(0), retentionSnapshot(-1, -1, 20, 5_000)));

    assertThat(survivor.join().trimResult()).isEmpty();
    assertThat(cancelled).isCancelled();
    assertThat(loads).hasValue(1);
  }

  @Test
  void executesOneExactPlanThroughCheckpointBarrierAndDurableListener() {
    var checkpoint = checkpoint(40);
    var captured = snapshot(binding(), head(0), retentionSnapshot(250, 2_500, 20, 5_000));
    var current = snapshot(binding(checkpoint), head(0), retentionSnapshot(250, 2_500, 20, 5_000));
    AtomicReference<com.nereusstream.api.StableStreamHeadSnapshot> durableHead =
        new AtomicReference<>(head(0));
    AtomicLong notified = new AtomicLong(-1);
    KafkaTrimBarrier barrier =
        new KafkaTrimBarrier(
            planner,
            () -> CompletableFuture.completedFuture(current),
            (snapshot, target) -> CompletableFuture.completedFuture(verified(checkpoint)),
            storage(
                durableHead,
                target -> {
                  durableHead.set(head(target));
                  return CompletableFuture.completedFuture(null);
                }),
            Duration.ofSeconds(5),
            (snapshot, offset, reference) -> {
              notified.set(offset);
              return CompletableFuture.completedFuture(null);
            });
    KafkaRetentionCoordinator coordinator =
        new KafkaRetentionCoordinator(
            () -> CompletableFuture.completedFuture(captured), planner, barrier);

    KafkaRetentionCoordinator.RunResult result = coordinator.runOnce().join();

    assertThat(result.plan().candidateLogStartOffset()).isEqualTo(20);
    assertThat(result.trimResult()).isPresent();
    assertThat(result.trimResult().orElseThrow().durableTrimOffset()).isEqualTo(20);
    assertThat(notified).hasValue(20);
  }

  private static StreamStorage storage(
      AtomicReference<com.nereusstream.api.StableStreamHeadSnapshot> head,
      java.util.function.LongFunction<CompletableFuture<Void>> trim) {
    return (StreamStorage)
        Proxy.newProxyInstance(
            StreamStorage.class.getClassLoader(),
            new Class<?>[] {StreamStorage.class},
            (proxy, method, arguments) ->
                switch (method.getName()) {
                  case "trim" -> trim.apply((long) arguments[1]);
                  case "getStableHeadSnapshot" -> CompletableFuture.completedFuture(head.get());
                  case "close" -> null;
                  case "toString" -> "KafkaRetentionCoordinatorTest.storage";
                  case "hashCode" -> System.identityHashCode(proxy);
                  case "equals" -> proxy == arguments[0];
                  default -> throw new UnsupportedOperationException(method.getName());
                });
  }
}
