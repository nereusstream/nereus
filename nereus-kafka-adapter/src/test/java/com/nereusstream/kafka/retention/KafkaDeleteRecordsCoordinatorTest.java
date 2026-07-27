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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.StableStreamHeadSnapshot;
import com.nereusstream.api.StreamStorage;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState.LogConfigHistoryEntry;
import com.nereusstream.metadata.oxia.records.KafkaCheckpointReferenceRecord;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongFunction;
import org.junit.jupiter.api.Test;

class KafkaDeleteRecordsCoordinatorTest {
  private final KafkaRetentionPlanner planner = new KafkaRetentionPlanner();

  @Test
  void advancesAnExactMidSegmentOffsetThroughTheSharedCheckpointBarrier() {
    Fixture fixture = fixture(15);
    KafkaDeleteRecordsCoordinator.Result result =
        new KafkaDeleteRecordsCoordinator(fixture.barrier()).deleteTo(fixture.captured, 15).join();

    assertThat(result.requestedOffset()).isEqualTo(15);
    assertThat(result.durableLowWatermark()).isEqualTo(15);
    assertThat(result.advanced()).isTrue();
    assertThat(result.trimResult().orElseThrow().requestedTrimOffset()).isEqualTo(15);
    assertThat(fixture.trimCalls).hasValue(1);
    assertThat(fixture.notified).hasValue(15);
    assertThat(fixture.trimReason.get())
        .isEqualTo(
            "KAFKA_DELETE_RECORDS_V1:config=7/"
                + fixture.captured.retention().policy().configDigest().value()
                + ":from=0:to=15:leader=1/3:brokerEpoch=4");
  }

  @Test
  void advancesTheNormalizedHighWatermarkExactly() {
    Fixture fixture = fixture(20);

    KafkaDeleteRecordsCoordinator.Result result =
        new KafkaDeleteRecordsCoordinator(fixture.barrier()).deleteTo(fixture.captured, 20).join();

    assertThat(result.requestedOffset()).isEqualTo(20);
    assertThat(result.durableLowWatermark()).isEqualTo(20);
    assertThat(fixture.trimCalls).hasValue(1);
  }

  @Test
  void returnsTheCurrentDurableLowWatermarkForAnAlreadyDeletedOffset() {
    AtomicInteger checkpointCalls = new AtomicInteger();
    KafkaTrimBarrier.Snapshot captured =
        snapshot(
            binding(10),
            head(10),
            retentionSnapshot(10, -1, -1, 20, 5_000, LogConfigHistoryEntry.CLEANUP_DELETE_FLAG));
    KafkaTrimBarrier barrier =
        new KafkaTrimBarrier(
            planner,
            () -> CompletableFuture.failedFuture(new AssertionError("must not reload")),
            (snapshot, target) -> {
              checkpointCalls.incrementAndGet();
              return CompletableFuture.failedFuture(new AssertionError("must not checkpoint"));
            },
            storage(
                new AtomicReference<>(head(10)),
                new AtomicInteger(),
                new AtomicReference<>(),
                target -> CompletableFuture.failedFuture(new AssertionError("must not trim"))),
            Duration.ofSeconds(5),
            (snapshot, offset, checkpoint) ->
                CompletableFuture.failedFuture(new AssertionError("must not notify")));

    KafkaDeleteRecordsCoordinator.Result result =
        new KafkaDeleteRecordsCoordinator(barrier).deleteTo(captured, 5).join();

    assertThat(result.requestedOffset()).isEqualTo(5);
    assertThat(result.durableLowWatermark()).isEqualTo(10);
    assertThat(result.advanced()).isFalse();
    assertThat(checkpointCalls).hasValue(0);
  }

  @Test
  void rejectsUnnormalizedNegativeAndBeyondHighWatermarkOffsets() {
    Fixture fixture = fixture(15);
    KafkaDeleteRecordsCoordinator coordinator =
        new KafkaDeleteRecordsCoordinator(fixture.barrier());

    assertThatThrownBy(() -> coordinator.deleteTo(fixture.captured, -1).join())
        .hasRootCauseMessage("Kafka DeleteRecords requires a non-negative normalized offset");
    assertThatThrownBy(() -> coordinator.deleteTo(fixture.captured, 21).join())
        .hasRootCauseMessage("Kafka DeleteRecords offset exceeds the frozen high watermark");
    assertThat(fixture.trimCalls).hasValue(0);
  }

  @Test
  void rejectsCompactOnlyPolicyBeforeCheckpointOrTrim() {
    AtomicInteger checkpointCalls = new AtomicInteger();
    KafkaTrimBarrier.Snapshot compactOnly =
        snapshot(
            binding(),
            head(0),
            retentionSnapshot(0, -1, -1, 20, 5_000, LogConfigHistoryEntry.CLEANUP_COMPACT_FLAG));
    KafkaTrimBarrier barrier =
        new KafkaTrimBarrier(
            planner,
            () -> CompletableFuture.failedFuture(new AssertionError("must not reload")),
            (snapshot, target) -> {
              checkpointCalls.incrementAndGet();
              return CompletableFuture.failedFuture(new AssertionError("must not checkpoint"));
            },
            storage(
                new AtomicReference<>(head(0)),
                new AtomicInteger(),
                new AtomicReference<>(),
                target -> CompletableFuture.completedFuture(null)),
            Duration.ofSeconds(5),
            (snapshot, offset, checkpoint) -> CompletableFuture.completedFuture(null));

    assertThatThrownBy(
            () -> new KafkaDeleteRecordsCoordinator(barrier).deleteTo(compactOnly, 10).join())
        .hasRootCauseMessage("Kafka DeleteRecords requires cleanup.policy containing delete");
    assertThat(checkpointCalls).hasValue(0);
  }

  @Test
  void abortsWhenTheKraftPolicyChangesBeforeTheDurableMutation() {
    KafkaCheckpointReferenceRecord rootedCheckpoint = checkpoint(40);
    KafkaTrimBarrier.Snapshot captured =
        snapshot(binding(), head(0), retentionSnapshot(-1, -1, 20, 5_000));
    KafkaTrimBarrier.Snapshot changed =
        snapshot(binding(rootedCheckpoint), head(0), retentionSnapshot(1_000, -1, 20, 5_000));
    AtomicInteger trimCalls = new AtomicInteger();
    KafkaTrimBarrier barrier =
        new KafkaTrimBarrier(
            planner,
            () -> CompletableFuture.completedFuture(changed),
            (snapshot, target) -> CompletableFuture.completedFuture(verified(rootedCheckpoint)),
            storage(
                new AtomicReference<>(head(0)),
                trimCalls,
                new AtomicReference<>(),
                target -> CompletableFuture.completedFuture(null)),
            Duration.ofSeconds(5),
            (snapshot, offset, checkpoint) -> CompletableFuture.completedFuture(null));

    assertThatThrownBy(
            () -> new KafkaDeleteRecordsCoordinator(barrier).deleteTo(captured, 15).join())
        .hasRootCauseMessage("Kafka DeleteRecords config changed before trim");
    assertThat(trimCalls).hasValue(0);
  }

  private Fixture fixture(long target) {
    return new Fixture(target);
  }

  private final class Fixture {
    private final KafkaCheckpointReferenceRecord checkpoint = checkpoint(40);
    private final KafkaTrimBarrier.Snapshot captured =
        snapshot(binding(), head(0), retentionSnapshot(-1, -1, 20, 5_000));
    private final KafkaTrimBarrier.Snapshot current =
        snapshot(binding(checkpoint), head(0), retentionSnapshot(-1, -1, 20, 5_000));
    private final AtomicReference<StableStreamHeadSnapshot> durableHead =
        new AtomicReference<>(head(0));
    private final AtomicInteger trimCalls = new AtomicInteger();
    private final AtomicLong notified = new AtomicLong(-1);
    private final AtomicReference<String> trimReason = new AtomicReference<>();
    private final long target;

    private Fixture(long target) {
      this.target = target;
    }

    private KafkaTrimBarrier barrier() {
      return new KafkaTrimBarrier(
          planner,
          () -> CompletableFuture.completedFuture(current),
          (snapshot, requested) -> CompletableFuture.completedFuture(verified(checkpoint)),
          storage(
              durableHead,
              trimCalls,
              trimReason,
              requested -> {
                assertThat(requested).isEqualTo(target);
                durableHead.set(head(requested));
                return CompletableFuture.completedFuture(null);
              }),
          Duration.ofSeconds(5),
          (snapshot, offset, checkpoint) -> {
            notified.set(offset);
            return CompletableFuture.completedFuture(null);
          });
    }
  }

  private static StreamStorage storage(
      AtomicReference<StableStreamHeadSnapshot> head,
      AtomicInteger trimCalls,
      AtomicReference<String> trimReason,
      LongFunction<CompletableFuture<Void>> trim) {
    return (StreamStorage)
        Proxy.newProxyInstance(
            StreamStorage.class.getClassLoader(),
            new Class<?>[] {StreamStorage.class},
            (proxy, method, arguments) ->
                switch (method.getName()) {
                  case "trim" -> {
                    trimCalls.incrementAndGet();
                    trimReason.set(((com.nereusstream.api.TrimOptions) arguments[2]).reason());
                    yield trim.apply((long) arguments[1]);
                  }
                  case "getStableHeadSnapshot" -> CompletableFuture.completedFuture(head.get());
                  case "close" -> null;
                  case "toString" -> "KafkaDeleteRecordsCoordinatorTest.storage";
                  case "hashCode" -> System.identityHashCode(proxy);
                  case "equals" -> proxy == arguments[0];
                  default -> throw new UnsupportedOperationException(method.getName());
                });
  }
}
