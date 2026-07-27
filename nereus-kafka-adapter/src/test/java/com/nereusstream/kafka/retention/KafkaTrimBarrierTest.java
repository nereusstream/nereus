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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.AcquiredAppendSession;
import com.nereusstream.api.AppendAuthority;
import com.nereusstream.api.AppendSession;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.StableStreamHeadSnapshot;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamState;
import com.nereusstream.api.StreamStorage;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState.LogConfigHistoryEntry;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState.RollReason;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState.SegmentState;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState.VirtualSegment;
import com.nereusstream.kafka.partition.KafkaPartitionIdentity;
import com.nereusstream.metadata.oxia.KafkaPartitionMetadataTransitions;
import com.nereusstream.metadata.oxia.VersionedKafkaPartitionBinding;
import com.nereusstream.metadata.oxia.records.KafkaCheckpointReferenceRecord;
import com.nereusstream.metadata.oxia.records.KafkaPartitionOperationType;
import com.nereusstream.metadata.oxia.records.KafkaPartitionPendingOperationRecord;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongFunction;
import org.junit.jupiter.api.Test;

class KafkaTrimBarrierTest {
  private static final StreamId STREAM_ID = new StreamId("kafka-stream-1");
  private static final KafkaPartitionIdentity IDENTITY = identity();
  private static final KafkaCheckpointReferenceRecord CHECKPOINT = checkpoint(40);
  private final KafkaRetentionPlanner planner = new KafkaRetentionPlanner();

  @Test
  void publishesOrSelectsCheckpointThenTrimsAndNotifiesOnlyAfterDurableConfirmation() {
    Fixture fixture = fixture();
    AtomicLong notified = new AtomicLong(-1);
    KafkaTrimBarrier barrier =
        fixture.barrier(
            (snapshot, target) -> CompletableFuture.completedFuture(verified(CHECKPOINT)),
            () -> CompletableFuture.completedFuture(fixture.currentWithCheckpoint()),
            target -> {
              fixture.durableHead.set(head(target));
              return CompletableFuture.completedFuture(null);
            },
            (snapshot, durableTrim, checkpoint) -> {
              notified.set(durableTrim);
              return CompletableFuture.completedFuture(null);
            });

    KafkaTrimBarrier.Result result = barrier.advance(fixture.captured(), fixture.plan()).join();

    assertThat(result.requestedTrimOffset()).isEqualTo(20);
    assertThat(result.durableTrimOffset()).isEqualTo(20);
    assertThat(result.checkpoint()).isEqualTo(CHECKPOINT);
    assertThat(result.alreadyApplied()).isFalse();
    assertThat(notified).hasValue(20);
    assertThat(fixture.trimCalls).hasValue(1);
    assertThat(fixture.trimReason.get()).startsWith("KAFKA_RETENTION_V1:TIME+SIZE:");
  }

  @Test
  void neverTrimsWhenCheckpointPublicationOrVerificationFails() {
    Fixture fixture = fixture();
    KafkaTrimBarrier barrier =
        fixture.barrier(
            (snapshot, target) ->
                CompletableFuture.failedFuture(
                    new IllegalStateException("checkpoint upload failed")),
            () -> CompletableFuture.completedFuture(fixture.currentWithCheckpoint()),
            target -> CompletableFuture.completedFuture(null),
            completedListener());

    assertThatThrownBy(() -> barrier.advance(fixture.captured(), fixture.plan()).join())
        .hasRootCauseMessage("checkpoint upload failed");
    assertThat(fixture.trimCalls).hasValue(0);
  }

  @Test
  void rejectsInsufficientOrUnrootedVerifiedCheckpointsBeforeMutation() {
    Fixture insufficient = fixture();
    KafkaTrimBarrier insufficientBarrier =
        insufficient.barrier(
            (snapshot, target) -> CompletableFuture.completedFuture(verified(checkpoint(10))),
            () -> CompletableFuture.completedFuture(insufficient.currentWithCheckpoint()),
            target -> CompletableFuture.completedFuture(null),
            completedListener());

    assertThatThrownBy(
            () -> insufficientBarrier.advance(insufficient.captured(), insufficient.plan()).join())
        .hasRootCauseMessage("verified Kafka checkpoint is insufficient for requested trim");
    assertThat(insufficient.trimCalls).hasValue(0);

    Fixture unrooted = fixture();
    KafkaTrimBarrier unrootedBarrier =
        unrooted.barrier(
            (snapshot, target) -> CompletableFuture.completedFuture(verified(CHECKPOINT)),
            () -> CompletableFuture.completedFuture(unrooted.captured()),
            target -> CompletableFuture.completedFuture(null),
            completedListener());

    assertThatThrownBy(() -> unrootedBarrier.advance(unrooted.captured(), unrooted.plan()).join())
        .hasRootCauseMessage(
            "Kafka retention config or verified checkpoint root changed before trim");
    assertThat(unrooted.trimCalls).hasValue(0);
  }

  @Test
  void abortsWhenTheCurrentKraftPolicyChangedAfterPlanning() {
    Fixture fixture = fixture();
    KafkaTrimBarrier.Snapshot changed =
        snapshot(
            fixture.bindingWithCheckpoint(), head(0), retentionSnapshot(300, 2_500, 20, 5_000));
    KafkaTrimBarrier barrier =
        fixture.barrier(
            (snapshot, target) -> CompletableFuture.completedFuture(verified(CHECKPOINT)),
            () -> CompletableFuture.completedFuture(changed),
            target -> CompletableFuture.completedFuture(null),
            completedListener());

    assertThatThrownBy(() -> barrier.advance(fixture.captured(), fixture.plan()).join())
        .hasRootCauseMessage(
            "Kafka retention config or verified checkpoint root changed before trim");
    assertThat(fixture.trimCalls).hasValue(0);
  }

  @Test
  void treatsTrimResponseLossAsSuccessOnlyWhenReloadedHeadReachedTarget() {
    Fixture fixture = fixture();
    AtomicLong notified = new AtomicLong(-1);
    KafkaTrimBarrier barrier =
        fixture.barrier(
            (snapshot, target) -> CompletableFuture.completedFuture(verified(CHECKPOINT)),
            () -> CompletableFuture.completedFuture(fixture.currentWithCheckpoint()),
            target -> {
              fixture.durableHead.set(head(target));
              return CompletableFuture.failedFuture(
                  new IllegalStateException("trim response lost"));
            },
            (snapshot, durableTrim, checkpoint) -> {
              notified.set(durableTrim);
              return CompletableFuture.completedFuture(null);
            });

    KafkaTrimBarrier.Result result = barrier.advance(fixture.captured(), fixture.plan()).join();

    assertThat(result.durableTrimOffset()).isEqualTo(20);
    assertThat(notified).hasValue(20);
  }

  @Test
  void propagatesTrimFailureWhenReloadedHeadDidNotAdvance() {
    Fixture fixture = fixture();
    AtomicInteger notifications = new AtomicInteger();
    KafkaTrimBarrier barrier =
        fixture.barrier(
            (snapshot, target) -> CompletableFuture.completedFuture(verified(CHECKPOINT)),
            () -> CompletableFuture.completedFuture(fixture.currentWithCheckpoint()),
            target -> CompletableFuture.failedFuture(new IllegalStateException("trim rejected")),
            (snapshot, durableTrim, checkpoint) -> {
              notifications.incrementAndGet();
              return CompletableFuture.completedFuture(null);
            });

    assertThatThrownBy(() -> barrier.advance(fixture.captured(), fixture.plan()).join())
        .hasRootCauseMessage("trim rejected");
    assertThat(notifications).hasValue(0);
  }

  private Fixture fixture() {
    return new Fixture();
  }

  private final class Fixture {
    private final AtomicReference<StableStreamHeadSnapshot> durableHead =
        new AtomicReference<>(head(0));
    private final AtomicInteger trimCalls = new AtomicInteger();
    private final AtomicReference<String> trimReason = new AtomicReference<>();
    private final KafkaRetentionPlanner.Snapshot retention =
        retentionSnapshot(250, 2_500, 20, 5_000);
    private final VersionedKafkaPartitionBinding binding = binding(false);

    private KafkaTrimBarrier.Snapshot captured() {
      return snapshot(binding, head(0), retention);
    }

    private VersionedKafkaPartitionBinding bindingWithCheckpoint() {
      return binding(true);
    }

    private KafkaTrimBarrier.Snapshot currentWithCheckpoint() {
      return snapshot(bindingWithCheckpoint(), head(0), retention);
    }

    private KafkaRetentionPlanner.Plan plan() {
      return planner.plan(retention);
    }

    private KafkaTrimBarrier barrier(
        KafkaTrimBarrier.CheckpointGate checkpointGate,
        KafkaTrimBarrier.SnapshotLoader snapshotLoader,
        LongFunction<CompletableFuture<Void>> trim,
        KafkaTrimBarrier.DurableTrimListener listener) {
      StreamStorage storage =
          storage(
              target -> {
                trimCalls.incrementAndGet();
                return trim.apply(target);
              },
              durableHead,
              trimReason);
      return new KafkaTrimBarrier(
          planner, snapshotLoader, checkpointGate, storage, Duration.ofSeconds(5), listener);
    }
  }

  private static KafkaTrimBarrier.DurableTrimListener completedListener() {
    return (snapshot, durableTrim, checkpoint) -> CompletableFuture.completedFuture(null);
  }

  private static KafkaTrimBarrier.Snapshot snapshot(
      VersionedKafkaPartitionBinding binding,
      StableStreamHeadSnapshot head,
      KafkaRetentionPlanner.Snapshot retention) {
    return new KafkaTrimBarrier.Snapshot(IDENTITY, binding, head, retention);
  }

  private static KafkaTrimBarrier.VerifiedCheckpoint verified(
      KafkaCheckpointReferenceRecord checkpoint) {
    return new KafkaTrimBarrier.VerifiedCheckpoint(checkpoint, checkpoint.objectSha256());
  }

  private static KafkaRetentionPlanner.Snapshot retentionSnapshot(
      long retentionBytes, long retentionMs, long highWatermark, long nowMillis) {
    int flags =
        LogConfigHistoryEntry.CLEANUP_DELETE_FLAG | LogConfigHistoryEntry.CLEANUP_COMPACT_FLAG;
    LogConfigHistoryEntry config =
        LogConfigHistoryEntry.create(
            7,
            0,
            1_024,
            60_000,
            0,
            1_024,
            64,
            retentionBytes,
            retentionMs,
            0,
            86_400_000,
            0,
            Long.MAX_VALUE,
            0.5,
            flags);
    KafkaVirtualSegmentState state =
        new KafkaVirtualSegmentState(
            0,
            40,
            List.of(
                segment(config, 0, 10, 0, 100, 200, 1_000, 0, 100, SegmentState.CLOSED),
                segment(config, 10, 20, 1, 200, 300, 2_000, 100, 200, SegmentState.CLOSED),
                segment(config, 20, 30, 2, 300, 400, 10_000, 200, 300, SegmentState.CLOSED),
                segment(config, 30, 40, 3, 400, 0, 11_000, 300, 400, SegmentState.ACTIVE)),
            List.of(config));
    return new KafkaRetentionPlanner.Snapshot(
        state, KafkaRetentionPlanner.Policy.from(config), 0, highWatermark, nowMillis);
  }

  private static VirtualSegment segment(
      LogConfigHistoryEntry config,
      long baseOffset,
      long endOffset,
      long rollSequence,
      long createdAt,
      long closedAt,
      long largestTimestamp,
      long firstCumulativeBytes,
      long lastCumulativeBytes,
      SegmentState state) {
    return new VirtualSegment(
        baseOffset,
        endOffset,
        rollSequence,
        createdAt,
        closedAt,
        0,
        largestTimestamp,
        endOffset - 1,
        lastCumulativeBytes - firstCumulativeBytes,
        firstCumulativeBytes,
        lastCumulativeBytes,
        config.configDigest(),
        rollSequence == 0 ? RollReason.INITIAL : RollReason.SIZE,
        state);
  }

  private static StableStreamHeadSnapshot head(long trimOffset) {
    AppendAuthority authority =
        new AppendAuthority(
            "kafka-partition-leader-v1",
            IDENTITY.durableId().canonicalIdentity(),
            3,
            "broker-1",
            4);
    AcquiredAppendSession acquired =
        new AcquiredAppendSession(
            new AppendSession(STREAM_ID, "writer-1", 1, "token-1", 1, 10_000),
            Optional.of(authority));
    return new StableStreamHeadSnapshot(
        STREAM_ID,
        StreamState.ACTIVE,
        StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT,
        trimOffset,
        40,
        400,
        1,
        "commit-1",
        Optional.of(acquired),
        sha256('c'),
        1);
  }

  private static VersionedKafkaPartitionBinding binding(boolean includeCheckpoint) {
    KafkaPartitionPendingOperationRecord operation =
        new KafkaPartitionPendingOperationRecord(
            KafkaPartitionOperationType.CREATE.wireId(),
            "create-test",
            "broker-1",
            1,
            20_000,
            7,
            10_000,
            "");
    var creating =
        KafkaPartitionMetadataTransitions.creating(
            IDENTITY.durableId(),
            IDENTITY.observedTopicName(),
            StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT.name(),
            7,
            10_000,
            operation);
    var active =
        KafkaPartitionMetadataTransitions.activate(
            creating, "kafka-stream", STREAM_ID.value(), 7, 10_001);
    var observed =
        KafkaPartitionMetadataTransitions.observe(
            active, IDENTITY.observedTopicName(), 7, 1, 3, 4, 0, 40, 10_002);
    var root =
        includeCheckpoint
            ? KafkaPartitionMetadataTransitions.prependCheckpoint(
                observed, CHECKPOINT, 0, 40, 10_003)
            : observed;
    return new VersionedKafkaPartitionBinding(
        "/test/kafka-binding", root, 0, sha256(includeCheckpoint ? 'b' : 'a'));
  }

  private static KafkaCheckpointReferenceRecord checkpoint(long checkpointOffset) {
    byte[] objectSha = new byte[32];
    byte[] headSha = new byte[32];
    Arrays.fill(objectSha, (byte) 0x5a);
    Arrays.fill(headSha, (byte) 0x6b);
    return new KafkaCheckpointReferenceRecord(
        1,
        "checkpoint-" + checkpointOffset,
        "kafka/checkpoint-" + checkpointOffset,
        1_024,
        objectSha,
        checkpointOffset,
        0,
        1,
        headSha,
        "test-build",
        10_000 + checkpointOffset);
  }

  private static KafkaPartitionIdentity identity() {
    ByteBuffer bytes = ByteBuffer.allocate(16).putLong(0x1234_5678_9abc_def0L).putLong(99);
    return new KafkaPartitionIdentity(
        "kraft",
        Base64.getUrlEncoder().withoutPadding().encodeToString(bytes.array()),
        3,
        "orders");
  }

  private static Checksum sha256(char value) {
    return new Checksum(ChecksumType.SHA256, Character.toString(value).repeat(64));
  }

  private static StreamStorage storage(
      LongFunction<CompletableFuture<Void>> trim,
      AtomicReference<StableStreamHeadSnapshot> head,
      AtomicReference<String> reason) {
    return (StreamStorage)
        Proxy.newProxyInstance(
            StreamStorage.class.getClassLoader(),
            new Class<?>[] {StreamStorage.class},
            (proxy, method, arguments) ->
                switch (method.getName()) {
                  case "trim" -> {
                    reason.set(((com.nereusstream.api.TrimOptions) arguments[2]).reason());
                    yield trim.apply((long) arguments[1]);
                  }
                  case "getStableHeadSnapshot" -> CompletableFuture.completedFuture(head.get());
                  case "close" -> null;
                  case "toString" -> "KafkaTrimBarrierTest.storage";
                  case "hashCode" -> System.identityHashCode(proxy);
                  case "equals" -> proxy == arguments[0];
                  default -> throw new UnsupportedOperationException(method.getName());
                });
  }
}
