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

import static com.nereusstream.kafka.retention.KafkaRetentionTestFixtures.IDENTITY;
import static com.nereusstream.kafka.retention.KafkaRetentionTestFixtures.STREAM_ID;
import static com.nereusstream.kafka.retention.KafkaRetentionTestFixtures.binding;
import static com.nereusstream.kafka.retention.KafkaRetentionTestFixtures.checkpoint;
import static com.nereusstream.kafka.retention.KafkaRetentionTestFixtures.head;
import static com.nereusstream.kafka.retention.KafkaRetentionTestFixtures.retentionSnapshot;
import static com.nereusstream.kafka.retention.KafkaRetentionTestFixtures.verified;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.StableStreamHeadSnapshot;
import com.nereusstream.api.StreamStorage;
import com.nereusstream.kafka.checkpoint.KafkaCanonicalCheckpointState;
import com.nereusstream.kafka.checkpoint.KafkaCheckpointSourceState;
import com.nereusstream.kafka.checkpoint.KafkaCheckpointSourceValidator;
import com.nereusstream.kafka.checkpoint.KafkaDerivedIndexState;
import com.nereusstream.kafka.checkpoint.KafkaDerivedIndexState.SegmentLogicalByteIndex;
import com.nereusstream.kafka.checkpoint.KafkaLeaderEpochState;
import com.nereusstream.kafka.checkpoint.KafkaProducerTransactionState;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState;
import com.nereusstream.metadata.oxia.KafkaPartitionMetadataStore;
import com.nereusstream.metadata.oxia.VersionedKafkaPartitionBinding;
import com.nereusstream.metadata.oxia.records.KafkaCheckpointReferenceRecord;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class DefaultKafkaPartitionMaintenanceTest {
  private static final Clock CLOCK =
      Clock.fixed(Instant.ofEpochMilli(5_000), ZoneOffset.UTC);

  @Test
  void exactMidBatchDeleteUsesProductCheckpointBarrierAndPublishesLocalStartLast() {
    KafkaCheckpointReferenceRecord rooted = checkpoint(40);
    AtomicReference<StableStreamHeadSnapshot> durableHead =
        new AtomicReference<>(head(0));
    AtomicReference<VersionedKafkaPartitionBinding> durableBinding =
        new AtomicReference<>(binding(rooted));
    AtomicInteger checkpointCalls = new AtomicInteger();
    AtomicInteger trimCalls = new AtomicInteger();
    AtomicLong localStart = new AtomicLong(-1);
    AtomicReference<String> trimReason = new AtomicReference<>();
    StreamStorage streams =
        storage(durableHead, trimCalls, trimReason, requested -> durableHead.set(head(requested)));
    KafkaPartitionMetadataStore bindings = bindings(durableBinding);
    KafkaCheckpointSourceValidator sourceValidator = sourceValidator(durableHead);
    DefaultKafkaPartitionMaintenance maintenance =
        new DefaultKafkaPartitionMaintenance(
            IDENTITY,
            3,
            STREAM_ID,
            sourceValidator,
            bindings,
            streams,
            ignored -> {
              checkpointCalls.incrementAndGet();
              return (snapshot, target) -> CompletableFuture.completedFuture(verified(rooted));
            },
            Duration.ofSeconds(5),
            CLOCK);
    KafkaPartitionMaintenance.Hooks hooks =
        hooks(
            source -> canonical(source, 20, 20),
            (snapshot, offset, published) -> {
              assertThat(published).isEqualTo(durableBinding.get());
              assertThat(published.value().observedLogStartOffset()).isEqualTo(offset);
              localStart.set(offset);
              return CompletableFuture.completedFuture(null);
            });

    KafkaDeleteRecordsCoordinator.Result result =
        maintenance.deleteRecords(hooks, 15).join();

    assertThat(result.requestedOffset()).isEqualTo(15);
    assertThat(result.durableLowWatermark()).isEqualTo(15);
    assertThat(result.advanced()).isTrue();
    assertThat(checkpointCalls).hasValue(1);
    assertThat(trimCalls).hasValue(1);
    assertThat(localStart).hasValue(15);
    assertThat(durableBinding.get().value().observedLogStartOffset()).isEqualTo(15);
    assertThat(trimReason.get())
        .contains("KAFKA_DELETE_RECORDS_V1:")
        .contains(":from=0:to=15:")
        .contains(":leader=1/3:brokerEpoch=4");
  }

  @Test
  void alreadyDeletedRequestReturnsDurableFloorWithoutCheckpointOrUpdater() {
    AtomicReference<StableStreamHeadSnapshot> durableHead =
        new AtomicReference<>(head(10));
    AtomicReference<VersionedKafkaPartitionBinding> durableBinding =
        new AtomicReference<>(binding(10));
    AtomicInteger checkpointCalls = new AtomicInteger();
    AtomicInteger captureCalls = new AtomicInteger();
    AtomicInteger updateCalls = new AtomicInteger();
    DefaultKafkaPartitionMaintenance maintenance =
        new DefaultKafkaPartitionMaintenance(
            IDENTITY,
            3,
            STREAM_ID,
            sourceValidator(durableHead),
            bindings(durableBinding),
            storage(
                durableHead,
                new AtomicInteger(),
                new AtomicReference<>(),
                requested -> {
                  throw new AssertionError("idempotent delete must not trim");
                }),
            ignored -> {
              checkpointCalls.incrementAndGet();
              return (snapshot, target) ->
                  CompletableFuture.failedFuture(
                      new AssertionError("idempotent delete must not checkpoint"));
            },
            Duration.ofSeconds(5),
            CLOCK);
    KafkaPartitionMaintenance.Hooks hooks =
        hooks(
            source -> {
              captureCalls.incrementAndGet();
              return canonical(source, 20, 20);
            },
            (snapshot, offset, published) -> {
              updateCalls.incrementAndGet();
              return CompletableFuture.failedFuture(
                  new AssertionError("idempotent delete must not update local state"));
            });

    KafkaDeleteRecordsCoordinator.Result result =
        maintenance.deleteRecords(hooks, 5).join();

    assertThat(result.requestedOffset()).isEqualTo(5);
    assertThat(result.durableLowWatermark()).isEqualTo(10);
    assertThat(result.advanced()).isFalse();
    assertThat(captureCalls).hasValue(1);
    assertThat(checkpointCalls).hasValue(1);
    assertThat(updateCalls).hasValue(0);
  }

  @Test
  void rejectsForkCaptureFromAnotherStableSourceBeforeCheckpointOrTrim() {
    AtomicReference<StableStreamHeadSnapshot> durableHead =
        new AtomicReference<>(head(0));
    AtomicInteger checkpointCalls = new AtomicInteger();
    AtomicInteger trimCalls = new AtomicInteger();
    DefaultKafkaPartitionMaintenance maintenance =
        new DefaultKafkaPartitionMaintenance(
            IDENTITY,
            3,
            STREAM_ID,
            sourceValidator(durableHead),
            bindings(new AtomicReference<>(binding())),
            storage(durableHead, trimCalls, new AtomicReference<>(), ignored -> {}),
            ignored -> {
              checkpointCalls.incrementAndGet();
              return (snapshot, target) ->
                  CompletableFuture.failedFuture(
                      new AssertionError("stale capture must not checkpoint"));
            },
            Duration.ofSeconds(5),
            CLOCK);
    KafkaPartitionMaintenance.Hooks hooks =
        hooks(
            source -> canonical(source, 20, 20, 10),
            (snapshot, offset, published) -> CompletableFuture.completedFuture(null));

    assertThatThrownBy(() -> maintenance.deleteRecords(hooks, 15).join())
        .hasRootCauseMessage("Kafka maintenance hook captured another stable source");
    assertThat(checkpointCalls).hasValue(1);
    assertThat(trimCalls).hasValue(0);
  }

  private static KafkaPartitionMaintenance.Hooks hooks(
      java.util.function.Function<
              KafkaCheckpointSourceState, KafkaPartitionMaintenance.Capture>
          capture,
      KafkaRetentionDurableTrimListener.LocalLogStartUpdater updater) {
    return new KafkaPartitionMaintenance.Hooks() {
      @Override
      public CompletableFuture<KafkaPartitionMaintenance.Capture> capture(
          KafkaCheckpointSourceState currentSource) {
        return CompletableFuture.completedFuture(capture.apply(currentSource));
      }

      @Override
      public CompletableFuture<Void> advanceLogStart(
          KafkaTrimBarrier.Snapshot revalidated,
          long durableTrimOffset,
          VersionedKafkaPartitionBinding publishedBinding) {
        return updater.advance(revalidated, durableTrimOffset, publishedBinding);
      }
    };
  }

  private static KafkaPartitionMaintenance.Capture canonical(
      KafkaCheckpointSourceState source, long highWatermark, long lastStableOffset) {
    return canonical(source, highWatermark, lastStableOffset, source.trimOffset());
  }

  private static KafkaPartitionMaintenance.Capture canonical(
      KafkaCheckpointSourceState source,
      long highWatermark,
      long lastStableOffset,
      long canonicalLogStart) {
    KafkaVirtualSegmentState virtual =
        retentionSnapshot(
                canonicalLogStart,
                -1,
                -1,
                highWatermark,
                CLOCK.millis(),
                KafkaVirtualSegmentState.LogConfigHistoryEntry.CLEANUP_DELETE_FLAG)
            .virtualSegments();
    KafkaDerivedIndexState indexes =
        new KafkaDerivedIndexState(
            canonicalLogStart,
            source.endOffset(),
            List.of(),
            virtual.segments().stream()
                .map(
                    segment ->
                        new SegmentLogicalByteIndex(
                            segment.baseOffset(), segment.logicalBytes(), List.of()))
                .toList());
    KafkaCanonicalCheckpointState state =
        new KafkaCanonicalCheckpointState(
            source.endOffset(),
            canonicalLogStart,
            source.endOffset(),
            new KafkaProducerTransactionState(
                source.endOffset(), List.of(), List.of(), List.of()),
            new KafkaLeaderEpochState(
                canonicalLogStart,
                source.endOffset(),
                List.of(new KafkaLeaderEpochState.LeaderEpochRange(3, canonicalLogStart))),
            virtual,
            indexes);
    return new KafkaPartitionMaintenance.Capture(state, highWatermark, lastStableOffset);
  }

  private static KafkaCheckpointSourceValidator sourceValidator(
      AtomicReference<StableStreamHeadSnapshot> head) {
    return new KafkaCheckpointSourceValidator() {
      @Override
      public CompletableFuture<KafkaCheckpointSourceState> loadCurrent() {
        return CompletableFuture.completedFuture(source(head.get()));
      }

      @Override
      public CompletableFuture<Boolean> isSourceCommitReachable(
          com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointHeader captured,
          KafkaCheckpointSourceState current) {
        return CompletableFuture.completedFuture(true);
      }
    };
  }

  private static KafkaCheckpointSourceState source(StableStreamHeadSnapshot head) {
    var acquired = head.appendSession().orElseThrow();
    var session = acquired.session();
    return new KafkaCheckpointSourceState(
        acquired.authority().orElseThrow(),
        session.writerId(),
        session.epoch(),
        session.fencingToken(),
        session.leaseVersion(),
        head.trimOffset(),
        head.committedEndOffset(),
        head.commitVersion(),
        head.lastCommitId(),
        head.durableHeadSha256(),
        false,
        head.committedEndOffset());
  }

  private static KafkaPartitionMetadataStore bindings(
      AtomicReference<VersionedKafkaPartitionBinding> current) {
    return (KafkaPartitionMetadataStore)
        Proxy.newProxyInstance(
            KafkaPartitionMetadataStore.class.getClassLoader(),
            new Class<?>[] {KafkaPartitionMetadataStore.class},
            (proxy, method, arguments) ->
                switch (method.getName()) {
                  case "get" -> CompletableFuture.completedFuture(Optional.of(current.get()));
                  case "compareAndSet" -> {
                    VersionedKafkaPartitionBinding expected =
                        (VersionedKafkaPartitionBinding) arguments[0];
                    assertThat(expected).isEqualTo(current.get());
                    var update =
                        (com.nereusstream.metadata.oxia.records.KafkaPartitionBindingRecord)
                            arguments[1];
                    VersionedKafkaPartitionBinding published =
                        new VersionedKafkaPartitionBinding(
                            expected.key(),
                            update,
                            update.metadataVersion(),
                            new Checksum(ChecksumType.SHA256, "d".repeat(64)));
                    current.set(published);
                    yield CompletableFuture.completedFuture(published);
                  }
                  case "close" -> null;
                  case "toString" -> "DefaultKafkaPartitionMaintenanceTest.bindings";
                  case "hashCode" -> System.identityHashCode(proxy);
                  case "equals" -> proxy == arguments[0];
                  default -> throw new UnsupportedOperationException(method.getName());
                });
  }

  private static StreamStorage storage(
      AtomicReference<StableStreamHeadSnapshot> head,
      AtomicInteger trimCalls,
      AtomicReference<String> trimReason,
      java.util.function.LongConsumer trim) {
    return (StreamStorage)
        Proxy.newProxyInstance(
            StreamStorage.class.getClassLoader(),
            new Class<?>[] {StreamStorage.class},
            (proxy, method, arguments) ->
                switch (method.getName()) {
                  case "getStableHeadSnapshot" ->
                      CompletableFuture.completedFuture(head.get());
                  case "trim" -> {
                    trimCalls.incrementAndGet();
                    trimReason.set(
                        ((com.nereusstream.api.TrimOptions) arguments[2]).reason());
                    trim.accept((long) arguments[1]);
                    yield CompletableFuture.completedFuture(null);
                  }
                  case "close" -> null;
                  case "toString" -> "DefaultKafkaPartitionMaintenanceTest.storage";
                  case "hashCode" -> System.identityHashCode(proxy);
                  case "equals" -> proxy == arguments[0];
                  default -> throw new UnsupportedOperationException(method.getName());
                });
  }
}
