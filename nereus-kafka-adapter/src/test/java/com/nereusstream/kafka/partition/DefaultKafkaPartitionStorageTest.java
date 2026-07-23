/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.partition;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.AcquiredAppendSession;
import com.nereusstream.api.AppendCompletionPolicy;
import com.nereusstream.api.AppendOutcome;
import com.nereusstream.api.AppendSession;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.kafka.checkpoint.KafkaCheckpointSourceState;
import com.nereusstream.kafka.codec.KafkaAppendBatchEncoder;
import com.nereusstream.kafka.codec.KafkaFetchAssembler;
import com.nereusstream.kafka.codec.KafkaRecordBatchCodec;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Delayed;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.record.CompressionType;
import org.junit.jupiter.api.Test;

class DefaultKafkaPartitionStorageTest {
    @Test
    void stableAppendAdvancesDurableEndBeforeStockDerivedVisibilityOffsets() {
        Fixture fixture = fixture(0, 0);
        byte[] records = KafkaPartitionStorageTestSupport.batch(0, CompressionType.GZIP, 1_000, "a", "b");

        CompletableFuture<KafkaStableAppendResult> pending = fixture.storage.append(
                ByteBuffer.wrap(records), context(0, (short) 1));

        assertThat(fixture.streams.appendCalls()).isEqualTo(1);
        assertThat(pending).isNotDone();
        assertThat(fixture.storage.stableSnapshot().stableEndOffset()).isZero();
        assertThat(fixture.streams.pendingPrecondition().expectedStartOffset()).hasValue(0);
        assertThat(fixture.streams.pendingOptions().appendSession()).contains(fixture.session.session());
        assertThat(fixture.streams.pendingOptions().completionPolicy())
                .isEqualTo(AppendCompletionPolicy.PROFILE_DEFAULT);
        assertThat(fixture.streams.pendingBatch().entries().get(0).payload()).isEqualTo(records);

        fixture.streams.completeNextSuccess();
        KafkaStableAppendResult result = pending.join();

        assertThat(result.appendResult().range().startOffset()).isZero();
        assertThat(result.appendResult().range().endOffset()).isEqualTo(2);
        assertThat(result.stableSnapshot())
                .isEqualTo(new KafkaStableSnapshot(0, 2, 0, 0, 1));
        assertThat(fixture.storage.stableSnapshot()).isEqualTo(result.stableSnapshot());
        assertThat(result.requiredAcks()).isEqualTo((short) 1);
        assertThat(fixture.storage.publishDerivedOffsets(2, 2, 0))
                .isEqualTo(new KafkaStableSnapshot(0, 2, 2, 0, 1));
        assertThat(fixture.storage.publishDerivedOffsets(2, 2, 2))
                .isEqualTo(new KafkaStableSnapshot(0, 2, 2, 2, 1));
        assertThatThrownBy(() -> fixture.storage.publishDerivedOffsets(1, 1, 1))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> fixture.storage.publishDerivedOffsets(2, 3, 2))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void serializesSamePartitionAppendsAndRejectsSpeculativeOffsetGaps() {
        Fixture fixture = fixture(0, 0);
        CompletableFuture<KafkaStableAppendResult> first = fixture.storage.append(
                ByteBuffer.wrap(KafkaPartitionStorageTestSupport.batch(
                        0, CompressionType.NONE, 1_000, "a")),
                context(0, (short) 0));
        CompletableFuture<KafkaStableAppendResult> second = fixture.storage.append(
                ByteBuffer.wrap(KafkaPartitionStorageTestSupport.batch(
                        1, CompressionType.NONE, 2_000, "b")),
                context(1, (short) -1));

        assertThat(fixture.streams.appendCalls()).isEqualTo(1);
        assertThat(first).isNotDone();
        assertThat(second).isNotDone();
        assertThatThrownBy(() -> fixture.storage.append(
                        ByteBuffer.wrap(KafkaPartitionStorageTestSupport.batch(
                                3, CompressionType.NONE, 3_000, "gap")),
                        context(3, (short) 1)).join())
                .hasRootCauseInstanceOf(NereusException.class)
                .rootCause()
                .extracting(value -> ((NereusException) value).code())
                .isEqualTo(ErrorCode.OFFSET_CONFLICT);

        fixture.streams.completeNextSuccess();
        assertThat(first.join().stableSnapshot().stableEndOffset()).isEqualTo(1);
        assertThat(fixture.streams.appendCalls()).isEqualTo(1);
        fixture.storage.publishDerivedOffsets(1, 1, 1);
        assertThat(fixture.streams.appendCalls()).isEqualTo(2);
        fixture.streams.completeNextSuccess();

        assertThat(second.join().stableSnapshot().stableEndOffset()).isEqualTo(2);
        fixture.storage.publishDerivedOffsets(2, 2, 2);
        assertThat(fixture.storage.stableSnapshot().highWatermark()).isEqualTo(2);
    }

    @Test
    void knownNotCommittedFailureDrainsSuccessorsAndAllowsRetryAtStableEnd() {
        Fixture fixture = fixture(0, 0);
        CompletableFuture<KafkaStableAppendResult> first = fixture.storage.append(
                ByteBuffer.wrap(KafkaPartitionStorageTestSupport.batch(
                        0, CompressionType.NONE, 1_000, "a")),
                context(0, (short) 1));
        CompletableFuture<KafkaStableAppendResult> successor = fixture.storage.append(
                ByteBuffer.wrap(KafkaPartitionStorageTestSupport.batch(
                        1, CompressionType.NONE, 2_000, "b")),
                context(1, (short) 1));

        fixture.streams.failNext(new NereusException(
                ErrorCode.TIMEOUT, true, "known safe timeout", AppendOutcome.KNOWN_NOT_COMMITTED));

        assertFailureCode(first, ErrorCode.TIMEOUT);
        assertFailureCode(successor, ErrorCode.OFFSET_CONFLICT);
        assertThat(fixture.storage.state()).isEqualTo(KafkaPartitionState.LEADER_WRITABLE);
        assertThat(fixture.storage.stableSnapshot().stableEndOffset()).isZero();
        assertThat(fixture.streams.appendCalls()).isEqualTo(1);

        CompletableFuture<KafkaStableAppendResult> retry = fixture.storage.append(
                ByteBuffer.wrap(KafkaPartitionStorageTestSupport.batch(
                        0, CompressionType.NONE, 3_000, "retry")),
                context(0, (short) 1));
        fixture.streams.completeNextSuccess();
        assertThat(retry.join().stableSnapshot().stableEndOffset()).isEqualTo(1);
    }

    @Test
    void uncertainFailureFencesWritesButKeepsLastStableSnapshotReadable() {
        Fixture fixture = fixture(0, 0);
        CompletableFuture<KafkaStableAppendResult> append = fixture.storage.append(
                ByteBuffer.wrap(KafkaPartitionStorageTestSupport.batch(
                        0, CompressionType.NONE, 1_000, "a")),
                context(0, (short) 1));

        fixture.streams.failNext(new NereusException(
                ErrorCode.TIMEOUT, true, "uncertain", AppendOutcome.MAY_HAVE_COMMITTED));

        assertFailureCode(append, ErrorCode.TIMEOUT);
        assertThat(fixture.storage.state()).isEqualTo(KafkaPartitionState.WRITE_FENCED_RECOVERY_REQUIRED);
        assertFailureCode(fixture.storage.append(
                ByteBuffer.wrap(KafkaPartitionStorageTestSupport.batch(
                        0, CompressionType.NONE, 2_000, "retry")),
                context(0, (short) 1)), ErrorCode.FENCED_APPEND);
        KafkaStorageReadResult empty = fixture.storage.read(readRequest(0, 1, 1_024, true)).join();
        assertThat(empty.fetchAssembly().encodedRecords()).isEmpty();
    }

    @Test
    void stableResponseMismatchFencesInsteadOfPublishingSpeculativeOffsets() {
        Fixture fixture = fixture(0, 0);
        CompletableFuture<KafkaStableAppendResult> append = fixture.storage.append(
                ByteBuffer.wrap(KafkaPartitionStorageTestSupport.batch(
                        0, CompressionType.NONE, 1_000, "a")),
                context(0, (short) 1));
        fixture.streams.corruptNextResultStream();

        fixture.streams.completeNextSuccess();

        assertThatThrownBy(append::join)
                .hasRootCauseInstanceOf(IllegalStateException.class)
                .hasMessageContaining("does not exactly match");
        assertThat(fixture.storage.state()).isEqualTo(KafkaPartitionState.WRITE_FENCED_RECOVERY_REQUIRED);
        assertThat(fixture.storage.stableSnapshot().stableEndOffset()).isZero();
    }

    @Test
    void containingEntryReadHonorsStableUpperBoundAndFirstBatchOverflow() {
        Fixture fixture = fixture(0, 0);
        byte[] first = KafkaPartitionStorageTestSupport.batch(0, CompressionType.GZIP, 1_000, "a", "b");
        byte[] second = KafkaPartitionStorageTestSupport.batch(2, CompressionType.NONE, 2_000, "c");
        CompletableFuture<KafkaStableAppendResult> firstAppend = fixture.storage.append(
                ByteBuffer.wrap(first), context(0, (short) 1));
        fixture.streams.completeNextSuccess();
        firstAppend.join();
        fixture.storage.publishDerivedOffsets(2, 2, 2);
        CompletableFuture<KafkaStableAppendResult> secondAppend = fixture.storage.append(
                ByteBuffer.wrap(second), context(2, (short) 1));
        fixture.streams.completeNextSuccess();
        secondAppend.join();
        fixture.storage.publishDerivedOffsets(3, 3, 3);

        KafkaStorageReadResult containing = fixture.storage.read(readRequest(
                1, 3, first.length + second.length, true)).join();
        assertThat(containing.fetchAssembly().actualFirstBatchBaseOffset()).hasValue(0);
        assertThat(containing.fetchAssembly().nextLogicalOffset()).isEqualTo(3);
        assertThat(containing.fetchAssembly().encodedRecords())
                .isEqualTo(KafkaPartitionStorageTestSupport.concat(first, second));

        KafkaStorageReadResult bounded = fixture.storage.read(readRequest(
                1, 2, first.length + second.length, true)).join();
        assertThat(bounded.fetchAssembly().encodedRecords()).isEqualTo(first);
        assertThat(bounded.fetchAssembly().nextLogicalOffset()).isEqualTo(2);

        KafkaStorageReadResult overflow = fixture.storage.read(readRequest(
                1, 3, first.length - 1, true)).join();
        assertThat(overflow.fetchAssembly().encodedRecords()).isEqualTo(first);
        assertThat(overflow.fetchAssembly().firstEntryOverflow()).isTrue();

        KafkaStorageReadResult isolated = fixture.storage.read(readRequest(
                0, 1, first.length + second.length, true)).join();
        assertThat(isolated.fetchAssembly().encodedRecords()).isEmpty();
        assertThat(isolated.fetchAssembly().nextLogicalOffset()).isZero();
    }

    @Test
    void resignStopsAdmissionAndClosesOnlyAfterTheAppendLaneDrains() {
        Fixture fixture = fixture(0, 0);
        CompletableFuture<KafkaStableAppendResult> append = fixture.storage.append(
                ByteBuffer.wrap(KafkaPartitionStorageTestSupport.batch(
                        0, CompressionType.NONE, 1_000, "a")),
                context(0, (short) 1));
        CompletableFuture<KafkaStableAppendResult> queued = fixture.storage.append(
                ByteBuffer.wrap(KafkaPartitionStorageTestSupport.batch(
                        1, CompressionType.NONE, 2_000, "b")),
                context(1, (short) 1));

        CompletableFuture<Void> resign = fixture.storage.resign();

        assertThat(fixture.storage.state()).isEqualTo(KafkaPartitionState.RESIGNING);
        assertThat(resign).isNotDone();
        assertFailureCode(fixture.storage.append(
                ByteBuffer.wrap(KafkaPartitionStorageTestSupport.batch(
                        2, CompressionType.NONE, 3_000, "c")),
                context(2, (short) 1)), ErrorCode.FENCED_APPEND);

        fixture.streams.completeNextSuccess();
        append.join();
        fixture.storage.publishDerivedOffsets(1, 1, 1);
        assertThat(fixture.streams.appendCalls()).isEqualTo(2);
        assertThat(resign).isNotDone();
        fixture.streams.completeNextSuccess();
        queued.join();
        fixture.storage.publishDerivedOffsets(2, 2, 2);
        resign.join();
        assertThat(fixture.storage.state()).isEqualTo(KafkaPartitionState.CLOSED);
        assertThatThrownBy(() -> fixture.storage.read(readRequest(0, 1, 1_024, true)).join())
                .hasRootCauseInstanceOf(NereusException.class);
    }

    @Test
    void publishesStableAndLeadershipEventsWithoutLettingListenersReclassifyIo() {
        Fixture fixture = fixture(0, 0);
        List<KafkaPartitionEventType> events = new ArrayList<>();
        KafkaPartitionEventSubscription subscription = fixture.storage.subscribe(
                event -> events.add(event.type()));
        fixture.storage.subscribe(event -> {
            throw new IllegalStateException("observer failure");
        });
        CompletableFuture<KafkaStableAppendResult> append = fixture.storage.append(
                ByteBuffer.wrap(KafkaPartitionStorageTestSupport.batch(
                        0, CompressionType.NONE, 1_000, "a")),
                context(0, (short) 1));

        fixture.streams.completeNextSuccess();

        assertThat(append.join().stableSnapshot().stableEndOffset()).isEqualTo(1);
        fixture.storage.publishDerivedOffsets(1, 1, 1);
        fixture.storage.resign().join();
        assertThat(events).containsExactly(
                KafkaPartitionEventType.STABLE_APPEND,
                KafkaPartitionEventType.LEADERSHIP_LOST);
        subscription.close();
    }

    @Test
    void constructorRejectsAnythingOtherThanTheExactRecoveredAuthoritySession() {
        Fixture fixture = fixture(0, 0);
        KafkaCheckpointSourceState recovered = fixture.source;
        AcquiredAppendSession wrong = new AcquiredAppendSession(
                fixture.session.session(),
                Optional.of(new com.nereusstream.api.AppendAuthority(
                        "kafka-partition-leader-v1",
                        fixture.identity.durableId().canonicalIdentity(),
                        6,
                        "1",
                        9)));

        assertThatThrownBy(() -> storage(fixture.identity, fixture.streams, wrong, recovered))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exact recovered authority session");
    }

    @Test
    void renewsTheExactSessionAndUsesTheNewLeaseForLaterAppends() throws Exception {
        Fixture fixture = fixture(0, 0);
        ManualRenewalScheduler scheduler = new ManualRenewalScheduler();
        DefaultKafkaPartitionStorage storage = renewingStorage(fixture, scheduler);
        try {
            scheduler.fireRenewal();
            AppendSession renewed = new AppendSession(
                    fixture.session.session().streamId(),
                    fixture.session.session().writerId(),
                    fixture.session.session().epoch(),
                    fixture.session.session().fencingToken(),
                    fixture.session.session().leaseVersion() + 1,
                    fixture.session.session().expiresAtMillis() + 100_000);
            fixture.streams.completeRenewal(renewed);

            CompletableFuture<KafkaStableAppendResult> append = storage.append(
                    ByteBuffer.wrap(KafkaPartitionStorageTestSupport.batch(
                            0, CompressionType.NONE, 1_000, "renewed")),
                    context(0, (short) 1));

            assertThat(fixture.streams.pendingOptions().appendSession()).contains(renewed);
            fixture.streams.completeNextSuccess();
            assertThat(append.join().stableSnapshot().stableEndOffset()).isEqualTo(1);
            storage.publishDerivedOffsets(1, 1, 1);
            assertThat(fixture.streams.renewalCalls()).isGreaterThanOrEqualTo(1);
            storage.resign().get(5, TimeUnit.SECONDS);
        } finally {
            storage.resign();
            scheduler.shutdownNow();
        }
    }

    @Test
    void renewalFailureFencesWritesAndPublishesLeadershipLoss() throws Exception {
        Fixture fixture = fixture(0, 0);
        ManualRenewalScheduler scheduler = new ManualRenewalScheduler();
        DefaultKafkaPartitionStorage storage = renewingStorage(fixture, scheduler);
        List<KafkaPartitionEventType> events = new ArrayList<>();
        storage.subscribe(event -> events.add(event.type()));
        try {
            scheduler.fireRenewal();
            CompletableFuture<KafkaStableAppendResult> inFlight = storage.append(
                    ByteBuffer.wrap(KafkaPartitionStorageTestSupport.batch(
                            0, CompressionType.NONE, 1_000, "in-flight")),
                    context(0, (short) 1));
            CompletableFuture<KafkaStableAppendResult> queued = storage.append(
                    ByteBuffer.wrap(KafkaPartitionStorageTestSupport.batch(
                            1, CompressionType.NONE, 2_000, "queued")),
                    context(1, (short) 1));
            fixture.streams.failRenewal(new NereusException(
                    ErrorCode.METADATA_UNAVAILABLE, true, "renewal unavailable"));

            assertThat(storage.state()).isEqualTo(KafkaPartitionState.WRITE_FENCED_RECOVERY_REQUIRED);
            assertThat(events).containsExactly(KafkaPartitionEventType.LEADERSHIP_LOST);
            fixture.streams.completeNextSuccess();
            assertThat(inFlight.join().stableSnapshot().stableEndOffset()).isEqualTo(1);
            storage.publishDerivedOffsets(1, 1, 1);
            assertFailureCode(queued, ErrorCode.FENCED_APPEND);
            assertThat(fixture.streams.appendCalls()).isEqualTo(1);
            assertFailureCode(storage.append(
                    ByteBuffer.wrap(KafkaPartitionStorageTestSupport.batch(
                            1, CompressionType.NONE, 3_000, "rejected")),
                    context(1, (short) 1)), ErrorCode.FENCED_APPEND);
            storage.resign().get(5, TimeUnit.SECONDS);
        } finally {
            storage.resign();
            scheduler.shutdownNow();
        }
    }

    private static Fixture fixture(long trimOffset, long endOffset) {
        KafkaPartitionIdentity identity = KafkaPartitionStorageTestSupport.identity();
        StreamId streamId = new StreamId("kafka-partition-stream");
        var authority = new com.nereusstream.api.AppendAuthority(
                "kafka-partition-leader-v1",
                identity.durableId().canonicalIdentity(),
                5,
                "1",
                9);
        AppendSession session = new AppendSession(streamId, "broker-run", 7, "token", 11, 100_000);
        AcquiredAppendSession acquired = new AcquiredAppendSession(session, Optional.of(authority));
        KafkaCheckpointSourceState source = new KafkaCheckpointSourceState(
                authority,
                session.writerId(),
                session.epoch(),
                session.fencingToken(),
                session.leaseVersion(),
                trimOffset,
                endOffset,
                endOffset == 0 ? 0 : 1,
                endOffset == 0 ? "" : "commit-1",
                new Checksum(ChecksumType.SHA256, "00".repeat(32)),
                false,
                endOffset);
        KafkaPartitionStreamStorageFake streams = new KafkaPartitionStreamStorageFake(
                streamId, endOffset, endOffset == 0 ? 0 : 1);
        return new Fixture(identity, streams, acquired, source, storage(identity, streams, acquired, source));
    }

    private static DefaultKafkaPartitionStorage storage(
            KafkaPartitionIdentity identity,
            KafkaPartitionStreamStorageFake streams,
            AcquiredAppendSession session,
            KafkaCheckpointSourceState source) {
        KafkaRecordBatchCodec codec = new KafkaRecordBatchCodec();
        return new DefaultKafkaPartitionStorage(
                identity,
                streams,
                session.session().streamId(),
                session,
                source,
                KafkaStorageProfilePolicy.forProfile(StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT),
                new KafkaAppendBatchEncoder(codec),
                new KafkaFetchAssembler(codec));
    }

    private static DefaultKafkaPartitionStorage renewingStorage(
            Fixture fixture, ScheduledExecutorService scheduler) {
        KafkaRecordBatchCodec codec = new KafkaRecordBatchCodec();
        return new DefaultKafkaPartitionStorage(
                fixture.identity,
                fixture.streams,
                fixture.session.session().streamId(),
                fixture.session,
                fixture.source,
                KafkaStorageProfilePolicy.forProfile(StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT),
                new KafkaAppendBatchEncoder(codec),
                new KafkaFetchAssembler(codec),
                scheduler,
                Duration.ofSeconds(30),
                Duration.ofMillis(10));
    }

    private static KafkaAppendContext context(long expectedStart, short requiredAcks) {
        return new KafkaAppendContext(
                expectedStart, 5, requiredAcks, Duration.ofSeconds(5), Map.of("origin", "test"));
    }

    private static KafkaStorageReadRequest readRequest(
            long startOffset, long maxOffset, int maxPartitionBytes, boolean minOneMessage) {
        return new KafkaStorageReadRequest(
                startOffset,
                maxOffset,
                100,
                maxPartitionBytes,
                1024 * 1024,
                minOneMessage,
                0,
                0,
                Duration.ofSeconds(5));
    }

    private static void assertFailureCode(CompletableFuture<?> future, ErrorCode expected) {
        assertThatThrownBy(future::join)
                .isInstanceOf(CompletionException.class)
                .hasRootCauseInstanceOf(NereusException.class)
                .rootCause()
                .extracting(value -> ((NereusException) value).code())
                .isEqualTo(expected);
    }

    private record Fixture(
            KafkaPartitionIdentity identity,
            KafkaPartitionStreamStorageFake streams,
            AcquiredAppendSession session,
            KafkaCheckpointSourceState source,
            DefaultKafkaPartitionStorage storage) {}

    private static final class ManualRenewalScheduler extends ScheduledThreadPoolExecutor {
        private ManualScheduledFuture renewal;

        private ManualRenewalScheduler() {
            super(1);
        }

        @Override
        public synchronized ScheduledFuture<?> schedule(
                Runnable command, long delay, TimeUnit unit) {
            if (renewal != null && !renewal.isDone()) {
                throw new AssertionError("only one append-session renewal may be scheduled");
            }
            renewal = new ManualScheduledFuture(command);
            return renewal;
        }

        private synchronized void fireRenewal() {
            if (renewal == null) throw new AssertionError("append-session renewal was not scheduled");
            renewal.run();
        }
    }

    private static final class ManualScheduledFuture
            extends FutureTask<Void> implements ScheduledFuture<Void> {
        private ManualScheduledFuture(Runnable command) {
            super(command, null);
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return 0;
        }

        @Override
        public int compareTo(Delayed other) {
            return 0;
        }
    }
}
