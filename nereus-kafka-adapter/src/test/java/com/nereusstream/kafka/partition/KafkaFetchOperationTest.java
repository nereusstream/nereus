/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.partition;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.AppendOutcome;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.ReadResult;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.SemanticReadResult;
import com.nereusstream.api.StreamId;
import com.nereusstream.kafka.codec.KafkaFetchAssembler;
import com.nereusstream.kafka.codec.KafkaRecordBatchCodec;
import com.nereusstream.kafka.fetch.KafkaFetchOperation;
import com.nereusstream.kafka.fetch.KafkaFetchOperationRequest;
import com.nereusstream.kafka.fetch.KafkaFetchOperationResult;
import com.nereusstream.kafka.fetch.KafkaFetchOperationState;
import com.nereusstream.kafka.fetch.KafkaFetchPartitionRequest;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Delayed;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.record.CompressionType;
import org.junit.jupiter.api.Test;

class KafkaFetchOperationTest {
    @Test
    void waitsForActualMinBytesAndStableAppendWakesExactlyOneCallback() {
        ManualScheduler scheduler = new ManualScheduler();
        try {
            FakePartitionStorage storage = new FakePartitionStorage(identity(1));
            byte[] batch = KafkaPartitionStorageTestSupport.batch(
                    0, CompressionType.GZIP, 1_000, "a", "b");
            storage.enqueue(completed(emptyResult(0)));
            storage.enqueue(completed(dataResult(batch, 2)));
            AtomicInteger callbackDispatches = new AtomicInteger();
            KafkaFetchOperation operation = operation(
                    List.of(storage), 1, 1024 * 1024, Duration.ofSeconds(5), 8,
                    command -> {
                        callbackDispatches.incrementAndGet();
                        command.run();
                    }, scheduler);

            CompletableFuture<KafkaFetchOperationResult> completion = operation.start();

            assertThat(operation.state()).isEqualTo(KafkaFetchOperationState.WAITING);
            assertThat(completion).isNotDone();
            assertThat(completion.cancel(false)).isFalse();
            assertThat(completion.complete(null)).isFalse();
            storage.emit(KafkaPartitionEventType.STABLE_APPEND);

            KafkaFetchOperationResult result = completion.join();
            assertThat(result.responseBytes()).isEqualTo(batch.length);
            assertThat(result.timedOut()).isFalse();
            assertThat(result.partitions().getFirst().readResult()).isPresent();
            assertThat(operation.state()).isEqualTo(KafkaFetchOperationState.COMPLETE);
            assertThat(storage.listenerCount()).isZero();
            assertThat(callbackDispatches).hasValue(1);
        } finally {
            scheduler.shutdownNow();
        }
    }

    @Test
    void coalescesEventsAllowsOneReadPerPartitionAndPerformsTimedFinalRead() {
        ManualScheduler scheduler = new ManualScheduler();
        try {
            FakePartitionStorage storage = new FakePartitionStorage(identity(2));
            KafkaStorageReadResult empty = emptyResult(0);
            storage.enqueue(completed(empty));
            KafkaFetchOperation operation = operation(
                    List.of(storage), 1, 1024 * 1024, Duration.ofSeconds(5), 8,
                    Runnable::run, scheduler);
            CompletableFuture<KafkaFetchOperationResult> completion = operation.start();
            CompletableFuture<KafkaStorageReadResult> pending = new CompletableFuture<>();
            storage.enqueue(pending);

            storage.emit(KafkaPartitionEventType.STABLE_APPEND);
            storage.emit(KafkaPartitionEventType.STABLE_APPEND);
            storage.emit(KafkaPartitionEventType.STABLE_APPEND);

            assertThat(storage.readCalls()).isEqualTo(2);
            assertThat(storage.maxInFlight()).isEqualTo(1);
            storage.enqueue(completed(empty));
            pending.complete(empty);
            assertThat(storage.readCalls()).isEqualTo(3);
            assertThat(operation.state()).isEqualTo(KafkaFetchOperationState.WAITING);

            storage.enqueue(completed(empty));
            scheduler.fireDeadline();
            KafkaFetchOperationResult result = completion.join();

            assertThat(result.timedOut()).isTrue();
            assertThat(result.responseBytes()).isZero();
            assertThat(result.readAttempts()).isEqualTo(4);
            assertThat(storage.readCalls()).isEqualTo(4);
            assertThat(storage.maxInFlight()).isEqualTo(1);
            assertThat(storage.listenerCount()).isZero();
        } finally {
            scheduler.shutdownNow();
        }
    }

    @Test
    void appliesRequestWideBudgetInPartitionOrderWithoutLeakingOmittedBuffers() {
        ManualScheduler scheduler = new ManualScheduler();
        try {
            byte[] batch = KafkaPartitionStorageTestSupport.batch(
                    0, CompressionType.NONE, 1_000, "value");
            FakePartitionStorage first = new FakePartitionStorage(identity(3));
            FakePartitionStorage second = new FakePartitionStorage(identity(4));
            first.enqueue(completed(dataResult(batch, 1)));
            second.enqueue(completed(dataResult(batch, 1)));
            KafkaFetchOperation operation = operation(
                    List.of(first, second), batch.length, batch.length, Duration.ofSeconds(5), 8,
                    Runnable::run, scheduler);

            KafkaFetchOperationResult result = operation.start().join();

            assertThat(result.responseBytes()).isEqualTo(batch.length);
            assertThat(result.responseBudgetExhausted()).isTrue();
            assertThat(result.partitions()).hasSize(2);
            assertThat(result.partitions().get(0).readResult()).isPresent();
            assertThat(result.partitions().get(1).readResult()).isEmpty();
            assertThat(result.partitions().get(1).omittedForResponseBudget()).isTrue();
        } finally {
            scheduler.shutdownNow();
        }
    }

    @Test
    void leadershipLossAndRuntimeCancelReleaseListenersAndPendingReads() {
        ManualScheduler scheduler = new ManualScheduler();
        try {
            AtomicInteger callbackDispatches = new AtomicInteger();
            FakePartitionStorage fenced = new FakePartitionStorage(identity(5));
            CompletableFuture<KafkaStorageReadResult> fencedRead = new CompletableFuture<>();
            fenced.enqueue(fencedRead);
            KafkaFetchOperation fencedOperation = operation(
                    List.of(fenced), 1, 1024 * 1024, Duration.ofSeconds(5), 8,
                    command -> {
                        callbackDispatches.incrementAndGet();
                        command.run();
                    }, scheduler);
            CompletableFuture<KafkaFetchOperationResult> fencedCompletion = fencedOperation.start();

            fenced.emit(KafkaPartitionEventType.LEADERSHIP_LOST);

            assertFailureCode(fencedCompletion, ErrorCode.FENCED_APPEND);
            assertThat(fencedOperation.state()).isEqualTo(KafkaFetchOperationState.COMPLETE);
            assertThat(fencedOperation.inFlightReads()).isZero();
            assertThat(fencedRead).isCancelled();
            assertThat(fenced.listenerCount()).isZero();

            FakePartitionStorage cancelled = new FakePartitionStorage(identity(6));
            CompletableFuture<KafkaStorageReadResult> cancelledRead = new CompletableFuture<>();
            cancelled.enqueue(cancelledRead);
            KafkaFetchOperation cancelledOperation = operation(
                    List.of(cancelled), 1, 1024 * 1024, Duration.ofSeconds(5), 8,
                    command -> {
                        callbackDispatches.incrementAndGet();
                        command.run();
                    }, scheduler);
            CompletableFuture<KafkaFetchOperationResult> cancelledCompletion = cancelledOperation.start();

            cancelledOperation.cancel();

            assertFailureCode(cancelledCompletion, ErrorCode.CANCELLED);
            assertThat(cancelledOperation.state()).isEqualTo(KafkaFetchOperationState.CANCELLED);
            assertThat(cancelledOperation.inFlightReads()).isZero();
            assertThat(cancelledRead).isCancelled();
            assertThat(cancelled.listenerCount()).isZero();
            assertThat(callbackDispatches).hasValue(2);
        } finally {
            scheduler.shutdownNow();
        }
    }

    @Test
    void executorRejectionFailsBeforeReadAndReleasesTheListener() {
        ManualScheduler scheduler = new ManualScheduler();
        try {
            FakePartitionStorage storage = new FakePartitionStorage(identity(7));
            KafkaFetchOperation operation = new KafkaFetchOperation(
                    new KafkaFetchOperationRequest(
                            List.of(new KafkaFetchPartitionRequest(
                                    storage, readRequest(1024 * 1024))),
                            1,
                            1024 * 1024,
                            Duration.ofSeconds(5),
                            8),
                    command -> {
                        throw new RejectedExecutionException("full");
                    },
                    Runnable::run,
                    scheduler);

            CompletableFuture<KafkaFetchOperationResult> completion = operation.start();

            assertFailureCode(completion, ErrorCode.BACKPRESSURE_REJECTED);
            assertThat(storage.readCalls()).isZero();
            assertThat(storage.listenerCount()).isZero();
            assertThat(operation.state()).isEqualTo(KafkaFetchOperationState.COMPLETE);
        } finally {
            scheduler.shutdownNow();
        }
    }

    private static KafkaFetchOperation operation(
            List<FakePartitionStorage> storages,
            int minBytes,
            int maxResponseBytes,
            Duration maxWait,
            int maxRereads,
            java.util.concurrent.Executor callbackExecutor,
            ScheduledExecutorService scheduler) {
        List<KafkaFetchPartitionRequest> partitions = storages.stream()
                .map(storage -> new KafkaFetchPartitionRequest(
                        storage, readRequest(maxResponseBytes)))
                .toList();
        return new KafkaFetchOperation(
                new KafkaFetchOperationRequest(
                        partitions, minBytes, maxResponseBytes, maxWait, maxRereads),
                Runnable::run,
                callbackExecutor,
                scheduler);
    }

    private static KafkaStorageReadRequest readRequest(int maxResponseBytes) {
        return new KafkaStorageReadRequest(
                0, 10, 100, maxResponseBytes, maxResponseBytes, true, 0, 0, Duration.ofSeconds(5));
    }

    private static KafkaStorageReadResult emptyResult(long stableEnd) {
        ReadResult result = new ReadResult(
                STREAM_ID, 0, 0, List.of(), stableEnd == 0);
        return new KafkaStorageReadResult(
                ASSEMBLER.assemble(
                        new SemanticReadResult(ReadView.COMMITTED, result, 0),
                        1024 * 1024,
                        false,
                        0,
                        0,
                        List.of()),
                KafkaStableSnapshot.nonTransactional(0, stableEnd, 1));
    }

    private static KafkaStorageReadResult dataResult(byte[] batch, long endOffset) {
        ReadResult result = new ReadResult(
                STREAM_ID,
                0,
                endOffset,
                List.of(KafkaPartitionStorageTestSupport.readBatch(
                        new OffsetRange(0, endOffset), batch, 1)),
                true);
        return new KafkaStorageReadResult(
                ASSEMBLER.assemble(
                        new SemanticReadResult(ReadView.COMMITTED, result, endOffset),
                        1024 * 1024,
                        false,
                        0,
                        0,
                        List.of()),
                KafkaStableSnapshot.nonTransactional(0, endOffset, 1));
    }

    private static CompletableFuture<KafkaStorageReadResult> completed(KafkaStorageReadResult result) {
        return CompletableFuture.completedFuture(result);
    }

    private static KafkaPartitionIdentity identity(int partition) {
        KafkaPartitionIdentity base = KafkaPartitionStorageTestSupport.identity();
        return new KafkaPartitionIdentity(
                base.kafkaClusterId(), base.topicId(), partition, base.observedTopicName());
    }

    private static void assertFailureCode(
            CompletableFuture<?> completion, ErrorCode expected) {
        assertThatThrownBy(completion::join)
                .isInstanceOf(CompletionException.class)
                .satisfies(failure -> {
                    assertThat(failure.getCause()).isInstanceOf(NereusException.class);
                    assertThat(((NereusException) failure.getCause()).code()).isEqualTo(expected);
                });
    }

    private static final StreamId STREAM_ID = new StreamId("fetch-operation-test");
    private static final KafkaFetchAssembler ASSEMBLER =
            new KafkaFetchAssembler(new KafkaRecordBatchCodec());

    private static final class FakePartitionStorage implements KafkaPartitionStorage {
        private final KafkaPartitionIdentity identity;
        private final ArrayDeque<CompletableFuture<KafkaStorageReadResult>> reads = new ArrayDeque<>();
        private final Set<KafkaPartitionEventListener> listeners = new HashSet<>();
        private final AtomicInteger readCalls = new AtomicInteger();
        private final AtomicInteger maxInFlight = new AtomicInteger();
        private CompletableFuture<KafkaStorageReadResult> previousRead;

        private FakePartitionStorage(KafkaPartitionIdentity identity) {
            this.identity = identity;
        }

        private synchronized void enqueue(CompletableFuture<KafkaStorageReadResult> result) {
            reads.addLast(result);
        }

        private void emit(KafkaPartitionEventType type) {
            List<KafkaPartitionEventListener> snapshot;
            synchronized (this) {
                snapshot = new ArrayList<>(listeners);
            }
            KafkaPartitionEvent event = new KafkaPartitionEvent(
                    identity, type, KafkaStableSnapshot.nonTransactional(0, 0, 1));
            snapshot.forEach(listener -> listener.onPartitionEvent(event));
        }

        private int readCalls() {
            return readCalls.get();
        }

        private int maxInFlight() {
            return maxInFlight.get();
        }

        private synchronized int listenerCount() {
            return listeners.size();
        }

        @Override
        public KafkaPartitionIdentity identity() {
            return identity;
        }

        @Override
        public int leaderEpoch() {
            return 1;
        }

        @Override
        public KafkaPartitionState state() {
            return KafkaPartitionState.LEADER_WRITABLE;
        }

        @Override
        public KafkaStableSnapshot stableSnapshot() {
            return KafkaStableSnapshot.nonTransactional(0, 0, 1);
        }

        @Override
        public CompletableFuture<KafkaStableAppendResult> append(
                ByteBuffer validatedRecords, KafkaAppendContext context) {
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.INVALID_ARGUMENT,
                    false,
                    "append is unsupported by the Fetch test fake",
                    AppendOutcome.KNOWN_NOT_COMMITTED));
        }

        @Override
        public synchronized CompletableFuture<KafkaStorageReadResult> read(
                KafkaStorageReadRequest request) {
            if (reads.isEmpty()) throw new AssertionError("no scripted Kafka Fetch read result");
            if (previousRead != null && !previousRead.isDone()) {
                throw new AssertionError("a second read started while the previous partition read was in flight");
            }
            readCalls.incrementAndGet();
            CompletableFuture<KafkaStorageReadResult> result = reads.removeFirst();
            previousRead = result;
            maxInFlight.accumulateAndGet(1, Math::max);
            return result;
        }

        @Override
        public synchronized KafkaPartitionEventSubscription subscribe(
                KafkaPartitionEventListener listener) {
            listeners.add(listener);
            AtomicBoolean removed = new AtomicBoolean();
            return () -> {
                if (removed.compareAndSet(false, true)) {
                    synchronized (FakePartitionStorage.this) {
                        listeners.remove(listener);
                    }
                }
            };
        }

        @Override
        public CompletableFuture<Void> resign() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void close() {}
    }

    private static final class ManualScheduler extends ScheduledThreadPoolExecutor {
        private ManualScheduledFuture deadline;

        private ManualScheduler() {
            super(1);
        }

        @Override
        public synchronized ScheduledFuture<?> schedule(
                Runnable command, long delay, TimeUnit unit) {
            if (deadline != null && !deadline.isDone()) {
                throw new AssertionError("only one active Fetch deadline may be scheduled");
            }
            deadline = new ManualScheduledFuture(command);
            return deadline;
        }

        private synchronized void fireDeadline() {
            if (deadline == null) throw new AssertionError("Fetch deadline was not scheduled");
            deadline.run();
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
