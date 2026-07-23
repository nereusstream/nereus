/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.AppendOutcome;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class KafkaBoundedAppendExecutorTest {
    @Test
    void snapshotCopiesExactRemainingBytesAndReleasesItsLeaseOnce() {
        KafkaByteBudget budget = new KafkaByteBudget(16);
        ByteBuffer request = ByteBuffer.allocate(8).putInt(1).putInt(2).flip();
        request.position(4);
        int originalPosition = request.position();
        KafkaProduceBufferSnapshot snapshot = KafkaProduceBufferSnapshot.capture(request, budget);

        request.putInt(4, 99);

        assertThat(request.position()).isEqualTo(originalPosition);
        assertThat(snapshot.sizeInBytes()).isEqualTo(4);
        assertThat(snapshot.buffer().isReadOnly()).isTrue();
        assertThat(snapshot.buffer().getInt()).isEqualTo(2);
        assertThat(budget.usedBytes()).isEqualTo(4);
        snapshot.close();
        snapshot.close();
        assertThat(budget.usedBytes()).isZero();
        assertThatThrownBy(snapshot::buffer).hasMessageContaining("closed");
    }

    @Test
    void queueRejectionOccursBeforeTaskAndCancellationDoesNotCancelAdmittedWork() throws Exception {
        KafkaByteBudget budget = new KafkaByteBudget(64);
        KafkaBoundedAppendExecutor executor = new KafkaBoundedAppendExecutor(1, 1, budget, "f9-append");
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        CountDownLatch secondStarted = new CountDownLatch(1);
        CountDownLatch releaseSecond = new CountDownLatch(1);
        AtomicBoolean rejectedRan = new AtomicBoolean();

        CompletableFuture<Integer> first = executor.submit(bytes(1), owned -> {
            firstStarted.countDown();
            require(releaseFirst.await(5, TimeUnit.SECONDS));
            return owned.getInt();
        });
        require(firstStarted.await(5, TimeUnit.SECONDS));
        CompletableFuture<Integer> second = executor.submit(bytes(2), owned -> {
            secondStarted.countDown();
            require(releaseSecond.await(5, TimeUnit.SECONDS));
            return owned.getInt();
        });
        CompletableFuture<Integer> rejected = executor.submit(bytes(3), owned -> {
            rejectedRan.set(true);
            return owned.getInt();
        });

        assertFailure(rejected, ErrorCode.BACKPRESSURE_REJECTED, AppendOutcome.KNOWN_NOT_COMMITTED);
        assertThat(rejectedRan).isFalse();
        assertThat(executor.ownedBufferBytes()).isEqualTo(8);
        assertThat(second.cancel(false)).isTrue();

        releaseFirst.countDown();
        assertThat(first.join()).isEqualTo(1);
        require(secondStarted.await(5, TimeUnit.SECONDS));
        CompletableFuture<Integer> barrier = executor.submit(bytes(4), ByteBuffer::getInt);
        releaseSecond.countDown();
        assertThat(barrier.join()).isEqualTo(4);

        assertThat(second).isCancelled();
        assertThat(executor.ownedBufferBytes()).isZero();
        executor.close();
    }

    @Test
    void byteBudgetRejectsBeforeQueueAdmissionAndTaskFailureReleasesOwnership() {
        KafkaByteBudget budget = new KafkaByteBudget(4);
        KafkaBoundedAppendExecutor executor = new KafkaBoundedAppendExecutor(1, 1, budget, "f9-append");
        AtomicBoolean ran = new AtomicBoolean();

        CompletableFuture<Integer> tooLarge = executor.submit(ByteBuffer.allocate(5), owned -> {
            ran.set(true);
            return 1;
        });
        assertFailure(tooLarge, ErrorCode.BACKPRESSURE_REJECTED, AppendOutcome.KNOWN_NOT_COMMITTED);
        assertThat(ran).isFalse();
        assertThat(executor.queuedTasks()).isZero();

        CompletableFuture<Integer> failed = executor.submit(bytes(1), owned -> {
            throw new IllegalStateException("task failure");
        });
        assertThatThrownBy(failed::join).hasRootCauseMessage("task failure");
        assertThat(executor.ownedBufferBytes()).isZero();
        executor.close();
        assertFailure(executor.submit(bytes(2), ByteBuffer::getInt),
                ErrorCode.STORAGE_CLOSED, AppendOutcome.KNOWN_NOT_COMMITTED);
    }

    @Test
    void orderingKeyIsFifoWhileDifferentKeysRunConcurrentlyAndDrainWaitsForAcceptedWork() throws Exception {
        KafkaBoundedAppendExecutor executor =
                new KafkaBoundedAppendExecutor(2, 4, new KafkaByteBudget(64), "f9-keyed-append");
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        CountDownLatch otherKeyStarted = new CountDownLatch(1);
        List<Integer> sameKeyOrder = new CopyOnWriteArrayList<>();

        CompletableFuture<Integer> first = executor.submit("partition-a", bytes(1), owned -> {
            firstStarted.countDown();
            require(releaseFirst.await(5, TimeUnit.SECONDS));
            sameKeyOrder.add(owned.getInt());
            return 1;
        });
        require(firstStarted.await(5, TimeUnit.SECONDS));
        CompletableFuture<Integer> second = executor.submit("partition-a", bytes(2), owned -> {
            sameKeyOrder.add(owned.getInt());
            return 2;
        });
        CompletableFuture<Integer> other = executor.submit("partition-b", bytes(3), owned -> {
            otherKeyStarted.countDown();
            return owned.getInt();
        });

        require(otherKeyStarted.await(5, TimeUnit.SECONDS));
        assertThat(other.join()).isEqualTo(3);
        assertThat(second).isNotDone();
        assertThat(executor.ownedBufferBytes()).isEqualTo(8);

        CompletableFuture<Void> cancelledView = executor.drainedFuture();
        assertThat(cancelledView.cancel(false)).isTrue();
        CompletableFuture<Void> drained = executor.drainedFuture();
        executor.close();
        assertThat(drained).isNotDone();

        releaseFirst.countDown();
        assertThat(first.join()).isEqualTo(1);
        assertThat(second.join()).isEqualTo(2);
        drained.get(5, TimeUnit.SECONDS);
        assertThat(sameKeyOrder).containsExactly(1, 2);
        assertThat(executor.activeTasks()).isZero();
        assertThat(executor.queuedTasks()).isZero();
        assertThat(executor.ownedBufferBytes()).isZero();
    }

    @Test
    void oneWorkerYieldsBetweenSameKeyTasksAndCloseStillDrainsLogicalLaneQueue() throws Exception {
        KafkaBoundedAppendExecutor executor =
                new KafkaBoundedAppendExecutor(1, 3, new KafkaByteBudget(64), "f9-fair-append");
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        List<Integer> executionOrder = new CopyOnWriteArrayList<>();

        CompletableFuture<Integer> first = executor.submit("partition-a", bytes(1), owned -> {
            firstStarted.countDown();
            require(releaseFirst.await(5, TimeUnit.SECONDS));
            executionOrder.add(owned.getInt());
            return 1;
        });
        require(firstStarted.await(5, TimeUnit.SECONDS));
        CompletableFuture<Integer> sameKey = executor.submit("partition-a", bytes(2), owned -> {
            executionOrder.add(owned.getInt());
            return 2;
        });
        CompletableFuture<Integer> otherKey = executor.submit("partition-b", bytes(3), owned -> {
            executionOrder.add(owned.getInt());
            return 3;
        });

        executor.close();
        releaseFirst.countDown();
        assertThat(first.join()).isEqualTo(1);
        assertThat(otherKey.join()).isEqualTo(3);
        assertThat(sameKey.join()).isEqualTo(2);
        executor.drainedFuture().get(5, TimeUnit.SECONDS);
        assertThat(executionOrder).containsExactly(1, 3, 2);
    }

    private static ByteBuffer bytes(int value) {
        return ByteBuffer.allocate(4).putInt(value).flip();
    }

    private static void assertFailure(
            CompletableFuture<?> future, ErrorCode code, AppendOutcome outcome) {
        assertThatThrownBy(future::join)
                .isInstanceOf(CompletionException.class)
                .satisfies(value -> {
                    assertThat(value.getCause()).isInstanceOf(NereusException.class);
                    NereusException failure = (NereusException) value.getCause();
                    assertThat(failure.code()).isEqualTo(code);
                    assertThat(failure.appendOutcome()).contains(outcome);
                });
    }

    private static void require(boolean value) {
        if (!value) throw new AssertionError("timed out waiting for deterministic test event");
    }
}
