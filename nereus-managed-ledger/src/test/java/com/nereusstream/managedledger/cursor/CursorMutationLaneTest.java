/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.junit.jupiter.api.Test;

class CursorMutationLaneTest {
    @Test
    void runsOneOperationAtATimeInFifoOrder() {
        ManualExecutor executor = new ManualExecutor();
        CursorMutationLane lane = new CursorMutationLane(4, executor);
        List<String> events = new ArrayList<>();
        CompletableFuture<String> firstOperation = new CompletableFuture<>();

        CompletableFuture<String> first = lane.submit(() -> {
            events.add("first-start");
            return firstOperation;
        });
        CompletableFuture<String> second = lane.submit(() -> {
            events.add("second-start");
            return CompletableFuture.completedFuture("second");
        });

        assertThat(lane.pendingOperations()).isEqualTo(2);
        executor.runNext();
        assertThat(events).containsExactly("first-start");
        assertThat(second).isNotDone();

        firstOperation.complete("first");
        assertThat(first).isCompletedWithValue("first");
        assertThat(events).containsExactly("first-start");
        executor.runNext();

        assertThat(second).isCompletedWithValue("second");
        assertThat(events).containsExactly("first-start", "second-start");
        assertThat(lane.pendingOperations()).isZero();
    }

    @Test
    void rejectsAdmissionAtBoundWithoutStartingTheRejectedOperation() {
        ManualExecutor executor = new ManualExecutor();
        CursorMutationLane lane = new CursorMutationLane(2, executor);
        CompletableFuture<String> blocker = new CompletableFuture<>();
        lane.submit(() -> blocker);
        lane.submit(() -> CompletableFuture.completedFuture("queued"));

        CompletableFuture<String> rejected = lane.submit(
                () -> CompletableFuture.completedFuture("must-not-run"));

        assertThatThrownBy(rejected::join)
                .hasCauseInstanceOf(ManagedLedgerException.TooManyRequestsException.class)
                .hasRootCauseMessage("durable cursor mutation queue is full");
        assertThat(lane.pendingOperations()).isEqualTo(2);
    }

    @Test
    void closeFailsQueuedAndNewOperationsButLetsRunningOperationFinish() {
        ManualExecutor executor = new ManualExecutor();
        CursorMutationLane lane = new CursorMutationLane(3, executor);
        CompletableFuture<String> runningOperation = new CompletableFuture<>();
        CompletableFuture<String> running = lane.submit(() -> runningOperation);
        CompletableFuture<String> queued = lane.submit(
                () -> CompletableFuture.completedFuture("queued"));
        executor.runNext();
        IllegalStateException closed = new IllegalStateException("closed");

        lane.close(closed);

        assertThatThrownBy(queued::join).hasCause(closed);
        assertThat(lane.pendingOperations()).isEqualTo(1);
        CompletableFuture<String> afterClose = lane.submit(
                () -> CompletableFuture.completedFuture("late"));
        assertThatThrownBy(afterClose::join).hasCause(closed);

        runningOperation.complete("done");
        assertThat(running).isCompletedWithValue("done");
        assertThat(lane.pendingOperations()).isZero();
        lane.close(new IllegalArgumentException("ignored"));
    }

    @Test
    void operationFailureIsUnwrappedAndDoesNotBlockFollowingWork() {
        CursorMutationLane lane = new CursorMutationLane(3, Runnable::run);
        IllegalArgumentException failure = new IllegalArgumentException("bad mutation");

        CompletableFuture<String> failed = lane.submit(
                () -> CompletableFuture.failedFuture(failure));
        CompletableFuture<String> following = lane.submit(
                () -> CompletableFuture.completedFuture("ok"));

        assertThatThrownBy(failed::join).hasCause(failure);
        assertThat(following).isCompletedWithValue("ok");
        assertThat(lane.pendingOperations()).isZero();
    }

    @Test
    void executorRejectionFailsEveryAdmittedOperationExactlyOnce() {
        RejectedExecutionException rejection = new RejectedExecutionException("executor stopped");
        Executor executor = command -> {
            throw rejection;
        };
        CursorMutationLane lane = new CursorMutationLane(2, executor);

        CompletableFuture<String> result = lane.submit(
                () -> CompletableFuture.completedFuture("never"));

        assertThatThrownBy(result::join).hasCause(rejection);
        assertThat(lane.pendingOperations()).isZero();
        assertThatThrownBy(() -> lane.submit(
                        () -> CompletableFuture.completedFuture("late"))
                .join())
                .hasCause(rejection);
    }

    private static final class ManualExecutor implements Executor {
        private final ArrayDeque<Runnable> tasks = new ArrayDeque<>();

        @Override
        public void execute(Runnable command) {
            tasks.addLast(command);
        }

        private void runNext() {
            Runnable task = tasks.pollFirst();
            if (task == null) {
                throw new AssertionError("no scheduled task");
            }
            task.run();
        }
    }
}
