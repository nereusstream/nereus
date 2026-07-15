/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import com.nereusstream.metadata.oxia.VersionedMaterializationTask;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;

class DefaultMaterializationTaskDispatcherTest {
    @Test
    void enforcesGlobalAndPerStreamConcurrencyWhileSkippingASaturatedStream() throws Exception {
        ControlledWorker worker = new ControlledWorker();
        DefaultMaterializationTaskDispatcher dispatcher = new DefaultMaterializationTaskDispatcher(
                worker,
                (task, output) -> CompletableFuture.completedFuture(null),
                Runnable::run,
                2,
                1);
        MaterializationTask a1 = task(new StreamId("stream-a"), false);
        MaterializationTask a2 = task(new StreamId("stream-a"), true);
        MaterializationTask b1 = task(new StreamId("stream-b"), false);
        MaterializationTask b2 = task(new StreamId("stream-b"), true);

        CompletableFuture<Void> first = dispatcher.dispatch(durable(a1, 1), a1);
        CompletableFuture<Void> second = dispatcher.dispatch(durable(a2, 2), a2);
        CompletableFuture<Void> third = dispatcher.dispatch(durable(b1, 3), b1);
        CompletableFuture<Void> fourth = dispatcher.dispatch(durable(b2, 4), b2);

        assertThat(worker.started).containsExactly(a1.taskId(), b1.taskId());
        assertThat(dispatcher.activeTaskCount()).isEqualTo(2);
        assertThat(dispatcher.queuedTaskCount()).isEqualTo(2);

        worker.complete(a1);
        await(() -> worker.started.contains(a2.taskId()));
        assertThat(worker.started).containsExactly(a1.taskId(), b1.taskId(), a2.taskId());
        assertThat(dispatcher.activeTaskCount()).isEqualTo(2);

        worker.complete(b1);
        await(() -> worker.started.contains(b2.taskId()));
        worker.complete(a2);
        worker.complete(b2);
        CompletableFuture.allOf(first, second, third, fourth).get(2, TimeUnit.SECONDS);
        assertThat(dispatcher.activeTaskCount()).isZero();
        assertThat(dispatcher.queuedTaskCount()).isZero();
    }

    @Test
    void duplicateLocalDispatchesShareOneWorkerAndPublication() throws Exception {
        ControlledWorker worker = new ControlledWorker();
        AtomicInteger publications = new AtomicInteger();
        DefaultMaterializationTaskDispatcher dispatcher = new DefaultMaterializationTaskDispatcher(
                worker,
                (task, output) -> {
                    publications.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                },
                Runnable::run,
                2,
                1);
        MaterializationTask task = task(new StreamId("stream-duplicate"), false);
        VersionedMaterializationTask durable = durable(task, 1);

        CompletableFuture<Void> first = dispatcher.dispatch(durable, task);
        CompletableFuture<Void> duplicate = dispatcher.dispatch(durable, task);
        assertThat(worker.started).containsExactly(task.taskId());

        worker.complete(task);
        CompletableFuture.allOf(first, duplicate).get(2, TimeUnit.SECONDS);

        assertThat(worker.started).containsExactly(task.taskId());
        assertThat(publications).hasValue(1);
    }

    @Test
    void closeRejectsQueuedWorkAndCancelsActiveWorkAtTheDeadline() throws Exception {
        ControlledWorker worker = new ControlledWorker();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        DefaultMaterializationTaskDispatcher dispatcher = new DefaultMaterializationTaskDispatcher(
                worker,
                (task, output) -> CompletableFuture.completedFuture(null),
                Runnable::run,
                1,
                1);
        MaterializationTask active = task(new StreamId("stream-close-active"), false);
        MaterializationTask queued = task(new StreamId("stream-close-queued"), false);
        CompletableFuture<Void> activeCall = dispatcher.dispatch(durable(active, 1), active);
        CompletableFuture<Void> queuedCall = dispatcher.dispatch(durable(queued, 2), queued);
        try {
            CompletableFuture<Void> close = dispatcher.closeAsync(
                    Duration.ofMillis(50), scheduler);

            assertThatThrownBy(queuedCall::join)
                    .hasRootCauseInstanceOf(NereusException.class)
                    .satisfies(failure -> assertThat(root(failure))
                            .isInstanceOfSatisfying(NereusException.class, nereus ->
                                    assertThat(nereus.code()).isEqualTo(ErrorCode.STORAGE_CLOSED)));
            close.get(2, TimeUnit.SECONDS);
            assertThat(worker.cancellations).hasValue(1);
            assertThat(activeCall).isCompletedExceptionally();
            assertThat(dispatcher.activeTaskCount()).isZero();
            assertThatThrownBy(() -> dispatcher.dispatch(durable(active, 3), active).join())
                    .hasRootCauseInstanceOf(NereusException.class);
        } finally {
            scheduler.shutdownNow();
        }
    }

    private static MaterializationTask task(StreamId streamId, boolean alternatePolicy) {
        List<VersionedGenerationCandidate> candidates = List.of(
                MaterializationPlannerTestSupport.zero("/index/dispatch-2", 0, 2, 0, 100, 2),
                MaterializationPlannerTestSupport.zero("/index/dispatch-4", 2, 4, 100, 100, 4));
        MaterializationTask base = MaterializationPlannerTestSupport.planner(
                        candidates, List.of(), 0, 4)
                .plan(
                        MaterializationPlannerTestSupport.STREAM,
                        new OffsetRange(0, 4),
                        MaterializationPlannerTestSupport.policy(),
                        1)
                .join()
                .get(0);
        MaterializationPolicy policy = alternatePolicy
                ? MaterializationPolicyFactory.losslessCommitted(
                        2, 16, 1_000, 1_000_000, 128, "UNCOMPRESSED")
                : base.policy();
        return MaterializationTask.create(streamId, base.coverage(), base.sources(), policy);
    }

    private static VersionedMaterializationTask durable(MaterializationTask task, long version) {
        return MaterializationPlannerTestSupport.durableTask(task, version);
    }

    private static void await(BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() >= deadline) {
                throw new AssertionError("condition did not become true before timeout");
            }
            Thread.sleep(5);
        }
    }

    private static Throwable root(Throwable failure) {
        Throwable current = failure;
        while (current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static final class ControlledWorker implements MaterializationWorker {
        private final List<String> started = new CopyOnWriteArrayList<>();
        private final Map<String, CompletableFuture<MaterializationOutput>> operations =
                new ConcurrentHashMap<>();
        private final AtomicInteger cancellations = new AtomicInteger();

        @Override
        public CompletableFuture<MaterializationOutput> execute(MaterializationTask task) {
            started.add(task.taskId());
            CompletableFuture<MaterializationOutput> operation = new CompletableFuture<>();
            operations.put(task.taskId(), operation);
            return operation;
        }

        @Override
        public void cancel(MaterializationTask task) {
            cancellations.incrementAndGet();
            CompletableFuture<MaterializationOutput> operation = operations.get(task.taskId());
            if (operation != null) {
                operation.cancel(true);
            }
        }

        private void complete(MaterializationTask task) {
            operations.get(task.taskId()).complete(null);
        }
    }
}
