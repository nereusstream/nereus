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

package com.nereusstream.kafka.fetch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class KafkaFetchWaveOperationTest {
    @Test
    void completesInitialEnoughResponseOnCallbackExecutor() throws Exception {
        try (ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor()) {
            NamedExecutor readExecutor = new NamedExecutor("fetch-read");
            NamedExecutor callbackExecutor = new NamedExecutor("fetch-callback");
            TestSource source = new TestSource();
            source.results.add(CompletableFuture.completedFuture(12));
            KafkaFetchWaveOperation<Integer> operation = operation(
                    source, 10, Duration.ofSeconds(1), 4, readExecutor, callbackExecutor, scheduler);

            KafkaFetchWaveResult<Integer> result = operation.start().get(5, TimeUnit.SECONDS);

            assertThat(result.response()).isEqualTo(12);
            assertThat(result.responseBytes()).isEqualTo(12);
            assertThat(result.timedOut()).isFalse();
            assertThat(result.readAttempts()).isOne();
            assertThat(source.initialFlags).containsExactly(true);
            assertThat(source.closed).isTrue();
            assertThat(callbackExecutor.lastThreadName).startsWith("fetch-callback");
            readExecutor.close();
            callbackExecutor.close();
        }
    }

    @Test
    void coalescesSignalsAndKeepsOneReadInFlight() throws Exception {
        try (ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor()) {
            NamedExecutor readExecutor = new NamedExecutor("fetch-read");
            NamedExecutor callbackExecutor = new NamedExecutor("fetch-callback");
            TestSource source = new TestSource();
            source.results.add(CompletableFuture.completedFuture(0));
            CompletableFuture<Integer> reread = new CompletableFuture<>();
            source.results.add(reread);
            KafkaFetchWaveOperation<Integer> operation = operation(
                    source, 10, Duration.ofSeconds(2), 4, readExecutor, callbackExecutor, scheduler);

            CompletableFuture<KafkaFetchWaveResult<Integer>> completion = operation.start();
            await(() -> operation.state() == KafkaFetchOperationState.WAITING);
            source.signal();
            await(operation::inFlight);
            source.signal();
            source.signal();

            assertThat(operation.readAttempts()).isEqualTo(2);
            reread.complete(10);
            KafkaFetchWaveResult<Integer> result = completion.get(5, TimeUnit.SECONDS);

            assertThat(result.response()).isEqualTo(10);
            assertThat(result.readAttempts()).isEqualTo(2);
            assertThat(source.maxConcurrentReads.get()).isOne();
            assertThat(source.initialFlags).containsExactly(true, false);
            readExecutor.close();
            callbackExecutor.close();
        }
    }

    @Test
    void deadlinePerformsFinalReadAfterEventRereadBudgetIsExhausted() throws Exception {
        try (ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor()) {
            NamedExecutor readExecutor = new NamedExecutor("fetch-read");
            NamedExecutor callbackExecutor = new NamedExecutor("fetch-callback");
            TestSource source = new TestSource();
            source.results.add(CompletableFuture.completedFuture(0));
            source.results.add(CompletableFuture.completedFuture(0));
            source.results.add(CompletableFuture.completedFuture(3));
            KafkaFetchWaveOperation<Integer> operation = operation(
                    source, 10, Duration.ofMillis(100), 1, readExecutor, callbackExecutor, scheduler);

            CompletableFuture<KafkaFetchWaveResult<Integer>> completion = operation.start();
            await(() -> operation.state() == KafkaFetchOperationState.WAITING);
            source.signal();
            await(() -> operation.readAttempts() == 2
                    && operation.state() == KafkaFetchOperationState.WAITING);

            KafkaFetchWaveResult<Integer> result = completion.get(5, TimeUnit.SECONDS);

            assertThat(result.response()).isEqualTo(3);
            assertThat(result.timedOut()).isTrue();
            assertThat(result.readAttempts()).isEqualTo(3);
            assertThat(source.initialFlags).containsExactly(true, false, false);
            readExecutor.close();
            callbackExecutor.close();
        }
    }

    @Test
    void executorRejectionFailsBeforeReadAndCleansSubscription() {
        try (ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor()) {
            TestSource source = new TestSource();
            Executor rejected = ignored -> {
                throw new java.util.concurrent.RejectedExecutionException("full");
            };
            KafkaFetchWaveOperation<Integer> operation = operation(
                    source, 1, Duration.ofSeconds(1), 2, rejected, Runnable::run, scheduler);

            assertThatThrownBy(operation.start()::join)
                    .hasCauseInstanceOf(com.nereusstream.api.NereusException.class)
                    .hasRootCauseInstanceOf(java.util.concurrent.RejectedExecutionException.class);
            assertThat(source.readCalls).hasValue(0);
            assertThat(source.closed).isTrue();
        }
    }

    @Test
    void externalCancellationCannotBypassExplicitCancelCleanup() {
        try (ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor()) {
            TestSource source = new TestSource();
            source.results.add(new CompletableFuture<>());
            KafkaFetchWaveOperation<Integer> operation = operation(
                    source, 1, Duration.ofSeconds(1), 2, Runnable::run, Runnable::run, scheduler);

            CompletableFuture<KafkaFetchWaveResult<Integer>> completion = operation.start();
            assertThat(completion.cancel(true)).isFalse();
            assertThat(completion.isDone()).isFalse();

            operation.cancel();

            assertThatThrownBy(completion::join)
                    .hasCauseInstanceOf(com.nereusstream.api.NereusException.class);
            assertThat(operation.state()).isEqualTo(KafkaFetchOperationState.CANCELLED);
            assertThat(source.closed).isTrue();
        }
    }

    private static KafkaFetchWaveOperation<Integer> operation(
            TestSource source,
            int minBytes,
            Duration maxWait,
            int maxRereads,
            Executor readExecutor,
            Executor callbackExecutor,
            ScheduledExecutorService scheduler) {
        return new KafkaFetchWaveOperation<>(
                source,
                minBytes,
                maxWait,
                maxRereads,
                Integer::intValue,
                ignored -> false,
                readExecutor,
                callbackExecutor,
                scheduler);
    }

    private static void await(java.util.function.BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() >= deadline) {
                throw new AssertionError("condition did not become true");
            }
            Thread.onSpinWait();
        }
    }

    private static final class TestSource implements KafkaFetchWaveSource<Integer> {
        private final ArrayDeque<CompletableFuture<Integer>> results = new ArrayDeque<>();
        private final java.util.List<Boolean> initialFlags =
                new java.util.concurrent.CopyOnWriteArrayList<>();
        private final AtomicInteger readCalls = new AtomicInteger();
        private final AtomicInteger concurrentReads = new AtomicInteger();
        private final AtomicInteger maxConcurrentReads = new AtomicInteger();
        private final AtomicBoolean closed = new AtomicBoolean();
        private volatile Runnable wakeup;

        @Override
        public CompletableFuture<Integer> read(boolean initialWave) {
            readCalls.incrementAndGet();
            initialFlags.add(initialWave);
            int concurrent = concurrentReads.incrementAndGet();
            maxConcurrentReads.accumulateAndGet(concurrent, Math::max);
            CompletableFuture<Integer> result = results.removeFirst();
            result.whenComplete((ignored, failure) -> concurrentReads.decrementAndGet());
            return result;
        }

        @Override
        public AutoCloseable subscribe(Runnable wakeup) {
            this.wakeup = wakeup;
            return () -> closed.set(true);
        }

        private void signal() {
            wakeup.run();
        }
    }

    private static final class NamedExecutor implements Executor, AutoCloseable {
        private final java.util.concurrent.ExecutorService delegate;
        private volatile String lastThreadName;

        private NamedExecutor(String prefix) {
            AtomicInteger id = new AtomicInteger();
            delegate = Executors.newSingleThreadExecutor(task -> {
                Thread thread = new Thread(task, prefix + "-" + id.incrementAndGet());
                thread.setDaemon(true);
                return thread;
            });
        }

        @Override
        public void execute(Runnable command) {
            delegate.execute(() -> {
                lastThreadName = Thread.currentThread().getName();
                command.run();
            });
        }

        @Override
        public void close() throws Exception {
            delegate.shutdownNow();
            assertThat(delegate.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }
}
