/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.kafka.fetch.KafkaFetchWaveOperation;
import com.nereusstream.kafka.fetch.KafkaFetchWaveResult;
import com.nereusstream.kafka.fetch.KafkaFetchWaveSource;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * F9-M7 shared 1,000-operation Produce/Fetch pressure boundary.
 *
 * <p>The operations are all admitted before either provider gate is released. This makes queue,
 * thread, byte-ownership, progress, and release-once assertions deterministic instead of relying on
 * timing or an eventually saturated machine.
 */
class KafkaIoConcurrencyStressTest {
    private static final int APPENDS = 500;
    private static final int FETCHES = 500;
    private static final int WORKERS = 8;
    private static final int APPEND_QUEUE = APPENDS - WORKERS;
    private static final int FETCH_QUEUE = FETCHES;

    @Test
    @Timeout(value = 1, unit = TimeUnit.MINUTES)
    void scenarioKfScl003() throws Exception {
        KafkaByteBudget byteBudget = new KafkaByteBudget(APPENDS * Integer.BYTES);
        KafkaBoundedAppendExecutor appends =
                new KafkaBoundedAppendExecutor(
                        WORKERS, APPEND_QUEUE, byteBudget, "f9-scale-append");
        CountDownLatch appendWorkersStarted = new CountDownLatch(WORKERS);
        CountDownLatch releaseAppends = new CountDownLatch(1);
        List<CompletableFuture<Integer>> appendResults =
                new ArrayList<>(APPENDS);
        for (int index = 0; index < APPENDS; index++) {
            int expected = index;
            ByteBuffer request = ByteBuffer.allocate(Integer.BYTES).putInt(index).flip();
            appendResults.add(
                    appends.submit(
                            index,
                            request,
                            owned -> {
                                appendWorkersStarted.countDown();
                                if (!releaseAppends.await(30, TimeUnit.SECONDS)) {
                                    throw new AssertionError(
                                            "append scale gate was not released");
                                }
                                return owned.getInt();
                            }));
            assertThat(appendResults.getLast())
                    .as("append %s must be admitted", expected)
                    .isNotCompletedExceptionally();
        }
        assertThat(appendWorkersStarted.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(appends.activeTasks()).isEqualTo(WORKERS);
        assertThat(appends.queuedTasks()).isEqualTo(APPEND_QUEUE);
        assertThat(appends.ownedBufferBytes())
                .isEqualTo((long) APPENDS * Integer.BYTES);

        AtomicInteger fetchThreadIds = new AtomicInteger();
        ThreadFactory fetchThreadFactory =
                runnable -> {
                    Thread thread =
                            new Thread(
                                    runnable,
                                    "f9-scale-fetch-"
                                            + fetchThreadIds.incrementAndGet());
                    thread.setDaemon(true);
                    return thread;
                };
        ThreadPoolExecutor fetchExecutor =
                new ThreadPoolExecutor(
                        WORKERS,
                        WORKERS,
                        0,
                        TimeUnit.MILLISECONDS,
                        new ArrayBlockingQueue<>(FETCH_QUEUE),
                        fetchThreadFactory,
                        new ThreadPoolExecutor.AbortPolicy());
        java.util.concurrent.ScheduledThreadPoolExecutor deadlines =
                new java.util.concurrent.ScheduledThreadPoolExecutor(1);
        deadlines.setRemoveOnCancelPolicy(true);
        CompletableFuture<Void> releaseFetches = new CompletableFuture<>();
        CountDownLatch allFetchReadsStarted = new CountDownLatch(FETCHES);
        AtomicInteger activeFetchReads = new AtomicInteger();
        AtomicInteger maximumFetchReads = new AtomicInteger();
        AtomicInteger closedFetchSources = new AtomicInteger();
        List<CompletableFuture<KafkaFetchWaveResult<Integer>>> fetchResults =
                new ArrayList<>(FETCHES);
        try {
            for (int index = 0; index < FETCHES; index++) {
                HoldingFetchSource source =
                        new HoldingFetchSource(
                                releaseFetches,
                                allFetchReadsStarted,
                                activeFetchReads,
                                maximumFetchReads,
                                closedFetchSources);
                KafkaFetchWaveOperation<Integer> operation =
                        new KafkaFetchWaveOperation<>(
                                source,
                                1,
                                Duration.ofMinutes(1),
                                1,
                                Integer::intValue,
                                ignored -> false,
                                fetchExecutor,
                                Runnable::run,
                                deadlines);
                fetchResults.add(operation.start());
            }
            assertThat(allFetchReadsStarted.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(activeFetchReads).hasValue(FETCHES);
            assertThat(maximumFetchReads).hasValue(FETCHES);
            assertThat(fetchExecutor.getLargestPoolSize())
                    .isLessThanOrEqualTo(WORKERS);
            assertThat(fetchExecutor.getQueue().size())
                    .isLessThanOrEqualTo(FETCH_QUEUE);

            releaseAppends.countDown();
            releaseFetches.complete(null);
            CompletableFuture.allOf(
                            appendResults.toArray(CompletableFuture[]::new))
                    .get(30, TimeUnit.SECONDS);
            CompletableFuture.allOf(
                            fetchResults.toArray(CompletableFuture[]::new))
                    .get(30, TimeUnit.SECONDS);

            for (int index = 0; index < APPENDS; index++) {
                assertThat(appendResults.get(index).join()).isEqualTo(index);
            }
            assertThat(fetchResults)
                    .allSatisfy(
                            result -> {
                                KafkaFetchWaveResult<Integer> fetched =
                                        result.join();
                                assertThat(fetched.response()).isOne();
                                assertThat(fetched.responseBytes()).isOne();
                                assertThat(fetched.readAttempts()).isOne();
                            });
            assertThat(activeFetchReads).hasValue(0);
            assertThat(closedFetchSources).hasValue(FETCHES);
            assertThat(appends.ownedBufferBytes()).isZero();
            assertThat(appends.activeTasks()).isZero();
            assertThat(appends.queuedTasks()).isZero();
        } finally {
            releaseAppends.countDown();
            releaseFetches.complete(null);
            appends.close();
            appends.drainedFuture().get(30, TimeUnit.SECONDS);
            fetchExecutor.shutdownNow();
            deadlines.shutdownNow();
        }
    }

    private static final class HoldingFetchSource
            implements KafkaFetchWaveSource<Integer> {
        private final CompletableFuture<Void> release;
        private final CountDownLatch started;
        private final AtomicInteger active;
        private final AtomicInteger maximumActive;
        private final AtomicInteger closedSources;
        private final AtomicBoolean subscribed = new AtomicBoolean();

        private HoldingFetchSource(
                CompletableFuture<Void> release,
                CountDownLatch started,
                AtomicInteger active,
                AtomicInteger maximumActive,
                AtomicInteger closedSources) {
            this.release = release;
            this.started = started;
            this.active = active;
            this.maximumActive = maximumActive;
            this.closedSources = closedSources;
        }

        @Override
        public CompletableFuture<Integer> read(boolean initialWave) {
            assertThat(initialWave).isTrue();
            int current = active.incrementAndGet();
            maximumActive.accumulateAndGet(current, Math::max);
            started.countDown();
            return release.thenApply(ignored -> 1)
                    .whenComplete((ignored, failure) -> active.decrementAndGet());
        }

        @Override
        public AutoCloseable subscribe(Runnable wakeup) {
            assertThat(subscribed.compareAndSet(false, true)).isTrue();
            return () -> closedSources.incrementAndGet();
        }
    }
}
