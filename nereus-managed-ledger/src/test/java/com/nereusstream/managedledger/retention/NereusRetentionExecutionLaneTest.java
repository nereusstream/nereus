/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.retention;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StreamId;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class NereusRetentionExecutionLaneTest {
    @Test
    void coalescesOneExecutionPerStreamButReturnsIndependentCompletions()
            throws Exception {
        NereusRetentionExecutionLane lane = lane(2, 4, Duration.ofSeconds(2));
        try {
            StreamId stream = new StreamId("retention-coalesce");
            CountDownLatch started = new CountDownLatch(1);
            AtomicInteger invocations = new AtomicInteger();
            CompletableFuture<Optional<RetentionCandidate>> source =
                    new CompletableFuture<>();
            CompletableFuture<Optional<RetentionCandidate>> first = lane.submit(
                    stream,
                    () -> {
                        invocations.incrementAndGet();
                        started.countDown();
                        return source;
                    });
            assertThat(started.await(2, TimeUnit.SECONDS)).isTrue();
            CompletableFuture<Optional<RetentionCandidate>> second = lane.submit(
                    stream,
                    () -> {
                        invocations.incrementAndGet();
                        return CompletableFuture.completedFuture(Optional.empty());
                    });

            assertThat(first).isNotSameAs(second);
            assertThat(first.cancel(false)).isTrue();
            source.complete(Optional.empty());

            assertThat(second.join()).isEmpty();
            assertThat(invocations).hasValue(1);
        } finally {
            lane.close();
        }
    }

    @Test
    void holdsConcurrencyUntilAsyncCompletionAndRejectsQueueOverflow()
            throws Exception {
        NereusRetentionExecutionLane lane = lane(1, 1, Duration.ofSeconds(2));
        try {
            CountDownLatch firstStarted = new CountDownLatch(1);
            CountDownLatch secondStarted = new CountDownLatch(1);
            CompletableFuture<Optional<RetentionCandidate>> firstSource =
                    new CompletableFuture<>();
            CompletableFuture<Optional<RetentionCandidate>> first = lane.submit(
                    new StreamId("retention-running"),
                    () -> {
                        firstStarted.countDown();
                        return firstSource;
                    });
            assertThat(firstStarted.await(2, TimeUnit.SECONDS)).isTrue();
            CompletableFuture<Optional<RetentionCandidate>> second = lane.submit(
                    new StreamId("retention-queued"),
                    () -> {
                        secondStarted.countDown();
                        return CompletableFuture.completedFuture(Optional.empty());
                    });
            CompletableFuture<Optional<RetentionCandidate>> rejected = lane.submit(
                    new StreamId("retention-rejected"),
                    () -> CompletableFuture.completedFuture(Optional.empty()));

            assertFailureCode(rejected, ErrorCode.BACKPRESSURE_REJECTED);
            assertThat(secondStarted.getCount()).isEqualTo(1);

            firstSource.complete(Optional.empty());
            assertThat(first.join()).isEmpty();
            assertThat(second.join()).isEmpty();
            assertThat(secondStarted.getCount()).isZero();
        } finally {
            lane.close();
        }
    }

    @Test
    void timesOutTheWholeOperationAndRejectsAfterClose() {
        NereusRetentionExecutionLane lane = lane(
                1,
                1,
                Duration.ofMillis(50));
        CompletableFuture<Optional<RetentionCandidate>> source =
                new CompletableFuture<>();
        CompletableFuture<Optional<RetentionCandidate>> timedOut = lane.submit(
                new StreamId("retention-timeout"),
                () -> source);

        assertFailureCode(timedOut, ErrorCode.TIMEOUT);
        assertThat(source).isCancelled();

        lane.close();
        assertFailureCode(
                lane.submit(
                        new StreamId("retention-after-close"),
                        () -> CompletableFuture.completedFuture(Optional.empty())),
                ErrorCode.STORAGE_CLOSED);
    }

    private static NereusRetentionExecutionLane lane(
            int concurrent,
            int queued,
            Duration operationTimeout) {
        AtomicInteger threadIds = new AtomicInteger();
        return new NereusRetentionExecutionLane(
                new NereusRetentionConfig(
                        16,
                        concurrent,
                        queued,
                        operationTimeout,
                        Duration.ofSeconds(2)),
                command -> {
                    Thread thread = new Thread(
                            command,
                            "retention-test-" + threadIds.incrementAndGet());
                    thread.setDaemon(true);
                    return thread;
                });
    }

    private static void assertFailureCode(
            CompletableFuture<?> future,
            ErrorCode code) {
        assertThatThrownBy(future::join)
                .isInstanceOf(CompletionException.class)
                .satisfies(failure -> {
                    assertThat(failure.getCause())
                            .isInstanceOf(NereusException.class);
                    assertThat(((NereusException) failure.getCause()).code())
                            .isEqualTo(code);
                });
    }
}
