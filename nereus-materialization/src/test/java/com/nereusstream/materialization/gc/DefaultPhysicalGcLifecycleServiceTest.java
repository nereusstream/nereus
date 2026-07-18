/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class DefaultPhysicalGcLifecycleServiceTest {
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor();
    private final ExecutorService callbacks = Executors.newSingleThreadExecutor();

    @AfterEach
    void closeExecutors() {
        scheduler.shutdownNow();
        callbacks.shutdownNow();
    }

    @Test
    void coalescesOneImmediatePassAndUsesFixedDelayAfterCompletion() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        List<CompletableFuture<PhysicalGcLifecyclePassResult>> sources =
                java.util.Collections.synchronizedList(new ArrayList<>());
        DefaultPhysicalGcLifecycleService service =
                new DefaultPhysicalGcLifecycleService(
                        () -> {
                            calls.incrementAndGet();
                            CompletableFuture<PhysicalGcLifecyclePassResult> source =
                                    new CompletableFuture<>();
                            sources.add(source);
                            return source;
                        },
                        config(Duration.ofDays(1), Duration.ofSeconds(1)),
                        scheduler,
                        callbacks);

        service.start().join();
        awaitCalls(calls, 1);
        CompletableFuture<PhysicalGcLifecyclePassResult> firstHint = service.scanNow();
        CompletableFuture<PhysicalGcLifecyclePassResult> secondHint = service.scanNow();
        sources.get(0).complete(PhysicalGcLifecyclePassTest.result());
        assertThat(firstHint.get(1, TimeUnit.SECONDS))
                .isEqualTo(PhysicalGcLifecyclePassTest.result());
        assertThat(secondHint.get(1, TimeUnit.SECONDS))
                .isEqualTo(PhysicalGcLifecyclePassTest.result());

        awaitCalls(calls, 2);
        sources.get(1).complete(PhysicalGcLifecyclePassTest.result());
        Thread.sleep(50);
        assertThat(calls).hasValue(2);

        service.close();
        assertThat(scheduler.isShutdown()).isFalse();
        assertThat(callbacks.isShutdown()).isFalse();
    }

    @Test
    void failedPassStillSchedulesTheNextFixedDelayPass() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        DefaultPhysicalGcLifecycleService service =
                new DefaultPhysicalGcLifecycleService(
                        () -> calls.incrementAndGet() == 1
                                ? CompletableFuture.failedFuture(
                                        new IllegalStateException("injected"))
                                : CompletableFuture.completedFuture(
                                        PhysicalGcLifecyclePassTest.result()),
                        config(Duration.ofMillis(20), Duration.ofSeconds(1)),
                        scheduler,
                        callbacks);

        service.start().join();
        awaitCalls(calls, 2);

        service.close();
        assertThat(calls.get()).isGreaterThanOrEqualTo(2);
    }

    @Test
    void closeDeadlineCancelsHungPassAndRejectsNewWork() throws Exception {
        CompletableFuture<PhysicalGcLifecyclePassResult> hung = new CompletableFuture<>();
        AtomicInteger calls = new AtomicInteger();
        DefaultPhysicalGcLifecycleService service =
                new DefaultPhysicalGcLifecycleService(
                        () -> {
                            calls.incrementAndGet();
                            return hung;
                        },
                        config(Duration.ofDays(1), Duration.ofMillis(50)),
                        scheduler,
                        callbacks);
        service.start().join();
        awaitCalls(calls, 1);

        service.closeAsync().get(1, TimeUnit.SECONDS);

        assertThat(hung.isCancelled()).isTrue();
        assertThat(service.isRunning()).isFalse();
        assertThatThrownBy(() -> service.scanNow().join())
                .hasRootCauseMessage("physical-GC lifecycle is closing");
        assertThat(scheduler.isShutdown()).isFalse();
        assertThat(callbacks.isShutdown()).isFalse();
    }

    private static void awaitCalls(AtomicInteger calls, int expected) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (calls.get() < expected && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertThat(calls.get()).isGreaterThanOrEqualTo(expected);
    }

    private static PhysicalGcConfig config(
            Duration interval, Duration closeTimeout) {
        return new PhysicalGcConfig(
                true,
                true,
                1,
                1,
                1,
                4_096,
                100,
                100,
                interval,
                Duration.ofSeconds(10),
                Duration.ofSeconds(3),
                Duration.ZERO,
                Duration.ofSeconds(10),
                Duration.ofMinutes(1),
                Duration.ofHours(25),
                Duration.ofDays(7),
                Duration.ofSeconds(1),
                closeTimeout);
    }
}
