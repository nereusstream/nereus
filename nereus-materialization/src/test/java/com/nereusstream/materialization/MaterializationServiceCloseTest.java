/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.metadata.oxia.VersionedMaterializationTask;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MaterializationServiceCloseTest {
    private static final RegisteredMaterializationScanResult EMPTY_PASS =
            new RegisteredMaterializationScanResult(64, 0, 0, 0, 0, 0, 0);

    @TempDir
    Path temporaryDirectory;

    @Test
    void keepsFullPassesNonOverlappingAndCoalescesAnInFlightHint() throws Exception {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        List<CompletableFuture<RegisteredMaterializationScanResult>> passes =
                new CopyOnWriteArrayList<>();
        AtomicInteger active = new AtomicInteger();
        AtomicInteger maximumActive = new AtomicInteger();
        DefaultMaterializationService service = new DefaultMaterializationService(
                () -> {
                    active.incrementAndGet();
                    maximumActive.accumulateAndGet(active.get(), Math::max);
                    CompletableFuture<RegisteredMaterializationScanResult> pass =
                            new CompletableFuture<>();
                    passes.add(pass);
                    return pass.whenComplete((ignored, failure) -> active.decrementAndGet());
                },
                (durable, task) -> CompletableFuture.completedFuture(null),
                config("non-overlap", Duration.ofSeconds(5), Duration.ofSeconds(2)),
                scheduler,
                Runnable::run,
                MaterializationMetricsObserver.noop());
        try {
            service.start().join();
            await(() -> passes.size() == 1);

            CompletableFuture<RegisteredMaterializationScanResult> hinted = service.scanNow();
            assertThat(passes).hasSize(1);
            passes.get(0).complete(EMPTY_PASS);
            assertThat(hinted.join()).isEqualTo(EMPTY_PASS);
            await(() -> passes.size() == 2);

            passes.get(1).complete(EMPTY_PASS);
            await(() -> active.get() == 0);
            assertThat(maximumActive).hasValue(1);
            service.closeAsync().get(2, TimeUnit.SECONDS);
            assertThat(scheduler.isShutdown()).isFalse();
        } finally {
            scheduler.shutdownNow();
        }
    }

    @Test
    void closeDeadlineCancelsAHungPassRejectsNewWorkAndLeavesExecutorsBorrowed() throws Exception {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        CompletableFuture<RegisteredMaterializationScanResult> hung = new CompletableFuture<>();
        AtomicInteger calls = new AtomicInteger();
        AtomicBoolean forced = new AtomicBoolean();
        DefaultMaterializationService service = new DefaultMaterializationService(
                () -> {
                    calls.incrementAndGet();
                    return hung;
                },
                (durable, task) -> CompletableFuture.completedFuture(null),
                config("forced-close", Duration.ofMillis(50), Duration.ofHours(1)),
                scheduler,
                Runnable::run,
                new MaterializationMetricsObserver() {
                    @Override
                    public void closeCompleted(boolean deadlineForced, Duration elapsed) {
                        forced.set(deadlineForced);
                    }
                });
        try {
            service.start().join();
            await(() -> calls.get() == 1);

            service.closeAsync().get(2, TimeUnit.SECONDS);

            assertThat(hung.isCancelled()).isTrue();
            assertThat(forced).isTrue();
            assertThat(service.isRunning()).isFalse();
            assertThat(scheduler.isShutdown()).isFalse();
            assertThatThrownBy(() -> service.scanNow().join())
                    .hasRootCauseInstanceOf(NereusException.class)
                    .satisfies(failure -> assertThat(root(failure))
                            .isInstanceOfSatisfying(NereusException.class, nereus ->
                                    assertThat(nereus.code()).isEqualTo(ErrorCode.STORAGE_CLOSED)));
        } finally {
            scheduler.shutdownNow();
        }
    }

    @Test
    void normalCloseWaitsForDispatcherDrainWithoutOwningItsScheduler() throws Exception {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        CompletableFuture<Void> dispatcherDrain = new CompletableFuture<>();
        AtomicBoolean forced = new AtomicBoolean(true);
        MaterializationTaskDispatcher dispatcher = new MaterializationTaskDispatcher() {
            @Override
            public CompletableFuture<Void> dispatch(
                    VersionedMaterializationTask durableTask,
                    MaterializationTask task) {
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public CompletableFuture<Void> closeAsync(
                    Duration timeout,
                    ScheduledExecutorService ignored) {
                return dispatcherDrain;
            }
        };
        DefaultMaterializationService service = new DefaultMaterializationService(
                () -> CompletableFuture.completedFuture(EMPTY_PASS),
                dispatcher,
                config("drain-close", Duration.ofSeconds(2), Duration.ofHours(1)),
                scheduler,
                Runnable::run,
                new MaterializationMetricsObserver() {
                    @Override
                    public void closeCompleted(boolean deadlineForced, Duration elapsed) {
                        forced.set(deadlineForced);
                    }
                });
        try {
            service.start().join();
            CompletableFuture<Void> close = service.closeAsync();
            assertThat(close).isNotDone();

            dispatcherDrain.complete(null);

            close.get(2, TimeUnit.SECONDS);
            assertThat(forced).isFalse();
            assertThat(scheduler.isShutdown()).isFalse();
        } finally {
            scheduler.shutdownNow();
        }
    }

    private MaterializationConfig config(
            String name,
            Duration closeTimeout,
            Duration scanInterval) throws Exception {
        Path staging = Files.createDirectory(temporaryDirectory.resolve(name));
        Files.setPosixFilePermissions(staging, PosixFilePermissions.fromString("rwx------"));
        MaterializationPolicy policy = MaterializationPolicyFactory.losslessCommitted(
                2, 16, 10_000, 64L << 20, 1_024, "ZSTD");
        return new MaterializationConfig(
                policy,
                16,
                scanInterval,
                16,
                16,
                16,
                4,
                1,
                128,
                1L << 20,
                staging,
                256L << 20,
                1 << 20,
                Duration.ofMinutes(2),
                Duration.ofSeconds(30),
                Duration.ofSeconds(5),
                Duration.ofSeconds(30),
                closeTimeout,
                Duration.ofMillis(100),
                Duration.ofSeconds(5),
                8,
                100,
                200,
                1_000,
                2_000,
                Duration.ofMinutes(5),
                Duration.ofHours(1),
                Duration.ofHours(1),
                Duration.ofHours(2),
                1_000,
                128L << 20);
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
}
