/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/** Non-overlapping full-pass loop that owns no injected store or executor. */
public final class DefaultMaterializationService implements MaterializationService {
    private enum State {
        NEW,
        RUNNING,
        CLOSING,
        CLOSED
    }

    private final Supplier<CompletableFuture<RegisteredMaterializationScanResult>> scanner;
    private final MaterializationTaskDispatcher dispatcher;
    private final MaterializationConfig config;
    private final ScheduledExecutorService scheduler;
    private final Executor callbackExecutor;
    private final MaterializationMetricsObserver observer;
    private final Object monitor = new Object();

    private State state = State.NEW;
    private ScheduledFuture<?> scheduledScan;
    private CompletableFuture<RegisteredMaterializationScanResult> activeScan;
    private CompletableFuture<RegisteredMaterializationScanResult> activeScanSource;
    private boolean rescanRequested;
    private CompletableFuture<Void> closeFuture;
    private ScheduledFuture<?> closeDeadline;
    private long closeStartedNanos;
    private boolean closeDeadlineForced;

    public DefaultMaterializationService(
            RegisteredMaterializationStreamScanner scanner,
            MaterializationTaskDispatcher dispatcher,
            MaterializationConfig config,
            ScheduledExecutorService scheduler,
            Executor callbackExecutor,
            MaterializationMetricsObserver observer) {
        this.scanner = Objects.requireNonNull(scanner, "scanner")::scanOnce;
        this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
        this.config = Objects.requireNonNull(config, "config");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.callbackExecutor = Objects.requireNonNull(callbackExecutor, "callbackExecutor");
        this.observer = Objects.requireNonNull(observer, "observer");
    }

    DefaultMaterializationService(
            Supplier<CompletableFuture<RegisteredMaterializationScanResult>> scanner,
            MaterializationTaskDispatcher dispatcher,
            MaterializationConfig config,
            ScheduledExecutorService scheduler,
            Executor callbackExecutor,
            MaterializationMetricsObserver observer) {
        this.scanner = Objects.requireNonNull(scanner, "scanner");
        this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
        this.config = Objects.requireNonNull(config, "config");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.callbackExecutor = Objects.requireNonNull(callbackExecutor, "callbackExecutor");
        this.observer = Objects.requireNonNull(observer, "observer");
    }

    @Override
    public CompletableFuture<Void> start() {
        synchronized (monitor) {
            if (state == State.RUNNING) {
                return CompletableFuture.completedFuture(null);
            }
            if (state != State.NEW) {
                return CompletableFuture.failedFuture(closedFailure(
                        "materialization service cannot start after close begins", null));
            }
            state = State.RUNNING;
            try {
                scheduleLocked(Duration.ZERO);
            } catch (RuntimeException failure) {
                state = State.CLOSED;
                return CompletableFuture.failedFuture(failure);
            }
            return CompletableFuture.completedFuture(null);
        }
    }

    @Override
    public CompletableFuture<RegisteredMaterializationScanResult> scanNow() {
        CompletableFuture<RegisteredMaterializationScanResult> result;
        boolean launch = false;
        synchronized (monitor) {
            if (state != State.RUNNING) {
                return CompletableFuture.failedFuture(state == State.NEW
                        ? new NereusException(
                                ErrorCode.METADATA_CONDITION_FAILED,
                                true,
                                "materialization service has not started")
                        : closedFailure("materialization service is closing", null));
            }
            if (activeScan != null) {
                rescanRequested = true;
                return activeScan.thenApply(value -> value);
            }
            cancelScheduledScanLocked();
            activeScan = new CompletableFuture<>();
            result = activeScan;
            launch = true;
        }
        if (launch) {
            launch(result);
        }
        return result.thenApply(value -> value);
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        CompletableFuture<RegisteredMaterializationScanResult> scan;
        CompletableFuture<Void> result;
        synchronized (monitor) {
            if (closeFuture != null) {
                return closeFuture;
            }
            closeFuture = new CompletableFuture<>();
            result = closeFuture;
            closeStartedNanos = System.nanoTime();
            if (state == State.CLOSED) {
                closeFuture.complete(null);
                return closeFuture;
            }
            state = State.CLOSING;
            rescanRequested = false;
            cancelScheduledScanLocked();
            scan = activeScan;
        }

        CompletableFuture<Void> dispatcherClose;
        try {
            dispatcherClose = Objects.requireNonNull(
                    dispatcher.closeAsync(config.closeTimeout(), scheduler),
                    "dispatcher close future");
        } catch (Throwable failure) {
            dispatcherClose = CompletableFuture.failedFuture(failure);
        }
        CompletableFuture<Void> scanClose = scan == null
                ? CompletableFuture.completedFuture(null)
                : scan.handle((ignored, failure) -> null);
        CompletableFuture.allOf(
                        dispatcherClose.handle((ignored, failure) -> null),
                        scanClose)
                .whenComplete((ignored, failure) -> executeCallback(
                        () -> completeClose(false)));
        try {
            long timeoutNanos = config.closeTimeout().toNanos();
            ScheduledFuture<?> deadline = scheduler.schedule(
                    () -> {
                        markCloseDeadlineForced();
                        cancelActiveScan(scan);
                        completeClose(true);
                    },
                    timeoutNanos,
                    TimeUnit.NANOSECONDS);
            synchronized (monitor) {
                closeDeadline = deadline;
                if (closeFuture.isDone()) {
                    deadline.cancel(false);
                }
            }
        } catch (ArithmeticException | RejectedExecutionException failure) {
            markCloseDeadlineForced();
            cancelActiveScan(scan);
            completeClose(true);
        }
        return result;
    }

    @Override
    public boolean isRunning() {
        synchronized (monitor) {
            return state == State.RUNNING;
        }
    }

    @Override
    public void close() {
        closeAsync().join();
    }

    private void scheduledTrigger() {
        CompletableFuture<RegisteredMaterializationScanResult> target;
        synchronized (monitor) {
            scheduledScan = null;
            if (state != State.RUNNING) {
                return;
            }
            if (activeScan != null) {
                rescanRequested = true;
                return;
            }
            activeScan = new CompletableFuture<>();
            target = activeScan;
        }
        launch(target);
    }

    private void launch(CompletableFuture<RegisteredMaterializationScanResult> target) {
        long started = System.nanoTime();
        CompletableFuture<RegisteredMaterializationScanResult> source;
        try {
            source = Objects.requireNonNull(scanner.get(), "registry scan future");
        } catch (Throwable failure) {
            source = CompletableFuture.failedFuture(failure);
        }
        CompletableFuture<RegisteredMaterializationScanResult> admittedSource = source;
        boolean admitted;
        synchronized (monitor) {
            admitted = activeScan == target && state != State.CLOSED;
            if (admitted) {
                activeScanSource = admittedSource;
            }
        }
        admittedSource.whenComplete((value, failure) -> executeCallback(
                () -> finishScan(target, value, failure, started)));
        target.whenComplete((ignored, failure) -> {
            if (target.isCancelled()) {
                admittedSource.cancel(true);
            }
        });
        if (!admitted) {
            admittedSource.cancel(true);
            target.cancel(true);
        }
    }

    private void finishScan(
            CompletableFuture<RegisteredMaterializationScanResult> target,
            RegisteredMaterializationScanResult value,
            Throwable failure,
            long startedNanos) {
        Duration elapsed = elapsed(startedNanos);
        if (failure == null) {
            target.complete(value);
            observe(() -> observer.scanCompleted(value, elapsed));
        } else {
            target.completeExceptionally(failure);
            observe(() -> observer.scanFailed(failure, elapsed));
        }
        synchronized (monitor) {
            if (activeScan == target) {
                activeScan = null;
                activeScanSource = null;
            }
            if (state == State.RUNNING) {
                Duration delay = rescanRequested
                        ? Duration.ZERO
                        : config.registryScanInterval();
                rescanRequested = false;
                try {
                    scheduleLocked(delay);
                } catch (RuntimeException schedulingFailure) {
                    observe(() -> observer.scanFailed(schedulingFailure, Duration.ZERO));
                }
            }
        }
    }

    private void scheduleLocked(Duration delay) {
        if (state != State.RUNNING || scheduledScan != null) {
            return;
        }
        long nanos;
        try {
            nanos = delay.toNanos();
        } catch (ArithmeticException overflow) {
            nanos = Long.MAX_VALUE;
        }
        try {
            scheduledScan = scheduler.schedule(
                    this::scheduledTrigger,
                    nanos,
                    TimeUnit.NANOSECONDS);
        } catch (RejectedExecutionException failure) {
            throw closedFailure(
                    "materialization scheduler rejected a registry scan",
                    failure);
        }
    }

    private void cancelScheduledScanLocked() {
        if (scheduledScan != null) {
            scheduledScan.cancel(false);
            scheduledScan = null;
        }
    }

    private void completeClose(boolean deadlineForced) {
        CompletableFuture<Void> result;
        Duration elapsed;
        boolean forced;
        synchronized (monitor) {
            if (closeFuture == null || closeFuture.isDone()) {
                return;
            }
            state = State.CLOSED;
            activeScan = null;
            activeScanSource = null;
            if (closeDeadline != null) {
                closeDeadline.cancel(false);
                closeDeadline = null;
            }
            elapsed = elapsed(closeStartedNanos);
            forced = deadlineForced || closeDeadlineForced;
            result = closeFuture;
        }
        observe(() -> observer.closeCompleted(forced, elapsed));
        result.complete(null);
    }

    private void cancelActiveScan(
            CompletableFuture<RegisteredMaterializationScanResult> scan) {
        if (scan == null) {
            return;
        }
        CompletableFuture<RegisteredMaterializationScanResult> source;
        synchronized (monitor) {
            source = activeScan == scan ? activeScanSource : null;
        }
        if (source != null) {
            source.cancel(true);
        }
        scan.cancel(true);
    }

    private void markCloseDeadlineForced() {
        synchronized (monitor) {
            closeDeadlineForced = true;
        }
    }

    private void executeCallback(Runnable callback) {
        try {
            callbackExecutor.execute(callback);
        } catch (RejectedExecutionException failure) {
            callback.run();
        }
    }

    private static void observe(Runnable callback) {
        try {
            callback.run();
        } catch (Throwable ignored) {
            // Metrics never own materialization correctness or lifecycle progress.
        }
    }

    private static Duration elapsed(long startedNanos) {
        long nanos = Math.max(0, System.nanoTime() - startedNanos);
        return Duration.ofNanos(nanos);
    }

    private static NereusException closedFailure(String message, Throwable cause) {
        return new NereusException(ErrorCode.STORAGE_CLOSED, false, message, cause);
    }
}
