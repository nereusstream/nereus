/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

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

/** Non-overlapping fixed-delay owner for production BookKeeper retention passes. */
public final class BookKeeperLedgerRetentionService implements AutoCloseable {
    private enum State {
        NEW,
        RUNNING,
        CLOSING,
        CLOSED
    }

    private final Supplier<CompletableFuture<BookKeeperLedgerRetentionScanResult>> scanner;
    private final Duration scanInterval;
    private final Duration closeTimeout;
    private final ScheduledExecutorService scheduler;
    private final Executor callbackExecutor;
    private final Object monitor = new Object();

    private State state = State.NEW;
    private ScheduledFuture<?> scheduled;
    private CompletableFuture<BookKeeperLedgerRetentionScanResult> active;
    private CompletableFuture<BookKeeperLedgerRetentionScanResult> activeSource;
    private boolean rescanRequested;
    private CompletableFuture<Void> closeFuture;
    private ScheduledFuture<?> closeDeadline;

    public BookKeeperLedgerRetentionService(
            BookKeeperLedgerRetentionScanner scanner,
            Duration scanInterval,
            Duration closeTimeout,
            ScheduledExecutorService scheduler,
            Executor callbackExecutor) {
        this(
                Objects.requireNonNull(scanner, "scanner")::scanOnce,
                scanInterval,
                closeTimeout,
                scheduler,
                callbackExecutor);
    }

    BookKeeperLedgerRetentionService(
            Supplier<CompletableFuture<BookKeeperLedgerRetentionScanResult>> scanner,
            Duration scanInterval,
            Duration closeTimeout,
            ScheduledExecutorService scheduler,
            Executor callbackExecutor) {
        this.scanner = Objects.requireNonNull(scanner, "scanner");
        this.scanInterval = positive(scanInterval, "scanInterval");
        this.closeTimeout = positive(closeTimeout, "closeTimeout");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.callbackExecutor = Objects.requireNonNull(callbackExecutor, "callbackExecutor");
    }

    public CompletableFuture<Void> start() {
        synchronized (monitor) {
            if (state == State.RUNNING) {
                return CompletableFuture.completedFuture(null);
            }
            if (state != State.NEW) {
                return CompletableFuture.failedFuture(closed("retention service cannot restart"));
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

    public CompletableFuture<BookKeeperLedgerRetentionScanResult> scanNow() {
        CompletableFuture<BookKeeperLedgerRetentionScanResult> target;
        synchronized (monitor) {
            if (state != State.RUNNING) {
                return CompletableFuture.failedFuture(state == State.NEW
                        ? condition("BookKeeper retention service has not started")
                        : closed("BookKeeper retention service is closing"));
            }
            if (active != null) {
                rescanRequested = true;
                return active.thenApply(result -> result);
            }
            cancelScheduledLocked();
            active = new CompletableFuture<>();
            target = active;
        }
        launch(target);
        return target.thenApply(result -> result);
    }

    public boolean isRunning() {
        synchronized (monitor) {
            return state == State.RUNNING;
        }
    }

    public CompletableFuture<Void> closeAsync() {
        CompletableFuture<BookKeeperLedgerRetentionScanResult> target;
        CompletableFuture<BookKeeperLedgerRetentionScanResult> source;
        synchronized (monitor) {
            if (closeFuture != null) {
                return closeFuture;
            }
            closeFuture = new CompletableFuture<>();
            if (state == State.CLOSED) {
                closeFuture.complete(null);
                return closeFuture;
            }
            state = State.CLOSING;
            rescanRequested = false;
            cancelScheduledLocked();
            target = active;
            source = activeSource;
        }
        CompletableFuture<Void> drained = target == null
                ? CompletableFuture.completedFuture(null)
                : target.handle((ignored, failure) -> null);
        drained.whenComplete((ignored, failure) -> execute(this::completeClose));
        try {
            ScheduledFuture<?> deadline = scheduler.schedule(
                    () -> {
                        if (source != null) source.cancel(true);
                        if (target != null) target.cancel(true);
                        completeClose();
                    },
                    closeTimeout.toNanos(),
                    TimeUnit.NANOSECONDS);
            synchronized (monitor) {
                if (closeFuture.isDone()) {
                    deadline.cancel(false);
                } else {
                    closeDeadline = deadline;
                }
            }
        } catch (RejectedExecutionException failure) {
            if (source != null) source.cancel(true);
            if (target != null) target.cancel(true);
            completeClose();
        }
        return closeFuture;
    }

    @Override
    public void close() {
        closeAsync().join();
    }

    private void scheduledTrigger() {
        CompletableFuture<BookKeeperLedgerRetentionScanResult> target;
        synchronized (monitor) {
            scheduled = null;
            if (state != State.RUNNING) return;
            if (active != null) {
                rescanRequested = true;
                return;
            }
            active = new CompletableFuture<>();
            target = active;
        }
        launch(target);
    }

    private void launch(CompletableFuture<BookKeeperLedgerRetentionScanResult> target) {
        CompletableFuture<BookKeeperLedgerRetentionScanResult> source;
        try {
            source = Objects.requireNonNull(scanner.get(), "retention scan future");
        } catch (Throwable failure) {
            source = CompletableFuture.failedFuture(failure);
        }
        synchronized (monitor) {
            if (active != target || state != State.RUNNING) {
                source.cancel(true);
                target.cancel(true);
                return;
            }
            activeSource = source;
        }
        source.whenComplete((result, failure) -> execute(() -> finish(target, result, failure)));
    }

    private void finish(
            CompletableFuture<BookKeeperLedgerRetentionScanResult> target,
            BookKeeperLedgerRetentionScanResult result,
            Throwable failure) {
        boolean runAgain;
        synchronized (monitor) {
            if (active != target) return;
            active = null;
            activeSource = null;
            runAgain = state == State.RUNNING && rescanRequested;
            rescanRequested = false;
            if (state == State.RUNNING && !runAgain) {
                try {
                    scheduleLocked(scanInterval);
                } catch (RuntimeException scheduleFailure) {
                    if (failure == null) failure = scheduleFailure;
                    state = State.CLOSED;
                }
            }
        }
        if (failure == null) {
            target.complete(result);
        } else {
            target.completeExceptionally(failure);
        }
        if (runAgain) {
            CompletableFuture<BookKeeperLedgerRetentionScanResult> next;
            synchronized (monitor) {
                if (state != State.RUNNING || active != null) return;
                active = new CompletableFuture<>();
                next = active;
            }
            launch(next);
        }
    }

    private void completeClose() {
        synchronized (monitor) {
            if (state == State.CLOSED) return;
            state = State.CLOSED;
            if (closeDeadline != null) {
                closeDeadline.cancel(false);
                closeDeadline = null;
            }
            if (closeFuture != null) closeFuture.complete(null);
        }
    }

    private void scheduleLocked(Duration delay) {
        scheduled = scheduler.schedule(this::scheduledTrigger, delay.toNanos(), TimeUnit.NANOSECONDS);
    }

    private void cancelScheduledLocked() {
        if (scheduled != null) {
            scheduled.cancel(false);
            scheduled = null;
        }
    }

    private void execute(Runnable task) {
        try {
            callbackExecutor.execute(task);
        } catch (RejectedExecutionException failure) {
            task.run();
        }
    }

    private static Duration positive(Duration value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(field + " must be positive");
        }
        value.toNanos();
        return value;
    }

    private static NereusException condition(String message) {
        return new NereusException(ErrorCode.METADATA_CONDITION_FAILED, true, message);
    }

    private static NereusException closed(String message) {
        return new NereusException(ErrorCode.STORAGE_CLOSED, false, message);
    }
}
