/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

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

/** Fixed-delay, non-overlapping complete-pass loop that owns no injected store or executor. */
public final class DefaultPhysicalGcLifecycleService
        implements PhysicalGcLifecycleService {
    private enum State {
        NEW,
        RUNNING,
        CLOSING,
        CLOSED
    }

    private final Supplier<CompletableFuture<PhysicalGcLifecyclePassResult>> pass;
    private final PhysicalGcConfig config;
    private final ScheduledExecutorService scheduler;
    private final Executor callbackExecutor;
    private final Object monitor = new Object();

    private State state = State.NEW;
    private ScheduledFuture<?> scheduledPass;
    private CompletableFuture<PhysicalGcLifecyclePassResult> activePass;
    private CompletableFuture<PhysicalGcLifecyclePassResult> activeSource;
    private boolean rescanRequested;
    private CompletableFuture<Void> closeFuture;
    private ScheduledFuture<?> closeDeadline;

    public DefaultPhysicalGcLifecycleService(
            PhysicalGcLifecyclePass pass,
            PhysicalGcConfig config,
            ScheduledExecutorService scheduler,
            Executor callbackExecutor) {
        this(Objects.requireNonNull(pass, "pass")::scan,
                config,
                scheduler,
                callbackExecutor);
    }

    DefaultPhysicalGcLifecycleService(
            Supplier<CompletableFuture<PhysicalGcLifecyclePassResult>> pass,
            PhysicalGcConfig config,
            ScheduledExecutorService scheduler,
            Executor callbackExecutor) {
        this.pass = Objects.requireNonNull(pass, "pass");
        this.config = Objects.requireNonNull(config, "config");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.callbackExecutor = Objects.requireNonNull(
                callbackExecutor, "callbackExecutor");
    }

    @Override
    public CompletableFuture<Void> start() {
        synchronized (monitor) {
            if (state == State.RUNNING) {
                return CompletableFuture.completedFuture(null);
            }
            if (state != State.NEW) {
                return CompletableFuture.failedFuture(closed(
                        "physical-GC lifecycle cannot start after close begins", null));
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
    public CompletableFuture<PhysicalGcLifecyclePassResult> scanNow() {
        CompletableFuture<PhysicalGcLifecyclePassResult> target;
        synchronized (monitor) {
            if (state != State.RUNNING) {
                return CompletableFuture.failedFuture(state == State.NEW
                        ? new NereusException(
                                ErrorCode.METADATA_CONDITION_FAILED,
                                true,
                                "physical-GC lifecycle has not started")
                        : closed("physical-GC lifecycle is closing", null));
            }
            if (activePass != null) {
                rescanRequested = true;
                return activePass.thenApply(value -> value);
            }
            cancelScheduledLocked();
            activePass = new CompletableFuture<>();
            target = activePass;
        }
        launch(target);
        return target.thenApply(value -> value);
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        CompletableFuture<PhysicalGcLifecyclePassResult> target;
        CompletableFuture<PhysicalGcLifecyclePassResult> source;
        CompletableFuture<Void> result;
        synchronized (monitor) {
            if (closeFuture != null) {
                return closeFuture;
            }
            closeFuture = new CompletableFuture<>();
            result = closeFuture;
            if (state == State.CLOSED) {
                closeFuture.complete(null);
                return closeFuture;
            }
            state = State.CLOSING;
            rescanRequested = false;
            cancelScheduledLocked();
            target = activePass;
            source = activeSource;
        }

        CompletableFuture<Void> drained = target == null
                ? CompletableFuture.completedFuture(null)
                : target.handle((ignored, failure) -> null);
        drained.whenComplete((ignored, failure) ->
                executeCallback(this::completeClose));
        try {
            ScheduledFuture<?> deadline = scheduler.schedule(
                    () -> {
                        if (source != null) {
                            source.cancel(true);
                        }
                        if (target != null) {
                            target.cancel(true);
                        }
                        completeClose();
                    },
                    toNanos(config.closeTimeout()),
                    TimeUnit.NANOSECONDS);
            synchronized (monitor) {
                if (closeFuture.isDone()) {
                    deadline.cancel(false);
                } else {
                    closeDeadline = deadline;
                }
            }
        } catch (RejectedExecutionException failure) {
            if (source != null) {
                source.cancel(true);
            }
            if (target != null) {
                target.cancel(true);
            }
            completeClose();
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
        CompletableFuture<PhysicalGcLifecyclePassResult> target;
        synchronized (monitor) {
            scheduledPass = null;
            if (state != State.RUNNING) {
                return;
            }
            if (activePass != null) {
                rescanRequested = true;
                return;
            }
            activePass = new CompletableFuture<>();
            target = activePass;
        }
        launch(target);
    }

    private void launch(CompletableFuture<PhysicalGcLifecyclePassResult> target) {
        CompletableFuture<PhysicalGcLifecyclePassResult> source;
        try {
            source = Objects.requireNonNull(
                    pass.get(), "physical-GC lifecycle pass returned null");
        } catch (Throwable failure) {
            source = CompletableFuture.failedFuture(failure);
        }
        synchronized (monitor) {
            if (activePass == target) {
                activeSource = source;
            }
        }
        CompletableFuture<PhysicalGcLifecyclePassResult> admitted = source;
        admitted.whenComplete((value, failure) ->
                executeCallback(() -> finish(target, admitted, value, failure)));
        target.whenComplete((ignored, failure) -> {
            if (target.isCancelled()) {
                admitted.cancel(true);
            }
        });
    }

    private void finish(
            CompletableFuture<PhysicalGcLifecyclePassResult> target,
            CompletableFuture<PhysicalGcLifecyclePassResult> source,
            PhysicalGcLifecyclePassResult value,
            Throwable failure) {
        if (failure == null) {
            target.complete(value);
        } else {
            target.completeExceptionally(failure);
        }
        synchronized (monitor) {
            if (activePass == target) {
                activePass = null;
            }
            if (activeSource == source) {
                activeSource = null;
            }
            if (state == State.RUNNING) {
                Duration delay = rescanRequested
                        ? Duration.ZERO
                        : config.scanInterval();
                rescanRequested = false;
                try {
                    scheduleLocked(delay);
                } catch (RuntimeException ignored) {
                    // A rejected borrowed scheduler is surfaced by the next explicit call or close.
                }
            }
        }
    }

    private void scheduleLocked(Duration delay) {
        if (state != State.RUNNING || scheduledPass != null) {
            return;
        }
        try {
            scheduledPass = scheduler.schedule(
                    this::scheduledTrigger,
                    toNanos(delay),
                    TimeUnit.NANOSECONDS);
        } catch (RejectedExecutionException failure) {
            throw closed(
                    "physical-GC scheduler rejected a complete pass", failure);
        }
    }

    private void cancelScheduledLocked() {
        if (scheduledPass != null) {
            scheduledPass.cancel(false);
            scheduledPass = null;
        }
    }

    private void completeClose() {
        CompletableFuture<Void> result;
        synchronized (monitor) {
            if (closeFuture == null || closeFuture.isDone()) {
                return;
            }
            state = State.CLOSED;
            activePass = null;
            activeSource = null;
            if (closeDeadline != null) {
                closeDeadline.cancel(false);
                closeDeadline = null;
            }
            result = closeFuture;
        }
        result.complete(null);
    }

    private void executeCallback(Runnable callback) {
        try {
            callbackExecutor.execute(callback);
        } catch (RejectedExecutionException failure) {
            callback.run();
        }
    }

    private static long toNanos(Duration duration) {
        try {
            return duration.toNanos();
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    private static NereusException closed(String message, Throwable cause) {
        return new NereusException(ErrorCode.STORAGE_CLOSED, false, message, cause);
    }
}
