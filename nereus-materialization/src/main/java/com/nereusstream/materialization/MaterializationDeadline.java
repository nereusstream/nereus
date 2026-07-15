/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import java.time.Duration;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/** One monotonic deadline shared by every asynchronous cut in a materialization operation. */
final class MaterializationDeadline implements AutoCloseable {
    private final long deadlineNanos;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Set<BoundState<?>> active = ConcurrentHashMap.newKeySet();

    MaterializationDeadline(
            Duration timeout,
            ScheduledExecutorService scheduler) {
        Objects.requireNonNull(timeout, "timeout");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
        long now = System.nanoTime();
        long timeoutNanos;
        try {
            timeoutNanos = timeout.toNanos();
        } catch (ArithmeticException overflow) {
            timeoutNanos = Long.MAX_VALUE;
        }
        this.deadlineNanos = timeoutNanos >= Long.MAX_VALUE - now
                ? Long.MAX_VALUE
                : now + timeoutNanos;
    }

    Duration remaining() {
        long nanos = deadlineNanos == Long.MAX_VALUE
                ? Long.MAX_VALUE
                : deadlineNanos - System.nanoTime();
        if (nanos <= 0) {
            throw timeout("materialization operation deadline expired");
        }
        return Duration.ofNanos(nanos);
    }

    <T> CompletableFuture<T> bound(
            Supplier<CompletableFuture<T>> operation,
            String stage) {
        if (closed.get()) {
            return CompletableFuture.failedFuture(closed(stage + " rejected after close"));
        }
        long nanos;
        try {
            nanos = remaining().toNanos();
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        CompletableFuture<T> source;
        try {
            source = Objects.requireNonNull(operation.get(), stage + " future");
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        CompletableFuture<T> result = new CompletableFuture<>();
        BoundState<T> state = new BoundState<>(source, result, stage);
        active.add(state);
        if (closed.get()) {
            state.fail(closed(stage + " cancelled during close"));
            return result;
        }
        try {
            state.timeout = scheduler.schedule(
                    () -> state.fail(timeout(stage + " timed out")),
                    nanos,
                    TimeUnit.NANOSECONDS);
        } catch (RejectedExecutionException failure) {
            state.fail(new NereusException(
                    ErrorCode.STORAGE_CLOSED,
                    false,
                    stage + " timeout scheduler rejected admitted work",
                    failure));
            return result;
        }
        source.whenComplete(state::completeFromSource);
        result.whenComplete((ignored, failure) -> {
            if (result.isCancelled()) {
                state.cancelFromCaller();
            }
        });
        return result;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        for (BoundState<?> state : Set.copyOf(active)) {
            state.fail(closed(state.stage + " cancelled during close"));
        }
    }

    private final class BoundState<T> {
        private final CompletableFuture<T> source;
        private final CompletableFuture<T> result;
        private final String stage;
        private final AtomicBoolean terminal = new AtomicBoolean();
        private volatile ScheduledFuture<?> timeout;

        private BoundState(
                CompletableFuture<T> source,
                CompletableFuture<T> result,
                String stage) {
            this.source = source;
            this.result = result;
            this.stage = stage;
        }

        private void completeFromSource(T value, Throwable failure) {
            if (!terminal.compareAndSet(false, true)) {
                return;
            }
            cleanupTimer();
            active.remove(this);
            if (failure == null) {
                result.complete(value);
            } else {
                result.completeExceptionally(failure);
            }
        }

        private void fail(Throwable failure) {
            if (!terminal.compareAndSet(false, true)) {
                return;
            }
            cleanupTimer();
            active.remove(this);
            source.cancel(true);
            result.completeExceptionally(failure);
        }

        private void cancelFromCaller() {
            if (!terminal.compareAndSet(false, true)) {
                return;
            }
            cleanupTimer();
            active.remove(this);
            source.cancel(true);
        }

        private void cleanupTimer() {
            ScheduledFuture<?> scheduled = timeout;
            if (scheduled != null) {
                scheduled.cancel(false);
            }
        }
    }

    private static NereusException timeout(String message) {
        return new NereusException(ErrorCode.TIMEOUT, true, message);
    }

    private static NereusException closed(String message) {
        return new NereusException(ErrorCode.STORAGE_CLOSED, false, message);
    }
}
