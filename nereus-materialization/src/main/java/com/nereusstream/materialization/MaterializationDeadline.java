/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/** One monotonic deadline shared by every asynchronous cut in a materialization operation. */
final class MaterializationDeadline {
    private final long deadlineNanos;
    private final ScheduledExecutorService scheduler;

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
        ScheduledFuture<?> timeout = scheduler.schedule(
                () -> result.completeExceptionally(timeout(stage + " timed out")),
                nanos,
                TimeUnit.NANOSECONDS);
        source.whenComplete((value, failure) -> {
            timeout.cancel(false);
            if (failure == null) {
                result.complete(value);
            } else {
                result.completeExceptionally(failure);
            }
        });
        return result;
    }

    private static NereusException timeout(String message) {
        return new NereusException(ErrorCode.TIMEOUT, true, message);
    }
}
