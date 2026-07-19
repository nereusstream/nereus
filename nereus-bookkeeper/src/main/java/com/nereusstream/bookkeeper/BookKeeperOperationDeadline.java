/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.api.AppendOutcome;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** One monotonic budget shared by every nested BookKeeper operation in a caller workflow. */
public final class BookKeeperOperationDeadline {
    private final long deadlineNanos;

    public BookKeeperOperationDeadline(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isZero() || timeout.isNegative()) throw new IllegalArgumentException("timeout must be positive");
        long nanos;
        try { nanos = timeout.toNanos(); }
        catch (ArithmeticException overflow) { nanos = Long.MAX_VALUE; }
        long now = System.nanoTime();
        try { deadlineNanos = Math.addExact(now, nanos); }
        catch (ArithmeticException overflow) { throw new IllegalArgumentException("timeout exceeds monotonic domain"); }
    }

    public Duration remaining() {
        long remaining = deadlineNanos - System.nanoTime();
        if (remaining <= 0) throw new NereusException(ErrorCode.TIMEOUT, true, "BookKeeper operation timed out");
        return Duration.ofNanos(remaining);
    }

    public <T> CompletableFuture<T> bound(CompletableFuture<T> source) {
        Objects.requireNonNull(source, "source");
        Duration remaining = remaining();
        return source.orTimeout(remaining.toNanos(), TimeUnit.NANOSECONDS).handle((value, failure) -> {
            if (failure == null) return value;
            Throwable cause = failure;
            while (cause instanceof CompletionException && cause.getCause() != null) cause = cause.getCause();
            if (cause instanceof TimeoutException) {
                throw new NereusException(ErrorCode.TIMEOUT, true, "BookKeeper operation timed out", cause);
            }
            if (cause instanceof RuntimeException runtimeException) throw runtimeException;
            throw new CompletionException(cause);
        });
    }
}
