/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.core.append;

import com.nereusstream.api.AppendOutcome;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

final class AppendDeadline {
    private final long deadlineNanos;
    private final CompletableFuture<Void> cancellation = new CompletableFuture<>();

    AppendDeadline(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        long timeoutNanos;
        try {
            timeoutNanos = timeout.toNanos();
        } catch (ArithmeticException e) {
            timeoutNanos = Long.MAX_VALUE;
        }
        long now = System.nanoTime();
        deadlineNanos = timeoutNanos >= Long.MAX_VALUE - now ? Long.MAX_VALUE : now + timeoutNanos;
    }

    Duration remaining() {
        long remaining = deadlineNanos - System.nanoTime();
        return Duration.ofNanos(Math.max(0, remaining));
    }

    void cancel() {
        cancellation.complete(null);
    }

    void check(AppendOutcome outcome, String operation) {
        if (cancellation.isDone()) {
            throw new NereusException(ErrorCode.CANCELLED, true, operation + " was cancelled", outcome);
        }
        if (remaining().isZero()) {
            throw new NereusException(ErrorCode.TIMEOUT, true, operation + " timed out", outcome);
        }
    }

    <T> CompletableFuture<T> bound(
            Supplier<CompletableFuture<T>> operation,
            AppendOutcome uncertainOutcome,
            String operationName) {
        return bound(operation, uncertainOutcome, uncertainOutcome, operationName);
    }

    <T> CompletableFuture<T> bound(
            Supplier<CompletableFuture<T>> operation,
            AppendOutcome beforeStartOutcome,
            AppendOutcome inFlightOutcome,
            String operationName) {
        check(beforeStartOutcome, operationName);
        CompletableFuture<T> result = new CompletableFuture<>();
        CompletableFuture<T> source;
        try {
            source = Objects.requireNonNull(operation.get(), "operation future");
        } catch (Throwable t) {
            result.completeExceptionally(t);
            return result;
        }
        source.whenComplete((value, error) -> {
            if (error == null) {
                result.complete(value);
            } else {
                result.completeExceptionally(error);
            }
        });
        cancellation.thenRun(() -> result.completeExceptionally(new NereusException(
                ErrorCode.CANCELLED,
                true,
                operationName + " was cancelled",
                inFlightOutcome)));
        long delay = Math.max(0, deadlineNanos - System.nanoTime());
        result.orTimeout(delay, java.util.concurrent.TimeUnit.NANOSECONDS);
        return result.handle((value, error) -> {
            if (error == null) {
                return value;
            }
            Throwable cause = error instanceof java.util.concurrent.CompletionException && error.getCause() != null
                    ? error.getCause()
                    : error;
            if (cause instanceof TimeoutException) {
                throw new NereusException(
                        ErrorCode.TIMEOUT,
                        true,
                        operationName + " timed out",
                        inFlightOutcome);
            }
            if (cause instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new java.util.concurrent.CompletionException(cause);
        });
    }
}
