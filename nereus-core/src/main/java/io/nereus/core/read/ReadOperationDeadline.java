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

package io.nereus.core.read;

import io.nereus.api.ErrorCode;
import io.nereus.api.NereusException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

final class ReadOperationDeadline {
    private final long deadlineNanos;
    private final CompletableFuture<Void> cancellation = new CompletableFuture<>();

    ReadOperationDeadline(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        long timeoutNanos;
        try {
            timeoutNanos = timeout.toNanos();
        } catch (ArithmeticException e) {
            timeoutNanos = Long.MAX_VALUE;
        }
        long now = System.nanoTime();
        long calculated;
        try {
            calculated = Math.addExact(now, timeoutNanos);
        } catch (ArithmeticException e) {
            calculated = Long.MAX_VALUE;
        }
        deadlineNanos = calculated;
    }

    Duration remaining() {
        check("read operation");
        return Duration.ofNanos(deadlineNanos - System.nanoTime());
    }

    void cancel() {
        cancellation.complete(null);
    }

    void check(String operation) {
        if (cancellation.isDone()) {
            throw new NereusException(ErrorCode.CANCELLED, true, operation + " was cancelled");
        }
        if (deadlineNanos - System.nanoTime() <= 0) {
            throw new NereusException(ErrorCode.TIMEOUT, true, operation + " timed out");
        }
    }

    <T> CompletableFuture<T> bound(
            Supplier<CompletableFuture<T>> operation,
            String operationName) {
        check(operationName);
        CompletableFuture<T> source;
        try {
            source = Objects.requireNonNull(operation.get(), "operation future");
        } catch (Throwable t) {
            return CompletableFuture.failedFuture(t);
        }
        CompletableFuture<T> result = new CompletableFuture<>();
        source.whenComplete((value, error) -> {
            if (error == null) {
                result.complete(value);
            } else {
                result.completeExceptionally(error);
            }
        });
        cancellation.thenRun(() -> result.completeExceptionally(
                new NereusException(ErrorCode.CANCELLED, true, operationName + " was cancelled")));
        long delay = Math.max(0, deadlineNanos - System.nanoTime());
        result.orTimeout(delay, TimeUnit.NANOSECONDS);
        return result.handle((value, error) -> {
            if (error == null) {
                return value;
            }
            Throwable cause = unwrap(error);
            if (cause instanceof TimeoutException) {
                throw new NereusException(ErrorCode.TIMEOUT, true, operationName + " timed out");
            }
            if (cause instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new CompletionException(cause);
        });
    }

    private static Throwable unwrap(Throwable error) {
        Throwable current = error;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
