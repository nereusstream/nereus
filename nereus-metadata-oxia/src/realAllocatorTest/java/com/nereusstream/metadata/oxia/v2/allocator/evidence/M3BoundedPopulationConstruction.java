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

package com.nereusstream.metadata.oxia.v2.allocator.evidence;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/** One interruptible outer bound around an exact population-construction phase. */
final class M3BoundedPopulationConstruction {
    private static final long INTERRUPT_DRAIN_SECONDS = 5;

    private M3BoundedPopulationConstruction() {}

    static void run(
            ExecutorService workers,
            Duration timeout,
            String label,
            Supplier<String> progress,
            Construction operation)
            throws Exception {
        Objects.requireNonNull(workers, "workers");
        Objects.requireNonNull(timeout, "timeout");
        Objects.requireNonNull(progress, "progress");
        Objects.requireNonNull(operation, "operation");
        if (timeout.isZero() || timeout.isNegative() || label == null || label.isBlank()) {
            throw new IllegalArgumentException("allocator population-construction bound is invalid");
        }

        CountDownLatch stopped = new CountDownLatch(1);
        Future<Void> batch = workers.submit(() -> {
            try {
                operation.run();
                return null;
            } finally {
                stopped.countDown();
            }
        });
        try {
            batch.get(timeout.toNanos(), TimeUnit.NANOSECONDS);
        } catch (TimeoutException timeoutFailure) {
            batch.cancel(true);
            Throwable drainFailure = awaitInterruptDrain(stopped);
            TimeoutException exact = new TimeoutException(label
                    + " did not finish within "
                    + describe(timeout)
                    + "; "
                    + safeProgress(progress));
            exact.initCause(timeoutFailure);
            addSuppressed(exact, drainFailure);
            throw exact;
        } catch (InterruptedException interrupted) {
            batch.cancel(true);
            Throwable drainFailure = awaitInterruptDrain(stopped);
            IllegalStateException exact =
                    new IllegalStateException(label + " was interrupted during bounded cleanup", interrupted);
            addSuppressed(exact, drainFailure);
            Thread.currentThread().interrupt();
            throw exact;
        } catch (CancellationException cancelled) {
            throw new IllegalStateException(label + " was cancelled outside its bounded owner", cancelled);
        } catch (ExecutionException failed) {
            rethrow(failed.getCause());
        }
    }

    private static Throwable awaitInterruptDrain(CountDownLatch stopped) {
        try {
            if (!stopped.await(INTERRUPT_DRAIN_SECONDS, TimeUnit.SECONDS)) {
                return new IllegalStateException(
                        "allocator population-construction batch ignored its interrupt for "
                                + INTERRUPT_DRAIN_SECONDS
                                + " seconds");
            }
            return null;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return interrupted;
        }
    }

    private static void addSuppressed(Throwable failure, Throwable suppressed) {
        if (suppressed != null) {
            failure.addSuppressed(suppressed);
        }
    }

    private static String safeProgress(Supplier<String> progress) {
        try {
            String exact = progress.get();
            return exact == null || exact.isBlank() ? "progress=UNAVAILABLE" : exact;
        } catch (RuntimeException failure) {
            return "progress=UNAVAILABLE(" + failure.getClass().getSimpleName() + ")";
        }
    }

    private static String describe(Duration timeout) {
        if (timeout.toNanosPart() == 0) {
            return timeout.toSeconds() + " seconds";
        }
        return timeout.toMillis() + " milliseconds";
    }

    private static void rethrow(Throwable failure) throws Exception {
        Throwable exact = failure == null
                ? new IllegalStateException("allocator population construction failed without a cause")
                : failure;
        if (exact instanceof Exception exception) {
            throw exception;
        }
        if (exact instanceof Error error) {
            throw error;
        }
        throw new RuntimeException(exact);
    }

    @FunctionalInterface
    interface Construction {
        void run() throws Exception;
    }
}
