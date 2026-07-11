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

package com.nereusstream.core.trim;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.TrimOptions;
import com.nereusstream.core.StreamStorageConfig;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.records.TrimRecord;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/** Coordinates the metadata-only Phase 1 trim operation. */
public final class TrimCoordinator implements AutoCloseable {
    private final StreamStorageConfig config;
    private final OxiaMetadataStore metadataStore;
    private final Consumer<StreamId> cacheInvalidator;
    private final TrimMetricsObserver observer;
    private final Executor callbackExecutor;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Object lifecycleMonitor = new Object();
    private int activeTrims;

    public TrimCoordinator(
            StreamStorageConfig config,
            OxiaMetadataStore metadataStore,
            Consumer<StreamId> cacheInvalidator,
            TrimMetricsObserver observer,
            Executor callbackExecutor) {
        this.config = Objects.requireNonNull(config, "config");
        this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
        this.cacheInvalidator = Objects.requireNonNull(cacheInvalidator, "cacheInvalidator");
        this.observer = Objects.requireNonNull(observer, "observer");
        this.callbackExecutor = Objects.requireNonNull(callbackExecutor, "callbackExecutor");
    }

    public CompletableFuture<Void> trim(
            StreamId streamId,
            long beforeOffset,
            TrimOptions options) {
        CancelAwareFuture<Void> result = new CancelAwareFuture<>();
        if (closed.get()) {
            result.completeExceptionally(
                    new NereusException(ErrorCode.STORAGE_CLOSED, false, "stream storage is closed"));
            return result;
        }
        if (streamId == null || options == null) {
            result.completeExceptionally(new NereusException(
                    ErrorCode.INVALID_ARGUMENT, false, "streamId and trim options are required"));
            return result;
        }
        if (beforeOffset < 0) {
            result.completeExceptionally(new NereusException(
                    ErrorCode.INVALID_ARGUMENT, false, "beforeOffset must be non-negative"));
            return result;
        }
        try {
            trimAccepted();
        } catch (NereusException e) {
            result.completeExceptionally(e);
            return result;
        }

        TrimOperationDeadline deadline = new TrimOperationDeadline(options.timeout());
        result.onCancel(deadline::cancel);
        AtomicReference<CompletableFuture<TrimRecord>> sourceOperation = new AtomicReference<>();
        AtomicBoolean lifecycleReleased = new AtomicBoolean();
        Runnable releaseLifecycle = () -> {
            if (lifecycleReleased.compareAndSet(false, true)) {
                trimCompleted();
            }
        };
        CompletableFuture<Void> pipeline = deadline
                .bound(
                        () -> {
                            CompletableFuture<TrimRecord> source = metadataStore
                                    .updateTrim(config.cluster(), streamId, beforeOffset, options.reason())
                                    .thenApplyAsync(record -> {
                                        validateResult(streamId, beforeOffset, record);
                                        cacheInvalidator.accept(streamId);
                                        return record;
                                    }, callbackExecutor);
                            sourceOperation.set(source);
                            source.whenComplete((ignored, error) -> releaseLifecycle.run());
                            return source;
                        },
                        "update stream trim offset")
                .thenApplyAsync(record -> {
                    observe(() -> observer.onTrimSucceeded(streamId, beforeOffset));
                    return null;
                }, callbackExecutor)
                .handle((ignored, error) -> {
                    if (error == null) {
                        return null;
                    }
                    NereusException failure = normalizeFailure(error);
                    observe(() -> observer.onTrimFailed(failure.code()));
                    throw failure;
                });
        pipeline.whenComplete((ignored, error) -> {
            if (sourceOperation.get() == null) {
                releaseLifecycle.run();
            }
        });
        completeFrom(result, pipeline);
        return result;
    }

    private static void validateResult(
            StreamId streamId,
            long beforeOffset,
            TrimRecord record) {
        if (record == null
                || !record.streamId().equals(streamId.value())
                || record.trimOffset() != beforeOffset) {
            throw new NereusException(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    "trim response does not match the requested stream and offset");
        }
    }

    private void trimAccepted() {
        synchronized (lifecycleMonitor) {
            if (closed.get()) {
                throw new NereusException(ErrorCode.STORAGE_CLOSED, false, "stream storage is closed");
            }
            activeTrims++;
        }
    }

    private void trimCompleted() {
        synchronized (lifecycleMonitor) {
            activeTrims--;
            lifecycleMonitor.notifyAll();
        }
    }

    private static NereusException normalizeFailure(Throwable error) {
        Throwable cause = unwrap(error);
        if (cause instanceof NereusException nereus) {
            return nereus;
        }
        if (cause instanceof CancellationException) {
            return new NereusException(ErrorCode.CANCELLED, true, "trim was cancelled", cause);
        }
        if (cause instanceof IllegalArgumentException) {
            return new NereusException(ErrorCode.INVALID_ARGUMENT, false, cause.getMessage(), cause);
        }
        return new NereusException(
                ErrorCode.METADATA_UNAVAILABLE,
                true,
                "trim failed with an unexpected asynchronous error",
                cause);
    }

    private static Throwable unwrap(Throwable error) {
        Throwable current = error;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static <T> void completeFrom(
            CompletableFuture<T> target,
            CompletableFuture<T> source) {
        source.whenComplete((value, error) -> {
            if (error == null) {
                target.complete(value);
            } else {
                target.completeExceptionally(unwrap(error));
            }
        });
    }

    private static void observe(Runnable callback) {
        try {
            callback.run();
        } catch (RuntimeException ignored) {
            // Metrics callbacks cannot alter trim correctness.
        }
    }

    @Override
    public void close() {
        beginClose();
        awaitClose(config.shutdownGrace());
    }

    /** Stops admission without waiting for already accepted trims. */
    public void beginClose() {
        closed.set(true);
    }

    /** Waits for accepted trims using the caller's remaining global shutdown budget. */
    public void awaitClose(Duration grace) {
        Objects.requireNonNull(grace, "grace");
        long graceNanos;
        try {
            graceNanos = grace.toNanos();
        } catch (ArithmeticException e) {
            graceNanos = Long.MAX_VALUE;
        }
        long start = System.nanoTime();
        synchronized (lifecycleMonitor) {
            while (activeTrims > 0) {
                long remaining = graceNanos - (System.nanoTime() - start);
                if (remaining <= 0) {
                    break;
                }
                try {
                    lifecycleMonitor.wait(remaining / 1_000_000, (int) (remaining % 1_000_000));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    private static final class CancelAwareFuture<T> extends CompletableFuture<T> {
        private Runnable cancellation = () -> {
        };

        synchronized void onCancel(Runnable action) {
            cancellation = Objects.requireNonNull(action, "action");
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            if (isDone()) {
                return false;
            }
            Runnable action;
            synchronized (this) {
                action = cancellation;
            }
            action.run();
            return true;
        }
    }
}
