/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.retention;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StreamId;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/** Shared bounded lane that coalesces one active logical-retention plan per stream. */
public final class NereusRetentionExecutionLane implements AutoCloseable {
    private final NereusRetentionConfig config;
    private final ThreadPoolExecutor executor;
    private final ConcurrentMap<StreamId, PlanTask> flights =
            new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    public NereusRetentionExecutionLane(
            NereusRetentionConfig config,
            ThreadFactory threadFactory) {
        this.config = Objects.requireNonNull(config, "config");
        this.executor = new ThreadPoolExecutor(
                config.maxConcurrentPlans(),
                config.maxConcurrentPlans(),
                0,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(config.maxQueuedPlans()),
                Objects.requireNonNull(threadFactory, "threadFactory"),
                new ThreadPoolExecutor.AbortPolicy());
    }

    /**
     * Returns an independent completion for the caller while sharing one admitted execution for duplicate streams.
     */
    public CompletableFuture<Optional<RetentionCandidate>> submit(
            StreamId streamId,
            Supplier<CompletableFuture<Optional<RetentionCandidate>>> operation) {
        StreamId exactStream = Objects.requireNonNull(streamId, "streamId");
        Supplier<CompletableFuture<Optional<RetentionCandidate>>> exactOperation =
                Objects.requireNonNull(operation, "operation");
        if (closed.get()) {
            return failedClosed();
        }
        while (true) {
            PlanTask existing = flights.get(exactStream);
            if (existing != null) {
                return mirror(existing.result());
            }
            PlanTask created = new PlanTask(exactStream, exactOperation);
            existing = flights.putIfAbsent(exactStream, created);
            if (existing != null) {
                continue;
            }
            try {
                if (closed.get()) {
                    throw new RejectedExecutionException(
                            "logical-retention lane is closing");
                }
                executor.execute(created);
            } catch (RejectedExecutionException failure) {
                flights.remove(exactStream, created);
                created.reject(closed.get()
                        ? closedFailure()
                        : new NereusException(
                                ErrorCode.BACKPRESSURE_REJECTED,
                                true,
                                "logical-retention plan queue is full",
                                failure));
            }
            return mirror(created.result());
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        executor.shutdown();
        final boolean terminated;
        try {
            terminated = executor.awaitTermination(
                    config.closeTimeout().toNanos(),
                    TimeUnit.NANOSECONDS);
        } catch (ArithmeticException failure) {
            forceClose();
            throw new IllegalStateException(
                    "logical-retention close timeout is not nanosecond-representable",
                    failure);
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            forceClose();
            throw new IllegalStateException(
                    "interrupted while closing logical-retention lane",
                    failure);
        }
        if (!terminated) {
            forceClose();
            throw new IllegalStateException(
                    "logical-retention lane close deadline expired");
        }
    }

    private void forceClose() {
        NereusException failure = closedFailure();
        List<Runnable> queued = executor.shutdownNow();
        for (Runnable command : queued) {
            if (command instanceof PlanTask task) {
                task.reject(failure);
            }
        }
        flights.values().forEach(task -> task.reject(failure));
    }

    private static CompletableFuture<Optional<RetentionCandidate>> mirror(
            CompletableFuture<Optional<RetentionCandidate>> source) {
        CompletableFuture<Optional<RetentionCandidate>> result =
                new CompletableFuture<>();
        source.whenComplete((value, failure) -> {
            if (failure == null) {
                result.complete(value);
            } else {
                result.completeExceptionally(failure);
            }
        });
        return result;
    }

    private static CompletableFuture<Optional<RetentionCandidate>> failedClosed() {
        return CompletableFuture.failedFuture(closedFailure());
    }

    private static NereusException closedFailure() {
        return new NereusException(
                ErrorCode.STORAGE_CLOSED,
                false,
                "logical-retention lane is closed");
    }

    private final class PlanTask implements Runnable {
        private final StreamId streamId;
        private final Supplier<CompletableFuture<Optional<RetentionCandidate>>>
                operation;
        private final CompletableFuture<Optional<RetentionCandidate>> result =
                new CompletableFuture<>();
        private volatile CompletableFuture<Optional<RetentionCandidate>> source;

        private PlanTask(
                StreamId streamId,
                Supplier<CompletableFuture<Optional<RetentionCandidate>>>
                        operation) {
            this.streamId = streamId;
            this.operation = operation;
        }

        private CompletableFuture<Optional<RetentionCandidate>> result() {
            return result;
        }

        @Override
        public void run() {
            try {
                source = Objects.requireNonNull(
                        operation.get(),
                        "logical-retention operation returned null future");
                Optional<RetentionCandidate> value = source.get(
                        config.operationTimeout().toMillis(),
                        TimeUnit.MILLISECONDS);
                result.complete(Objects.requireNonNull(
                        value,
                        "logical-retention operation returned null result"));
            } catch (TimeoutException failure) {
                cancelSource();
                result.completeExceptionally(new NereusException(
                        ErrorCode.TIMEOUT,
                        true,
                        "logical-retention operation deadline expired",
                        failure));
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                cancelSource();
                result.completeExceptionally(closed.get()
                        ? closedFailure()
                        : new NereusException(
                                ErrorCode.CANCELLED,
                                true,
                                "logical-retention execution was interrupted",
                                failure));
            } catch (ExecutionException failure) {
                result.completeExceptionally(
                        failure.getCause() == null ? failure : failure.getCause());
            } catch (CancellationException failure) {
                result.completeExceptionally(new NereusException(
                        ErrorCode.CANCELLED,
                        true,
                        "logical-retention operation was cancelled",
                        failure));
            } catch (Throwable failure) {
                result.completeExceptionally(failure);
            } finally {
                flights.remove(streamId, this);
            }
        }

        private void cancelSource() {
            CompletableFuture<Optional<RetentionCandidate>> current = source;
            if (current != null) {
                current.cancel(true);
            }
        }

        private void reject(Throwable failure) {
            cancelSource();
            result.completeExceptionally(failure);
            flights.remove(streamId, this);
        }
    }
}
