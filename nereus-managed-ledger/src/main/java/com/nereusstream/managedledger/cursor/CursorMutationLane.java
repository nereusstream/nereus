/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import java.util.ArrayDeque;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import org.apache.bookkeeper.mledger.ManagedLedgerException;

/** Bounded FIFO lane for one local cursor handle; Oxia CAS remains the distributed lock. */
final class CursorMutationLane {
    private final int maximumPending;
    private final Executor executor;
    private final ArrayDeque<Pending<?>> queue = new ArrayDeque<>();

    private boolean running;
    private Throwable closeCause;
    private int pendingOperations;

    CursorMutationLane(int maximumPending, Executor executor) {
        if (maximumPending <= 0) {
            throw new IllegalArgumentException("maximumPending must be positive");
        }
        this.maximumPending = maximumPending;
        this.executor = Objects.requireNonNull(executor, "executor");
    }

    <T> CompletableFuture<T> submit(Supplier<CompletableFuture<T>> operation) {
        Objects.requireNonNull(operation, "operation");
        Pending<T> pending = new Pending<>(operation);
        boolean schedule = false;
        synchronized (this) {
            if (closeCause != null) {
                return CompletableFuture.failedFuture(closeCause);
            }
            if (pendingOperations >= maximumPending) {
                return CompletableFuture.failedFuture(
                        new ManagedLedgerException.TooManyRequestsException(
                                "durable cursor mutation queue is full"));
            }
            queue.addLast(pending);
            pendingOperations++;
            if (!running) {
                running = true;
                schedule = true;
            }
        }
        if (schedule) {
            executeNext();
        }
        return pending.result;
    }

    synchronized int pendingOperations() {
        return pendingOperations;
    }

    void close(Throwable cause) {
        Objects.requireNonNull(cause, "cause");
        ArrayDeque<Pending<?>> rejected = new ArrayDeque<>();
        synchronized (this) {
            if (closeCause != null) {
                return;
            }
            closeCause = cause;
            while (!queue.isEmpty()) {
                rejected.addLast(queue.removeFirst());
                pendingOperations--;
            }
            if (pendingOperations == 0) {
                running = false;
            }
        }
        rejected.forEach(item -> item.result.completeExceptionally(cause));
    }

    private void executeNext() {
        try {
            executor.execute(this::runNext);
        } catch (RuntimeException error) {
            failQueued(error);
        }
    }

    private void runNext() {
        Pending<?> pending;
        synchronized (this) {
            pending = queue.pollFirst();
            if (pending == null) {
                if (pendingOperations == 0) {
                    running = false;
                }
                return;
            }
        }
        pending.start().whenComplete((ignored, error) -> {
            synchronized (this) {
                pendingOperations--;
            }
            if (error == null) {
                pending.completeFromOperation();
            } else {
                pending.completeExceptionally(unwrap(error));
            }
            boolean hasNext;
            synchronized (this) {
                hasNext = !queue.isEmpty();
                if (!hasNext) {
                    running = false;
                }
            }
            if (hasNext) {
                executeNext();
            }
        });
    }

    private void failQueued(Throwable failure) {
        ArrayDeque<Pending<?>> rejected = new ArrayDeque<>();
        synchronized (this) {
            if (closeCause == null) {
                closeCause = failure;
            }
            while (!queue.isEmpty()) {
                rejected.addLast(queue.removeFirst());
                pendingOperations--;
            }
            if (pendingOperations == 0) {
                running = false;
            }
        }
        rejected.forEach(item -> item.result.completeExceptionally(failure));
    }

    private static Throwable unwrap(Throwable error) {
        Throwable current = error;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static final class Pending<T> {
        private final Supplier<CompletableFuture<T>> operation;
        private final CompletableFuture<T> result = new CompletableFuture<>();
        private CompletableFuture<T> operationResult;

        private Pending(Supplier<CompletableFuture<T>> operation) {
            this.operation = operation;
        }

        private CompletableFuture<T> start() {
            try {
                operationResult = Objects.requireNonNull(
                        operation.get(), "cursor mutation operation returned null future");
            } catch (Throwable error) {
                operationResult = CompletableFuture.failedFuture(error);
            }
            return operationResult;
        }

        private void completeFromOperation() {
            result.complete(operationResult.join());
        }

        private void completeExceptionally(Throwable failure) {
            result.completeExceptionally(failure);
        }
    }
}
