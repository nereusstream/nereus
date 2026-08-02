/* Licensed under the Apache License, Version 2.0 */

package com.nereusstream.kafka.runtime;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

/**
 * Ordered lifecycle for multiple manager-bound services sharing the same borrowed executors.
 */
final class CompositeKafkaRuntimeBackgroundService implements KafkaRuntimeBackgroundService {
    private final Object guard = new Object();
    private final List<KafkaRuntimeBackgroundService> services;
    private CompletableFuture<Void> startOperation;
    private CompletableFuture<Void> closeOperation;
    private int started;

    CompositeKafkaRuntimeBackgroundService(List<KafkaRuntimeBackgroundService> services) {
        this.services = List.copyOf(Objects.requireNonNull(services, "services"));
    }

    @Override
    public CompletionStage<Void> start() {
        synchronized (guard) {
            if (startOperation != null) {
                return startOperation.copy();
            }
            if (closeOperation != null) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("Kafka background services are closing"));
            }
            startOperation = new CompletableFuture<>();
        }
        startNext(0);
        synchronized (guard) {
            return startOperation.copy();
        }
    }

    @Override
    public CompletionStage<Void> closeAsync() {
        CompletableFuture<Void> result;
        int lastStarted;
        synchronized (guard) {
            if (closeOperation != null) {
                return closeOperation.copy();
            }
            closeOperation = new CompletableFuture<>();
            result = closeOperation;
            lastStarted = started - 1;
        }
        closePrevious(lastStarted, null, result);
        return result.copy();
    }

    private void startNext(int index) {
        if (index == services.size()) {
            synchronized (guard) {
                startOperation.complete(null);
            }
            return;
        }
        CompletionStage<Void> operation;
        try {
            operation = Objects.requireNonNull(services.get(index).start(), "Kafka background-service start future");
        } catch (Throwable failure) {
            rollbackStart(index - 1, unwrap(failure));
            return;
        }
        operation.whenComplete((ignored, failure) -> {
            if (failure != null) {
                rollbackStart(index - 1, unwrap(failure));
                return;
            }
            synchronized (guard) {
                started = index + 1;
            }
            startNext(index + 1);
        });
    }

    private void rollbackStart(int index, Throwable failure) {
        CompletableFuture<Void> rollback = new CompletableFuture<>();
        closePrevious(index, failure, rollback);
        rollback.whenComplete((ignored, rollbackFailure) -> {
            Throwable exact = rollbackFailure == null ? failure : unwrap(rollbackFailure);
            synchronized (guard) {
                startOperation.completeExceptionally(exact);
            }
        });
    }

    private void closePrevious(int index, Throwable failure, CompletableFuture<Void> result) {
        if (index < 0) {
            if (failure == null) {
                result.complete(null);
            } else {
                result.completeExceptionally(failure);
            }
            return;
        }
        CompletionStage<Void> operation;
        try {
            operation =
                    Objects.requireNonNull(services.get(index).closeAsync(), "Kafka background-service close future");
        } catch (Throwable closeFailure) {
            closePrevious(index - 1, combine(failure, unwrap(closeFailure)), result);
            return;
        }
        operation.whenComplete((ignored, closeFailure) ->
                closePrevious(index - 1, combine(failure, unwrapNullable(closeFailure)), result));
    }

    private static Throwable combine(Throwable first, Throwable second) {
        if (first == null) {
            return second;
        }
        if (second != null && second != first) {
            first.addSuppressed(second);
        }
        return first;
    }

    private static Throwable unwrapNullable(Throwable failure) {
        return failure == null ? null : unwrap(failure);
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = Objects.requireNonNull(failure, "failure");
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
