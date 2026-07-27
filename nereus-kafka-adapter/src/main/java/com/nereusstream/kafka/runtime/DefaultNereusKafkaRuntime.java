/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.runtime;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.kafka.partition.KafkaPartitionStorageManager;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Default process lifecycle around an injected partition manager and explicitly owned resources. Provider
 * construction, activation and capability publication remain the responsibility of the startup action supplied by the
 * runtime factory.
 */
public final class DefaultNereusKafkaRuntime implements NereusKafkaRuntime {
    private final Object guard = new Object();
    private final KafkaStorageAdmission admission;
    private final KafkaPartitionStorageManager partitionStorageManager;
    private final KafkaRuntimeStartup startup;
    private final KafkaRuntimeBackgroundService backgroundService;
    private final KafkaRuntimeResources resources;
    private CompletableFuture<Void> startOperation;
    private CompletableFuture<Void> backgroundStartOperation;
    private CompletableFuture<Void> drainOperation;
    private boolean closed;

    public DefaultNereusKafkaRuntime(
            KafkaStorageAdmission admission,
            KafkaPartitionStorageManager partitionStorageManager,
            Supplier<? extends CompletionStage<Void>> startupAction,
            KafkaRuntimeResources resources) {
        this(
                admission,
                partitionStorageManager,
                KafkaRuntimeStartup.from(startupAction),
                KafkaRuntimeBackgroundService.none(),
                resources);
    }

    public DefaultNereusKafkaRuntime(
            KafkaStorageAdmission admission,
            KafkaPartitionStorageManager partitionStorageManager,
            KafkaRuntimeStartup startup,
            KafkaRuntimeResources resources) {
        this(
                admission,
                partitionStorageManager,
                startup,
                KafkaRuntimeBackgroundService.none(),
                resources);
    }

    public DefaultNereusKafkaRuntime(
            KafkaStorageAdmission admission,
            KafkaPartitionStorageManager partitionStorageManager,
            KafkaRuntimeStartup startup,
            KafkaRuntimeBackgroundService backgroundService,
            KafkaRuntimeResources resources) {
        this.admission = Objects.requireNonNull(admission, "admission");
        this.partitionStorageManager = Objects.requireNonNull(
                partitionStorageManager, "partitionStorageManager");
        this.startup = Objects.requireNonNull(startup, "startup");
        this.backgroundService = Objects.requireNonNull(backgroundService, "backgroundService");
        this.resources = Objects.requireNonNull(resources, "resources");
        if (admission.state() != KafkaStorageAdmissionState.STARTING) {
            throw new IllegalArgumentException("Kafka runtime admission must begin in STARTING state");
        }
    }

    @Override
    public CompletionStage<Void> start() {
        CompletableFuture<Void> operation;
        synchronized (guard) {
            if (closed || admission.state() == KafkaStorageAdmissionState.DRAINING
                    || admission.state() == KafkaStorageAdmissionState.CLOSED) {
                return failedClosed("Kafka runtime cannot start after drain or close");
            }
            if (startOperation != null) {
                return protectedView(startOperation);
            }
            startOperation = new CompletableFuture<>();
            operation = startOperation;
        }
        CompletionStage<Void> startup;
        try {
            startup = Objects.requireNonNull(
                    this.startup.start(admission), "Kafka runtime startup future");
        } catch (Throwable failure) {
            completeStartup(operation, failure);
            return protectedView(operation);
        }
        startup.whenComplete((ignored, failure) -> {
            if (failure != null) {
                completeStartup(operation, failure);
            } else {
                startBackground(operation);
            }
        });
        return protectedView(operation);
    }

    @Override
    public KafkaStorageAdmission admission() {
        return admission;
    }

    @Override
    public KafkaPartitionStorageManager partitionStorageManager() {
        return partitionStorageManager;
    }

    @Override
    public KafkaStorageHealth health() {
        return admission.health();
    }

    @Override
    public CompletionStage<Void> beginDrain(DrainReason reason) {
        Objects.requireNonNull(reason, "reason");
        admission.beginDrain(reason);
        CompletableFuture<Void> operation;
        synchronized (guard) {
            if (drainOperation != null) {
                return protectedView(drainOperation);
            }
            drainOperation = new CompletableFuture<>();
            operation = drainOperation;
        }
        stopBackgroundThenManager().whenComplete((ignored, failure) -> {
            if (failure == null) operation.complete(null);
            else operation.completeExceptionally(unwrap(failure));
        });
        return protectedView(operation);
    }

    @Override
    public CompletionStage<Void> awaitDrained(Duration timeout) {
        Duration exactTimeout = positive(timeout);
        CompletableFuture<Void> operation;
        synchronized (guard) {
            if (drainOperation == null) {
                return CompletableFuture.failedFuture(new IllegalStateException(
                        "Kafka runtime drain has not started"));
            }
            operation = drainOperation;
        }
        return operation.copy().orTimeout(exactTimeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        synchronized (guard) {
            if (closed) {
                return;
            }
            closed = true;
        }
        CompletableFuture<Void> drain = beginDrain(DrainReason.OPERATOR_REQUEST).toCompletableFuture();
        admission.close();
        Throwable failure = null;
        try {
            drain.join();
        } catch (Throwable drainFailure) {
            failure = unwrap(drainFailure);
        }
        try {
            partitionStorageManager.close();
        } catch (Throwable managerFailure) {
            failure = combine(failure, managerFailure);
        }
        try {
            resources.close();
        } catch (Throwable resourceFailure) {
            failure = combine(failure, resourceFailure);
        }
        if (failure != null) {
            if (failure instanceof RuntimeException runtimeFailure) {
                throw runtimeFailure;
            }
            throw new IllegalStateException("Failed to close Kafka runtime", failure);
        }
    }

    private void startBackground(CompletableFuture<Void> operation) {
        CompletableFuture<Void> backgroundOperation;
        synchronized (guard) {
            if (closed || admission.state() != KafkaStorageAdmissionState.STARTING) {
                completeStartup(operation, new NereusException(
                        ErrorCode.STORAGE_CLOSED,
                        false,
                        "Kafka runtime startup completed after drain or close"));
                return;
            }
            backgroundStartOperation = new CompletableFuture<>();
            backgroundOperation = backgroundStartOperation;
        }
        CompletionStage<Void> backgroundStart;
        try {
            backgroundStart = Objects.requireNonNull(
                    backgroundService.start(), "Kafka runtime background-service start future");
        } catch (Throwable failure) {
            backgroundOperation.completeExceptionally(unwrap(failure));
            completeStartup(operation, failure);
            return;
        }
        backgroundStart.whenComplete((ignored, failure) -> {
            if (failure == null) backgroundOperation.complete(null);
            else backgroundOperation.completeExceptionally(unwrap(failure));
            completeStartup(operation, failure);
        });
    }

    private CompletableFuture<Void> stopBackgroundThenManager() {
        CompletableFuture<Void> backgroundStart;
        synchronized (guard) {
            backgroundStart = backgroundStartOperation;
        }
        CompletionStage<Void> readyToStop = backgroundStart == null
                ? CompletableFuture.completedFuture(null)
                : backgroundStart.handle((ignored, failure) -> null);
        CompletableFuture<Void> result = new CompletableFuture<>();
        readyToStop
                .thenCompose(ignored -> stopBackground())
                .handle((ignored, backgroundFailure) -> unwrapNullable(backgroundFailure))
                .thenCompose(backgroundFailure -> stopManager()
                        .handle((ignored, managerFailure) -> combine(
                                backgroundFailure, unwrapNullable(managerFailure))))
                .whenComplete((failure, chainFailure) -> {
                    Throwable cause = combine(failure, unwrapNullable(chainFailure));
                    if (cause == null) result.complete(null);
                    else result.completeExceptionally(cause);
                });
        return result;
    }

    private CompletionStage<Void> stopBackground() {
        try {
            return Objects.requireNonNull(
                    backgroundService.closeAsync(),
                    "Kafka runtime background-service close future");
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(unwrap(failure));
        }
    }

    private CompletionStage<Void> stopManager() {
        try {
            return Objects.requireNonNull(
                    partitionStorageManager.shutdown(),
                    "Kafka partition manager shutdown future");
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(unwrap(failure));
        }
    }

    private void completeStartup(CompletableFuture<Void> operation, Throwable failure) {
        if (failure != null) {
            Throwable cause = unwrap(failure);
            admission.markNotReady("runtime startup failed: " + cause.getClass().getSimpleName());
            operation.completeExceptionally(cause);
        } else if (admission.markReady()) {
            operation.complete(null);
        } else {
            operation.completeExceptionally(new NereusException(
                    ErrorCode.STORAGE_CLOSED,
                    false,
                    "Kafka runtime startup completed after drain or close"));
        }
    }

    private static Duration positive(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative() || timeout.isZero() || timeout.toMillis() <= 0) {
            throw new IllegalArgumentException("timeout must be positive and millisecond-representable");
        }
        return timeout;
    }

    private static CompletionStage<Void> failedClosed(String message) {
        return CompletableFuture.failedFuture(new NereusException(
                ErrorCode.STORAGE_CLOSED, false, message));
    }

    private static CompletableFuture<Void> protectedView(CompletableFuture<Void> operation) {
        return operation.copy();
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static Throwable unwrapNullable(Throwable failure) {
        return failure == null ? null : unwrap(failure);
    }

    private static Throwable combine(Throwable primary, Throwable additional) {
        if (primary == null) return additional;
        if (additional != null && additional != primary) primary.addSuppressed(additional);
        return primary;
    }
}
