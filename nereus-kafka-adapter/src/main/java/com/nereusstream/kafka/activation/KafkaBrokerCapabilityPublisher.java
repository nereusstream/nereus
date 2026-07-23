/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.activation;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.metadata.oxia.KafkaStorageActivationMetadataStore;
import com.nereusstream.metadata.oxia.VersionedKafkaBrokerCapability;
import java.time.Clock;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/** Publishes one broker-epoch capability and owns only its scheduled heartbeat operation. */
public final class KafkaBrokerCapabilityPublisher implements AutoCloseable {
    private final Object guard = new Object();
    private final KafkaStorageActivationMetadataStore store;
    private final KafkaBrokerCapabilitySpecification specification;
    private final ScheduledExecutorService scheduler;
    private final Clock clock;
    private final Consumer<Throwable> heartbeatFailureHandler;
    private final AtomicReference<VersionedKafkaBrokerCapability> current = new AtomicReference<>();
    private final AtomicReference<Throwable> heartbeatFailure = new AtomicReference<>();
    private final AtomicBoolean heartbeatInFlight = new AtomicBoolean();
    private CompletableFuture<VersionedKafkaBrokerCapability> startOperation;
    private ScheduledFuture<?> heartbeatTask;
    private boolean closed;

    public KafkaBrokerCapabilityPublisher(
            KafkaStorageActivationMetadataStore store,
            KafkaBrokerCapabilitySpecification specification,
            ScheduledExecutorService scheduler,
            Clock clock,
            Consumer<Throwable> heartbeatFailureHandler) {
        this.store = Objects.requireNonNull(store, "store");
        this.specification = Objects.requireNonNull(specification, "specification");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.heartbeatFailureHandler = Objects.requireNonNull(
                heartbeatFailureHandler, "heartbeatFailureHandler");
    }

    public CompletionStage<VersionedKafkaBrokerCapability> start() {
        CompletableFuture<VersionedKafkaBrokerCapability> operation;
        synchronized (guard) {
            if (closed) {
                return CompletableFuture.failedFuture(closedFailure());
            }
            if (startOperation != null) {
                return startOperation.copy();
            }
            startOperation = new CompletableFuture<>();
            operation = startOperation;
        }
        publishInitial().whenComplete((published, failure) -> completeStart(operation, published, failure));
        return operation.copy();
    }

    public Optional<VersionedKafkaBrokerCapability> current() {
        return Optional.ofNullable(current.get());
    }

    public Optional<Throwable> heartbeatFailure() {
        return Optional.ofNullable(heartbeatFailure.get());
    }

    private CompletableFuture<VersionedKafkaBrokerCapability> publishInitial() {
        return store.getCapability(specification.identity()).thenCompose(existing -> {
            long now = clock.millis();
            if (existing.isEmpty()) {
                return store.createCapability(specification.initialRecord(now));
            }
            VersionedKafkaBrokerCapability exact = existing.orElseThrow();
            if (!specification.matchesImmutableFacts(exact.value())) {
                return CompletableFuture.failedFuture(new NereusException(
                        ErrorCode.METADATA_INVARIANT_VIOLATION,
                        false,
                        "broker epoch capability is already owned by different immutable facts"));
            }
            return store.heartbeatCapability(
                    exact, specification.heartbeatRecord(exact.value(), now));
        });
    }

    private void completeStart(
            CompletableFuture<VersionedKafkaBrokerCapability> operation,
            VersionedKafkaBrokerCapability published,
            Throwable failure) {
        if (failure != null) {
            operation.completeExceptionally(unwrap(failure));
            return;
        }
        Throwable scheduleFailure = null;
        synchronized (guard) {
            if (closed) {
                scheduleFailure = closedFailure();
            } else {
                current.set(Objects.requireNonNull(published, "published capability"));
                try {
                    long interval = specification.heartbeatInterval().toMillis();
                    heartbeatTask = scheduler.scheduleWithFixedDelay(
                            this::heartbeat, interval, interval, TimeUnit.MILLISECONDS);
                } catch (Throwable failureToSchedule) {
                    scheduleFailure = failureToSchedule;
                }
            }
        }
        if (scheduleFailure == null) {
            operation.complete(published);
        } else {
            operation.completeExceptionally(scheduleFailure);
        }
    }

    private void heartbeat() {
        if (closedOrFailed() || !heartbeatInFlight.compareAndSet(false, true)) {
            return;
        }
        VersionedKafkaBrokerCapability expected = current.get();
        if (expected == null) {
            heartbeatInFlight.set(false);
            failHeartbeat(new IllegalStateException("capability heartbeat started without a published value"));
            return;
        }
        CompletableFuture<VersionedKafkaBrokerCapability> attempt;
        try {
            attempt = store.heartbeatCapability(
                    expected,
                    specification.heartbeatRecord(expected.value(), clock.millis()));
        } catch (Throwable failure) {
            heartbeatInFlight.set(false);
            failHeartbeat(failure);
            return;
        }
        attempt.whenComplete((updated, failure) -> {
            heartbeatInFlight.set(false);
            if (failure != null) {
                failHeartbeat(unwrap(failure));
            } else if (!closedOrFailed()) {
                current.compareAndSet(expected, Objects.requireNonNull(updated, "updated capability"));
            }
        });
    }

    private boolean closedOrFailed() {
        synchronized (guard) {
            return closed || heartbeatFailure.get() != null;
        }
    }

    private void failHeartbeat(Throwable supplied) {
        Throwable failure = unwrap(supplied);
        if (!heartbeatFailure.compareAndSet(null, failure)) {
            return;
        }
        synchronized (guard) {
            if (heartbeatTask != null) heartbeatTask.cancel(false);
        }
        try {
            heartbeatFailureHandler.accept(failure);
        } catch (Throwable callbackFailure) {
            failure.addSuppressed(callbackFailure);
        }
    }

    @Override
    public void close() {
        synchronized (guard) {
            if (closed) return;
            closed = true;
            if (heartbeatTask != null) heartbeatTask.cancel(false);
        }
    }

    private static Throwable unwrap(Throwable supplied) {
        Throwable current = supplied;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static NereusException closedFailure() {
        return new NereusException(ErrorCode.STORAGE_CLOSED, false, "capability publisher is closed");
    }
}
