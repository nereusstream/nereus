/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.activation;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.kafka.runtime.KafkaRuntimeStartup;
import com.nereusstream.kafka.runtime.KafkaStorageAdmission;
import com.nereusstream.metadata.oxia.KafkaStorageActivationMetadataStore;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/** Broker startup/health component that owns capability renewal and bounded activation polling. */
public final class KafkaStorageActivationRuntime implements KafkaRuntimeStartup, AutoCloseable {
    private final Object guard = new Object();
    private final KafkaStorageActivationMetadataStore store;
    private final KafkaBrokerCapabilitySpecification capability;
    private final KafkaStorageClusterSnapshotProvider clusterSnapshots;
    private final ScheduledExecutorService scheduler;
    private final Clock clock;
    private final Duration activationWaitTimeout;
    private final Duration activationPollInterval;
    private final Supplier<? extends CompletionStage<Void>> downstreamStartup;
    private KafkaBrokerCapabilityPublisher publisher;
    private KafkaStorageAdmission admission;
    private CompletableFuture<Void> startOperation;
    private ScheduledFuture<?> pendingPoll;
    private long deadlineMillis;
    private int remainingVerificationAttempts;
    private boolean closed;

    public KafkaStorageActivationRuntime(
            KafkaStorageActivationMetadataStore store,
            KafkaBrokerCapabilitySpecification capability,
            KafkaStorageClusterSnapshotProvider clusterSnapshots,
            ScheduledExecutorService scheduler,
            Clock clock,
            Duration activationWaitTimeout,
            Duration activationPollInterval,
            Supplier<? extends CompletionStage<Void>> downstreamStartup) {
        this.store = Objects.requireNonNull(store, "store");
        this.capability = Objects.requireNonNull(capability, "capability");
        this.clusterSnapshots = Objects.requireNonNull(clusterSnapshots, "clusterSnapshots");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.activationWaitTimeout = positive(activationWaitTimeout, "activationWaitTimeout");
        this.activationPollInterval = positive(activationPollInterval, "activationPollInterval");
        if (this.activationPollInterval.compareTo(this.activationWaitTimeout) > 0) {
            throw new IllegalArgumentException("activationPollInterval cannot exceed activationWaitTimeout");
        }
        this.downstreamStartup = Objects.requireNonNull(downstreamStartup, "downstreamStartup");
    }

    @Override
    public CompletionStage<Void> start(KafkaStorageAdmission suppliedAdmission) {
        KafkaStorageAdmission exactAdmission = Objects.requireNonNull(
                suppliedAdmission, "suppliedAdmission");
        CompletableFuture<Void> operation;
        KafkaBrokerCapabilityPublisher exactPublisher;
        synchronized (guard) {
            if (closed) return CompletableFuture.failedFuture(closedFailure());
            if (startOperation != null) {
                if (admission != exactAdmission) {
                    return CompletableFuture.failedFuture(new IllegalArgumentException(
                            "activation runtime cannot be rebound to another admission gate"));
                }
                return startOperation.copy();
            }
            admission = exactAdmission;
            startOperation = new CompletableFuture<>();
            operation = startOperation;
            deadlineMillis = addExact(clock.millis(), activationWaitTimeout.toMillis());
            remainingVerificationAttempts = verificationAttempts(
                    activationWaitTimeout, activationPollInterval);
            publisher = new KafkaBrokerCapabilityPublisher(
                    store, capability, scheduler, clock, this::failHeartbeat);
            exactPublisher = publisher;
        }
        exactPublisher.start().whenComplete((ignored, failure) -> {
            if (failure != null) {
                failStart(unwrap(failure));
            } else {
                verifyOrRetry();
            }
        });
        return operation.copy();
    }

    private void verifyOrRetry() {
        synchronized (guard) {
            if (closed || startOperation == null || startOperation.isDone()) return;
            pendingPoll = null;
        }
        KafkaStorageActivationVerifier verifier = new KafkaStorageActivationVerifier(
                store, capability, clusterSnapshots, clock);
        verifier.verifyCurrent().whenComplete((ignored, failure) -> {
            if (failure == null) {
                runDownstreamStartup();
            } else {
                retryOrFail(unwrap(failure));
            }
        });
    }

    private void retryOrFail(Throwable failure) {
        boolean retriable = failure instanceof NereusException nereus && nereus.retriable();
        long remaining = deadlineMillis - clock.millis();
        synchronized (guard) {
            remainingVerificationAttempts--;
        }
        if (!retriable || remaining <= 0 || remainingVerificationAttempts <= 0) {
            if (retriable) {
                failStart(new NereusException(
                        ErrorCode.TIMEOUT,
                        true,
                        "timed out waiting for Kafka storage ACTIVE/readiness authority",
                        failure));
            } else {
                failStart(failure);
            }
            return;
        }
        long delay = Math.min(activationPollInterval.toMillis(), remaining);
        try {
            synchronized (guard) {
                if (closed || startOperation == null || startOperation.isDone()) return;
                pendingPoll = scheduler.schedule(this::verifyOrRetry, delay, TimeUnit.MILLISECONDS);
            }
        } catch (Throwable scheduleFailure) {
            failStart(scheduleFailure);
        }
    }

    private void runDownstreamStartup() {
        CompletionStage<Void> downstream;
        try {
            downstream = Objects.requireNonNull(
                    downstreamStartup.get(), "downstream startup future");
        } catch (Throwable failure) {
            failStart(failure);
            return;
        }
        downstream.whenComplete((ignored, failure) -> {
            if (failure == null) completeStart();
            else failStart(unwrap(failure));
        });
    }

    private void completeStart() {
        synchronized (guard) {
            if (startOperation == null || startOperation.isDone()) return;
            if (closed) {
                startOperation.completeExceptionally(closedFailure());
            } else {
                startOperation.complete(null);
            }
        }
    }

    private void failStart(Throwable supplied) {
        Throwable failure = unwrap(supplied);
        synchronized (guard) {
            if (pendingPoll != null) pendingPoll.cancel(false);
            if (startOperation != null && !startOperation.isDone()) {
                startOperation.completeExceptionally(failure);
            }
        }
    }

    private void failHeartbeat(Throwable failure) {
        KafkaStorageAdmission currentAdmission;
        synchronized (guard) {
            currentAdmission = admission;
        }
        if (currentAdmission != null) {
            currentAdmission.markNotReady(
                    "capability heartbeat failed: " + failure.getClass().getSimpleName());
        }
        failStart(failure);
    }

    @Override
    public void close() {
        KafkaBrokerCapabilityPublisher exactPublisher;
        synchronized (guard) {
            if (closed) return;
            closed = true;
            if (pendingPoll != null) pendingPoll.cancel(false);
            exactPublisher = publisher;
            if (startOperation != null && !startOperation.isDone()) {
                startOperation.completeExceptionally(closedFailure());
            }
        }
        if (exactPublisher != null) exactPublisher.close();
    }

    private static Duration positive(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isZero() || value.isNegative() || value.toMillis() <= 0) {
            throw new IllegalArgumentException(name + " must be positive and millisecond-representable");
        }
        return value;
    }

    private static Throwable unwrap(Throwable supplied) {
        Throwable current = supplied;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static long addExact(long left, long right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException failure) {
            throw new IllegalArgumentException("activation wait deadline overflow", failure);
        }
    }

    private static int verificationAttempts(Duration timeout, Duration pollInterval) {
        long timeoutMillis = timeout.toMillis();
        long pollMillis = pollInterval.toMillis();
        long retries = timeoutMillis / pollMillis;
        if (timeoutMillis % pollMillis != 0) retries++;
        if (retries >= 1_000_000) {
            throw new IllegalArgumentException("activation wait permits too many polling attempts");
        }
        return Math.toIntExact(retries + 1);
    }

    private static NereusException closedFailure() {
        return new NereusException(ErrorCode.STORAGE_CLOSED, false, "activation runtime is closed");
    }
}
