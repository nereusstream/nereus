/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.backpressure;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StreamId;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Applies one bounded async-append delay or a retriable rejection from exact materialization-lag evidence.
 *
 * <p>The gate never pauses reads, repair, or workers. After a throttle delay it measures again so a recovered backlog
 * is admitted and a newly crossed reject boundary still fails before primary IO.
 */
public final class MaterializationLagGate {
    private final MaterializationLagSnapshotReader reader;
    private final MaterializationLagThresholds thresholds;
    private final ScheduledExecutorService scheduler;

    public MaterializationLagGate(
            MaterializationLagSnapshotReader reader,
            MaterializationLagThresholds thresholds,
            ScheduledExecutorService scheduler) {
        this.reader = Objects.requireNonNull(reader, "reader");
        this.thresholds = Objects.requireNonNull(
                thresholds, "thresholds");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
    }

    public CompletableFuture<MaterializationLagSnapshot> admit(
            StreamId streamId,
            Duration timeout) {
        final StreamId exactStream;
        final Deadline deadline;
        try {
            exactStream = Objects.requireNonNull(streamId, "streamId");
            deadline = new Deadline(timeout);
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return measure(exactStream, deadline)
                .thenCompose(first -> {
                    requireNotRejected(first);
                    if (!isThrottled(first)) {
                        return CompletableFuture.completedFuture(first);
                    }
                    return delay(deadline)
                            .thenCompose(ignored -> measure(
                                    exactStream, deadline))
                            .thenApply(second -> {
                                requireNotRejected(second);
                                return second;
                            });
                });
    }

    private CompletableFuture<MaterializationLagSnapshot> measure(
            StreamId streamId,
            Deadline deadline) {
        if (thresholds.allDisabled()) {
            return CompletableFuture.completedFuture(
                    new MaterializationLagSnapshot(
                            streamId,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0));
        }
        return reader.measure(streamId, deadline.remaining());
    }

    private CompletableFuture<Void> delay(Deadline deadline) {
        long delayNanos = thresholds.throttleDelay().toNanos();
        if (delayNanos >= deadline.remaining().toNanos()) {
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.TIMEOUT,
                    true,
                    "materialization lag throttle exceeds the append admission deadline"));
        }
        CompletableFuture<Void> result = new CompletableFuture<>();
        try {
            scheduler.schedule(
                    () -> result.complete(null),
                    delayNanos,
                    TimeUnit.NANOSECONDS);
        } catch (RejectedExecutionException failure) {
            result.completeExceptionally(new NereusException(
                    ErrorCode.STORAGE_CLOSED,
                    false,
                    "materialization lag throttle scheduler rejected admitted work",
                    failure));
        }
        return result;
    }

    private void requireNotRejected(
            MaterializationLagSnapshot snapshot) {
        String reason = rejectionReason(snapshot);
        if (reason != null) {
            throw new NereusException(
                    ErrorCode.BACKPRESSURE_REJECTED,
                    true,
                    "async append rejected by materialization lag " + reason);
        }
    }

    private String rejectionReason(
            MaterializationLagSnapshot snapshot) {
        if (thresholds.rejectRecords() > 0
                && snapshot.lagRecords()
                        >= thresholds.rejectRecords()) {
            return "records threshold";
        }
        if (thresholds.rejectBytes() > 0
                && snapshot.lagBytes()
                        >= thresholds.rejectBytes()) {
            return "bytes threshold";
        }
        if (!thresholds.rejectAge().isZero()
                && snapshot.oldestLagMillis()
                        >= thresholds.rejectAge().toMillis()) {
            return "age threshold";
        }
        return null;
    }

    private boolean isThrottled(
            MaterializationLagSnapshot snapshot) {
        return thresholds.throttleRecords() > 0
                        && snapshot.lagRecords()
                                >= thresholds.throttleRecords()
                || thresholds.throttleBytes() > 0
                        && snapshot.lagBytes()
                                >= thresholds.throttleBytes();
    }

    private static final class Deadline {
        private final long deadlineNanos;

        private Deadline(Duration timeout) {
            Objects.requireNonNull(timeout, "timeout");
            if (timeout.isZero() || timeout.isNegative()) {
                throw new IllegalArgumentException(
                        "timeout must be positive");
            }
            long now = System.nanoTime();
            long nanos;
            try {
                nanos = timeout.toNanos();
            } catch (ArithmeticException failure) {
                nanos = Long.MAX_VALUE;
            }
            deadlineNanos = nanos >= Long.MAX_VALUE - now
                    ? Long.MAX_VALUE
                    : now + nanos;
        }

        private Duration remaining() {
            long remaining = deadlineNanos == Long.MAX_VALUE
                    ? Long.MAX_VALUE
                    : deadlineNanos - System.nanoTime();
            if (remaining <= 0) {
                throw new NereusException(
                        ErrorCode.TIMEOUT,
                        true,
                        "materialization lag admission deadline expired");
            }
            return Duration.ofNanos(remaining);
        }
    }
}
