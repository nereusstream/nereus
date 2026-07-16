/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.backpressure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.NereusException;
import com.nereusstream.api.StreamId;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class MaterializationLagGateTest {
    private static final StreamId STREAM =
            new StreamId("materialization-lag-stream");

    @Test
    void admitsBelowThresholdWithoutDelay() {
        ScheduledExecutorService scheduler =
                Executors.newSingleThreadScheduledExecutor();
        try {
            AtomicInteger reads = new AtomicInteger();
            MaterializationLagGate gate = new MaterializationLagGate(
                    (stream, timeout) -> {
                        reads.incrementAndGet();
                        return CompletableFuture.completedFuture(
                                snapshot(9, 10, 1, 100, 1_000));
                    },
                    thresholds(),
                    scheduler);

            MaterializationLagSnapshot admitted = gate.admit(
                            STREAM, Duration.ofSeconds(2))
                    .join();

            assertThat(admitted.lagRecords()).isOne();
            assertThat(reads).hasValue(1);
        } finally {
            scheduler.shutdownNow();
        }
    }

    @Test
    void throttlesOnceAndRemeasuresBeforeAdmission() {
        ScheduledExecutorService scheduler =
                Executors.newSingleThreadScheduledExecutor();
        try {
            Queue<MaterializationLagSnapshot> snapshots =
                    new ArrayDeque<>();
            snapshots.add(snapshot(5, 10, 5, 500, 1_000));
            snapshots.add(snapshot(9, 10, 1, 100, 100));
            AtomicInteger reads = new AtomicInteger();
            MaterializationLagGate gate = new MaterializationLagGate(
                    (stream, timeout) -> {
                        reads.incrementAndGet();
                        return CompletableFuture.completedFuture(
                                snapshots.remove());
                    },
                    new MaterializationLagThresholds(
                            5,
                            10,
                            500,
                            1_000,
                            Duration.ofSeconds(30),
                            Duration.ofMillis(10)),
                    scheduler);

            MaterializationLagSnapshot admitted = gate.admit(
                            STREAM, Duration.ofSeconds(2))
                    .join();

            assertThat(admitted.lagRecords()).isOne();
            assertThat(reads).hasValue(2);
        } finally {
            scheduler.shutdownNow();
        }
    }

    @Test
    void rejectsRecordsBytesOrAgeBeforeDelay() {
        ScheduledExecutorService scheduler =
                Executors.newSingleThreadScheduledExecutor();
        try {
            assertRejected(
                    scheduler,
                    snapshot(0, 10, 10, 999, 1_000),
                    "records threshold");
            assertRejected(
                    scheduler,
                    snapshot(1, 10, 9, 1_000, 1_000),
                    "bytes threshold");
            assertRejected(
                    scheduler,
                    snapshot(1, 10, 9, 999, 30_000),
                    "age threshold");
        } finally {
            scheduler.shutdownNow();
        }
    }

    @Test
    void zeroThresholdsDisableMeasurementAndThrottleTimeoutFailsClosed() {
        ScheduledExecutorService scheduler =
                Executors.newSingleThreadScheduledExecutor();
        try {
            AtomicInteger reads = new AtomicInteger();
            MaterializationLagGate disabled = new MaterializationLagGate(
                    (stream, timeout) -> {
                        reads.incrementAndGet();
                        return CompletableFuture.failedFuture(
                                new AssertionError("reader must not run"));
                    },
                    new MaterializationLagThresholds(
                            0,
                            0,
                            0,
                            0,
                            Duration.ZERO,
                            Duration.ofMillis(10)),
                    scheduler);

            assertThat(disabled.admit(
                            STREAM, Duration.ofSeconds(1))
                    .join()
                    .lagRecords())
                    .isZero();
            assertThat(reads).hasValue(0);

            MaterializationLagGate throttled = new MaterializationLagGate(
                    (stream, timeout) -> CompletableFuture.completedFuture(
                            snapshot(5, 10, 5, 500, 1_000)),
                    new MaterializationLagThresholds(
                            5,
                            0,
                            0,
                            0,
                            Duration.ZERO,
                            Duration.ofSeconds(1)),
                    scheduler);
            assertThatThrownBy(() -> throttled.admit(
                            STREAM, Duration.ofMillis(10))
                    .join())
                    .hasRootCauseInstanceOf(NereusException.class)
                    .hasRootCauseMessage(
                            "materialization lag throttle exceeds the append admission deadline");
        } finally {
            scheduler.shutdownNow();
        }
    }

    private static void assertRejected(
            ScheduledExecutorService scheduler,
            MaterializationLagSnapshot snapshot,
            String reason) {
        MaterializationLagGate gate = new MaterializationLagGate(
                (stream, timeout) -> CompletableFuture.completedFuture(
                        snapshot),
                thresholds(),
                scheduler);

        assertThatThrownBy(() -> gate.admit(
                        STREAM, Duration.ofSeconds(2))
                .join())
                .hasRootCauseInstanceOf(NereusException.class)
                .hasRootCauseMessage(
                        "async append rejected by materialization lag "
                                + reason);
    }

    private static MaterializationLagThresholds thresholds() {
        return new MaterializationLagThresholds(
                5,
                10,
                500,
                1_000,
                Duration.ofSeconds(30),
                Duration.ofMillis(10));
    }

    private static MaterializationLagSnapshot snapshot(
            long covered,
            long head,
            long records,
            long bytes,
            long age) {
        return new MaterializationLagSnapshot(
                STREAM,
                covered,
                head,
                records,
                bytes,
                age,
                7,
                100_000);
    }
}
