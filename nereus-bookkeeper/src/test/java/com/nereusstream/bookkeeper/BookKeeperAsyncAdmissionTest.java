/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.DurabilityLevel;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.core.append.AppendAdmissionRequest;
import com.nereusstream.core.backpressure.MaterializationLagGate;
import com.nereusstream.core.backpressure.MaterializationLagSnapshot;
import com.nereusstream.core.backpressure.MaterializationLagThresholds;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class BookKeeperAsyncAdmissionTest {
    private static final StreamId STREAM = new StreamId("bk-async-admission-stream");

    @Test
    void throttlesAndRejectsBeforeWal() {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        Queue<MaterializationLagSnapshot> snapshots = new ArrayDeque<>();
        snapshots.add(snapshot(1));
        snapshots.add(snapshot(0));
        snapshots.add(snapshot(2));
        AtomicInteger measurements = new AtomicInteger();
        MaterializationLagGate gate = new MaterializationLagGate(
                (stream, timeout) -> {
                    measurements.incrementAndGet();
                    assertThat(stream).isEqualTo(STREAM);
                    assertThat(timeout).isPositive();
                    return CompletableFuture.completedFuture(snapshots.remove());
                },
                new MaterializationLagThresholds(
                        1,
                        2,
                        0,
                        0,
                        Duration.ZERO,
                        Duration.ofMillis(1)),
                scheduler);
        BookKeeperAsyncAppendAdmissionGuard guard = new BookKeeperAsyncAppendAdmissionGuard(gate);
        try {
            guard.admit(request(StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT)).join();

            assertThat(measurements).hasValue(2);
            assertThatThrownBy(() -> guard.admit(
                            request(StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT))
                    .join())
                    .hasRootCauseInstanceOf(NereusException.class)
                    .rootCause()
                    .extracting(failure -> ((NereusException) failure).code())
                    .isEqualTo(ErrorCode.BACKPRESSURE_REJECTED);
            assertThat(measurements).hasValue(3);

            guard.admit(request(StorageProfile.BOOKKEEPER_WAL_ONLY)).join();
            guard.admit(request(StorageProfile.BOOKKEEPER_WAL_SYNC_OBJECT)).join();
            assertThat(measurements).hasValue(3);
            assertThat(snapshots).isEmpty();
        } finally {
            scheduler.shutdownNow();
        }
    }

    private static AppendAdmissionRequest request(StorageProfile profile) {
        return new AppendAdmissionRequest(
                STREAM,
                profile,
                DurabilityLevel.WAL_DURABLE,
                Duration.ofSeconds(1));
    }

    private static MaterializationLagSnapshot snapshot(long lagRecords) {
        return new MaterializationLagSnapshot(
                STREAM,
                0,
                lagRecords,
                lagRecords,
                lagRecords,
                0,
                1,
                1);
    }
}
