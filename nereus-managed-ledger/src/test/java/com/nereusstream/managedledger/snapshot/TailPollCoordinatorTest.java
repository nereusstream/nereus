/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.snapshot;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamMetadata;
import com.nereusstream.api.StreamName;
import com.nereusstream.api.StreamState;
import java.time.Duration;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.junit.jupiter.api.Test;

class TailPollCoordinatorTest {
    @Test
    void manyWaitersShareOnePollAndOneRefreshedSnapshot() throws Exception {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        AtomicReference<StreamMetadata> current = new AtomicReference<>(metadata(0));
        AtomicInteger refreshes = new AtomicInteger();
        CountDownLatch signalled = new CountDownLatch(2);
        try (TailPollCoordinator coordinator = new TailPollCoordinator(
                scheduler,
                Duration.ofMillis(10),
                () -> {
                    refreshes.incrementAndGet();
                    current.set(metadata(1));
                    return CompletableFuture.completedFuture(current.get());
                },
                current::get)) {
            coordinator.register(waiter(signalled));
            coordinator.register(waiter(signalled));

            assertThat(signalled.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(refreshes).hasValue(1);
        } finally {
            scheduler.shutdownNow();
        }
    }

    private static PendingReadWaiter waiter(CountDownLatch signalled) {
        return new PendingReadWaiter() {
            @Override public long nextOffset() { return 0; }
            @Override public OptionalLong inclusiveMaxOffset() { return OptionalLong.empty(); }
            @Override public boolean trySignal(StreamMetadata metadata) {
                if (metadata.committedEndOffset() == 0) return false;
                signalled.countDown();
                return true;
            }
            @Override public boolean tryFail(ManagedLedgerException error) { return true; }
        };
    }

    private static StreamMetadata metadata(long end) {
        return new StreamMetadata(
                new StreamId("stream"), new StreamName("stream"), StreamState.ACTIVE,
                StorageProfile.OBJECT_WAL_SYNC_OBJECT, Map.of(), 1, end, end, end, 0);
    }
}
