/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.activation;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaBrokerCapabilityPublisherTest {
    private static final Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(10_000), ZoneOffset.UTC);

    @Test
    void publishesHeartbeatsWithMonotonicTimesAndStopsOnFailure() throws Exception {
        InMemoryKafkaStorageActivationStore store = new InMemoryKafkaStorageActivationStore();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        CountDownLatch failureObserved = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        KafkaBrokerCapabilityPublisher publisher = new KafkaBrokerCapabilityPublisher(
                store,
                KafkaActivationTestSupport.specification(3),
                scheduler,
                CLOCK,
                exact -> {
                    failure.set(exact);
                    failureObserved.countDown();
                });
        try {
            long initialVersion = publisher.start().toCompletableFuture().join().metadataVersion();
            awaitHeartbeats(store, 2);
            assertThat(publisher.current().orElseThrow().metadataVersion()).isGreaterThan(initialVersion);
            assertThat(publisher.current().orElseThrow().value().heartbeatAtMillis()).isGreaterThan(10_000);

            store.failHeartbeats();
            assertThat(failureObserved.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(failure.get()).hasMessage("heartbeat failed");
            int stoppedAt = store.heartbeatCount();
            Thread.sleep(50);
            assertThat(store.heartbeatCount()).isEqualTo(stoppedAt);
        } finally {
            publisher.close();
            scheduler.shutdownNow();
        }
    }

    @Test
    void rejectsASecondRuntimeUsingTheSameBrokerEpoch() {
        InMemoryKafkaStorageActivationStore store = new InMemoryKafkaStorageActivationStore();
        store.createCapability(KafkaActivationTestSupport.specification(3).initialRecord(10_000)).join();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        KafkaBrokerCapabilitySpecification conflicting = new KafkaBrokerCapabilitySpecification(
                KafkaActivationTestSupport.CLUSTER,
                KafkaActivationTestSupport.BROKER,
                "runtime-2",
                "4.3.0",
                "nereus-test",
                "21",
                java.util.Set.of(com.nereusstream.api.StorageProfile.OBJECT_WAL_SYNC_OBJECT),
                com.nereusstream.api.StorageProfile.OBJECT_WAL_SYNC_OBJECT,
                KafkaActivationTestSupport.bytes(1),
                KafkaActivationTestSupport.bytes(2),
                KafkaActivationTestSupport.bytes(3),
                java.time.Duration.ofMillis(10),
                java.time.Duration.ofMillis(100));
        KafkaBrokerCapabilityPublisher publisher = new KafkaBrokerCapabilityPublisher(
                store, conflicting, scheduler, CLOCK, ignored -> { });
        try {
            assertThatThrownBy(() -> publisher.start().toCompletableFuture().join())
                    .satisfies(failure -> {
                        Throwable exact = unwrap(failure);
                        assertThat(exact).isInstanceOf(NereusException.class);
                        assertThat(((NereusException) exact).code())
                                .isEqualTo(ErrorCode.METADATA_INVARIANT_VIOLATION);
                    });
        } finally {
            publisher.close();
            scheduler.shutdownNow();
        }
    }

    private static void awaitHeartbeats(InMemoryKafkaStorageActivationStore store, int expected)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (store.heartbeatCount() < expected && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertThat(store.heartbeatCount()).isGreaterThanOrEqualTo(expected);
    }

    private static Throwable unwrap(Throwable supplied) {
        Throwable current = supplied;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
