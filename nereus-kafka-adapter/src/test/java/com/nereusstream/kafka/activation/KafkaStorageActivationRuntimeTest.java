/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.activation;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.metadata.oxia.KafkaBrokerIdentity;
import com.nereusstream.metadata.oxia.records.KafkaBrokerCapabilityRecord;
import com.nereusstream.metadata.oxia.records.KafkaPayloadMapping;
import com.nereusstream.metadata.oxia.records.KafkaStorageActivationLifecycle;
import com.nereusstream.metadata.oxia.records.KafkaStorageProtocolActivationRecord;
import com.nereusstream.metadata.oxia.records.KafkaStorageReadinessRecord;
import com.nereusstream.kafka.runtime.KafkaStorageAdmission;
import com.nereusstream.kafka.runtime.KafkaStorageAdmissionState;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaStorageActivationRuntimeTest {
    @Test
    void publishesThenWaitsForAuthorityAndFencesAdmissionAfterHeartbeatFailure() throws Exception {
        InMemoryKafkaStorageActivationStore store = new InMemoryKafkaStorageActivationStore();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        KafkaBrokerCapabilitySpecification capability = KafkaActivationTestSupport.specification(3);
        KafkaStorageClusterSnapshot snapshot = snapshot();
        AtomicInteger downstreamStarts = new AtomicInteger();
        KafkaStorageActivationRuntime runtime = new KafkaStorageActivationRuntime(
                store,
                capability,
                () -> CompletableFuture.completedFuture(snapshot),
                scheduler,
                Clock.systemUTC(),
                Duration.ofSeconds(2),
                Duration.ofMillis(10),
                () -> {
                    downstreamStarts.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                });
        KafkaStorageAdmission admission = new KafkaStorageAdmission();
        try {
            CompletableFuture<Void> start = runtime.start(admission).toCompletableFuture();
            KafkaBrokerCapabilityRecord published = awaitCapability(store);
            installActive(store, published, System.currentTimeMillis());

            start.get(2, TimeUnit.SECONDS);
            assertThat(downstreamStarts).hasValue(1);
            assertThat(admission.markReady()).isTrue();
            store.failHeartbeats();
            awaitAdmission(admission, KafkaStorageAdmissionState.NOT_READY);
            assertThat(admission.health().detail()).startsWith("capability heartbeat failed:");
        } finally {
            runtime.close();
            assertThat(scheduler.isShutdown()).isFalse();
            scheduler.shutdownNow();
        }
    }

    @Test
    void timesOutWithoutActiveAuthorityAndNeverRunsDownstreamStartup() {
        InMemoryKafkaStorageActivationStore store = new InMemoryKafkaStorageActivationStore();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        AtomicInteger downstreamStarts = new AtomicInteger();
        KafkaStorageActivationRuntime runtime = new KafkaStorageActivationRuntime(
                store,
                KafkaActivationTestSupport.specification(3),
                () -> CompletableFuture.completedFuture(snapshot()),
                scheduler,
                Clock.systemUTC(),
                Duration.ofMillis(50),
                Duration.ofMillis(10),
                () -> {
                    downstreamStarts.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                });
        try {
            assertThatThrownBy(() -> runtime.start(new KafkaStorageAdmission())
                    .toCompletableFuture().join()).satisfies(failure -> {
                        Throwable exact = unwrap(failure);
                        assertThat(exact).isInstanceOf(NereusException.class);
                        assertThat(((NereusException) exact).code()).isEqualTo(ErrorCode.TIMEOUT);
                        assertThat(((NereusException) exact).retriable()).isTrue();
                    });
            assertThat(downstreamStarts).hasValue(0);
        } finally {
            runtime.close();
            scheduler.shutdownNow();
        }
    }

    private static KafkaBrokerCapabilityRecord awaitCapability(
            InMemoryKafkaStorageActivationStore store) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (System.nanoTime() < deadline) {
            var capability = store.getCapability(KafkaActivationTestSupport.BROKER).join();
            if (capability.isPresent()) return capability.orElseThrow().value();
            Thread.sleep(5);
        }
        throw new AssertionError("capability was not published");
    }

    private static void installActive(
            InMemoryKafkaStorageActivationStore store,
            KafkaBrokerCapabilityRecord capability,
            long now) {
        List<KafkaBrokerIdentity> brokers = List.of(KafkaActivationTestSupport.BROKER);
        byte[] capabilitySha256 = KafkaStorageCapabilityDigests.compatibilitySha256(capability);
        store.createReadiness(new KafkaStorageReadinessRecord(
                KafkaStorageReadinessRecord.RECORD_VERSION,
                KafkaActivationTestSupport.CLUSTER,
                1,
                100,
                brokers,
                KafkaStorageReadinessRecord.brokerSetSha256(brokers),
                capabilitySha256,
                capability.providerScopeSha256(),
                now,
                now + 30_000,
                0)).join();
        store.createActivation(new KafkaStorageProtocolActivationRecord(
                KafkaStorageProtocolActivationRecord.RECORD_VERSION,
                KafkaStorageActivationLifecycle.ACTIVE.wireId(),
                KafkaActivationTestSupport.CLUSTER,
                KafkaStorageProtocolActivationRecord.PROTOCOL_VERSION,
                KafkaStorageProtocolActivationRecord.API_VERSION,
                KafkaStorageProtocolActivationRecord.STREAM_HEAD_SESSION_VERSION,
                KafkaStorageProtocolActivationRecord.BINDING_VERSION,
                KafkaPayloadMapping.KAFKA_RECORD_BATCH_V1.wireId(),
                KafkaStorageProtocolActivationRecord.OBJECT_WAL_ENTRY_INDEX_VERSION,
                KafkaStorageProtocolActivationRecord.NCP_VERSION,
                KafkaStorageProtocolActivationRecord.NTC_VERSION,
                KafkaStorageProtocolActivationRecord.CHECKPOINT_VERSION,
                KafkaStorageProtocolActivationRecord.COMPACTION_STRATEGY_VERSION,
                capability.supportedStorageProfiles(),
                StorageProfile.OBJECT_WAL_SYNC_OBJECT.name(),
                capabilitySha256,
                KafkaStorageReadinessRecord.brokerSetSha256(brokers),
                KafkaStorageProtocolActivationRecord.KAFKA_FEATURE_LEVEL,
                100,
                1,
                now,
                now,
                0)).join();
    }

    private static KafkaStorageClusterSnapshot snapshot() {
        return new KafkaStorageClusterSnapshot(
                KafkaActivationTestSupport.CLUSTER,
                101,
                KafkaStorageProtocolActivationRecord.KAFKA_FEATURE_LEVEL,
                List.of(KafkaActivationTestSupport.BROKER),
                false,
                false,
                false);
    }

    private static void awaitAdmission(
            KafkaStorageAdmission admission,
            KafkaStorageAdmissionState expected) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (admission.state() != expected && System.nanoTime() < deadline) Thread.sleep(5);
        assertThat(admission.state()).isEqualTo(expected);
    }

    private static Throwable unwrap(Throwable supplied) {
        Throwable current = supplied;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
