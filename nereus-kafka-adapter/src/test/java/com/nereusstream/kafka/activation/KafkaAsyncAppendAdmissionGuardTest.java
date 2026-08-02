/* Licensed under the Apache License, Version 2.0 */

package com.nereusstream.kafka.activation;

import static org.assertj.core.api.Assertions.assertThat;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.core.append.AppendAdmissionRequest;
import com.nereusstream.core.backpressure.MaterializationLagGate;
import com.nereusstream.core.backpressure.MaterializationLagSnapshot;
import com.nereusstream.core.backpressure.MaterializationLagThresholds;
import com.nereusstream.kafka.metadata.KafkaMaterializationStreamRegistration;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationMetadataStoreTestFactory;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class KafkaAsyncAppendAdmissionGuardTest {
    private static final Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(10_000), ZoneOffset.UTC);

    @Test
    void admitsBothAsyncProfilesOnlyAfterAuthorityAndLag() {
        GenerationMetadataStore generations = GenerationMetadataStoreTestFactory.inMemory(CLOCK);
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            StreamId objectStream = new StreamId("kafka-object-async-admission");
            StreamId bookKeeperStream = new StreamId("kafka-bookkeeper-async-admission");
            KafkaMaterializationStreamRegistration registrations =
                    new KafkaMaterializationStreamRegistration(KafkaActivationTestSupport.CLUSTER, generations, CLOCK);
            registrations
                    .ensure(objectStream, StorageProfile.OBJECT_WAL_ASYNC_OBJECT)
                    .join();
            registrations
                    .ensure(bookKeeperStream, StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT)
                    .join();
            KafkaBrokerCapabilitySpecification specification = specification();
            KafkaGenerationProtocolActivationGuard activation = new KafkaGenerationProtocolActivationGuard(
                    KafkaActivationTestSupport.CLUSTER,
                    generations,
                    KafkaStorageActivationVerifierTest.verifier(
                            KafkaStorageActivationVerifierTest.activeStore(specification, 40_000),
                            specification,
                            KafkaStorageActivationVerifierTest.snapshot()),
                    CLOCK);
            AtomicInteger measurements = new AtomicInteger();
            MaterializationLagGate lagGate = new MaterializationLagGate(
                    (stream, timeout) -> {
                        measurements.incrementAndGet();
                        return CompletableFuture.completedFuture(
                                new MaterializationLagSnapshot(stream, 0, 0, 0, 0, 0, 1, CLOCK.millis()));
                    },
                    new MaterializationLagThresholds(1, 2, 1, 2, Duration.ofSeconds(1), Duration.ofMillis(1)),
                    scheduler);
            KafkaAsyncAppendAdmissionGuard guard = new KafkaAsyncAppendAdmissionGuard(activation, lagGate);

            guard.admit(request(objectStream, StorageProfile.OBJECT_WAL_ASYNC_OBJECT))
                    .join();
            guard.admit(request(bookKeeperStream, StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT))
                    .join();
            guard.admit(request(objectStream, StorageProfile.OBJECT_WAL_SYNC_OBJECT))
                    .join();
            guard.admit(request(bookKeeperStream, StorageProfile.BOOKKEEPER_WAL_ONLY))
                    .join();

            assertThat(measurements).hasValue(2);
        } finally {
            scheduler.shutdownNow();
            generations.close();
        }
    }

    private static AppendAdmissionRequest request(StreamId streamId, StorageProfile profile) {
        return new AppendAdmissionRequest(streamId, profile, profile.defaultDurabilityLevel(), Duration.ofSeconds(1));
    }

    private static KafkaBrokerCapabilitySpecification specification() {
        return new KafkaBrokerCapabilitySpecification(
                KafkaActivationTestSupport.CLUSTER,
                KafkaActivationTestSupport.BROKER,
                "runtime-1",
                "4.3.0",
                "nereus-test",
                "21",
                Set.of(
                        StorageProfile.OBJECT_WAL_SYNC_OBJECT,
                        StorageProfile.OBJECT_WAL_ASYNC_OBJECT,
                        StorageProfile.BOOKKEEPER_WAL_ONLY,
                        StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT,
                        StorageProfile.BOOKKEEPER_WAL_SYNC_OBJECT),
                StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT,
                KafkaActivationTestSupport.bytes(1),
                KafkaActivationTestSupport.bytes(2),
                KafkaActivationTestSupport.bytes(3),
                Duration.ofMillis(10),
                Duration.ofMillis(100));
    }
}
