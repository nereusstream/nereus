/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.metadata.oxia.records.KafkaStorageActivationLifecycle;
import com.nereusstream.metadata.oxia.records.KafkaStorageProtocolActivationRecord;
import io.oxia.testcontainers.OxiaContainer;
import java.time.Clock;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class KafkaStorageActivationMetadataOxiaIntegrationTest {
    private static final String IMAGE = "oxia/oxia:0.16.3";

    @Container
    private static final OxiaContainer OXIA =
            new OxiaContainer(DockerImageName.parse(IMAGE)).withShards(4);

    @Test
    void activationCapabilityAndReadinessSurviveRealOxiaRuntimeRestart() {
        String nereusCluster = "f9/activation/" + UUID.randomUUID();
        OxiaClientConfiguration configuration = configuration();
        VersionedKafkaStorageProtocolActivation active;
        VersionedKafkaBrokerCapability heartbeat;
        VersionedKafkaStorageReadiness readiness;

        try (SharedOxiaClientRuntime runtime = SharedOxiaClientRuntime.connect(
                        configuration, Clock.systemUTC());
                KafkaStorageActivationMetadataStore store =
                        KafkaStorageActivationMetadataStore.usingSharedRuntime(
                                configuration,
                                runtime,
                                nereusCluster,
                                KafkaActivationTestValues.KAFKA_CLUSTER)) {
            VersionedKafkaBrokerCapability capability = store.createCapability(
                    KafkaActivationTestValues.capability(1_100, 31_100)).join();
            heartbeat = store.heartbeatCapability(
                    capability,
                    KafkaActivationTestValues.capability(1_200, 31_200)).join();
            readiness = store.createReadiness(
                    KafkaActivationTestValues.readiness(7, 101, 1_300)).join();
            VersionedKafkaStorageProtocolActivation prepared = store.createActivation(
                    KafkaActivationTestValues.activation(
                            KafkaStorageActivationLifecycle.PREPARED, 0)).join();
            active = store.compareAndSetActivation(
                    prepared, active(prepared.value(), 1_500)).join();
        }

        try (SharedOxiaClientRuntime runtime = SharedOxiaClientRuntime.connect(
                        configuration, Clock.systemUTC());
                KafkaStorageActivationMetadataStore store =
                        KafkaStorageActivationMetadataStore.usingSharedRuntime(
                                configuration,
                                runtime,
                                nereusCluster,
                                KafkaActivationTestValues.KAFKA_CLUSTER)) {
            assertThat(store.getCapability(heartbeat.value().identity()).join()).contains(heartbeat);
            assertThat(store.getReadiness().join()).contains(readiness);
            assertThat(store.getActivation().join()).contains(active);
        }
    }

    private static KafkaStorageProtocolActivationRecord active(
            KafkaStorageProtocolActivationRecord current,
            long activatedAtMillis) {
        return new KafkaStorageProtocolActivationRecord(
                current.recordVersion(),
                KafkaStorageActivationLifecycle.ACTIVE.wireId(),
                current.kafkaClusterId(),
                current.protocolVersion(),
                current.apiVersion(),
                current.streamHeadSessionVersion(),
                current.bindingVersion(),
                current.payloadMappingId(),
                current.objectWalEntryIndexVersion(),
                current.ncpVersion(),
                current.ntcVersion(),
                current.checkpointVersion(),
                current.compactionStrategyVersion(),
                current.allowedStorageProfiles(),
                current.defaultStorageProfile(),
                current.requiredCapabilitySha256(),
                current.requiredBrokerSetSha256(),
                current.kafkaFeatureLevel(),
                current.preparedAtMetadataOffset(),
                current.activationEpoch(),
                current.preparedAtMillis(),
                activatedAtMillis,
                0);
    }

    private static OxiaClientConfiguration configuration() {
        return new OxiaClientConfiguration(
                OXIA.getServiceAddress(),
                "default",
                Duration.ofSeconds(10),
                Duration.ofSeconds(30),
                100,
                1_024);
    }
}
