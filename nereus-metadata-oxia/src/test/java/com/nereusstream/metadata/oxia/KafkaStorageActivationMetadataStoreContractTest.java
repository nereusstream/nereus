/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.metadata.oxia.records.KafkaBrokerCapabilityRecord;
import com.nereusstream.metadata.oxia.records.KafkaStorageActivationLifecycle;
import com.nereusstream.metadata.oxia.records.KafkaStorageProtocolActivationRecord;
import com.nereusstream.metadata.oxia.records.KafkaStorageReadinessRecord;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaStorageActivationMetadataStoreContractTest {
    @Test
    void createsAndMonotonicallyAdvancesAllThreeControlPlaneAuthorities() {
        InMemoryPartitionedOxiaBackend backend = new InMemoryPartitionedOxiaBackend();
        KafkaStorageActivationMetadataStore store = store(backend);
        try {
            assertThat(store.getActivation().join()).isEmpty();
            assertThat(store.getReadiness().join()).isEmpty();

            VersionedKafkaBrokerCapability capability = store.createCapability(
                    KafkaActivationTestValues.capability(1_100, 31_100)).join();
            VersionedKafkaBrokerCapability heartbeat = store.heartbeatCapability(
                    capability,
                    KafkaActivationTestValues.capability(1_200, 31_200)).join();
            assertThat(heartbeat.metadataVersion()).isGreaterThan(capability.metadataVersion());
            assertThat(store.getCapability(capability.value().identity()).join())
                    .contains(heartbeat);

            VersionedKafkaStorageReadiness readiness = store.createReadiness(
                    KafkaActivationTestValues.readiness(7, 101, 1_300)).join();
            VersionedKafkaStorageReadiness nextReadiness = store.compareAndSetReadiness(
                    readiness,
                    KafkaActivationTestValues.readiness(8, 102, 1_400)).join();
            assertThat(nextReadiness.value().readinessEpoch()).isEqualTo(8);

            VersionedKafkaStorageProtocolActivation prepared = store.createActivation(
                    KafkaActivationTestValues.activation(
                            KafkaStorageActivationLifecycle.PREPARED, 0)).join();
            VersionedKafkaStorageProtocolActivation active = store.compareAndSetActivation(
                    prepared, active(prepared.value(), 1_500)).join();
            assertThat(active.value().lifecycle()).isEqualTo(KafkaStorageActivationLifecycle.ACTIVE);
            assertThat(store.getActivation().join()).contains(active);

            assertConditionFailure(() -> store.heartbeatCapability(
                    capability,
                    KafkaActivationTestValues.capability(1_300, 31_300)).join());
            assertConditionFailure(() -> store.createCapability(
                    KafkaActivationTestValues.capability(1_300, 31_300)).join());
            assertInvariant(() -> store.compareAndSetActivation(
                    active,
                    KafkaActivationTestValues.activation(
                            KafkaStorageActivationLifecycle.PREPARED, 0)).join());
            assertInvariant(() -> store.compareAndSetReadiness(
                    nextReadiness,
                    KafkaActivationTestValues.readiness(8, 103, 1_500)).join());
        } finally {
            store.close();
        }

        assertThatThrownBy(store::getActivation)
                .isInstanceOf(NereusException.class)
                .extracting(failure -> ((NereusException) failure).code())
                .isEqualTo(ErrorCode.STORAGE_CLOSED);
    }

    @Test
    void recoversCreateAndCasWhenOxiaAppliesMutationButLosesResponse() {
        ResponseLossPartitionedOxiaBackend backend = new ResponseLossPartitionedOxiaBackend();
        KafkaStorageActivationMetadataStore store = store(backend);
        try {
            KafkaStorageProtocolActivationRecord preparedValue = KafkaActivationTestValues.activation(
                    KafkaStorageActivationLifecycle.PREPARED, 0);
            backend.loseNextResponse(ResponseLossPartitionedOxiaBackend.Operation.PUT_IF_ABSENT);
            VersionedKafkaStorageProtocolActivation prepared = store.createActivation(preparedValue).join();

            KafkaStorageProtocolActivationRecord activeValue = active(prepared.value(), 1_500);
            backend.loseNextResponse(ResponseLossPartitionedOxiaBackend.Operation.PUT_IF_VERSION);
            VersionedKafkaStorageProtocolActivation active = store.compareAndSetActivation(
                    prepared, activeValue).join();

            assertThat(backend.responseWasLost()).isTrue();
            assertThat(active.value().withMetadataVersion(0)).isEqualTo(activeValue);
            assertThat(store.getActivation().join()).contains(active);
        } finally {
            store.close();
        }
    }

    @Test
    void rejectsCapabilityFactDriftAndReadinessMetadataRegressionBeforeIo() {
        KafkaBrokerCapabilityRecord capability = KafkaActivationTestValues.capability(1_100, 31_100);
        KafkaBrokerCapabilityRecord changedBuild = copyCapability(
                capability, "different-build", 1_200, 31_200);
        assertInvariant(() -> KafkaStorageActivationTransitions.requireCapabilityHeartbeat(
                capability, changedBuild));

        KafkaStorageReadinessRecord readiness = KafkaActivationTestValues.readiness(7, 101, 1_300);
        KafkaStorageReadinessRecord regression = KafkaActivationTestValues.readiness(8, 100, 1_400);
        assertInvariant(() -> KafkaStorageActivationTransitions.requireReadinessReplacement(
                readiness, regression));
    }

    private static KafkaStorageActivationMetadataStore store(
            PartitionedOxiaClient.Backend backend) {
        return new OxiaJavaKafkaStorageActivationMetadataStore(
                new PartitionedOxiaClient(backend),
                new KafkaPartitionKeyspace("nereus", KafkaActivationTestValues.KAFKA_CLUSTER));
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

    private static KafkaBrokerCapabilityRecord copyCapability(
            KafkaBrokerCapabilityRecord current,
            String nereusBuild,
            long heartbeatAtMillis,
            long expiresAtMillis) {
        return new KafkaBrokerCapabilityRecord(
                current.recordVersion(),
                current.kafkaClusterId(),
                current.brokerId(),
                current.brokerEpoch(),
                current.runtimeInstanceId(),
                current.kafkaVersion(),
                nereusBuild,
                current.javaVersion(),
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
                current.kafkaFeatureLevel(),
                current.supportedStorageProfiles(),
                current.configCompatibilitySha256(),
                current.codeCapabilitySha256(),
                current.providerScopeSha256(),
                current.startedAtMillis(),
                heartbeatAtMillis,
                expiresAtMillis,
                0);
    }

    private static void assertConditionFailure(Runnable operation) {
        assertThatThrownBy(operation::run).satisfies(failure -> assertThat(unwrap(failure))
                .isInstanceOf(KafkaMetadataConditionFailedException.class));
    }

    private static void assertInvariant(Runnable operation) {
        assertThatThrownBy(operation::run).satisfies(failure -> {
            Throwable exact = unwrap(failure);
            assertThat(exact).isInstanceOf(NereusException.class);
            assertThat(((NereusException) exact).code())
                    .isEqualTo(ErrorCode.METADATA_INVARIANT_VIOLATION);
        });
    }

    private static Throwable unwrap(Throwable supplied) {
        Throwable current = supplied;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
