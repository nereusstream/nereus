/* Licensed under the Apache License, Version 2.0 */

package com.nereusstream.metadata.oxia;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.metadata.oxia.records.KafkaBrokerCapabilityRecord;
import com.nereusstream.metadata.oxia.records.KafkaStorageActivationLifecycle;
import com.nereusstream.metadata.oxia.records.KafkaStorageProtocolActivationRecord;
import com.nereusstream.metadata.oxia.records.KafkaStorageReadinessRecord;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class KafkaStorageActivationMetadataStoreContractTest {
    @Test
    void createsAndMonotonicallyAdvancesAllThreeControlPlaneAuthorities() {
        InMemoryPartitionedOxiaBackend backend = new InMemoryPartitionedOxiaBackend();
        KafkaStorageActivationMetadataStore store = store(backend);
        try {
            assertThat(store.getActivation().join()).isEmpty();
            assertThat(store.getReadiness().join()).isEmpty();

            VersionedKafkaBrokerCapability capability = store.createCapability(
                            KafkaActivationTestValues.capability(1_100, 31_100))
                    .join();
            VersionedKafkaBrokerCapability heartbeat = store.heartbeatCapability(
                            capability, KafkaActivationTestValues.capability(1_200, 31_200))
                    .join();
            assertThat(heartbeat.metadataVersion()).isGreaterThan(capability.metadataVersion());
            assertThat(store.getCapability(capability.value().identity()).join())
                    .contains(heartbeat);

            VersionedKafkaStorageReadiness readiness = store.createReadiness(
                            KafkaActivationTestValues.readiness(7, 101, 1_300))
                    .join();
            VersionedKafkaStorageReadiness nextReadiness = store.compareAndSetReadiness(
                            readiness, KafkaActivationTestValues.readiness(8, 102, 1_400))
                    .join();
            assertThat(nextReadiness.value().readinessEpoch()).isEqualTo(8);

            VersionedKafkaStorageProtocolActivation prepared = store.createActivation(
                            KafkaActivationTestValues.activation(KafkaStorageActivationLifecycle.PREPARED, 0))
                    .join();
            VersionedKafkaStorageProtocolActivation active = store.compareAndSetActivation(
                            prepared, active(prepared.value(), 1_500))
                    .join();
            assertThat(active.value().lifecycle()).isEqualTo(KafkaStorageActivationLifecycle.ACTIVE);
            assertThat(store.getActivation().join()).contains(active);

            assertConditionFailure(
                    () -> store.heartbeatCapability(capability, KafkaActivationTestValues.capability(1_300, 31_300))
                            .join());
            assertConditionFailure(() -> store.createCapability(KafkaActivationTestValues.capability(1_300, 31_300))
                    .join());
            assertInvariant(() -> store.compareAndSetActivation(
                            active, KafkaActivationTestValues.activation(KafkaStorageActivationLifecycle.PREPARED, 0))
                    .join());
            assertInvariant(() -> store.compareAndSetReadiness(
                            nextReadiness, KafkaActivationTestValues.readiness(8, 103, 1_500))
                    .join());
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
            KafkaStorageProtocolActivationRecord preparedValue =
                    KafkaActivationTestValues.activation(KafkaStorageActivationLifecycle.PREPARED, 0);
            backend.loseNextResponse(ResponseLossPartitionedOxiaBackend.Operation.PUT_IF_ABSENT);
            VersionedKafkaStorageProtocolActivation prepared =
                    store.createActivation(preparedValue).join();

            KafkaStorageProtocolActivationRecord activeValue = active(prepared.value(), 1_500);
            backend.loseNextResponse(ResponseLossPartitionedOxiaBackend.Operation.PUT_IF_VERSION);
            VersionedKafkaStorageProtocolActivation active =
                    store.compareAndSetActivation(prepared, activeValue).join();

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
        KafkaBrokerCapabilityRecord changedBuild = copyCapability(capability, "different-build", 1_200, 31_200);
        assertInvariant(() -> KafkaStorageActivationTransitions.requireCapabilityHeartbeat(capability, changedBuild));

        KafkaStorageReadinessRecord readiness = KafkaActivationTestValues.readiness(7, 101, 1_300);
        KafkaStorageReadinessRecord regression = KafkaActivationTestValues.readiness(8, 100, 1_400);
        assertInvariant(() -> KafkaStorageActivationTransitions.requireReadinessReplacement(readiness, regression));
    }

    @Test
    void normalizesRawTransportFailuresAsRetriableMetadataUnavailable() {
        TransportFailurePartitionedOxiaBackend backend = new TransportFailurePartitionedOxiaBackend();
        KafkaStorageActivationMetadataStore store = store(backend);
        try {
            backend.failNext(TransportFailurePartitionedOxiaBackend.Operation.GET);
            assertMetadataUnavailable(() -> store.getActivation().join(), "failed to read Kafka activation metadata");

            backend.failNext(TransportFailurePartitionedOxiaBackend.Operation.PUT_IF_ABSENT);
            assertMetadataUnavailable(
                    () -> store.createReadiness(KafkaActivationTestValues.readiness(7, 101, 1_300))
                            .join(),
                    "failed to create Kafka activation metadata");
        } finally {
            store.close();
        }
    }

    private static KafkaStorageActivationMetadataStore store(PartitionedOxiaClient.Backend backend) {
        return new OxiaJavaKafkaStorageActivationMetadataStore(
                new PartitionedOxiaClient(backend),
                new KafkaPartitionKeyspace("nereus", KafkaActivationTestValues.KAFKA_CLUSTER));
    }

    private static KafkaStorageProtocolActivationRecord active(
            KafkaStorageProtocolActivationRecord current, long activatedAtMillis) {
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
            KafkaBrokerCapabilityRecord current, String nereusBuild, long heartbeatAtMillis, long expiresAtMillis) {
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
            assertThat(((NereusException) exact).code()).isEqualTo(ErrorCode.METADATA_INVARIANT_VIOLATION);
        });
    }

    private static void assertMetadataUnavailable(Runnable operation, String message) {
        assertThatThrownBy(operation::run).satisfies(failure -> {
            Throwable exact = unwrap(failure);
            assertThat(exact).isInstanceOf(NereusException.class);
            NereusException nereus = (NereusException) exact;
            assertThat(nereus.code()).isEqualTo(ErrorCode.METADATA_UNAVAILABLE);
            assertThat(nereus.retriable()).isTrue();
            assertThat(nereus).hasMessage(message);
            assertThat(nereus.getCause()).isInstanceOf(TransportFailurePartitionedOxiaBackend.TransportFailure.class);
        });
    }

    private static Throwable unwrap(Throwable supplied) {
        Throwable current = supplied;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static final class TransportFailurePartitionedOxiaBackend implements PartitionedOxiaClient.Backend {
        private final InMemoryPartitionedOxiaBackend delegate = new InMemoryPartitionedOxiaBackend();
        private Operation armed;

        private void failNext(Operation operation) {
            armed = operation;
        }

        @Override
        public CompletableFuture<Optional<PartitionedOxiaClient.VersionedValue>> get(
                String key, PartitionKey partitionKey) {
            if (take(Operation.GET)) {
                return CompletableFuture.failedFuture(new TransportFailure("Oxia transport is unavailable"));
            }
            return delegate.get(key, partitionKey);
        }

        @Override
        public CompletableFuture<PartitionedOxiaClient.WriteResult> putIfAbsent(
                String key, byte[] value, PartitionKey partitionKey) {
            if (take(Operation.PUT_IF_ABSENT)) {
                return CompletableFuture.failedFuture(new TransportFailure("Oxia transport is unavailable"));
            }
            return delegate.putIfAbsent(key, value, partitionKey);
        }

        @Override
        public CompletableFuture<PartitionedOxiaClient.WriteResult> putIfVersion(
                String key, byte[] value, long expectedVersion, PartitionKey partitionKey) {
            return delegate.putIfVersion(key, value, expectedVersion, partitionKey);
        }

        @Override
        public CompletableFuture<Void> deleteIfVersion(String key, long expectedVersion, PartitionKey partitionKey) {
            return delegate.deleteIfVersion(key, expectedVersion, partitionKey);
        }

        @Override
        public CompletableFuture<List<String>> list(
                String fromInclusive, String toExclusive, PartitionKey partitionKey) {
            return delegate.list(fromInclusive, toExclusive, partitionKey);
        }

        @Override
        public CompletableFuture<List<PartitionedOxiaClient.VersionedValue>> rangeScan(
                String fromInclusive, String toExclusive, int limit, PartitionKey partitionKey) {
            return delegate.rangeScan(fromInclusive, toExclusive, limit, partitionKey);
        }

        @Override
        public WatchRegistration watchPrefix(String prefix, PartitionKey partitionKey, Runnable invalidationCallback) {
            return delegate.watchPrefix(prefix, partitionKey, invalidationCallback);
        }

        private boolean take(Operation operation) {
            if (armed != operation) {
                return false;
            }
            armed = null;
            return true;
        }

        private enum Operation {
            GET,
            PUT_IF_ABSENT
        }

        private static final class TransportFailure extends RuntimeException {
            private TransportFailure(String message) {
                super(message);
            }
        }
    }
}
