/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.metadata.oxia.records.KafkaBrokerCapabilityRecord;
import com.nereusstream.metadata.oxia.records.KafkaStorageProtocolActivationRecord;
import com.nereusstream.metadata.oxia.records.KafkaStorageReadinessRecord;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Exact-key/CAS boundary for Kafka protocol activation, broker capability and readiness authority. */
public interface KafkaStorageActivationMetadataStore extends AutoCloseable {
    static KafkaStorageActivationMetadataStore usingSharedRuntime(
            OxiaClientConfiguration configuration,
            SharedOxiaClientRuntime runtime,
            String nereusCluster,
            String kafkaClusterId) {
        return OxiaJavaKafkaStorageActivationMetadataStore.usingSharedRuntime(
                configuration, runtime, nereusCluster, kafkaClusterId);
    }

    CompletableFuture<Optional<VersionedKafkaStorageProtocolActivation>> getActivation();

    CompletableFuture<VersionedKafkaStorageProtocolActivation> createActivation(
            KafkaStorageProtocolActivationRecord value);

    CompletableFuture<VersionedKafkaStorageProtocolActivation> compareAndSetActivation(
            VersionedKafkaStorageProtocolActivation expected,
            KafkaStorageProtocolActivationRecord replacement);

    CompletableFuture<Optional<VersionedKafkaBrokerCapability>> getCapability(
            KafkaBrokerIdentity identity);

    CompletableFuture<VersionedKafkaBrokerCapability> createCapability(
            KafkaBrokerCapabilityRecord value);

    CompletableFuture<VersionedKafkaBrokerCapability> heartbeatCapability(
            VersionedKafkaBrokerCapability expected,
            KafkaBrokerCapabilityRecord replacement);

    CompletableFuture<Optional<VersionedKafkaStorageReadiness>> getReadiness();

    CompletableFuture<VersionedKafkaStorageReadiness> createReadiness(
            KafkaStorageReadinessRecord value);

    CompletableFuture<VersionedKafkaStorageReadiness> compareAndSetReadiness(
            VersionedKafkaStorageReadiness expected,
            KafkaStorageReadinessRecord replacement);

    @Override
    void close();
}
