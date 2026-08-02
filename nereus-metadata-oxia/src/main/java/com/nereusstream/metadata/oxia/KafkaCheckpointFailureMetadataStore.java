/* Licensed under the Apache License, Version 2.0 */

package com.nereusstream.metadata.oxia;

import com.nereusstream.metadata.oxia.records.KafkaCheckpointFailureRecord;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Exact immutable durable quarantine store for unusable Kafka checkpoint references.
 */
public interface KafkaCheckpointFailureMetadataStore extends AutoCloseable {
    static KafkaCheckpointFailureMetadataStore usingSharedRuntime(
            OxiaClientConfiguration configuration,
            SharedOxiaClientRuntime runtime,
            String nereusCluster,
            String kafkaClusterId) {
        return OxiaJavaKafkaCheckpointFailureMetadataStore.usingSharedRuntime(
                configuration, runtime, nereusCluster, kafkaClusterId);
    }

    CompletableFuture<Optional<VersionedKafkaCheckpointFailure>> get(
            KafkaPartitionId identity, long partitionIncarnation, String objectId);

    CompletableFuture<VersionedKafkaCheckpointFailure> putIfAbsent(KafkaCheckpointFailureRecord value);

    @Override
    void close();
}
