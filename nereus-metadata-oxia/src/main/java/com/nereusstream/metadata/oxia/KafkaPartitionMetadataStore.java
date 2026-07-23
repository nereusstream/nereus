/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.metadata.oxia.records.KafkaPartitionBindingRecord;
import com.nereusstream.metadata.oxia.records.KafkaPartitionPendingOperationRecord;
import com.nereusstream.metadata.oxia.records.KafkaPartitionRegistryRecord;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Exact single-key CAS contract for native Kafka partition metadata. */
public interface KafkaPartitionMetadataStore extends AutoCloseable {
    CompletableFuture<Optional<VersionedKafkaPartitionBinding>> get(KafkaPartitionId id);

    CompletableFuture<VersionedKafkaPartitionBinding> putCreatingIfAbsent(
            KafkaPartitionBindingRecord value);

    CompletableFuture<VersionedKafkaPartitionBinding> compareAndSet(
            VersionedKafkaPartitionBinding expected,
            KafkaPartitionBindingRecord update);

    default CompletableFuture<VersionedKafkaPartitionBinding> claimOperation(
            VersionedKafkaPartitionBinding expected,
            KafkaPartitionPendingOperationRecord operation,
            long nowMillis) {
        return compareAndSet(expected,
                KafkaPartitionMetadataTransitions.claimOperation(expected.value(), operation, nowMillis));
    }

    default CompletableFuture<VersionedKafkaPartitionBinding> clearOperation(
            VersionedKafkaPartitionBinding expected,
            String attemptId,
            long updatedAtMillis) {
        return compareAndSet(expected,
                KafkaPartitionMetadataTransitions.clearOperation(expected.value(), attemptId, updatedAtMillis));
    }

    CompletableFuture<Void> putRegistryHint(KafkaPartitionRegistryRecord value);

    CompletableFuture<KafkaPartitionScanPage> scanRegistry(
            int shard,
            Optional<String> continuation,
            int limit);

    @Override
    void close();
}
