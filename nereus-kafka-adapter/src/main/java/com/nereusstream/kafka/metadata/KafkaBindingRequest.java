/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.metadata;

import com.nereusstream.api.StorageProfile;
import com.nereusstream.kafka.partition.KafkaPartitionIdentity;
import java.time.Duration;
import java.util.Objects;

public record KafkaBindingRequest(
        KafkaPartitionIdentity identity,
        StorageProfile storageProfile,
        long metadataOffset,
        String operationOwnerId,
        long operationOwnerEpoch,
        Duration operationTtl) {
    public KafkaBindingRequest {
        Objects.requireNonNull(identity, "identity");
        storageProfile = Objects.requireNonNull(storageProfile, "storageProfile").canonical();
        Objects.requireNonNull(operationOwnerId, "operationOwnerId");
        operationTtl = Objects.requireNonNull(operationTtl, "operationTtl");
        if (metadataOffset < 0 || operationOwnerId.isBlank() || operationOwnerEpoch <= 0
                || operationTtl.isZero() || operationTtl.isNegative()) {
            throw new IllegalArgumentException("invalid Kafka binding request");
        }
    }
}
