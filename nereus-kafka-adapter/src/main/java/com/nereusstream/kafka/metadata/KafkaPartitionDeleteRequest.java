/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.metadata;

import com.nereusstream.kafka.partition.KafkaPartitionIdentity;
import java.time.Duration;
import java.util.Objects;

/** Exact durable deletion inputs supplied by the process-owned storage manager. */
public record KafkaPartitionDeleteRequest(
        KafkaPartitionIdentity identity,
        long metadataOffset,
        String operationOwnerId,
        long operationOwnerEpoch,
        Duration operationTtl,
        Duration streamTimeout) {
    public KafkaPartitionDeleteRequest {
        Objects.requireNonNull(identity, "identity");
        Objects.requireNonNull(operationOwnerId, "operationOwnerId");
        Objects.requireNonNull(operationTtl, "operationTtl");
        Objects.requireNonNull(streamTimeout, "streamTimeout");
        if (metadataOffset < 0 || operationOwnerId.isBlank() || operationOwnerEpoch <= 0
                || operationTtl.isZero() || operationTtl.isNegative()
                || streamTimeout.isZero() || streamTimeout.isNegative()) {
            throw new IllegalArgumentException("invalid Kafka partition delete request");
        }
    }
}
