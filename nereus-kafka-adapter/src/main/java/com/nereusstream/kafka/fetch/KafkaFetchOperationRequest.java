/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.fetch;

import com.nereusstream.kafka.partition.KafkaPartitionIdentity;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Immutable request-wide Fetch limits and ordered partition reads. */
public record KafkaFetchOperationRequest(
        List<KafkaFetchPartitionRequest> partitions,
        int minBytes,
        int maxResponseBytes,
        Duration maxWait,
        int maxRereads) {
    public KafkaFetchOperationRequest {
        partitions = List.copyOf(Objects.requireNonNull(partitions, "partitions"));
        Objects.requireNonNull(maxWait, "maxWait");
        if (partitions.isEmpty()) throw new IllegalArgumentException("Fetch requires at least one partition");
        if (minBytes < 0 || maxResponseBytes <= 0 || minBytes > maxResponseBytes) {
            throw new IllegalArgumentException("invalid Fetch response byte limits");
        }
        if (maxWait.isNegative() || maxRereads <= 0) {
            throw new IllegalArgumentException("invalid Fetch wait/reread limits");
        }
        try {
            maxWait.toNanos();
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("Fetch maxWait is not nanosecond-representable", overflow);
        }
        Set<KafkaPartitionIdentity> identities = new HashSet<>();
        for (KafkaFetchPartitionRequest partition : partitions) {
            if (!identities.add(partition.identity())) {
                throw new IllegalArgumentException("duplicate partition in one Fetch operation");
            }
            if (partition.readRequest().hardMaxResponseBytes() > maxResponseBytes) {
                throw new IllegalArgumentException(
                        "partition hard response limit exceeds the Fetch response limit");
            }
        }
    }
}
