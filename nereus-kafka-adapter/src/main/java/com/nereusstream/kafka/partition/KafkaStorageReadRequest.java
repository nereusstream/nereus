/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.partition;

import java.time.Duration;
import java.util.Objects;

/** Bounded committed Kafka read mapped to Nereus containing-entry semantics. */
public record KafkaStorageReadRequest(
        long startOffset,
        long maxOffsetExclusive,
        int maxRecords,
        int maxPartitionBytes,
        int hardMaxResponseBytes,
        boolean minOneMessage,
        long virtualSegmentBaseOffset,
        long relativeLogicalBytePosition,
        Duration timeout) {
    public KafkaStorageReadRequest {
        Objects.requireNonNull(timeout, "timeout");
        if (startOffset < 0
                || maxOffsetExclusive < startOffset
                || maxRecords <= 0
                || maxPartitionBytes <= 0
                || hardMaxResponseBytes <= 0
                || virtualSegmentBaseOffset < 0
                || relativeLogicalBytePosition < 0) {
            throw new IllegalArgumentException("invalid Kafka storage read bounds");
        }
        if (timeout.isZero() || timeout.isNegative() || timeout.toMillis() <= 0) {
            throw new IllegalArgumentException("Kafka read timeout must be positive and millisecond-representable");
        }
    }
}
