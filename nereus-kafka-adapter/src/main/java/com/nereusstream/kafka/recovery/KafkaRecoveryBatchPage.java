/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.recovery;

import java.util.List;
import java.util.Objects;

/** One exact, dense COMMITTED recovery page whose next offset is the next page cursor. */
public record KafkaRecoveryBatchPage(
        long requestedOffset,
        long nextOffset,
        List<KafkaReplayBatch> batches) {
    public KafkaRecoveryBatchPage {
        batches = List.copyOf(Objects.requireNonNull(batches, "batches"));
        if (requestedOffset < 0 || nextOffset < requestedOffset) {
            throw new IllegalArgumentException("invalid Kafka recovery page offsets");
        }
        if (batches.isEmpty()) {
            if (nextOffset != requestedOffset) {
                throw new IllegalArgumentException("empty Kafka recovery page cannot advance");
            }
        } else {
            long cursor = requestedOffset;
            for (KafkaReplayBatch batch : batches) {
                if (batch.baseOffset() != cursor) {
                    throw new IllegalArgumentException("Kafka recovery page is not exact and dense");
                }
                try {
                    cursor = Math.addExact(batch.lastOffset(), 1);
                } catch (ArithmeticException failure) {
                    throw new IllegalArgumentException("Kafka recovery page offset overflows", failure);
                }
            }
            if (cursor != nextOffset) {
                throw new IllegalArgumentException("Kafka recovery page cursor does not match its batches");
            }
        }
    }
}
