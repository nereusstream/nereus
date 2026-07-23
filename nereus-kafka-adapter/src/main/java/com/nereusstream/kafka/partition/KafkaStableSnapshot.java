/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.partition;

/** Atomically published Kafka offset facts backed only by the stable Nereus stream head. */
public record KafkaStableSnapshot(
        long logStartOffset,
        long stableEndOffset,
        long highWatermark,
        long lastStableOffset,
        long commitVersion) {
    public KafkaStableSnapshot {
        if (logStartOffset < 0
                || lastStableOffset < logStartOffset
                || highWatermark < lastStableOffset
                || stableEndOffset < highWatermark
                || commitVersion <= 0) {
            throw new IllegalArgumentException("invalid Kafka stable offset snapshot");
        }
    }

    public static KafkaStableSnapshot nonTransactional(
            long logStartOffset, long stableEndOffset, long commitVersion) {
        return new KafkaStableSnapshot(
                logStartOffset, stableEndOffset, stableEndOffset, stableEndOffset, commitVersion);
    }
}
