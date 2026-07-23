/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.recovery;

import java.util.Arrays;

/** One exact committed Kafka batch supplied to the recovery state codec. */
public record KafkaReplayBatch(long baseOffset, long lastOffset, byte[] encodedBatch) {
    public KafkaReplayBatch {
        if (baseOffset < 0 || lastOffset < baseOffset || encodedBatch == null || encodedBatch.length == 0) {
            throw new IllegalArgumentException("invalid Kafka replay batch");
        }
        encodedBatch = encodedBatch.clone();
    }

    @Override
    public byte[] encodedBatch() {
        return encodedBatch.clone();
    }

    @Override
    public boolean equals(Object other) {
        return this == other || other instanceof KafkaReplayBatch that
                && baseOffset == that.baseOffset
                && lastOffset == that.lastOffset
                && Arrays.equals(encodedBatch, that.encodedBatch);
    }

    @Override
    public int hashCode() {
        return 31 * java.util.Objects.hash(baseOffset, lastOffset) + Arrays.hashCode(encodedBatch);
    }
}
