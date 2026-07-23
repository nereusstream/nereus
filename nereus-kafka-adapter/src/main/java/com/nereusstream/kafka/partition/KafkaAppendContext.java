/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.partition;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;

/** Immutable Kafka request facts captured before one partition append is admitted. */
public record KafkaAppendContext(
        long expectedStartOffset,
        int leaderEpoch,
        short requiredAcks,
        Duration timeout,
        Map<String, String> tags) {
    public KafkaAppendContext {
        Objects.requireNonNull(timeout, "timeout");
        tags = Map.copyOf(Objects.requireNonNull(tags, "tags"));
        if (expectedStartOffset < 0 || leaderEpoch < 0) {
            throw new IllegalArgumentException("Kafka append offsets and leader epoch must be non-negative");
        }
        if (requiredAcks != 0 && requiredAcks != 1 && requiredAcks != -1) {
            throw new IllegalArgumentException("Kafka requiredAcks must be 0, 1, or -1");
        }
        if (timeout.isZero() || timeout.isNegative() || timeout.toMillis() <= 0) {
            throw new IllegalArgumentException("Kafka append timeout must be positive and millisecond-representable");
        }
    }
}
