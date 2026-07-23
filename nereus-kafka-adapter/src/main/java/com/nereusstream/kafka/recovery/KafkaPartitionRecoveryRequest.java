/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.recovery;

import java.time.Duration;
import java.util.Objects;

/** Inputs frozen after authority acquisition for one no-append-admission leader recovery. */
public record KafkaPartitionRecoveryRequest(
        KafkaCheckpointRecoveryRequest checkpointRequest,
        Duration timeout) {
    public KafkaPartitionRecoveryRequest {
        Objects.requireNonNull(checkpointRequest, "checkpointRequest");
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isZero() || timeout.isNegative() || timeout.toMillis() <= 0) {
            throw new IllegalArgumentException("timeout must be positive and millisecond-representable");
        }
    }
}
