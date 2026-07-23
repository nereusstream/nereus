/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.recovery;

import com.nereusstream.kafka.checkpoint.KafkaCheckpointSourceState;
import com.nereusstream.kafka.checkpoint.KafkaCheckpointSourceValidator;
import com.nereusstream.kafka.partition.KafkaPartitionIdentity;
import com.nereusstream.metadata.oxia.VersionedKafkaPartitionBinding;
import java.time.Duration;
import java.util.Objects;

public record KafkaCheckpointRecoveryRequest(
        KafkaPartitionIdentity identity,
        VersionedKafkaPartitionBinding binding,
        KafkaCheckpointSourceState currentSource,
        KafkaCheckpointSourceValidator sourceValidator,
        Duration timeout) {
    public KafkaCheckpointRecoveryRequest {
        Objects.requireNonNull(identity, "identity");
        Objects.requireNonNull(binding, "binding");
        Objects.requireNonNull(currentSource, "currentSource");
        Objects.requireNonNull(sourceValidator, "sourceValidator");
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
    }
}
