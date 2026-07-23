/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.recovery;

import java.util.Objects;
import java.util.Optional;

/** Selected verified checkpoint, or an explicit full-replay decision when trim is zero. */
public record KafkaCheckpointRecoveryResult(Optional<KafkaRecoveredCheckpoint> checkpoint) {
    public KafkaCheckpointRecoveryResult {
        checkpoint = Objects.requireNonNull(checkpoint, "checkpoint");
    }

    public static KafkaCheckpointRecoveryResult fullReplay() {
        return new KafkaCheckpointRecoveryResult(Optional.empty());
    }
}
