/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.recovery;

import com.nereusstream.kafka.checkpoint.KafkaCheckpointSourceState;
import java.util.Objects;
import java.util.Optional;

/** Complete fresh Kafka state at the exact frozen stable end, ready for atomic publication. */
public record KafkaRecoveredPartition<S>(
        S state,
        KafkaCheckpointSourceState frozenSource,
        long replayStartOffset,
        long replayEndOffset,
        int replayedBatchCount,
        Optional<String> checkpointObjectId) {
    public KafkaRecoveredPartition {
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(frozenSource, "frozenSource");
        checkpointObjectId = Objects.requireNonNull(checkpointObjectId, "checkpointObjectId");
        if (replayStartOffset < 0 || replayEndOffset < replayStartOffset || replayedBatchCount < 0) {
            throw new IllegalArgumentException("invalid Kafka recovered partition range");
        }
    }
}
