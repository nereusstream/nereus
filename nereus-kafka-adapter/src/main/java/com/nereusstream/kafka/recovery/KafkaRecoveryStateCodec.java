/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.recovery;

import com.nereusstream.kafka.checkpoint.KafkaCheckpointSourceState;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointSection;
import java.util.List;

/** Kafka-fork seam for fresh checkpoint hydration, exact batch replay, and state validation. */
public interface KafkaRecoveryStateCodec<S> {
    S freshState();

    void hydrateCheckpoint(
            S freshState, List<KafkaCheckpointSection> sections, long checkpointOffset);

    void replayBatch(S freshState, KafkaReplayBatch batch);

    void validateRecoveredState(S freshState, KafkaCheckpointSourceState frozenSource);
}
