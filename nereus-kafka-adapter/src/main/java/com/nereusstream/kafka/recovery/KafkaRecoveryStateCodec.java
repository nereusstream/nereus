/* Licensed under the Apache License, Version 2.0 */

package com.nereusstream.kafka.recovery;

import com.nereusstream.kafka.checkpoint.KafkaCheckpointSourceState;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointHeader;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointSection;
import java.util.List;

/**
 * Kafka-fork seam for fresh checkpoint hydration, exact batch replay, and state validation.
 */
public interface KafkaRecoveryStateCodec<StateT> {
    StateT freshState();

    void hydrateCheckpoint(StateT freshState, KafkaCheckpointHeader header, List<KafkaCheckpointSection> sections);

    void replayBatch(StateT freshState, KafkaReplayBatch batch);

    void validateRecoveredState(StateT freshState, KafkaCheckpointSourceState frozenSource);
}
