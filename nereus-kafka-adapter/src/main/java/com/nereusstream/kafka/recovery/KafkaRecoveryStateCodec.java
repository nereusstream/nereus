/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.recovery;

import com.nereusstream.kafka.checkpoint.KafkaCheckpointSourceState;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointHeader;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointSection;
import java.util.List;

/** Kafka-fork seam for fresh checkpoint hydration, exact batch replay, and state validation. */
public interface KafkaRecoveryStateCodec<S> {
    S freshState();

    void hydrateCheckpoint(
            S freshState,
            KafkaCheckpointHeader header,
            List<KafkaCheckpointSection> sections);

    void replayBatch(S freshState, KafkaReplayBatch batch);

    void validateRecoveredState(S freshState, KafkaCheckpointSourceState frozenSource);
}
