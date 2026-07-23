/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.partition;

import com.nereusstream.metadata.oxia.KafkaPartitionId;
import java.util.Objects;

/** Adapter identity without a dependency on Kafka server classes during F9-M2. */
public record KafkaPartitionIdentity(
        String kafkaClusterId,
        String topicId,
        int partition,
        String observedTopicName) {
    public KafkaPartitionIdentity {
        new KafkaPartitionId(kafkaClusterId, topicId, partition);
        Objects.requireNonNull(observedTopicName, "observedTopicName");
        if (observedTopicName.isBlank()) {
            throw new IllegalArgumentException("observedTopicName cannot be blank");
        }
    }

    public KafkaPartitionId durableId() {
        return new KafkaPartitionId(kafkaClusterId, topicId, partition);
    }
}
