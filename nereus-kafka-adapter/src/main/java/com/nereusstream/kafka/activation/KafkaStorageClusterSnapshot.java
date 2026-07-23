/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.activation;

import com.nereusstream.metadata.oxia.KafkaBrokerIdentity;
import java.util.List;
import java.util.Objects;

/** Immutable KRaft facts consumed by activation without exposing Kafka implementation types. */
public record KafkaStorageClusterSnapshot(
        String kafkaClusterId,
        long metadataOffset,
        int kafkaFeatureLevel,
        List<KafkaBrokerIdentity> brokers,
        boolean topicsPresent,
        boolean authoritativeLocalLogsPresent,
        boolean bindingsPresent) {
    public KafkaStorageClusterSnapshot {
        Objects.requireNonNull(kafkaClusterId, "kafkaClusterId");
        if (kafkaClusterId.isBlank()) {
            throw new IllegalArgumentException("kafkaClusterId must be nonblank");
        }
        if (metadataOffset < 0 || kafkaFeatureLevel < 0) {
            throw new IllegalArgumentException("KRaft metadata offset and feature level must be non-negative");
        }
        brokers = List.copyOf(Objects.requireNonNull(brokers, "brokers"));
        if (brokers.isEmpty()) {
            throw new IllegalArgumentException("KRaft broker set must be non-empty");
        }
        for (int index = 1; index < brokers.size(); index++) {
            if (brokers.get(index - 1).compareTo(brokers.get(index)) >= 0) {
                throw new IllegalArgumentException("KRaft broker set must be strictly sorted and unique");
            }
        }
    }

    public boolean emptyForFirstActivation() {
        return !topicsPresent && !authoritativeLocalLogsPresent && !bindingsPresent;
    }
}
