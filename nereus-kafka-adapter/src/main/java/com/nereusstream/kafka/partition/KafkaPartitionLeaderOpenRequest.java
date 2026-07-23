/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.partition;

import com.nereusstream.api.StorageProfile;
import java.time.Duration;
import java.util.Objects;

/** KRaft metadata facts required to create or reopen one Nereus-backed leader. */
public record KafkaPartitionLeaderOpenRequest(
        KafkaPartitionIdentity identity,
        int leaderId,
        int leaderEpoch,
        long brokerEpoch,
        StorageProfile storageProfile,
        long metadataOffset,
        Duration timeout) {
    public KafkaPartitionLeaderOpenRequest {
        Objects.requireNonNull(identity, "identity");
        Objects.requireNonNull(storageProfile, "storageProfile");
        Objects.requireNonNull(timeout, "timeout");
        if (leaderId < 0 || leaderEpoch < 0 || brokerEpoch < 0 || metadataOffset < 0
                || timeout.isZero() || timeout.isNegative() || timeout.toMillis() <= 0) {
            throw new IllegalArgumentException("invalid Kafka partition leader open request");
        }
    }

    public KafkaLeaderAuthority authority() {
        return new KafkaLeaderAuthority(identity, leaderId, leaderEpoch, brokerEpoch);
    }
}
