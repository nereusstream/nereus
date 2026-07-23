/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.partition;

import com.nereusstream.kafka.metadata.KafkaPartitionBinding;
import com.nereusstream.metadata.oxia.records.KafkaPartitionLifecycle;
import java.time.Duration;
import java.util.Objects;

/** Exact immutable inputs handed to one authority-acquire and recovery operation. */
public record KafkaPartitionOpenPlan(
        KafkaLeaderAuthority authority,
        KafkaPartitionBinding binding,
        KafkaStorageProfilePolicy profilePolicy,
        Duration timeout) {
    public KafkaPartitionOpenPlan {
        Objects.requireNonNull(authority, "authority");
        Objects.requireNonNull(binding, "binding");
        Objects.requireNonNull(profilePolicy, "profilePolicy");
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isZero() || timeout.isNegative() || timeout.toMillis() <= 0) {
            throw new IllegalArgumentException("Kafka partition open timeout must be positive");
        }
        if (!binding.identity().equals(authority.identity())
                || binding.durableRoot().value().lifecycle() != KafkaPartitionLifecycle.ACTIVE
                || !binding.durableRoot().value().storageProfile().equals(profilePolicy.storageProfile().name())) {
            throw new IllegalArgumentException("Kafka partition open requires the exact ACTIVE binding and profile");
        }
    }

    boolean compatibleWith(KafkaPartitionOpenPlan other) {
        return authority.equals(other.authority)
                && binding.streamId().equals(other.binding.streamId())
                && binding.streamName().equals(other.binding.streamName())
                && profilePolicy.equals(other.profilePolicy);
    }
}
