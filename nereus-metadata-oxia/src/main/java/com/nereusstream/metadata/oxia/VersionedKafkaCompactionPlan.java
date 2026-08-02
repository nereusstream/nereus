/* Licensed under the Apache License, Version 2.0 */

package com.nereusstream.metadata.oxia;

import com.nereusstream.api.Checksum;
import com.nereusstream.metadata.oxia.records.KafkaCompactionPlanRecord;
import java.util.Objects;

/**
 * Exact version and durable-value digest for one immutable Kafka compaction plan.
 */
public record VersionedKafkaCompactionPlan(
        String key, KafkaCompactionPlanRecord value, long metadataVersion, Checksum durableValueSha256) {
    public VersionedKafkaCompactionPlan {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        Objects.requireNonNull(durableValueSha256, "durableValueSha256");
        if (key.isBlank() || metadataVersion < 0) {
            throw new IllegalArgumentException("invalid versioned Kafka compaction plan");
        }
    }
}
