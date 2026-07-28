/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.Checksum;
import com.nereusstream.metadata.oxia.records.KafkaCheckpointFailureRecord;

import java.util.Objects;

/** Exact version and durable-value digest for one immutable checkpoint quarantine record. */
public record VersionedKafkaCheckpointFailure(
        String key,
        KafkaCheckpointFailureRecord value,
        long metadataVersion,
        Checksum durableValueSha256) {
    public VersionedKafkaCheckpointFailure {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        Objects.requireNonNull(durableValueSha256, "durableValueSha256");
        if (key.isBlank() || metadataVersion < 0) {
            throw new IllegalArgumentException("invalid versioned Kafka checkpoint failure");
        }
    }
}
