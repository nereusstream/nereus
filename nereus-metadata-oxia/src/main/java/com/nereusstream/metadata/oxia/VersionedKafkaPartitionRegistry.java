/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.metadata.oxia.records.KafkaPartitionRegistryRecord;
import java.util.Objects;

public record VersionedKafkaPartitionRegistry(
        String key,
        KafkaPartitionRegistryRecord value,
        long metadataVersion,
        Checksum durableValueSha256) {
    public VersionedKafkaPartitionRegistry {
        if (key == null || key.isBlank()) throw new IllegalArgumentException("key cannot be blank");
        Objects.requireNonNull(value, "value");
        if (metadataVersion < 0 || value.metadataVersion() != metadataVersion) {
            throw new IllegalArgumentException("registry metadata version does not match wrapper");
        }
        Objects.requireNonNull(durableValueSha256, "durableValueSha256");
        if (durableValueSha256.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException("durableValueSha256 must use SHA256");
        }
    }
}
