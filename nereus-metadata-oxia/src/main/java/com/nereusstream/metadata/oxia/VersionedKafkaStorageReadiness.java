/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.Checksum;
import com.nereusstream.metadata.oxia.records.KafkaStorageReadinessRecord;
import java.util.Objects;

public record VersionedKafkaStorageReadiness(
        String key,
        KafkaStorageReadinessRecord value,
        long metadataVersion,
        Checksum durableValueSha256) {
    public VersionedKafkaStorageReadiness {
        VersionedKafkaStorageProtocolActivation.requireKeyAndDigest(
                key, metadataVersion, durableValueSha256);
        Objects.requireNonNull(value, "value");
        if (value.metadataVersion() != metadataVersion) {
            throw new IllegalArgumentException("readiness metadata version does not match wrapper");
        }
    }
}
