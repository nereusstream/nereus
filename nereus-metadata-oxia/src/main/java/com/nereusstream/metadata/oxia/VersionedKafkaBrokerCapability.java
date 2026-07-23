/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.Checksum;
import com.nereusstream.metadata.oxia.records.KafkaBrokerCapabilityRecord;
import java.util.Objects;

public record VersionedKafkaBrokerCapability(
        String key,
        KafkaBrokerCapabilityRecord value,
        long metadataVersion,
        Checksum durableValueSha256) {
    public VersionedKafkaBrokerCapability {
        VersionedKafkaStorageProtocolActivation.requireKeyAndDigest(
                key, metadataVersion, durableValueSha256);
        Objects.requireNonNull(value, "value");
        if (value.metadataVersion() != metadataVersion) {
            throw new IllegalArgumentException("capability metadata version does not match wrapper");
        }
    }
}
