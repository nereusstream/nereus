/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.metadata.oxia.records.KafkaStorageProtocolActivationRecord;
import java.util.Objects;

public record VersionedKafkaStorageProtocolActivation(
        String key,
        KafkaStorageProtocolActivationRecord value,
        long metadataVersion,
        Checksum durableValueSha256) {
    public VersionedKafkaStorageProtocolActivation {
        requireKeyAndDigest(key, metadataVersion, durableValueSha256);
        Objects.requireNonNull(value, "value");
        if (value.metadataVersion() != metadataVersion) {
            throw new IllegalArgumentException("activation metadata version does not match wrapper");
        }
    }

    static void requireKeyAndDigest(String key, long version, Checksum digest) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("key cannot be blank");
        }
        if (version < 0) {
            throw new IllegalArgumentException("metadataVersion must be non-negative");
        }
        Objects.requireNonNull(digest, "durableValueSha256");
        if (digest.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException("durableValueSha256 must use SHA256");
        }
    }
}
