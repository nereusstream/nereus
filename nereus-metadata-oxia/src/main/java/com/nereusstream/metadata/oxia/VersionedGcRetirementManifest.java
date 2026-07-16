/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.Checksum;
import com.nereusstream.metadata.oxia.records.GcRetirementManifestRecord;
import java.util.Objects;

public record VersionedGcRetirementManifest(
        String key,
        GcRetirementManifestRecord value,
        long metadataVersion,
        Checksum durableValueSha256) {
    public VersionedGcRetirementManifest {
        key = F4ValueValidation.text(key, "key");
        Objects.requireNonNull(value, "value");
        F4ValueValidation.version(metadataVersion);
        if (value.metadataVersion() != metadataVersion) {
            throw new IllegalArgumentException("GC retirement manifest value/version does not match wrapper");
        }
        durableValueSha256 = F4ValueValidation.sha256(
                durableValueSha256, "durableValueSha256");
    }
}
