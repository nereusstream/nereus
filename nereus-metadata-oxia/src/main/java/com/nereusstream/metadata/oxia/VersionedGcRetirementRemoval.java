/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.Checksum;
import com.nereusstream.metadata.oxia.records.GcRetirementRemovalRecord;
import java.util.Objects;

public record VersionedGcRetirementRemoval(
        String key,
        GcRetirementRemovalRecord value,
        long metadataVersion,
        Checksum durableValueSha256) {
    public VersionedGcRetirementRemoval {
        key = F4ValueValidation.text(key, "key");
        Objects.requireNonNull(value, "value");
        F4ValueValidation.version(metadataVersion);
        if (value.metadataVersion() != metadataVersion) {
            throw new IllegalArgumentException("GC retirement removal value/version does not match wrapper");
        }
        durableValueSha256 = F4ValueValidation.sha256(
                durableValueSha256, "durableValueSha256");
    }
}
