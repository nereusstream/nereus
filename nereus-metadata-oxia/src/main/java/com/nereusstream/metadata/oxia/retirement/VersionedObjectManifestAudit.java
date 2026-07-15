/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.retirement;

import com.nereusstream.api.Checksum;
import com.nereusstream.metadata.oxia.records.ObjectManifestRecord;
import java.util.Objects;

/** Exact optional Phase 1 object-manifest audit value retained under a DELETED F4 root. */
public record VersionedObjectManifestAudit(
        String key,
        ObjectManifestRecord value,
        long metadataVersion,
        Checksum durableValueSha256) {
    public VersionedObjectManifestAudit {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        if (key.isBlank() || metadataVersion < 0 || value.metadataVersion() != metadataVersion) {
            throw new IllegalArgumentException("versioned object-manifest audit is invalid");
        }
        durableValueSha256 = RetirementMetadataSupport.requireSha256(
                durableValueSha256, "durableValueSha256");
    }
}
