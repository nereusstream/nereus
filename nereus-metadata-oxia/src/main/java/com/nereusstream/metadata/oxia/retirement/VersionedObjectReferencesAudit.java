/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.retirement;

import com.nereusstream.api.Checksum;
import com.nereusstream.metadata.oxia.records.ObjectReferenceRecord;
import java.util.Objects;

/** Exact optional Phase 1 object-reference audit value retained under a DELETED F4 root. */
public record VersionedObjectReferencesAudit(
        String key,
        ObjectReferenceRecord value,
        long metadataVersion,
        Checksum durableValueSha256) {
    public VersionedObjectReferencesAudit {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        if (key.isBlank() || metadataVersion < 0 || value.metadataVersion() != metadataVersion) {
            throw new IllegalArgumentException("versioned object-reference audit is invalid");
        }
        durableValueSha256 = RetirementMetadataSupport.requireSha256(
                durableValueSha256, "durableValueSha256");
    }
}
