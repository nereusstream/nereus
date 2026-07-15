/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.append;

import com.nereusstream.api.Checksum;
import com.nereusstream.core.physical.GcReferenceQuery;
import com.nereusstream.metadata.oxia.MaterializedGenerationZero;
import com.nereusstream.metadata.oxia.ObjectProtectionIdentity;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import java.util.Objects;

/** Exact index-owned physical-reference proof required before strict append success. */
public record ProtectedGenerationZero(
        MaterializedGenerationZero materialized,
        ObjectProtectionIdentity protectionIdentity,
        long rootMetadataVersion,
        long rootLifecycleEpoch,
        long protectionMetadataVersion,
        Checksum protectionRecordSha256) {
    public ProtectedGenerationZero {
        Objects.requireNonNull(materialized, "materialized");
        Objects.requireNonNull(protectionIdentity, "protectionIdentity");
        if (!protectionIdentity.object().equals(
                        GenerationZeroProtectionIdentities.objectKeyHash(materialized.committedAppend()))
                || protectionIdentity.type() != ObjectProtectionType.VISIBLE_GENERATION
                || !protectionIdentity.referenceId().equals(
                        GenerationZeroProtectionIdentities.visibleGenerationReferenceId(materialized))) {
            throw new IllegalArgumentException("protected generation-zero identities do not match");
        }
        if (rootMetadataVersion < 0 || rootLifecycleEpoch <= 0 || protectionMetadataVersion < 0) {
            throw new IllegalArgumentException("protected generation-zero versions are invalid");
        }
        protectionRecordSha256 = GcReferenceQuery.requireSha256(
                protectionRecordSha256,
                "protectionRecordSha256");
    }
}
