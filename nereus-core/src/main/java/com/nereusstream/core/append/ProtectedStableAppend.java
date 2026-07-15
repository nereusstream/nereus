/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.append;

import com.nereusstream.api.Checksum;
import com.nereusstream.core.physical.GcReferenceQuery;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.metadata.oxia.ObjectProtectionIdentity;
import com.nereusstream.metadata.oxia.PreparedStableAppend;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import java.util.Objects;

/** Exact pre-head protection proof consumed once by the stable append committer. */
public record ProtectedStableAppend(
        PreparedStableAppend prepared,
        PhysicalObjectIdentity object,
        ObjectProtectionIdentity protectionIdentity,
        long rootMetadataVersion,
        long rootLifecycleEpoch,
        long protectionMetadataVersion,
        Checksum protectionRecordSha256) {
    public ProtectedStableAppend {
        Objects.requireNonNull(prepared, "prepared");
        Objects.requireNonNull(object, "object");
        Objects.requireNonNull(protectionIdentity, "protectionIdentity");
        if (!object.objectKeyHash().equals(prepared.objectKeyHash())
                || !protectionIdentity.object().equals(object.objectKeyHash())
                || protectionIdentity.type() != ObjectProtectionType.REACHABLE_APPEND
                || !protectionIdentity.referenceId().equals(
                        GenerationZeroProtectionIdentities.reachableAppendReferenceId(prepared))) {
            throw new IllegalArgumentException("protected stable append identities do not match");
        }
        if (rootMetadataVersion < 0 || rootLifecycleEpoch <= 0 || protectionMetadataVersion < 0) {
            throw new IllegalArgumentException("protected stable append versions are invalid");
        }
        protectionRecordSha256 = GcReferenceQuery.requireSha256(
                protectionRecordSha256,
                "protectionRecordSha256");
    }
}
