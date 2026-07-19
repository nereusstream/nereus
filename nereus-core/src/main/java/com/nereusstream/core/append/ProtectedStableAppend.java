/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.append;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.metadata.oxia.ObjectPhysicalReferenceProof;
import com.nereusstream.metadata.oxia.ObjectProtectionIdentity;
import com.nereusstream.metadata.oxia.PhysicalReferenceProof;
import com.nereusstream.metadata.oxia.PhysicalReferencePurpose;
import com.nereusstream.metadata.oxia.PreparedStableAppend;
import java.util.Objects;

/** Exact pre-head provider protection proof consumed once by the stable append committer. */
public record ProtectedStableAppend(
        PreparedStableAppend prepared,
        PhysicalReferenceProof proof) {
    public ProtectedStableAppend {
        Objects.requireNonNull(prepared, "prepared");
        Objects.requireNonNull(proof, "proof");
        if (proof.purpose() != PhysicalReferencePurpose.REACHABLE_APPEND
                || proof.targetType() != prepared.request().readTarget().type()
                || !proof.targetIdentitySha256().equals(prepared.primaryTargetIdentitySha256())
                || !proof.referenceId().equals(
                        GenerationZeroProtectionIdentities.reachableAppendReferenceId(prepared))) {
            throw new IllegalArgumentException("protected stable append identities do not match");
        }
    }

    /** Object-WAL compatibility constructor. */
    @Deprecated(forRemoval = true)
    public ProtectedStableAppend(
            PreparedStableAppend prepared,
            PhysicalObjectIdentity object,
            ObjectProtectionIdentity protectionIdentity,
            long rootMetadataVersion,
            long rootLifecycleEpoch,
            long protectionMetadataVersion,
            Checksum protectionRecordSha256) {
        this(prepared, new ObjectPhysicalReferenceProof(
                PhysicalReferencePurpose.REACHABLE_APPEND,
                prepared.primaryTargetIdentitySha256(),
                protectionIdentity,
                rootMetadataVersion,
                rootLifecycleEpoch,
                protectionMetadataVersion,
                protectionRecordSha256));
        Objects.requireNonNull(object, "object");
        if (!(prepared.request().readTarget() instanceof ObjectSliceReadTarget target)
                || !object.objectKeyHash().equals(com.nereusstream.api.ObjectKeyHash.from(target.objectKey()))
                || !protectionIdentity.object().equals(object.objectKeyHash())) {
            throw new IllegalArgumentException("protected Object stable append identities do not match");
        }
    }

    private ObjectPhysicalReferenceProof objectProof() {
        if (!(proof instanceof ObjectPhysicalReferenceProof object)) {
            throw new IllegalStateException("stable append does not carry an Object physical-reference proof");
        }
        return object;
    }

    @Deprecated(forRemoval = true)
    public ObjectProtectionIdentity protectionIdentity() { return objectProof().protectionIdentity(); }
    @Deprecated(forRemoval = true)
    public long rootMetadataVersion() { return objectProof().rootMetadataVersion(); }
    @Deprecated(forRemoval = true)
    public long rootLifecycleEpoch() { return objectProof().rootLifecycleEpoch(); }
    @Deprecated(forRemoval = true)
    public long protectionMetadataVersion() { return objectProof().protectionMetadataVersion(); }
    @Deprecated(forRemoval = true)
    public Checksum protectionRecordSha256() { return objectProof().protectionRecordSha256(); }
}
