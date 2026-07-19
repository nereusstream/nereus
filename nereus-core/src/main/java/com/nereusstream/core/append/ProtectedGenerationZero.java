/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.append;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ReadTargetIdentities;
import com.nereusstream.metadata.oxia.MaterializedGenerationZero;
import com.nereusstream.metadata.oxia.ObjectPhysicalReferenceProof;
import com.nereusstream.metadata.oxia.ObjectProtectionIdentity;
import com.nereusstream.metadata.oxia.PhysicalReferenceProof;
import com.nereusstream.metadata.oxia.PhysicalReferencePurpose;
import java.util.Objects;

/** Exact index-owned provider physical-reference proof required before strict append success. */
public record ProtectedGenerationZero(
        MaterializedGenerationZero materialized,
        PhysicalReferenceProof proof) {
    public ProtectedGenerationZero {
        Objects.requireNonNull(materialized, "materialized");
        Objects.requireNonNull(proof, "proof");
        if (proof.purpose() != PhysicalReferencePurpose.VISIBLE_GENERATION
                || proof.targetType() != materialized.committedAppend().readTarget().type()
                || !proof.targetIdentitySha256().equals(ReadTargetIdentities.sha256(
                        materialized.committedAppend().readTarget()))
                || !proof.referenceId().equals(
                        GenerationZeroProtectionIdentities.visibleGenerationReferenceId(materialized))) {
            throw new IllegalArgumentException("protected generation-zero identities do not match");
        }
    }

    /** Object-WAL compatibility constructor. */
    @Deprecated(forRemoval = true)
    public ProtectedGenerationZero(
            MaterializedGenerationZero materialized,
            ObjectProtectionIdentity protectionIdentity,
            long rootMetadataVersion,
            long rootLifecycleEpoch,
            long protectionMetadataVersion,
            Checksum protectionRecordSha256) {
        this(materialized, new ObjectPhysicalReferenceProof(
                PhysicalReferencePurpose.VISIBLE_GENERATION,
                ReadTargetIdentities.sha256(materialized.committedAppend().readTarget()),
                protectionIdentity,
                rootMetadataVersion,
                rootLifecycleEpoch,
                protectionMetadataVersion,
                protectionRecordSha256));
    }

    private ObjectPhysicalReferenceProof objectProof() {
        if (!(proof instanceof ObjectPhysicalReferenceProof object)) {
            throw new IllegalStateException("generation zero does not carry an Object physical-reference proof");
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
