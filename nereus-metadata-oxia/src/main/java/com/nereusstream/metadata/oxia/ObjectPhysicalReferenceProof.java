/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.target.ReadTargetType;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import java.util.Objects;

/** Exact Object-store root/protection proof, wrapped in the provider-neutral visibility contract. */
public record ObjectPhysicalReferenceProof(
        PhysicalReferencePurpose purpose,
        Checksum targetIdentitySha256,
        ObjectProtectionIdentity protectionIdentity,
        long rootMetadataVersion,
        long rootLifecycleEpoch,
        long protectionMetadataVersion,
        Checksum protectionRecordSha256) implements PhysicalReferenceProof {
    public ObjectPhysicalReferenceProof {
        Objects.requireNonNull(purpose, "purpose");
        Objects.requireNonNull(targetIdentitySha256, "targetIdentitySha256");
        Objects.requireNonNull(protectionIdentity, "protectionIdentity");
        Objects.requireNonNull(protectionRecordSha256, "protectionRecordSha256");
        if (targetIdentitySha256.type() != ChecksumType.SHA256
                || protectionRecordSha256.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException("physical reference identities must use SHA256");
        }
        ObjectProtectionType expectedType = switch (purpose) {
            case REACHABLE_APPEND -> ObjectProtectionType.REACHABLE_APPEND;
            case VISIBLE_GENERATION -> ObjectProtectionType.VISIBLE_GENERATION;
        };
        if (protectionIdentity.type() != expectedType) {
            throw new IllegalArgumentException("Object protection type does not match its physical-reference purpose");
        }
        if (rootMetadataVersion < 0 || rootLifecycleEpoch <= 0 || protectionMetadataVersion < 0) {
            throw new IllegalArgumentException("Object physical-reference proof versions are invalid");
        }
    }

    @Override
    public ReadTargetType targetType() {
        return ReadTargetType.OBJECT_SLICE;
    }

    @Override
    public String referenceId() {
        return protectionIdentity.referenceId();
    }
}
