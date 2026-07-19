/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.target.ReadTargetType;
import java.util.Objects;

/** Exact live root/protection versions consumed by a BookKeeper visibility cut. */
public record BookKeeperPhysicalReferenceProof(
        PhysicalReferencePurpose purpose,
        Checksum targetIdentitySha256,
        String referenceId,
        String providerScopeSha256,
        String ledgerIdentitySha256,
        String clusterAlias,
        long ledgerId,
        long rootLifecycleEpoch,
        int ledgerRangeSlot,
        int protectionSlot,
        long rootMetadataVersion,
        Checksum rootRecordSha256,
        long protectionMetadataVersion,
        Checksum protectionRecordSha256) implements PhysicalReferenceProof {
    public BookKeeperPhysicalReferenceProof {
        Objects.requireNonNull(purpose, "purpose");
        targetIdentitySha256 = sha256(targetIdentitySha256, "targetIdentitySha256");
        referenceId = text(referenceId, "referenceId");
        providerScopeSha256 = sha256(providerScopeSha256, "providerScopeSha256");
        ledgerIdentitySha256 = sha256(ledgerIdentitySha256, "ledgerIdentitySha256");
        clusterAlias = text(clusterAlias, "clusterAlias");
        if (ledgerId <= 0 || rootLifecycleEpoch <= 0 || ledgerRangeSlot < 0 || protectionSlot < 0
                || rootMetadataVersion < 0 || protectionMetadataVersion < 0) {
            throw new IllegalArgumentException("BookKeeper physical-reference proof positions are invalid");
        }
        rootRecordSha256 = sha256(rootRecordSha256, "rootRecordSha256");
        protectionRecordSha256 = sha256(protectionRecordSha256, "protectionRecordSha256");
        int expectedSlot = switch (purpose) {
            case REACHABLE_APPEND -> 0;
            case VISIBLE_GENERATION -> 1;
        };
        if (protectionSlot != expectedSlot) {
            throw new IllegalArgumentException("BookKeeper protection slot does not match proof purpose");
        }
    }

    @Override
    public ReadTargetType targetType() {
        return ReadTargetType.BOOKKEEPER_ENTRY_RANGE;
    }

    private static Checksum sha256(Checksum value, String name) {
        Objects.requireNonNull(value, name);
        if (value.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException(name + " must use SHA256");
        }
        return value;
    }

    private static String sha256(String value, String name) {
        return new Checksum(ChecksumType.SHA256, Objects.requireNonNull(value, name)).value();
    }

    private static String text(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) throw new IllegalArgumentException(name + " cannot be blank");
        return value;
    }
}
