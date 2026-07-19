/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import java.util.Objects;

/** Monotonic owner-retirement evidence produced by the existing trim/source-retirement authority. */
public record BookKeeperProtectionRetirementProof(
        String protectionKey,
        long protectionMetadataVersion,
        Checksum protectionRecordSha256,
        String ownerKey,
        long ownerMetadataVersion,
        Checksum ownerIdentitySha256,
        String authorityKey,
        long authorityMetadataVersion,
        Checksum authorityRecordSha256,
        Reason reason) {
    public enum Reason { LOGICAL_TRIM, HEALTHY_HIGHER_GENERATION, ABANDONED_APPEND }

    public BookKeeperProtectionRetirementProof {
        protectionKey = text(protectionKey, "protectionKey");
        ownerKey = text(ownerKey, "ownerKey");
        authorityKey = text(authorityKey, "authorityKey");
        if (protectionMetadataVersion < 0 || ownerMetadataVersion < 0 || authorityMetadataVersion < 0) {
            throw new IllegalArgumentException("BookKeeper retirement proof versions are invalid");
        }
        protectionRecordSha256 = sha(protectionRecordSha256, "protectionRecordSha256");
        ownerIdentitySha256 = sha(ownerIdentitySha256, "ownerIdentitySha256");
        authorityRecordSha256 = sha(authorityRecordSha256, "authorityRecordSha256");
        Objects.requireNonNull(reason, "reason");
    }

    private static Checksum sha(Checksum value, String name) {
        Objects.requireNonNull(value, name);
        if (value.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException(name + " must use SHA256");
        }
        return value;
    }

    private static String text(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) throw new IllegalArgumentException(name + " cannot be blank");
        return value;
    }
}
