/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.metadata.oxia.records.StreamCommitTargetRecord;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

/** Exact live commit evidence plus its canonical NRC1 generic envelope. */
public record AppendRecoveryCommit(
        String key,
        AppendRecoveryCommitEncoding sourceEncoding,
        StreamCommitTargetRecord canonicalCommit,
        long sourceMetadataVersion,
        Checksum sourceRecordSha256,
        ByteBuffer canonicalCommitRecord,
        Checksum canonicalCommitRecordSha256) {
    public AppendRecoveryCommit {
        key = requireText(key, "key");
        Objects.requireNonNull(sourceEncoding, "sourceEncoding");
        Objects.requireNonNull(canonicalCommit, "canonicalCommit");
        if (canonicalCommit.metadataVersion() != 0 || sourceMetadataVersion < 0) {
            throw new IllegalArgumentException("recovery commit versions are inconsistent");
        }
        sourceRecordSha256 = requireSha256(sourceRecordSha256, "sourceRecordSha256");
        canonicalCommitRecordSha256 = requireSha256(
                canonicalCommitRecordSha256, "canonicalCommitRecordSha256");
        ByteBuffer source = Objects.requireNonNull(
                canonicalCommitRecord, "canonicalCommitRecord").asReadOnlyBuffer();
        byte[] bytes = new byte[source.remaining()];
        source.get(bytes);
        if (bytes.length == 0 || !sha256(bytes).equals(canonicalCommitRecordSha256)) {
            throw new IllegalArgumentException("canonical commit bytes do not match their SHA-256");
        }
        canonicalCommitRecord = ByteBuffer.wrap(bytes).asReadOnlyBuffer();
    }

    @Override
    public ByteBuffer canonicalCommitRecord() {
        return canonicalCommitRecord.asReadOnlyBuffer();
    }

    private static Checksum requireSha256(Checksum value, String field) {
        Objects.requireNonNull(value, field);
        if (value.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException(field + " must use SHA256");
        }
        return value;
    }

    private static Checksum sha256(byte[] bytes) {
        try {
            return new Checksum(
                    ChecksumType.SHA256,
                    HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)));
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }
}
