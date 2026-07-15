/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.metadata.oxia.codec.F3MetadataCodecs;
import com.nereusstream.metadata.oxia.records.CursorRetentionRecord;
import com.nereusstream.metadata.oxia.records.CursorStateRecord;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

/** Exact durable-envelope SHA-256 identities for F3 cursor authorities. */
public final class CursorMetadataDigests {
    private CursorMetadataDigests() {
    }

    public static Checksum durableValueSha256(CursorRetentionRecord record) {
        return sha256(F3MetadataCodecs.encodeEnvelope(
                Objects.requireNonNull(record, "record"), CursorRetentionRecord.class));
    }

    public static Checksum durableValueSha256(CursorStateRecord record) {
        return sha256(F3MetadataCodecs.encodeEnvelope(
                Objects.requireNonNull(record, "record"), CursorStateRecord.class));
    }

    private static Checksum sha256(byte[] bytes) {
        try {
            return new Checksum(
                    ChecksumType.SHA256,
                    HexFormat.of().formatHex(
                            MessageDigest.getInstance("SHA-256").digest(bytes)));
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }
}
