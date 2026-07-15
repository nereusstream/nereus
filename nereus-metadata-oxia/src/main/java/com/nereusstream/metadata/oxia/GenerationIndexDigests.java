/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.metadata.oxia.codec.GenerationIndexRecordCodecV1;
import com.nereusstream.metadata.oxia.codec.MetadataRecordCodecFactory;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

/** Exact raw-record and durable-envelope digests used by NRC1 index recovery. */
public final class GenerationIndexDigests {
    private static final GenerationIndexRecordCodecV1 RECORD_CODEC =
            new GenerationIndexRecordCodecV1();

    private GenerationIndexDigests() {
    }

    public static Checksum canonicalRecordSha256(GenerationIndexRecord record) {
        return sha256(RECORD_CODEC.encode(Objects.requireNonNull(record, "record")));
    }

    public static Checksum durableValueSha256(GenerationIndexRecord record) {
        GenerationIndexRecord value = Objects.requireNonNull(record, "record");
        return sha256(MetadataRecordCodecFactory.encodeEnvelope(
                value, GenerationIndexRecord.class));
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
