/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.checkpoint;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

final class RecoveryCheckpointValidation {
    private RecoveryCheckpointValidation() {
    }

    static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        requireStringBytes(value, field);
        return value;
    }

    static String requirePossiblyEmptyText(String value, String field) {
        Objects.requireNonNull(value, field);
        requireStringBytes(value, field);
        return value;
    }

    static String requireBase32Id(String value, String field) {
        requireText(value, field);
        if (value.length() < 26 || value.length() > 128) {
            throw new IllegalArgumentException(field + " must encode at least 128 bits and be at most 128 characters");
        }
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            if (!((character >= 'a' && character <= 'z') || (character >= '2' && character <= '7'))) {
                throw new IllegalArgumentException(field + " must be lowercase base32 without padding");
            }
        }
        return value;
    }

    static Checksum requireSha256(Checksum checksum, String field) {
        Objects.requireNonNull(checksum, field);
        if (checksum.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException(field + " must use SHA256");
        }
        return checksum;
    }

    static Checksum requireCrc32c(Checksum checksum, String field) {
        Objects.requireNonNull(checksum, field);
        if (checksum.type() != ChecksumType.CRC32C) {
            throw new IllegalArgumentException(field + " must use CRC32C");
        }
        return checksum;
    }

    static ByteBuffer immutableRecordBytes(ByteBuffer value, Checksum expected, String field) {
        ByteBuffer source = Objects.requireNonNull(value, field).asReadOnlyBuffer();
        if (!source.hasRemaining() || source.remaining() > RecoveryCheckpointFormatV1.MAX_EMBEDDED_RECORD_BYTES) {
            throw new IllegalArgumentException(field + " must contain 1..64 KiB");
        }
        byte[] copied = new byte[source.remaining()];
        source.get(copied);
        if (!RecoveryCheckpointFormatV1.sha256(copied).equals(requireSha256(expected, field + "Sha256"))) {
            throw new IllegalArgumentException(field + " SHA256 does not match its exact bytes");
        }
        return ByteBuffer.wrap(copied).asReadOnlyBuffer();
    }

    static byte[] strictUtf8(String value, String field) {
        try {
            ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .encode(CharBuffer.wrap(value));
            byte[] result = new byte[encoded.remaining()];
            encoded.get(result);
            return result;
        } catch (CharacterCodingException failure) {
            throw new IllegalArgumentException(field + " is not valid Unicode text", failure);
        }
    }

    private static void requireStringBytes(String value, String field) {
        if (strictUtf8(value, field).length > RecoveryCheckpointFormatV1.MAX_STRING_BYTES) {
            throw new IllegalArgumentException(field + " exceeds the NRC1 UTF-8 byte limit");
        }
    }
}
