/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.generation;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ProjectionRef;
import com.nereusstream.api.ProjectionType;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionNames;
import com.nereusstream.metadata.oxia.records.ManagedLedgerProjectionIdentity;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Objects;
import java.util.zip.CRC32C;

/** Strict opaque NPR1 projection reference used by protocol-neutral F4 metadata. */
public record ManagedLedgerGenerationProjectionRefV1(
        String managedLedgerName,
        ManagedLedgerProjectionIdentity identity) {
    public static final String VALUE_PREFIX = "nereus-ml-v1.";
    private static final byte[] MAGIC = new byte[] {'N', 'P', 'R', '1'};
    private static final int CRC_BYTES = Integer.BYTES;
    private static final int MAX_CANONICAL_BYTES =
            MAGIC.length
                    + Integer.BYTES
                    + ManagedLedgerProjectionNames.MAX_MANAGED_LEDGER_NAME_BYTES
                    + Long.BYTES * 3
                    + Integer.BYTES
                    + 64
                    + CRC_BYTES;
    private static final int MAX_BASE64URL_CHARACTERS =
            (MAX_CANONICAL_BYTES * 4 + 2) / 3;

    public ManagedLedgerGenerationProjectionRefV1 {
        managedLedgerName =
                ManagedLedgerProjectionNames.requireManagedLedgerName(managedLedgerName);
        Objects.requireNonNull(identity, "identity");
        if (!ManagedLedgerProjectionNames.streamId(
                        managedLedgerName, identity.incarnation())
                .value()
                .equals(identity.streamId())) {
            throw new IllegalArgumentException(
                    "projection reference name/incarnation does not match its stream ID");
        }
    }

    public ProjectionRef toProjectionRef() {
        return new ProjectionRef(
                ProjectionType.VIRTUAL_LEDGER,
                VALUE_PREFIX
                        + Base64.getUrlEncoder()
                                .withoutPadding()
                                .encodeToString(canonicalBytes()));
    }

    public Checksum projectionIdentitySha256() {
        try {
            return new Checksum(
                    ChecksumType.SHA256,
                    HexFormat.of()
                            .formatHex(
                                    MessageDigest.getInstance("SHA-256")
                                            .digest(canonicalBytes())));
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }

    public byte[] canonicalBytes() {
        byte[] name = strictUtf8(managedLedgerName, "managedLedgerName");
        byte[] stream = strictUtf8(identity.streamId(), "streamId");
        ByteArrayOutputStream output = new ByteArrayOutputStream(
                MAGIC.length
                        + Integer.BYTES
                        + name.length
                        + Long.BYTES * 3
                        + Integer.BYTES
                        + stream.length
                        + CRC_BYTES);
        output.writeBytes(MAGIC);
        writeInt(output, name.length);
        output.writeBytes(name);
        writeLong(output, identity.storageClassBindingGeneration());
        writeLong(output, identity.incarnation());
        writeInt(output, stream.length);
        output.writeBytes(stream);
        writeLong(output, identity.virtualLedgerId());
        byte[] withoutCrc = output.toByteArray();
        writeInt(output, crc32c(withoutCrc));
        return output.toByteArray();
    }

    public static ManagedLedgerGenerationProjectionRefV1 from(ProjectionRef ref) {
        ProjectionRef exact = Objects.requireNonNull(ref, "ref");
        if (exact.type() != ProjectionType.VIRTUAL_LEDGER
                || !exact.value().startsWith(VALUE_PREFIX)) {
            throw new IllegalArgumentException(
                    "projection reference is not a managed-ledger NPR1 value");
        }
        String encoded = exact.value().substring(VALUE_PREFIX.length());
        if (encoded.isEmpty()
                || encoded.length() > MAX_BASE64URL_CHARACTERS
                || encoded.indexOf('=') >= 0) {
            throw new IllegalArgumentException(
                    "NPR1 projection reference must use unpadded base64url");
        }
        final byte[] bytes;
        try {
            bytes = Base64.getUrlDecoder().decode(encoded);
        } catch (IllegalArgumentException failure) {
            throw new IllegalArgumentException(
                    "NPR1 projection reference has invalid base64url", failure);
        }
        if (!Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString(bytes)
                .equals(encoded)) {
            throw new IllegalArgumentException(
                    "NPR1 projection reference is not canonically encoded");
        }
        if (bytes.length
                < MAGIC.length
                        + Integer.BYTES * 2
                        + Long.BYTES * 3
                        + CRC_BYTES) {
            throw new IllegalArgumentException("NPR1 projection reference is truncated");
        }
        int payloadLength = bytes.length - CRC_BYTES;
        ByteBuffer input =
                ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        byte[] magic = new byte[MAGIC.length];
        input.get(magic);
        if (!Arrays.equals(magic, MAGIC)) {
            throw new IllegalArgumentException(
                    "NPR1 projection reference has an unknown magic");
        }
        String managedLedgerName =
                readString(input, payloadLength, "managedLedgerName");
        long bindingGeneration = readLong(input, payloadLength, "storageClassBindingGeneration");
        long incarnation = readLong(input, payloadLength, "incarnation");
        String streamId = readString(input, payloadLength, "streamId");
        long virtualLedgerId = readLong(input, payloadLength, "virtualLedgerId");
        if (input.position() != payloadLength) {
            throw new IllegalArgumentException(
                    "NPR1 projection reference contains trailing payload bytes");
        }
        int expectedCrc = input.getInt();
        if (input.hasRemaining() || crc32c(bytes, 0, payloadLength) != expectedCrc) {
            throw new IllegalArgumentException(
                    "NPR1 projection reference CRC32C does not match");
        }
        ManagedLedgerGenerationProjectionRefV1 decoded =
                new ManagedLedgerGenerationProjectionRefV1(
                        managedLedgerName,
                        new ManagedLedgerProjectionIdentity(
                                bindingGeneration,
                                incarnation,
                                streamId,
                                virtualLedgerId));
        if (!decoded.toProjectionRef().equals(exact)) {
            throw new IllegalArgumentException(
                    "NPR1 projection reference is not canonical");
        }
        return decoded;
    }

    private static String readString(
            ByteBuffer input, int payloadLimit, String field) {
        if (input.position() > payloadLimit - Integer.BYTES) {
            throw new IllegalArgumentException(
                    "NPR1 projection reference is truncated before " + field);
        }
        long unsignedLength = Integer.toUnsignedLong(input.getInt());
        if (unsignedLength > Integer.MAX_VALUE
                || unsignedLength > payloadLimit - input.position()) {
            throw new IllegalArgumentException(
                    "NPR1 projection reference has an invalid " + field + " length");
        }
        byte[] bytes = new byte[(int) unsignedLength];
        input.get(bytes);
        try {
            return StandardCharsets.UTF_8
                    .newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes))
                    .toString();
        } catch (CharacterCodingException failure) {
            throw new IllegalArgumentException(
                    "NPR1 projection reference " + field + " is not strict UTF-8",
                    failure);
        }
    }

    private static long readLong(
            ByteBuffer input, int payloadLimit, String field) {
        if (input.position() > payloadLimit - Long.BYTES) {
            throw new IllegalArgumentException(
                    "NPR1 projection reference is truncated before " + field);
        }
        return input.getLong();
    }

    private static byte[] strictUtf8(String value, String field) {
        try {
            ByteBuffer encoded = StandardCharsets.UTF_8
                    .newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .encode(CharBuffer.wrap(value));
            byte[] bytes = new byte[encoded.remaining()];
            encoded.get(bytes);
            return bytes;
        } catch (CharacterCodingException failure) {
            throw new IllegalArgumentException(field + " must be strict UTF-8", failure);
        }
    }

    private static void writeInt(ByteArrayOutputStream output, int value) {
        output.writeBytes(ByteBuffer.allocate(Integer.BYTES)
                .order(ByteOrder.BIG_ENDIAN)
                .putInt(value)
                .array());
    }

    private static void writeLong(ByteArrayOutputStream output, long value) {
        output.writeBytes(ByteBuffer.allocate(Long.BYTES)
                .order(ByteOrder.BIG_ENDIAN)
                .putLong(value)
                .array());
    }

    private static int crc32c(byte[] bytes) {
        return crc32c(bytes, 0, bytes.length);
    }

    private static int crc32c(byte[] bytes, int offset, int length) {
        CRC32C crc = new CRC32C();
        crc.update(bytes, offset, length);
        return (int) crc.getValue();
    }
}
