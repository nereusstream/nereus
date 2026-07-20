/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/** Deterministic NBKA1 binary codec for the durable BookKeeper rollout authority. */
public final class BookKeeperProtocolActivationCodecV1 {
    private static final byte[] MAGIC = "NBKA1".getBytes(StandardCharsets.US_ASCII);
    private static final int MAX_TEXT_BYTES = 16 * 1024;

    private BookKeeperProtocolActivationCodecV1() {
    }

    public static byte[] encode(BookKeeperProtocolActivationValue value) {
        BookKeeperProtocolActivationValue exact = Objects.requireNonNull(value, "value");
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                output.write(MAGIC);
                output.writeInt(exact.schemaVersion());
                output.writeByte(exact.lifecycle().wireId());
                output.writeInt(exact.protocolVersion());
                text(output, exact.clusterAlias());
                text(output, exact.providerScopeSha256());
                output.writeLong(exact.brokerReadinessEpoch());
                text(output, exact.brokerReadinessSha256());
                text(output, exact.configurationBindingSha256());
                text(output, exact.ledgerIdNamespaceSha256());
                output.writeBoolean(exact.walOnlyPublicationEnabled());
                output.writeBoolean(exact.asyncPublicationEnabled());
                output.writeBoolean(exact.syncPublicationEnabled());
                output.writeBoolean(exact.ledgerDeletionEnabled());
                text(output, exact.rootCoverageProofSha256());
                text(output, exact.streamCoverageProofSha256());
                text(output, exact.bookKeeperScopeProofSha256());
                output.writeLong(exact.activatedAtMillis());
            }
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory BookKeeper activation encoding failed", impossible);
        }
    }

    public static BookKeeperProtocolActivationValue decode(byte[] encoded) {
        byte[] exact = Objects.requireNonNull(encoded, "encoded");
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(exact))) {
            byte[] magic = input.readNBytes(MAGIC.length);
            if (!Arrays.equals(magic, MAGIC)) {
                throw new IllegalArgumentException("invalid BookKeeper activation magic");
            }
            BookKeeperProtocolActivationValue value = new BookKeeperProtocolActivationValue(
                    input.readInt(),
                    BookKeeperProtocolActivationLifecycle.fromWireId(input.readUnsignedByte()),
                    input.readInt(),
                    text(input),
                    text(input),
                    input.readLong(),
                    text(input),
                    text(input),
                    text(input),
                    input.readBoolean(),
                    input.readBoolean(),
                    input.readBoolean(),
                    input.readBoolean(),
                    text(input),
                    text(input),
                    text(input),
                    input.readLong());
            if (input.read() != -1) {
                throw new IllegalArgumentException("BookKeeper activation has trailing bytes");
            }
            return value;
        } catch (EOFException failure) {
            throw new IllegalArgumentException("truncated BookKeeper activation", failure);
        } catch (IOException failure) {
            throw new IllegalArgumentException("invalid BookKeeper activation", failure);
        }
    }

    private static void text(DataOutputStream output, String value) throws IOException {
        byte[] bytes = Objects.requireNonNull(value, "value").getBytes(StandardCharsets.UTF_8);
        if (bytes.length == 0 || bytes.length > MAX_TEXT_BYTES) {
            throw new IllegalArgumentException("BookKeeper activation text is empty or oversized");
        }
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static String text(DataInputStream input) throws IOException {
        int length = input.readInt();
        if (length <= 0 || length > MAX_TEXT_BYTES) {
            throw new IllegalArgumentException("BookKeeper activation text length is invalid");
        }
        byte[] bytes = input.readNBytes(length);
        if (bytes.length != length) {
            throw new EOFException("BookKeeper activation text is truncated");
        }
        String decoded = new String(bytes, StandardCharsets.UTF_8);
        if (!Arrays.equals(decoded.getBytes(StandardCharsets.UTF_8), bytes)) {
            throw new IllegalArgumentException("BookKeeper activation text is not canonical UTF-8");
        }
        return decoded;
    }
}
