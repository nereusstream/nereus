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

/** Deterministic `NBLR1` binary codec for the separately provisioned namespace value. */
public final class BookKeeperLedgerIdNamespaceReservationCodecV1 {
    private static final byte[] MAGIC = "NBLR1".getBytes(StandardCharsets.US_ASCII);
    private static final int MAX_TEXT_BYTES = 16 * 1024;

    private BookKeeperLedgerIdNamespaceReservationCodecV1() {
    }

    public static byte[] encode(BookKeeperLedgerIdNamespaceReservationValue value) {
        BookKeeperLedgerIdNamespaceReservationValue exact = Objects.requireNonNull(value, "value");
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                output.write(MAGIC);
                output.writeInt(exact.schemaVersion());
                text(output, exact.reservationId());
                text(output, exact.nereusDeploymentId());
                text(output, exact.clusterAlias());
                text(output, exact.bookKeeperProviderScopeSha256());
                output.writeInt(exact.ledgerIdPrefixBits());
                output.writeLong(exact.ledgerIdPrefixValue());
                output.writeByte(exact.lifecycle()
                        == BookKeeperLedgerIdNamespaceReservation.Lifecycle.ACTIVE ? 1 : 2);
                output.writeLong(exact.reservationEpoch());
                output.writeLong(exact.createdAtMillis());
                output.writeLong(exact.revokedAtMillis());
                text(output, exact.operatorEvidenceSha256());
            }
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory namespace reservation encoding failed", impossible);
        }
    }

    public static BookKeeperLedgerIdNamespaceReservationValue decode(byte[] encoded) {
        byte[] exact = Objects.requireNonNull(encoded, "encoded");
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(exact))) {
            byte[] magic = input.readNBytes(MAGIC.length);
            if (!Arrays.equals(magic, MAGIC)) {
                throw new IllegalArgumentException("invalid BookKeeper namespace reservation magic");
            }
            int schemaVersion = input.readInt();
            String reservationId = text(input);
            String deploymentId = text(input);
            String clusterAlias = text(input);
            String providerScope = text(input);
            int prefixBits = input.readInt();
            long prefixValue = input.readLong();
            BookKeeperLedgerIdNamespaceReservation.Lifecycle lifecycle = switch (input.readUnsignedByte()) {
                case 1 -> BookKeeperLedgerIdNamespaceReservation.Lifecycle.ACTIVE;
                case 2 -> BookKeeperLedgerIdNamespaceReservation.Lifecycle.REVOKED;
                default -> throw new IllegalArgumentException(
                        "unknown BookKeeper namespace reservation lifecycle wire id");
            };
            long reservationEpoch = input.readLong();
            long createdAtMillis = input.readLong();
            long revokedAtMillis = input.readLong();
            String evidence = text(input);
            if (input.read() != -1) {
                throw new IllegalArgumentException("BookKeeper namespace reservation has trailing bytes");
            }
            return new BookKeeperLedgerIdNamespaceReservationValue(
                    schemaVersion,
                    reservationId,
                    deploymentId,
                    clusterAlias,
                    providerScope,
                    prefixBits,
                    prefixValue,
                    lifecycle,
                    reservationEpoch,
                    createdAtMillis,
                    revokedAtMillis,
                    evidence);
        } catch (EOFException failure) {
            throw new IllegalArgumentException("truncated BookKeeper namespace reservation", failure);
        } catch (IOException failure) {
            throw new IllegalArgumentException("invalid BookKeeper namespace reservation", failure);
        }
    }

    private static void text(DataOutputStream output, String value) throws IOException {
        byte[] encoded = Objects.requireNonNull(value, "value").getBytes(StandardCharsets.UTF_8);
        if (encoded.length == 0 || encoded.length > MAX_TEXT_BYTES) {
            throw new IllegalArgumentException("namespace reservation text is empty or oversized");
        }
        output.writeInt(encoded.length);
        output.write(encoded);
    }

    private static String text(DataInputStream input) throws IOException {
        int length = input.readInt();
        if (length <= 0 || length > MAX_TEXT_BYTES) {
            throw new IllegalArgumentException("namespace reservation text length is invalid");
        }
        byte[] encoded = input.readNBytes(length);
        if (encoded.length != length) {
            throw new EOFException("namespace reservation text is truncated");
        }
        String decoded = new String(encoded, StandardCharsets.UTF_8);
        if (!Arrays.equals(decoded.getBytes(StandardCharsets.UTF_8), encoded)) {
            throw new IllegalArgumentException("namespace reservation text is not canonical UTF-8");
        }
        return decoded;
    }
}
