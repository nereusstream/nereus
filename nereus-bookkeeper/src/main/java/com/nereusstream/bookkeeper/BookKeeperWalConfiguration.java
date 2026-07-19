/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Objects;

/** Immutable, secret-free semantic and resource binding for one BookKeeper primary-WAL runtime. */
public record BookKeeperWalConfiguration(
        String clusterAlias,
        String providerScopeSha256,
        int ledgerIdPrefixBits,
        long ledgerIdPrefixValue,
        String ledgerIdNamespaceReservationId,
        int ensembleSize,
        int writeQuorumSize,
        int ackQuorumSize,
        BookKeeperDigestType digestType,
        BookKeeperSecretRef passwordRef,
        long maxEntriesPerLedger,
        long maxBytesPerLedger,
        int maxAppendRangesPerLedger,
        int protectionSlotsPerRange,
        int maxReaderLeasesPerLedger,
        int maxUncertainAllocations,
        Duration maxLedgerAge,
        int maxWritesInFlight,
        int maxReadsInFlight,
        long maxReadBytesInFlight,
        Duration operationTimeout,
        Duration allocationTimeout,
        Duration sealTimeout,
        Duration deleteTimeout,
        Duration readerLeaseTtl,
        Duration readerLeaseRenewInterval,
        Duration retentionScanInterval,
        int retentionPageSize) {
    public BookKeeperWalConfiguration {
        clusterAlias = text(clusterAlias, "clusterAlias");
        providerScopeSha256 = sha256(providerScopeSha256, "providerScopeSha256");
        ledgerIdNamespaceReservationId = text(
                ledgerIdNamespaceReservationId, "ledgerIdNamespaceReservationId");
        Objects.requireNonNull(digestType, "digestType");
        Objects.requireNonNull(passwordRef, "passwordRef");
        requireNamespace(ledgerIdPrefixBits, ledgerIdPrefixValue);
        if (ensembleSize <= 0 || writeQuorumSize <= 0 || ackQuorumSize <= 0
                || ensembleSize < writeQuorumSize || writeQuorumSize < ackQuorumSize) {
            throw new IllegalArgumentException("BookKeeper quorums require ensemble >= write >= ack > 0");
        }
        if (maxEntriesPerLedger <= 0 || maxBytesPerLedger <= 0
                || maxAppendRangesPerLedger <= 0 || maxReaderLeasesPerLedger <= 0
                || maxWritesInFlight <= 0 || maxReadsInFlight <= 0 || maxReadBytesInFlight <= 0
                || retentionPageSize <= 0) {
            throw new IllegalArgumentException("BookKeeper capacity bounds must be positive");
        }
        if (protectionSlotsPerRange < 4 || protectionSlotsPerRange > 64) {
            throw new IllegalArgumentException("protectionSlotsPerRange must be in [4,64]");
        }
        if (maxUncertainAllocations < 1 || maxUncertainAllocations > 65_536) {
            throw new IllegalArgumentException("maxUncertainAllocations must be in [1,65536]");
        }
        if (Math.multiplyExact((long) maxAppendRangesPerLedger, protectionSlotsPerRange) > 65_536) {
            throw new IllegalArgumentException("fixed protection-slot product exceeds 65536");
        }
        positive(maxLedgerAge, "maxLedgerAge");
        positive(operationTimeout, "operationTimeout");
        positive(allocationTimeout, "allocationTimeout");
        positive(sealTimeout, "sealTimeout");
        positive(deleteTimeout, "deleteTimeout");
        positive(readerLeaseTtl, "readerLeaseTtl");
        positive(readerLeaseRenewInterval, "readerLeaseRenewInterval");
        positive(retentionScanInterval, "retentionScanInterval");
        if (readerLeaseRenewInterval.compareTo(readerLeaseTtl) >= 0) {
            throw new IllegalArgumentException("reader lease renewal interval must be below its TTL");
        }
    }

    public Checksum configurationBindingSha256() {
        MessageDigest digest = digest();
        frame(digest, "NBKC1");
        frame(digest, clusterAlias);
        frame(digest, providerScopeSha256);
        frame(digest, Integer.toString(ledgerIdPrefixBits));
        frame(digest, Long.toUnsignedString(ledgerIdPrefixValue));
        frame(digest, ledgerIdNamespaceReservationId);
        frame(digest, Integer.toString(ensembleSize));
        frame(digest, Integer.toString(writeQuorumSize));
        frame(digest, Integer.toString(ackQuorumSize));
        frame(digest, digestType.name());
        frame(digest, passwordRef.reference());
        frame(digest, passwordRef.identityVersion());
        frame(digest, Long.toString(maxEntriesPerLedger));
        frame(digest, Long.toString(maxBytesPerLedger));
        frame(digest, Integer.toString(maxAppendRangesPerLedger));
        frame(digest, Integer.toString(protectionSlotsPerRange));
        frame(digest, Integer.toString(maxReaderLeasesPerLedger));
        frame(digest, Integer.toString(maxUncertainAllocations));
        frame(digest, Long.toString(maxLedgerAge.toNanos()));
        frame(digest, Integer.toString(maxWritesInFlight));
        frame(digest, Integer.toString(maxReadsInFlight));
        frame(digest, Long.toString(maxReadBytesInFlight));
        frame(digest, Long.toString(operationTimeout.toNanos()));
        frame(digest, Long.toString(allocationTimeout.toNanos()));
        frame(digest, Long.toString(sealTimeout.toNanos()));
        frame(digest, Long.toString(deleteTimeout.toNanos()));
        frame(digest, Long.toString(readerLeaseTtl.toNanos()));
        frame(digest, Long.toString(readerLeaseRenewInterval.toNanos()));
        frame(digest, Long.toString(retentionScanInterval.toNanos()));
        frame(digest, Integer.toString(retentionPageSize));
        return new Checksum(ChecksumType.SHA256, HexFormat.of().formatHex(digest.digest()));
    }

    public BookKeeperLedgerIdNamespace ledgerIdNamespace() {
        return new BookKeeperLedgerIdNamespace(ledgerIdPrefixBits, ledgerIdPrefixValue);
    }

    private static void requireNamespace(int bits, long value) {
        if (bits < 8 || bits > 24) throw new IllegalArgumentException("ledger-id prefix bits must be in [8,24]");
        long limit = 1L << bits;
        if (value < (limit >>> 1) || value >= limit) {
            throw new IllegalArgumentException("ledger-id prefix must set its highest bit and fit its width");
        }
    }

    private static String text(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank() || value.getBytes(StandardCharsets.UTF_8).length > 16 * 1024) {
            throw new IllegalArgumentException(field + " must be nonblank and bounded");
        }
        return value;
    }

    static String sha256(String value, String field) {
        return new Checksum(ChecksumType.SHA256, Objects.requireNonNull(value, field)).value();
    }

    private static void positive(Duration value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isZero() || value.isNegative()) throw new IllegalArgumentException(field + " must be positive");
        value.toNanos();
    }

    private static MessageDigest digest() {
        try { return MessageDigest.getInstance("SHA-256"); }
        catch (NoSuchAlgorithmException e) { throw new IllegalStateException("SHA-256 is required", e); }
    }

    private static void frame(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
        digest.update(bytes);
    }
}
