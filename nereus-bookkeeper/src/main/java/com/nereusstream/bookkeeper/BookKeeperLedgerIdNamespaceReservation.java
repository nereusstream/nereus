/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

/** Exact provisioned prefix authority read by brokers; runtime code cannot create or renew it. */
public record BookKeeperLedgerIdNamespaceReservation(
        int schemaVersion,
        String reservationId,
        String nereusDeploymentId,
        String clusterAlias,
        String bookKeeperProviderScopeSha256,
        int ledgerIdPrefixBits,
        long ledgerIdPrefixValue,
        Lifecycle lifecycle,
        long reservationEpoch,
        long createdAtMillis,
        long revokedAtMillis,
        String operatorEvidenceSha256,
        long metadataVersion,
        Checksum storedValueSha256,
        String canonicalKey) {
    public enum Lifecycle { ACTIVE, REVOKED }

    public BookKeeperLedgerIdNamespaceReservation {
        if (schemaVersion != 1) throw new IllegalArgumentException("namespace reservation schema must be 1");
        reservationId = text(reservationId, "reservationId");
        nereusDeploymentId = text(nereusDeploymentId, "nereusDeploymentId");
        clusterAlias = text(clusterAlias, "clusterAlias");
        bookKeeperProviderScopeSha256 = BookKeeperWalConfiguration.sha256(
                bookKeeperProviderScopeSha256, "bookKeeperProviderScopeSha256");
        operatorEvidenceSha256 = BookKeeperWalConfiguration.sha256(
                operatorEvidenceSha256, "operatorEvidenceSha256");
        Objects.requireNonNull(lifecycle, "lifecycle");
        Objects.requireNonNull(storedValueSha256, "storedValueSha256");
        canonicalKey = text(canonicalKey, "canonicalKey");
        new BookKeeperLedgerIdNamespace(ledgerIdPrefixBits, ledgerIdPrefixValue);
        if (reservationEpoch <= 0 || createdAtMillis < 0 || metadataVersion < 0
                || (lifecycle == Lifecycle.ACTIVE && revokedAtMillis != 0)
                || (lifecycle == Lifecycle.REVOKED && revokedAtMillis < createdAtMillis)) {
            throw new IllegalArgumentException("invalid namespace reservation lifecycle fields");
        }
        if (storedValueSha256.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException("stored reservation checksum must be SHA256");
        }
    }

    /** Exact NBLN1 capability binding; a metadata-version or stored-value change changes this digest. */
    public Checksum ledgerIdNamespaceSha256() {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update("NBLN1".getBytes(StandardCharsets.US_ASCII));
            byte[] key = canonicalKey.getBytes(StandardCharsets.UTF_8);
            digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(key.length).array());
            digest.update(key);
            digest.update(ByteBuffer.allocate(Long.BYTES).putLong(metadataVersion).array());
            digest.update(HexFormat.of().parseHex(storedValueSha256.value()));
            return new Checksum(ChecksumType.SHA256, HexFormat.of().formatHex(digest.digest()));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required", e);
        }
    }

    private static String text(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) throw new IllegalArgumentException(field + " cannot be blank");
        return value;
    }
}
