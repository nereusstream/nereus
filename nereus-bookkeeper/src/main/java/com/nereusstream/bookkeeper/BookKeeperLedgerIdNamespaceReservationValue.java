/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import java.util.Objects;

/** Stored provisioned value; Oxia key/version/value digest are attached only after an authoritative read. */
public record BookKeeperLedgerIdNamespaceReservationValue(
        int schemaVersion,
        String reservationId,
        String nereusDeploymentId,
        String clusterAlias,
        String bookKeeperProviderScopeSha256,
        int ledgerIdPrefixBits,
        long ledgerIdPrefixValue,
        BookKeeperLedgerIdNamespaceReservation.Lifecycle lifecycle,
        long reservationEpoch,
        long createdAtMillis,
        long revokedAtMillis,
        String operatorEvidenceSha256) {
    public BookKeeperLedgerIdNamespaceReservationValue {
        if (schemaVersion != 1) {
            throw new IllegalArgumentException("namespace reservation schema must be 1");
        }
        Objects.requireNonNull(lifecycle, "lifecycle");
        new BookKeeperLedgerIdNamespaceReservation(
                schemaVersion,
                reservationId,
                nereusDeploymentId,
                clusterAlias,
                bookKeeperProviderScopeSha256,
                ledgerIdPrefixBits,
                ledgerIdPrefixValue,
                lifecycle,
                reservationEpoch,
                createdAtMillis,
                revokedAtMillis,
                operatorEvidenceSha256,
                0,
                new Checksum(ChecksumType.SHA256, "0".repeat(64)),
                BookKeeperLedgerIdNamespaceReservationKeys.key(
                        bookKeeperProviderScopeSha256,
                        ledgerIdPrefixBits,
                        ledgerIdPrefixValue));
    }

    public BookKeeperLedgerIdNamespaceReservation materialize(
            String canonicalKey,
            long metadataVersion,
            Checksum storedValueSha256) {
        return new BookKeeperLedgerIdNamespaceReservation(
                schemaVersion,
                reservationId,
                nereusDeploymentId,
                clusterAlias,
                bookKeeperProviderScopeSha256,
                ledgerIdPrefixBits,
                ledgerIdPrefixValue,
                lifecycle,
                reservationEpoch,
                createdAtMillis,
                revokedAtMillis,
                operatorEvidenceSha256,
                metadataVersion,
                storedValueSha256,
                canonicalKey);
    }
}
