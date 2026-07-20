/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Explicit, fail-closed provision/revoke state machine for one advanced-ledger-id prefix. */
public final class BookKeeperLedgerIdNamespaceProvisioningCoordinator {
    private final BookKeeperLedgerIdNamespaceReservationAdminStore store;
    private final Clock clock;

    public BookKeeperLedgerIdNamespaceProvisioningCoordinator(
            BookKeeperLedgerIdNamespaceReservationAdminStore store, Clock clock) {
        this.store = Objects.requireNonNull(store, "store");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    public CompletableFuture<BookKeeperLedgerIdNamespaceReservation> provision(
            BookKeeperWalConfiguration configuration,
            String nereusDeploymentId,
            String operatorEvidenceSha256,
            Duration timeout) {
        BookKeeperWalConfiguration exact = Objects.requireNonNull(configuration, "configuration");
        String deployment = text(nereusDeploymentId, "nereusDeploymentId");
        String evidence = BookKeeperWalConfiguration.sha256(
                operatorEvidenceSha256, "operatorEvidenceSha256");
        BookKeeperOperationDeadline deadline = new BookKeeperOperationDeadline(timeout);
        return store.read(
                        exact.providerScopeSha256(),
                        exact.ledgerIdPrefixBits(),
                        exact.ledgerIdPrefixValue(),
                        deadline.remaining())
                .thenCompose(existing -> {
                    if (existing.isPresent()) {
                        BookKeeperLedgerIdNamespaceReservation reservation = existing.orElseThrow();
                        if (sameProvisioningIdentity(
                                reservation, exact, deployment, evidence)) {
                            return CompletableFuture.completedFuture(reservation);
                        }
                        return CompletableFuture.failedFuture(condition(
                                "BookKeeper ledger-id namespace is already reserved or revoked"));
                    }
                    long now = clock.millis();
                    BookKeeperLedgerIdNamespaceReservationValue value =
                            new BookKeeperLedgerIdNamespaceReservationValue(
                                    1,
                                    exact.ledgerIdNamespaceReservationId(),
                                    deployment,
                                    exact.clusterAlias(),
                                    exact.providerScopeSha256(),
                                    exact.ledgerIdPrefixBits(),
                                    exact.ledgerIdPrefixValue(),
                                    BookKeeperLedgerIdNamespaceReservation.Lifecycle.ACTIVE,
                                    1,
                                    now,
                                    0,
                                    evidence);
                    return store.create(value, deadline.remaining());
                });
    }

    public CompletableFuture<BookKeeperLedgerIdNamespaceReservation> revoke(
            BookKeeperWalConfiguration configuration,
            String nereusDeploymentId,
            String revocationEvidenceSha256,
            long expectedMetadataVersion,
            Duration timeout) {
        BookKeeperWalConfiguration exact = Objects.requireNonNull(configuration, "configuration");
        String deployment = text(nereusDeploymentId, "nereusDeploymentId");
        String evidence = BookKeeperWalConfiguration.sha256(
                revocationEvidenceSha256, "revocationEvidenceSha256");
        if (expectedMetadataVersion < 0) {
            throw new IllegalArgumentException("expectedMetadataVersion must be non-negative");
        }
        BookKeeperOperationDeadline deadline = new BookKeeperOperationDeadline(timeout);
        return store.read(
                        exact.providerScopeSha256(),
                        exact.ledgerIdPrefixBits(),
                        exact.ledgerIdPrefixValue(),
                        deadline.remaining())
                .thenCompose(existing -> {
                    BookKeeperLedgerIdNamespaceReservation reservation = existing.orElseThrow(() ->
                            condition("BookKeeper ledger-id namespace reservation is absent"));
                    requireExactActive(reservation, exact, deployment, expectedMetadataVersion);
                    BookKeeperLedgerIdNamespaceReservationValue replacement =
                            new BookKeeperLedgerIdNamespaceReservationValue(
                                    1,
                                    reservation.reservationId(),
                                    reservation.nereusDeploymentId(),
                                    reservation.clusterAlias(),
                                    reservation.bookKeeperProviderScopeSha256(),
                                    reservation.ledgerIdPrefixBits(),
                                    reservation.ledgerIdPrefixValue(),
                                    BookKeeperLedgerIdNamespaceReservation.Lifecycle.REVOKED,
                                    Math.addExact(reservation.reservationEpoch(), 1),
                                    reservation.createdAtMillis(),
                                    Math.max(clock.millis(), reservation.createdAtMillis()),
                                    evidence);
                    return store.compareAndSet(
                            replacement,
                            expectedMetadataVersion,
                            deadline.remaining());
                });
    }

    private static boolean sameProvisioningIdentity(
            BookKeeperLedgerIdNamespaceReservation reservation,
            BookKeeperWalConfiguration configuration,
            String deployment,
            String evidence) {
        return reservation.lifecycle() == BookKeeperLedgerIdNamespaceReservation.Lifecycle.ACTIVE
                && reservation.reservationEpoch() == 1
                && reservation.reservationId().equals(
                        configuration.ledgerIdNamespaceReservationId())
                && reservation.nereusDeploymentId().equals(deployment)
                && reservation.clusterAlias().equals(configuration.clusterAlias())
                && reservation.bookKeeperProviderScopeSha256().equals(
                        configuration.providerScopeSha256())
                && reservation.ledgerIdPrefixBits() == configuration.ledgerIdPrefixBits()
                && reservation.ledgerIdPrefixValue() == configuration.ledgerIdPrefixValue()
                && reservation.operatorEvidenceSha256().equals(evidence);
    }

    private static void requireExactActive(
            BookKeeperLedgerIdNamespaceReservation reservation,
            BookKeeperWalConfiguration configuration,
            String deployment,
            long expectedMetadataVersion) {
        if (reservation.lifecycle() != BookKeeperLedgerIdNamespaceReservation.Lifecycle.ACTIVE
                || reservation.metadataVersion() != expectedMetadataVersion
                || !reservation.reservationId().equals(
                        configuration.ledgerIdNamespaceReservationId())
                || !reservation.nereusDeploymentId().equals(deployment)
                || !reservation.clusterAlias().equals(configuration.clusterAlias())
                || !reservation.bookKeeperProviderScopeSha256().equals(
                        configuration.providerScopeSha256())
                || reservation.ledgerIdPrefixBits() != configuration.ledgerIdPrefixBits()
                || reservation.ledgerIdPrefixValue() != configuration.ledgerIdPrefixValue()) {
            throw condition("BookKeeper ledger-id namespace revoke precondition failed");
        }
    }

    private static String text(String value, String name) {
        String exact = Objects.requireNonNull(value, name);
        if (exact.isBlank()) {
            throw new IllegalArgumentException(name + " cannot be blank");
        }
        return exact;
    }

    private static NereusException condition(String message) {
        return new NereusException(ErrorCode.METADATA_CONDITION_FAILED, true, message);
    }
}
