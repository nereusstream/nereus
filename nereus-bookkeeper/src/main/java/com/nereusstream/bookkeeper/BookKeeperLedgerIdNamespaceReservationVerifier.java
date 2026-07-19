/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Fail-closed runtime verifier; intentionally exposes no provision/create mutation. */
public final class BookKeeperLedgerIdNamespaceReservationVerifier {
    private final BookKeeperLedgerIdNamespaceReservationStore store;
    private final String deploymentId;

    public BookKeeperLedgerIdNamespaceReservationVerifier(
            BookKeeperLedgerIdNamespaceReservationStore store,
            String deploymentId) {
        this.store = Objects.requireNonNull(store, "store");
        this.deploymentId = Objects.requireNonNull(deploymentId, "deploymentId");
        if (deploymentId.isBlank()) throw new IllegalArgumentException("deploymentId cannot be blank");
    }

    public CompletableFuture<BookKeeperLedgerIdNamespaceReservation> requireActive(
            BookKeeperWalConfiguration configuration,
            Duration timeout) {
        Objects.requireNonNull(configuration, "configuration");
        return store.read(configuration.providerScopeSha256(), configuration.ledgerIdPrefixBits(),
                        configuration.ledgerIdPrefixValue(), Objects.requireNonNull(timeout, "timeout"))
                .thenApply(value -> {
                    BookKeeperLedgerIdNamespaceReservation reservation = value.orElseThrow(() -> unavailable(
                            "BookKeeper ledger-id namespace reservation is absent"));
                    if (reservation.lifecycle()
                                    != BookKeeperLedgerIdNamespaceReservation.Lifecycle.ACTIVE
                            || !reservation.reservationId().equals(
                                    configuration.ledgerIdNamespaceReservationId())
                            || !reservation.nereusDeploymentId().equals(deploymentId)
                            || !reservation.clusterAlias().equals(configuration.clusterAlias())
                            || !reservation.bookKeeperProviderScopeSha256().equals(
                                    configuration.providerScopeSha256())
                            || reservation.ledgerIdPrefixBits() != configuration.ledgerIdPrefixBits()
                            || reservation.ledgerIdPrefixValue() != configuration.ledgerIdPrefixValue()) {
                        throw unavailable("BookKeeper ledger-id namespace reservation drifted or was revoked");
                    }
                    return reservation;
                });
    }

    private static NereusException unavailable(String message) {
        return new NereusException(ErrorCode.UNSUPPORTED_STORAGE_PROFILE, false, message);
    }
}
