/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Explicit PREPARED -> ACTIVE monotonic activation state machine. */
public final class BookKeeperProtocolActivationCoordinator {
    private final BookKeeperProtocolActivationStore store;
    private final Clock clock;

    public BookKeeperProtocolActivationCoordinator(
            BookKeeperProtocolActivationStore store, Clock clock) {
        this.store = Objects.requireNonNull(store, "store");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    public CompletableFuture<BookKeeperProtocolActivation> prepare(
            BookKeeperWalConfiguration configuration,
            BookKeeperLedgerIdNamespaceReservation namespace,
            long brokerReadinessEpoch,
            String brokerReadinessSha256,
            Duration timeout) {
        BookKeeperWalConfiguration exact = Objects.requireNonNull(configuration, "configuration");
        BookKeeperLedgerIdNamespaceReservation reservation = requireNamespace(namespace, exact);
        BookKeeperOperationDeadline deadline = new BookKeeperOperationDeadline(timeout);
        BookKeeperProtocolActivationValue desired =
                BookKeeperProtocolActivationValue.prepared(
                        exact,
                        reservation,
                        brokerReadinessEpoch,
                        brokerReadinessSha256);
        return store.read(exact, reservation, deadline.remaining()).thenCompose(existing -> {
            if (existing.isEmpty()) {
                return store.create(desired, deadline.remaining());
            }
            BookKeeperProtocolActivation current = existing.orElseThrow();
            requireExact(current.value(), exact, reservation);
            if (current.value().equals(desired)
                    || current.value().lifecycle()
                            == BookKeeperProtocolActivationLifecycle.ACTIVE) {
                return CompletableFuture.completedFuture(current);
            }
            return CompletableFuture.failedFuture(condition(
                    "BookKeeper activation is already prepared with different readiness"));
        });
    }

    public CompletableFuture<BookKeeperProtocolActivation> activate(
            BookKeeperWalConfiguration configuration,
            BookKeeperLedgerIdNamespaceReservation namespace,
            BookKeeperProtocolActivationUpdate update,
            Duration timeout) {
        BookKeeperWalConfiguration exact = Objects.requireNonNull(configuration, "configuration");
        BookKeeperLedgerIdNamespaceReservation reservation = requireNamespace(namespace, exact);
        BookKeeperProtocolActivationUpdate requested = Objects.requireNonNull(update, "update");
        BookKeeperOperationDeadline deadline = new BookKeeperOperationDeadline(timeout);
        return store.read(exact, reservation, deadline.remaining()).thenCompose(existing -> {
            BookKeeperProtocolActivation current = existing.orElseThrow(() ->
                    condition("BookKeeper activation must be prepared first"));
            requireExact(current.value(), exact, reservation);
            if (current.metadataVersion() != requested.expectedMetadataVersion()) {
                return CompletableFuture.failedFuture(condition(
                        "BookKeeper activation metadata version mismatch"));
            }
            long activatedAt = Math.max(
                    Math.max(clock.millis(), 1),
                    current.value().activatedAtMillis());
            BookKeeperProtocolActivationValue replacement =
                    new BookKeeperProtocolActivationValue(
                            1,
                            BookKeeperProtocolActivationLifecycle.ACTIVE,
                            1,
                            exact.clusterAlias(),
                            exact.providerScopeSha256(),
                            requested.brokerReadinessEpoch(),
                            requested.brokerReadinessSha256(),
                            exact.configurationBindingSha256().value(),
                            reservation.ledgerIdNamespaceSha256().value(),
                            true,
                            requested.asyncPublicationEnabled(),
                            requested.syncPublicationEnabled(),
                            requested.ledgerDeletionEnabled(),
                            requested.rootCoverageProofSha256(),
                            requested.streamCoverageProofSha256(),
                            requested.bookKeeperScopeProofSha256(),
                            activatedAt);
            BookKeeperProtocolActivationValue.requireValidReplacement(
                    current.value(), replacement);
            return store.compareAndSet(
                    replacement,
                    requested.expectedMetadataVersion(),
                    deadline.remaining());
        });
    }

    private static BookKeeperLedgerIdNamespaceReservation requireNamespace(
            BookKeeperLedgerIdNamespaceReservation namespace,
            BookKeeperWalConfiguration configuration) {
        BookKeeperLedgerIdNamespaceReservation exact = Objects.requireNonNull(
                namespace, "namespace");
        if (exact.lifecycle() != BookKeeperLedgerIdNamespaceReservation.Lifecycle.ACTIVE
                || !exact.reservationId().equals(
                        configuration.ledgerIdNamespaceReservationId())
                || !exact.clusterAlias().equals(configuration.clusterAlias())
                || !exact.bookKeeperProviderScopeSha256().equals(
                        configuration.providerScopeSha256())
                || exact.ledgerIdPrefixBits() != configuration.ledgerIdPrefixBits()
                || exact.ledgerIdPrefixValue() != configuration.ledgerIdPrefixValue()) {
            throw condition("BookKeeper namespace is not active for the exact configuration");
        }
        return exact;
    }

    static void requireExact(
            BookKeeperProtocolActivationValue value,
            BookKeeperWalConfiguration configuration,
            BookKeeperLedgerIdNamespaceReservation namespace) {
        if (value.protocolVersion() != 1
                || !value.clusterAlias().equals(configuration.clusterAlias())
                || !value.providerScopeSha256().equals(configuration.providerScopeSha256())
                || !value.configurationBindingSha256().equals(
                        configuration.configurationBindingSha256().value())
                || !value.ledgerIdNamespaceSha256().equals(
                        namespace.ledgerIdNamespaceSha256().value())) {
            throw condition("BookKeeper activation does not match the exact configuration/namespace");
        }
    }

    private static NereusException condition(String message) {
        return new NereusException(ErrorCode.METADATA_CONDITION_FAILED, true, message);
    }
}
