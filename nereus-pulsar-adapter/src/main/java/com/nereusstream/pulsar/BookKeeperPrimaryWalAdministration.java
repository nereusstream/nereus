/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import com.nereusstream.bookkeeper.BookKeeperLedgerIdNamespaceProvisioningCoordinator;
import com.nereusstream.bookkeeper.BookKeeperLedgerIdNamespaceReservation;
import com.nereusstream.bookkeeper.BookKeeperLedgerIdNamespaceReservationVerifier;
import com.nereusstream.bookkeeper.BookKeeperOperationDeadline;
import com.nereusstream.bookkeeper.BookKeeperProtocolActivation;
import com.nereusstream.bookkeeper.BookKeeperProtocolActivationCoordinator;
import com.nereusstream.bookkeeper.BookKeeperProtocolActivationStore;
import com.nereusstream.bookkeeper.BookKeeperProtocolActivationUpdate;
import com.nereusstream.bookkeeper.BookKeeperWalConfiguration;
import com.nereusstream.metadata.oxia.OxiaClientConfiguration;
import com.nereusstream.metadata.oxia.SharedOxiaClientRuntime;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Explicit operator surface; broker bootstrap never invokes its mutation methods. */
public final class BookKeeperPrimaryWalAdministration {
    private final NereusBookKeeperRuntimeConfiguration configuration;
    private final BookKeeperLedgerIdNamespaceReservationVerifier namespaceVerifier;
    private final BookKeeperLedgerIdNamespaceProvisioningCoordinator namespaceCoordinator;
    private final BookKeeperProtocolActivationStore activationStore;
    private final BookKeeperProtocolActivationCoordinator activationCoordinator;
    private final Optional<BookKeeperDeletionActivationCoordinator> deletionActivationCoordinator;

    public static BookKeeperPrimaryWalAdministration usingSharedRuntime(
            NereusBookKeeperRuntimeConfiguration configuration,
            OxiaClientConfiguration oxia,
            SharedOxiaClientRuntime runtime,
            Clock clock) {
        Objects.requireNonNull(configuration, "configuration");
        OxiaBookKeeperLedgerIdNamespaceReservationStore namespaces =
                new OxiaBookKeeperLedgerIdNamespaceReservationStore(
                        Objects.requireNonNull(oxia, "oxia"),
                        Objects.requireNonNull(runtime, "runtime"));
        OxiaBookKeeperProtocolActivationStore activations =
                new OxiaBookKeeperProtocolActivationStore(oxia, runtime);
        return new BookKeeperPrimaryWalAdministration(
                configuration,
                new BookKeeperLedgerIdNamespaceReservationVerifier(
                        namespaces, configuration.deploymentId()),
                new BookKeeperLedgerIdNamespaceProvisioningCoordinator(
                        namespaces, Objects.requireNonNull(clock, "clock")),
                activations,
                new BookKeeperProtocolActivationCoordinator(activations, clock),
                Optional.empty());
    }

    BookKeeperPrimaryWalAdministration(
            NereusBookKeeperRuntimeConfiguration configuration,
            BookKeeperLedgerIdNamespaceReservationVerifier namespaceVerifier,
            BookKeeperLedgerIdNamespaceProvisioningCoordinator namespaceCoordinator,
            BookKeeperProtocolActivationStore activationStore,
            BookKeeperProtocolActivationCoordinator activationCoordinator,
            Optional<BookKeeperDeletionActivationCoordinator> deletionActivationCoordinator) {
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.namespaceVerifier = Objects.requireNonNull(namespaceVerifier, "namespaceVerifier");
        this.namespaceCoordinator = Objects.requireNonNull(namespaceCoordinator, "namespaceCoordinator");
        this.activationStore = Objects.requireNonNull(activationStore, "activationStore");
        this.activationCoordinator = Objects.requireNonNull(activationCoordinator, "activationCoordinator");
        this.deletionActivationCoordinator = Objects.requireNonNull(
                deletionActivationCoordinator, "deletionActivationCoordinator");
    }

    public CompletableFuture<BookKeeperLedgerIdNamespaceReservation> provisionNamespace(
            String operatorEvidenceSha256,
            Duration timeout) {
        return namespaceCoordinator.provision(
                configuration.wal(),
                configuration.deploymentId(),
                operatorEvidenceSha256,
                timeout);
    }

    public CompletableFuture<BookKeeperLedgerIdNamespaceReservation> revokeNamespace(
            String revocationEvidenceSha256,
            long expectedMetadataVersion,
            Duration timeout) {
        return namespaceCoordinator.revoke(
                configuration.wal(),
                configuration.deploymentId(),
                revocationEvidenceSha256,
                expectedMetadataVersion,
                timeout);
    }

    public CompletableFuture<BookKeeperProtocolActivation> prepareActivation(
            long brokerReadinessEpoch,
            String brokerReadinessSha256,
            Duration timeout) {
        BookKeeperOperationDeadline deadline = new BookKeeperOperationDeadline(timeout);
        BookKeeperWalConfiguration wal = configuration.wal();
        return namespaceVerifier.requireActive(wal, deadline.remaining())
                .thenCompose(namespace -> activationCoordinator.prepare(
                        wal,
                        namespace,
                        brokerReadinessEpoch,
                        brokerReadinessSha256,
                        deadline.remaining()));
    }

    public CompletableFuture<BookKeeperProtocolActivation> activate(
            BookKeeperProtocolActivationUpdate update,
            Duration timeout) {
        if (Objects.requireNonNull(update, "update").ledgerDeletionEnabled()) {
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                    "ledger deletion must use activateDeletion so proof digests are producer-owned"));
        }
        BookKeeperOperationDeadline deadline = new BookKeeperOperationDeadline(timeout);
        BookKeeperWalConfiguration wal = configuration.wal();
        return namespaceVerifier.requireActive(wal, deadline.remaining())
                .thenCompose(namespace -> activationCoordinator.activate(
                        wal,
                        namespace,
                        update,
                        deadline.remaining()));
    }

    public CompletableFuture<Optional<BookKeeperProtocolActivation>> readActivation(
            Duration timeout) {
        BookKeeperOperationDeadline deadline = new BookKeeperOperationDeadline(timeout);
        BookKeeperWalConfiguration wal = configuration.wal();
        return namespaceVerifier.requireActive(wal, deadline.remaining())
                .thenCompose(namespace -> activationStore.read(
                        wal, namespace, deadline.remaining()));
    }

    public CompletableFuture<BookKeeperDeletionActivationResult> activateDeletion(
            BookKeeperDeletionActivationRequest request) {
        return deletionActivationCoordinator
                .map(coordinator -> coordinator.activate(request))
                .orElseGet(() -> CompletableFuture.failedFuture(
                        new IllegalStateException(
                                "BookKeeper deletion proof producers are unavailable on this administration surface")));
    }
}
