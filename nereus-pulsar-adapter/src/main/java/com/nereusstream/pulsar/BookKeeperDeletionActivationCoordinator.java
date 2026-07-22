/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.bookkeeper.BookKeeperBrokerReadiness;
import com.nereusstream.bookkeeper.BookKeeperBrokerReadinessProvider;
import com.nereusstream.bookkeeper.BookKeeperLedgerGcConfiguration;
import com.nereusstream.bookkeeper.BookKeeperLedgerIdNamespaceReservation;
import com.nereusstream.bookkeeper.BookKeeperLedgerIdNamespaceReservationVerifier;
import com.nereusstream.bookkeeper.BookKeeperOperationDeadline;
import com.nereusstream.bookkeeper.BookKeeperProtocolActivation;
import com.nereusstream.bookkeeper.BookKeeperProtocolActivationCoordinator;
import com.nereusstream.bookkeeper.BookKeeperProtocolActivationStore;
import com.nereusstream.bookkeeper.BookKeeperProtocolActivationUpdate;
import com.nereusstream.bookkeeper.BookKeeperRootCoverageProof;
import com.nereusstream.bookkeeper.BookKeeperRootCoverageProofProducer;
import com.nereusstream.bookkeeper.BookKeeperScopeCapabilityProbe;
import com.nereusstream.bookkeeper.BookKeeperScopeCapabilityProof;
import com.nereusstream.bookkeeper.BookKeeperScopeCapabilityRequest;
import com.nereusstream.bookkeeper.BookKeeperWalConfiguration;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Produces every deletion proof under one stable broker readiness and installs all three plus the deletion bit in one
 * activation CAS. No public caller can inject a proof digest.
 */
public final class BookKeeperDeletionActivationCoordinator {
    private final BookKeeperWalConfiguration configuration;
    private final BookKeeperLedgerGcConfiguration gcConfiguration;
    private final BookKeeperLedgerIdNamespaceReservation namespace;
    private final BookKeeperLedgerIdNamespaceReservationVerifier namespaceVerifier;
    private final BookKeeperBrokerReadinessProvider readinessProvider;
    private final BookKeeperProtocolActivationStore activationStore;
    private final BookKeeperProtocolActivationCoordinator activationCoordinator;
    private final BiFunction<
                    BookKeeperBrokerReadiness,
                    Duration,
                    CompletableFuture<BookKeeperRootCoverageProof>>
            rootCoverage;
    private final BiFunction<
                    BookKeeperBrokerReadiness,
                    Duration,
                    CompletableFuture<BookKeeperStreamCoverageProof>>
            streamCoverage;
    private final Function<
                    BookKeeperScopeCapabilityRequest,
                    CompletableFuture<BookKeeperScopeCapabilityProof>>
            scopeProbe;

    public BookKeeperDeletionActivationCoordinator(
            BookKeeperWalConfiguration configuration,
            BookKeeperLedgerGcConfiguration gcConfiguration,
            BookKeeperLedgerIdNamespaceReservation namespace,
            BookKeeperLedgerIdNamespaceReservationVerifier namespaceVerifier,
            BookKeeperBrokerReadinessProvider readinessProvider,
            BookKeeperProtocolActivationStore activationStore,
            BookKeeperProtocolActivationCoordinator activationCoordinator,
            BookKeeperRootCoverageProofProducer rootCoverage,
            BookKeeperStreamCoverageProofProducer streamCoverage,
            BookKeeperScopeCapabilityProbe scopeProbe) {
        this(
                configuration,
                gcConfiguration,
                namespace,
                namespaceVerifier,
                readinessProvider,
                activationStore,
                activationCoordinator,
                Objects.requireNonNull(rootCoverage, "rootCoverage")::produce,
                Objects.requireNonNull(streamCoverage, "streamCoverage")::produce,
                Objects.requireNonNull(scopeProbe, "scopeProbe")::probe);
    }

    BookKeeperDeletionActivationCoordinator(
            BookKeeperWalConfiguration configuration,
            BookKeeperLedgerGcConfiguration gcConfiguration,
            BookKeeperLedgerIdNamespaceReservation namespace,
            BookKeeperLedgerIdNamespaceReservationVerifier namespaceVerifier,
            BookKeeperBrokerReadinessProvider readinessProvider,
            BookKeeperProtocolActivationStore activationStore,
            BookKeeperProtocolActivationCoordinator activationCoordinator,
            BiFunction<
                            BookKeeperBrokerReadiness,
                            Duration,
                            CompletableFuture<BookKeeperRootCoverageProof>>
                    rootCoverage,
            BiFunction<
                            BookKeeperBrokerReadiness,
                            Duration,
                            CompletableFuture<BookKeeperStreamCoverageProof>>
                    streamCoverage,
            Function<
                            BookKeeperScopeCapabilityRequest,
                            CompletableFuture<BookKeeperScopeCapabilityProof>>
                    scopeProbe) {
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.gcConfiguration = Objects.requireNonNull(gcConfiguration, "gcConfiguration");
        this.gcConfiguration.validateAgainst(configuration);
        this.namespace = Objects.requireNonNull(namespace, "namespace");
        this.namespaceVerifier = Objects.requireNonNull(namespaceVerifier, "namespaceVerifier");
        this.readinessProvider = Objects.requireNonNull(readinessProvider, "readinessProvider");
        this.activationStore = Objects.requireNonNull(activationStore, "activationStore");
        this.activationCoordinator = Objects.requireNonNull(
                activationCoordinator, "activationCoordinator");
        this.rootCoverage = Objects.requireNonNull(rootCoverage, "rootCoverage");
        this.streamCoverage = Objects.requireNonNull(streamCoverage, "streamCoverage");
        this.scopeProbe = Objects.requireNonNull(scopeProbe, "scopeProbe");
    }

    public CompletableFuture<BookKeeperDeletionActivationResult> activate(
            BookKeeperDeletionActivationRequest request) {
        final BookKeeperDeletionActivationRequest exact;
        final BookKeeperOperationDeadline deadline;
        try {
            exact = Objects.requireNonNull(request, "request");
            if (!gcConfiguration.enabled() || gcConfiguration.dryRun()) {
                throw notReady(
                        "BookKeeper deletion activation requires enabled non-dry-run ledger GC configuration");
            }
            deadline = new BookKeeperOperationDeadline(exact.timeout());
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return activationStore.read(configuration, namespace, deadline.remaining())
                .thenCompose(optional -> {
                    BookKeeperProtocolActivation current = optional.orElseThrow(() -> notReady(
                            "BookKeeper publication activation is absent"));
                    requirePublication(current);
                    return readinessProvider.requireBookKeeperPrimaryWalReadiness()
                            .thenCompose(readiness -> {
                                requireCapacity(readiness);
                                if (current.value().ledgerDeletionEnabled()
                                        && matchesReadiness(current, readiness)) {
                                    return currentResult(current, readiness, deadline);
                                }
                                if (current.metadataVersion()
                                        != exact.expectedActivationMetadataVersion()) {
                                    return CompletableFuture.failedFuture(notReady(
                                            "BookKeeper activation metadata version changed before deletion proof production"));
                                }
                                return produceAndInstall(
                                        exact, current, readiness, deadline);
                            });
                });
    }

    private CompletableFuture<BookKeeperDeletionActivationResult> produceAndInstall(
            BookKeeperDeletionActivationRequest request,
            BookKeeperProtocolActivation current,
            BookKeeperBrokerReadiness readiness,
            BookKeeperOperationDeadline deadline) {
        requireCapacity(readiness);
        return scopeProbe
                .apply(new BookKeeperScopeCapabilityRequest(
                        request.runId(), readiness, deadline.remaining()))
                .thenCompose(scope -> requireCurrent(readiness, deadline)
                        .thenApply(ignored -> requireScopeBinding(readiness, scope)))
                .thenCompose(scope -> rootCoverage
                        .apply(readiness, deadline.remaining())
                        .thenCompose(root -> requireCurrent(readiness, deadline)
                                .thenApply(ignored -> new ScopedCoverage(scope, requireRootBinding(
                                        readiness, root)))))
                .thenCompose(coverage -> streamCoverage
                        .apply(readiness, deadline.remaining())
                        .thenCompose(stream -> requireCurrent(readiness, deadline)
                                .thenApply(ignored -> new Proofs(
                                        coverage.root(),
                                        requireStreamBinding(readiness, stream),
                                        coverage.scope()))))
                .thenCompose(proofs -> namespaceVerifier
                        .requireActive(configuration, deadline.remaining())
                        .thenApply(this::requireNamespace)
                        .thenCompose(ignored -> install(
                                current, readiness, proofs, deadline)));
    }

    private CompletableFuture<BookKeeperDeletionActivationResult> install(
            BookKeeperProtocolActivation current,
            BookKeeperBrokerReadiness readiness,
            Proofs proofs,
            BookKeeperOperationDeadline deadline) {
        BookKeeperProtocolActivationUpdate update = new BookKeeperProtocolActivationUpdate(
                readiness.brokerReadinessEpoch(),
                readiness.brokerSetSha256().value(),
                true,
                true,
                true,
                proofs.root().coverageSha256().value(),
                proofs.stream().coverageSha256().value(),
                proofs.scope().capabilitySha256().value(),
                current.metadataVersion());
        return activationCoordinator
                .activate(configuration, namespace, update, deadline.remaining())
                .handle((installed, failure) -> {
                    if (failure == null) {
                        return CompletableFuture.completedFuture(installed);
                    }
                    return activationStore
                            .read(configuration, namespace, deadline.remaining())
                            .thenCompose(optional -> {
                                if (optional.isEmpty()) {
                                    return CompletableFuture.failedFuture(unwrap(failure));
                                }
                                BookKeeperProtocolActivation reloaded = optional.orElseThrow();
                                if (sameProofs(reloaded, readiness, proofs)) {
                                    return CompletableFuture.completedFuture(reloaded);
                                }
                                return CompletableFuture.failedFuture(unwrap(failure));
                            });
                })
                .thenCompose(Function.identity())
                .thenCompose(installed -> requireCurrent(readiness, deadline)
                        .thenCompose(ignored -> activationStore
                                .read(configuration, namespace, deadline.remaining()))
                        .thenApply(reloaded -> {
                            BookKeeperProtocolActivation exact = reloaded.orElseThrow(() -> notReady(
                                    "BookKeeper deletion activation disappeared after CAS"));
                            if (!sameProofs(exact, readiness, proofs)
                                    || !exact.equals(installed)) {
                                throw notReady(
                                        "BookKeeper deletion activation changed during final proof revalidation");
                            }
                            return result(exact, true);
                        }));
    }

    private CompletableFuture<BookKeeperDeletionActivationResult> currentResult(
            BookKeeperProtocolActivation current,
            BookKeeperBrokerReadiness readiness,
            BookKeeperOperationDeadline deadline) {
        requireReadiness(current, readiness);
        return requireCurrent(readiness, deadline)
                .thenCompose(ignored -> namespaceVerifier
                        .requireActive(configuration, deadline.remaining()))
                .thenApply(this::requireNamespace)
                .thenApply(ignored -> result(current, false))
                .thenCompose(result -> activationStore
                        .read(configuration, namespace, deadline.remaining())
                        .thenApply(reloaded -> {
                            BookKeeperProtocolActivation exact = reloaded.orElseThrow(() -> notReady(
                                    "BookKeeper deletion activation disappeared during idempotent read"));
                            if (!exact.equals(result.activation())) {
                                throw notReady(
                                        "BookKeeper deletion activation changed during idempotent read");
                            }
                            return result;
                        }));
    }

    private CompletableFuture<Void> requireCurrent(
            BookKeeperBrokerReadiness expected,
            BookKeeperOperationDeadline deadline) {
        return deadline.bound(readinessProvider.requireBookKeeperPrimaryWalReadiness())
                .thenAccept(actual -> {
                    if (!actual.equals(expected)) {
                        throw notReady(
                                "BookKeeper broker readiness changed during deletion proof production");
                    }
                    requireCapacity(actual);
                });
    }

    private void requirePublication(BookKeeperProtocolActivation activation) {
        if (!activation.supportsAllPublications()) {
            throw notReady(
                    "BookKeeper deletion requires WAL_ONLY, async and sync publication activation");
        }
        if (!activation.value().configurationBindingSha256()
                        .equals(configuration.configurationBindingSha256().value())
                || !activation.value().ledgerIdNamespaceSha256()
                        .equals(namespace.ledgerIdNamespaceSha256().value())) {
            throw notReady(
                    "BookKeeper publication activation does not match the exact deletion binding");
        }
    }

    private BookKeeperLedgerIdNamespaceReservation requireNamespace(
            BookKeeperLedgerIdNamespaceReservation actual) {
        BookKeeperLedgerIdNamespaceReservation exact = Objects.requireNonNull(actual, "namespace");
        if (!exact.equals(namespace)) {
            throw notReady("BookKeeper namespace changed during deletion proof production");
        }
        return exact;
    }

    private void requireCapacity(BookKeeperBrokerReadiness readiness) {
        if ((long) readiness.persistentBrokerCount() + 1L
                > configuration.maxReaderLeasesPerLedger()) {
            throw notReady(
                    "BookKeeper reader-lease slots cannot cover the broker set plus restart overlap");
        }
    }

    private static void requireReadiness(
            BookKeeperProtocolActivation activation,
            BookKeeperBrokerReadiness readiness) {
        if (!matchesReadiness(activation, readiness)) {
            throw notReady(
                    "BookKeeper deletion activation does not match live broker readiness");
        }
    }

    private static boolean matchesReadiness(
            BookKeeperProtocolActivation activation,
            BookKeeperBrokerReadiness readiness) {
        return activation.value().brokerReadinessEpoch()
                        == readiness.brokerReadinessEpoch()
                && activation.value().brokerReadinessSha256()
                        .equals(readiness.brokerSetSha256().value());
    }

    private static BookKeeperScopeCapabilityProof requireScopeBinding(
            BookKeeperBrokerReadiness readiness, BookKeeperScopeCapabilityProof proof) {
        requireProofBinding(
                readiness,
                proof.brokerReadinessEpoch(),
                proof.brokerSetSha256());
        return proof;
    }

    private static BookKeeperRootCoverageProof requireRootBinding(
            BookKeeperBrokerReadiness readiness, BookKeeperRootCoverageProof proof) {
        requireProofBinding(
                readiness,
                proof.brokerReadinessEpoch(),
                proof.brokerSetSha256());
        return proof;
    }

    private static BookKeeperStreamCoverageProof requireStreamBinding(
            BookKeeperBrokerReadiness readiness, BookKeeperStreamCoverageProof proof) {
        requireProofBinding(
                readiness,
                proof.brokerReadinessEpoch(),
                proof.brokerSetSha256());
        return proof;
    }

    private static void requireProofBinding(
            BookKeeperBrokerReadiness readiness,
            long proofReadinessEpoch,
            Checksum proofBrokerSetSha256) {
        if (proofReadinessEpoch != readiness.brokerReadinessEpoch()
                || !proofBrokerSetSha256.equals(readiness.brokerSetSha256())) {
            throw notReady("BookKeeper deletion proof does not match broker readiness");
        }
    }

    private static boolean sameProofs(
            BookKeeperProtocolActivation activation,
            BookKeeperBrokerReadiness readiness,
            Proofs proofs) {
        return activation.value().ledgerDeletionEnabled()
                && activation.value().brokerReadinessEpoch()
                        == readiness.brokerReadinessEpoch()
                && activation.value().brokerReadinessSha256()
                        .equals(readiness.brokerSetSha256().value())
                && activation.value().rootCoverageProofSha256()
                        .equals(proofs.root().coverageSha256().value())
                && activation.value().streamCoverageProofSha256()
                        .equals(proofs.stream().coverageSha256().value())
                && activation.value().bookKeeperScopeProofSha256()
                        .equals(proofs.scope().capabilitySha256().value());
    }

    private static BookKeeperDeletionActivationResult result(
            BookKeeperProtocolActivation activation, boolean newlyActivated) {
        return new BookKeeperDeletionActivationResult(
                activation,
                checksum(activation.value().rootCoverageProofSha256()),
                checksum(activation.value().streamCoverageProofSha256()),
                checksum(activation.value().bookKeeperScopeProofSha256()),
                newlyActivated);
    }

    private static Checksum checksum(String value) {
        return new Checksum(ChecksumType.SHA256, value);
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static NereusException notReady(String message) {
        return new NereusException(
                ErrorCode.METADATA_CONDITION_FAILED, true, message);
    }

    private record ScopedCoverage(
            BookKeeperScopeCapabilityProof scope,
            BookKeeperRootCoverageProof root) {
    }

    private record Proofs(
            BookKeeperRootCoverageProof root,
            BookKeeperStreamCoverageProof stream,
            BookKeeperScopeCapabilityProof scope) {
    }
}
