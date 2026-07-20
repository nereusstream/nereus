/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Production verifier for physical ledger deletion; reloads the exact durable record on every call. */
public final class DefaultBookKeeperProtocolActivationVerifier
        implements BookKeeperProtocolActivationVerifier {
    private final BookKeeperProtocolActivationStore store;
    private final BookKeeperWalConfiguration configuration;
    private final BookKeeperLedgerIdNamespaceReservation namespace;
    private final BookKeeperBrokerReadinessProvider brokerReadiness;

    public DefaultBookKeeperProtocolActivationVerifier(
            BookKeeperProtocolActivationStore store,
            BookKeeperWalConfiguration configuration,
            BookKeeperLedgerIdNamespaceReservation namespace,
            BookKeeperBrokerReadinessProvider brokerReadiness) {
        this.store = Objects.requireNonNull(store, "store");
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.namespace = Objects.requireNonNull(namespace, "namespace");
        this.brokerReadiness = Objects.requireNonNull(brokerReadiness, "brokerReadiness");
    }

    @Override
    public CompletableFuture<BookKeeperProtocolActivationProof> requireActive(
            Duration timeout) {
        return store.read(configuration, namespace, Objects.requireNonNull(timeout, "timeout"))
                .thenCompose(optional -> {
                    BookKeeperProtocolActivation activation = optional.orElseThrow(() ->
                            unavailable("BookKeeper deletion activation is absent"));
                    BookKeeperProtocolActivationCoordinator.requireExact(
                            activation.value(), configuration, namespace);
                    BookKeeperProtocolActivationProof proof = activation.deletionProof();
                    proof.requireExact(configuration, namespace);
                    return brokerReadiness.requireBookKeeperPrimaryWalReadiness()
                            .thenApply(current -> requireCurrentReadiness(proof, current));
                });
    }

    private BookKeeperProtocolActivationProof requireCurrentReadiness(
            BookKeeperProtocolActivationProof proof,
            BookKeeperBrokerReadiness current) {
        if (proof.brokerReadinessEpoch() != current.brokerReadinessEpoch()
                || !proof.brokerReadinessSha256().equals(current.brokerSetSha256())) {
            throw unavailable("BookKeeper deletion activation broker readiness is stale");
        }
        // One additional lease slot is reserved for rolling-restart ownership overlap.
        if ((long) current.persistentBrokerCount() + 1L
                > configuration.maxReaderLeasesPerLedger()) {
            throw unavailable("BookKeeper reader lease capacity cannot cover the broker set "
                    + "plus one rolling-restart overlap");
        }
        return proof;
    }

    private static NereusException unavailable(String message) {
        return new NereusException(ErrorCode.METADATA_CONDITION_FAILED, true, message);
    }
}
