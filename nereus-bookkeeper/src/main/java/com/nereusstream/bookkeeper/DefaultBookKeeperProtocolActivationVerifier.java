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

    public DefaultBookKeeperProtocolActivationVerifier(
            BookKeeperProtocolActivationStore store,
            BookKeeperWalConfiguration configuration,
            BookKeeperLedgerIdNamespaceReservation namespace) {
        this.store = Objects.requireNonNull(store, "store");
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.namespace = Objects.requireNonNull(namespace, "namespace");
    }

    @Override
    public CompletableFuture<BookKeeperProtocolActivationProof> requireActive(
            Duration timeout) {
        return store.read(configuration, namespace, Objects.requireNonNull(timeout, "timeout"))
                .thenApply(optional -> {
                    BookKeeperProtocolActivation activation = optional.orElseThrow(() ->
                            unavailable("BookKeeper deletion activation is absent"));
                    BookKeeperProtocolActivationCoordinator.requireExact(
                            activation.value(), configuration, namespace);
                    BookKeeperProtocolActivationProof proof = activation.deletionProof();
                    proof.requireExact(configuration, namespace);
                    return proof;
                });
    }

    private static NereusException unavailable(String message) {
        return new NereusException(ErrorCode.METADATA_CONDITION_FAILED, true, message);
    }
}
