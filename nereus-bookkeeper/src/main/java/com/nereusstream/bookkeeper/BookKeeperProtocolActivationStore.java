/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Exact-key/CAS store for one immutable BookKeeper configuration+namespace activation identity. */
public interface BookKeeperProtocolActivationStore {
    CompletableFuture<Optional<BookKeeperProtocolActivation>> read(
            BookKeeperWalConfiguration configuration,
            BookKeeperLedgerIdNamespaceReservation namespace,
            Duration timeout);

    CompletableFuture<BookKeeperProtocolActivation> create(
            BookKeeperProtocolActivationValue value, Duration timeout);

    CompletableFuture<BookKeeperProtocolActivation> compareAndSet(
            BookKeeperProtocolActivationValue replacement,
            long expectedMetadataVersion,
            Duration timeout);
}
