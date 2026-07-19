/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Read-only broker surface over the separately provisioned namespace authority. */
public interface BookKeeperLedgerIdNamespaceReservationStore {
    CompletableFuture<Optional<BookKeeperLedgerIdNamespaceReservation>> read(
            String providerScopeSha256, int prefixBits, long prefixValue, Duration timeout);
}
