/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/** Mutation surface reserved for explicit cluster provisioning; broker runtime receives only the read view. */
public interface BookKeeperLedgerIdNamespaceReservationAdminStore
        extends BookKeeperLedgerIdNamespaceReservationStore {
    CompletableFuture<BookKeeperLedgerIdNamespaceReservation> create(
            BookKeeperLedgerIdNamespaceReservationValue value, Duration timeout);

    CompletableFuture<BookKeeperLedgerIdNamespaceReservation> compareAndSet(
            BookKeeperLedgerIdNamespaceReservationValue replacement,
            long expectedMetadataVersion,
            Duration timeout);
}
