/* Licensed under the Apache License, Version 2.0 */

package com.nereusstream.bookkeeper;

import java.util.concurrent.CompletableFuture;

/**
 * Probes the exact BookKeeper provider scope before enabling physical ledger deletion.
 */
@FunctionalInterface
public interface BookKeeperScopeCapabilityProofProvider {
    CompletableFuture<BookKeeperScopeCapabilityProof> probe(BookKeeperScopeCapabilityRequest request);
}
