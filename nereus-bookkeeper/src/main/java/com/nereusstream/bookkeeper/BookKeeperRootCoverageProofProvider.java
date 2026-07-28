/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/** Produces a deterministic whole-ledger-root coverage proof under one readiness snapshot. */
@FunctionalInterface
public interface BookKeeperRootCoverageProofProvider {
    CompletableFuture<BookKeeperRootCoverageProof> produce(
            BookKeeperBrokerReadiness readiness,
            Duration timeout);
}
