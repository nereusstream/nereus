/* Licensed under the Apache License, Version 2.0 */

package com.nereusstream.bookkeeper;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Produces a provider-adapter stream-authority coverage proof under one readiness snapshot.
 */
@FunctionalInterface
public interface BookKeeperStreamCoverageProofProvider {
    CompletableFuture<BookKeeperStreamCoverageProof> produce(BookKeeperBrokerReadiness readiness, Duration timeout);
}
