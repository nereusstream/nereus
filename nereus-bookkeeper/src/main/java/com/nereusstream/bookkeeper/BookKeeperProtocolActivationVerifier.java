/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/** Reloads the one durable cluster activation record; local booleans cannot implement this interface safely. */
@FunctionalInterface
public interface BookKeeperProtocolActivationVerifier {
    CompletableFuture<BookKeeperProtocolActivationProof> requireActive(Duration timeout);
}
