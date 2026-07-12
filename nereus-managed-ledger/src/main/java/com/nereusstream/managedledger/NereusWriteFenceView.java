/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public interface NereusWriteFenceView {
    Optional<NereusWriteFenceSnapshot> currentWriteFence();

    CompletableFuture<NereusWriteFenceResolution> awaitWriteFence(long generation);
}
