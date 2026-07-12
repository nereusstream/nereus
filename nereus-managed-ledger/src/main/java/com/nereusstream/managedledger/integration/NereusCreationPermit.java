/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.integration;

import java.util.concurrent.CompletableFuture;

public interface NereusCreationPermit {
    String persistenceName();

    long bindingGeneration();

    CompletableFuture<Void> validateBeforeProjectionPublish();
}
