/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.integration;

import java.util.concurrent.CompletableFuture;

@FunctionalInterface
public interface NereusCreationGuard {
    CompletableFuture<NereusCreationPermit> acquire(String persistenceName);
}
