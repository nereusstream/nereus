/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.generation;

import java.util.concurrent.CompletableFuture;

/**
 * Product-owned cluster publication activation boundary. It never enables
 * physical deletion.
 */
public interface ManagedLedgerGenerationProtocolActivationCoordinator {
    CompletableFuture<Void> activatePublication();
}
