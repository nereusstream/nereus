/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.generation;

import java.util.concurrent.CompletableFuture;

/** Product-owned boundary that may atomically enable both V1 physical-deletion capabilities. */
public interface ManagedLedgerPhysicalDeletionActivationCoordinator {
    CompletableFuture<ManagedLedgerPhysicalDeletionActivationResult> activate(
            ManagedLedgerPhysicalDeletionActivationRequest request);
}
