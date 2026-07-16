/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.generation;

import com.nereusstream.metadata.oxia.records.ManagedLedgerProjectionIdentity;
import java.util.concurrent.CompletableFuture;

/**
 * Broker-safe boundary for installing the durable F4 work-discovery hint for
 * one exact managed-ledger incarnation.
 */
@FunctionalInterface
public interface ManagedLedgerMaterializationRegistrationCoordinator {
    CompletableFuture<Void> ensureRegistered(
            String managedLedgerName,
            ManagedLedgerProjectionIdentity expectedProjectionIdentity);
}
