/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.generation;

import com.nereusstream.core.capability.GenerationRegistrationBackfillCompletion;
import java.util.concurrent.CompletableFuture;

/** Product-owned durable completion boundary for the broker cold-topic traversal. */
public interface ManagedLedgerGenerationRegistrationBackfillProofCoordinator {
    CompletableFuture<Void> complete(
            GenerationRegistrationBackfillCompletion completion);
}
