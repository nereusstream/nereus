/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.generation;

import com.nereusstream.core.capability.GenerationRegistrationBackfillCompletion;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/** Product-owned durable completion boundary for the broker cold-topic traversal. */
public interface ManagedLedgerGenerationRegistrationBackfillProofCoordinator {
    CompletableFuture<Void> complete(
            GenerationRegistrationBackfillCompletion completion);

    /**
     * Preserves one caller-owned deadline for a possible deletion-active
     * readiness rollover. Pre-rollover implementations keep the old behavior.
     */
    default CompletableFuture<Void> complete(
            GenerationRegistrationBackfillCompletion completion,
            int maxConcurrentStreams,
            Duration timeout) {
        return complete(completion);
    }
}
