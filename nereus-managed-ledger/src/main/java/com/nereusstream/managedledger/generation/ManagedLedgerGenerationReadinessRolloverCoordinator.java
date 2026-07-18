/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.generation;

import com.nereusstream.core.capability.GenerationRegistrationBackfillCompletion;
import com.nereusstream.metadata.oxia.VersionedGenerationProtocolActivation;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Atomically refreshes every deletion-authority proof when broker readiness
 * advances after physical deletion has already been enabled.
 */
@FunctionalInterface
public interface ManagedLedgerGenerationReadinessRolloverCoordinator {
    CompletableFuture<VersionedGenerationProtocolActivation> rollover(
            GenerationRegistrationBackfillCompletion registration,
            int maxConcurrentStreams,
            Duration timeout,
            VersionedGenerationProtocolActivation current);
}
