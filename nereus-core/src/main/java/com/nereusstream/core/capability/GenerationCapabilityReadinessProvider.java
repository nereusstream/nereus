/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.capability;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Product-neutral view of the broker's two-stable-snapshot generation
 * capability barrier.
 */
public interface GenerationCapabilityReadinessProvider {
    CompletableFuture<GenerationCapabilityReadiness>
            requireGenerationCapabilityReadiness();

    Optional<GenerationCapabilityReadiness>
            currentGenerationCapabilityReadiness();

    static GenerationCapabilityReadinessProvider unavailable() {
        return new GenerationCapabilityReadinessProvider() {
            @Override
            public CompletableFuture<GenerationCapabilityReadiness>
                    requireGenerationCapabilityReadiness() {
                return CompletableFuture.failedFuture(
                        new IllegalStateException(
                                "NEREUS_GENERATION_CAPABILITY_NOT_READY"));
            }

            @Override
            public Optional<GenerationCapabilityReadiness>
                    currentGenerationCapabilityReadiness() {
                return Optional.empty();
            }
        };
    }
}
