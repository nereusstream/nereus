/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import java.time.Clock;
import java.util.Objects;

/** Public test-fixture entry point for the production F4 codec/CAS adapter over an in-memory backend. */
public final class GenerationMetadataStoreTestFactory {
    private GenerationMetadataStoreTestFactory() {
    }

    public static GenerationMetadataStore inMemory(Clock clock) {
        return new OxiaJavaGenerationMetadataStore(
                new PartitionedOxiaClient(new InMemoryPartitionedOxiaBackend()),
                Objects.requireNonNull(clock, "clock"));
    }
}
