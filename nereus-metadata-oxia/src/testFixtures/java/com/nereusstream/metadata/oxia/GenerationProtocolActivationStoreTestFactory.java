/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.metadata.oxia.records.ReferenceDomainVersionRecord;
import java.time.Clock;
import java.util.List;
import java.util.Objects;

/** Public test-fixture entry point for the production activation adapter over an in-memory backend. */
public final class GenerationProtocolActivationStoreTestFactory {
    private GenerationProtocolActivationStoreTestFactory() {
    }

    public static GenerationProtocolActivationStore inMemory(
            Clock clock,
            String activatingBrokerRunId,
            List<ReferenceDomainVersionRecord> requiredReferenceDomains) {
        return new OxiaJavaGenerationProtocolActivationStore(
                new PartitionedOxiaClient(new InMemoryPartitionedOxiaBackend()),
                Objects.requireNonNull(clock, "clock"),
                activatingBrokerRunId,
                requiredReferenceDomains);
    }
}
