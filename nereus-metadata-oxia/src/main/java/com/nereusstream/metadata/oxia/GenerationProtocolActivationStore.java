/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.metadata.oxia.records.GenerationProtocolActivationRecord;
import com.nereusstream.metadata.oxia.records.ReferenceDomainVersionRecord;
import java.time.Clock;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Focused single-key store for the durable cluster generation activation authority. */
public interface GenerationProtocolActivationStore extends AutoCloseable {
    static GenerationProtocolActivationStore usingSharedRuntime(
            OxiaClientConfiguration clientConfig,
            SharedOxiaClientRuntime runtime,
            Clock clock,
            String activatingBrokerRunId,
            List<ReferenceDomainVersionRecord> requiredReferenceDomains) {
        return OxiaJavaGenerationProtocolActivationStore.usingSharedRuntime(
                clientConfig,
                runtime,
                clock,
                activatingBrokerRunId,
                requiredReferenceDomains);
    }

    CompletableFuture<Optional<VersionedGenerationProtocolActivation>> get(
            String cluster);

    CompletableFuture<VersionedGenerationProtocolActivation> getOrCreate(
            String cluster);

    CompletableFuture<VersionedGenerationProtocolActivation> compareAndSet(
            String cluster,
            GenerationProtocolActivationRecord replacement,
            long expectedVersion);

    @Override
    void close();
}
