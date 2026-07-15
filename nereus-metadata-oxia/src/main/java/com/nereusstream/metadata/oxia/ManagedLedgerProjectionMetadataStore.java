/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.records.ManagedLedgerProjectionIdentity;
import com.nereusstream.metadata.oxia.records.TopicProjectionRecord;
import java.time.Clock;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Protocol-neutral metadata contract for authoritative and derived F2 projections. */
public interface ManagedLedgerProjectionMetadataStore extends AutoCloseable {
    static ManagedLedgerProjectionMetadataStore usingSharedRuntime(
            OxiaClientConfiguration clientConfig,
            SharedOxiaClientRuntime runtime,
            ProjectionMetadataStoreConfig storeConfig,
            Clock clock) {
        return OxiaJavaManagedLedgerProjectionMetadataStore.usingSharedRuntime(
                clientConfig, runtime, storeConfig, clock);
    }

    CompletableFuture<Optional<TopicProjectionRecord>> getProjection(
            String cluster,
            String managedLedgerName);

    CompletableFuture<ManagedLedgerStreamProjection> getProjectionByStream(
            String cluster,
            StreamId streamId);

    CompletableFuture<TopicProjectionRecord> createFirstProjection(
            String cluster,
            ProjectionCreateRequest request,
            ProjectionPublishGuard publishGuard);

    CompletableFuture<TopicProjectionRecord> recreateDeletedProjection(
            String cluster,
            ManagedLedgerProjectionIdentity expectedDeletedIdentity,
            long expectedTopicMetadataVersion,
            ProjectionCreateRequest request,
            ProjectionPublishGuard publishGuard);

    CompletableFuture<TopicProjectionRecord> updateProperties(
            String cluster,
            String managedLedgerName,
            ManagedLedgerProjectionIdentity expectedIdentity,
            long expectedVersion,
            Map<String, String> properties);

    CompletableFuture<TopicProjectionRecord> activateCursorProtocol(
            String cluster,
            String managedLedgerName,
            ManagedLedgerProjectionIdentity expectedIdentity,
            long expectedMetadataVersion);

    CompletableFuture<TopicProjectionRecord> activateGenerationProtocol(
            String cluster,
            String managedLedgerName,
            ManagedLedgerProjectionIdentity expectedIdentity,
            long expectedMetadataVersion);

    CompletableFuture<TopicProjectionRecord> mirrorFacadeState(
            String cluster,
            String managedLedgerName,
            ManagedLedgerProjectionIdentity expectedIdentity,
            long expectedVersion,
            ManagedLedgerFacadeState state);

    CompletableFuture<ProjectionRepairResult> repairProjectionIndexes(
            String cluster,
            TopicProjectionRecord authoritative);

    @Override
    void close();
}
