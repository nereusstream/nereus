/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.metadata.oxia.records.ManagedLedgerProjectionIdentity;
import com.nereusstream.metadata.oxia.records.TopicProjectionRecord;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Protocol-neutral metadata contract for authoritative and derived F2 projections. */
public interface ManagedLedgerProjectionMetadataStore extends AutoCloseable {
    CompletableFuture<Optional<TopicProjectionRecord>> getProjection(
            String cluster,
            String managedLedgerName);

    CompletableFuture<TopicProjectionRecord> createFirstProjection(
            String cluster,
            ProjectionCreateRequest request);

    CompletableFuture<TopicProjectionRecord> recreateDeletedProjection(
            String cluster,
            ManagedLedgerProjectionIdentity expectedDeletedIdentity,
            long expectedTopicMetadataVersion,
            ProjectionCreateRequest request);

    CompletableFuture<TopicProjectionRecord> updateProperties(
            String cluster,
            String managedLedgerName,
            ManagedLedgerProjectionIdentity expectedIdentity,
            long expectedVersion,
            Map<String, String> properties);

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
