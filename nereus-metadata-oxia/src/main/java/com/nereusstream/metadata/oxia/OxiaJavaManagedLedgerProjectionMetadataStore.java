/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.metadata.oxia.records.ManagedLedgerProjectionIdentity;
import com.nereusstream.metadata.oxia.records.TopicProjectionRecord;
import java.time.Clock;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Production F2 projection metadata adapter using a caller-owned shared Oxia runtime. */
public final class OxiaJavaManagedLedgerProjectionMetadataStore
        implements ManagedLedgerProjectionMetadataStore {
    private final ProjectionMetadataStoreCore core;

    public static OxiaJavaManagedLedgerProjectionMetadataStore usingSharedRuntime(
            OxiaClientConfiguration clientConfig,
            SharedOxiaClientRuntime runtime,
            ProjectionMetadataStoreConfig storeConfig,
            Clock clock) {
        Objects.requireNonNull(clientConfig, "clientConfig");
        Objects.requireNonNull(runtime, "runtime");
        Objects.requireNonNull(storeConfig, "storeConfig");
        Objects.requireNonNull(clock, "clock");
        runtime.requireCompatible(clientConfig);
        return new OxiaJavaManagedLedgerProjectionMetadataStore(runtime, storeConfig, clock);
    }

    private OxiaJavaManagedLedgerProjectionMetadataStore(
            SharedOxiaClientRuntime runtime,
            ProjectionMetadataStoreConfig storeConfig,
            Clock clock) {
        this.core = new ProjectionMetadataStoreCore(runtime.client(), storeConfig, clock, ignored -> { });
    }

    @Override
    public CompletableFuture<Optional<TopicProjectionRecord>> getProjection(
            String cluster,
            String managedLedgerName) {
        return core.getProjection(cluster, managedLedgerName);
    }

    @Override
    public CompletableFuture<TopicProjectionRecord> createFirstProjection(
            String cluster,
            ProjectionCreateRequest request,
            ProjectionPublishGuard publishGuard) {
        return core.createFirstProjection(cluster, request, publishGuard);
    }

    @Override
    public CompletableFuture<TopicProjectionRecord> recreateDeletedProjection(
            String cluster,
            ManagedLedgerProjectionIdentity expectedDeletedIdentity,
            long expectedTopicMetadataVersion,
            ProjectionCreateRequest request,
            ProjectionPublishGuard publishGuard) {
        return core.recreateDeletedProjection(
                cluster, expectedDeletedIdentity, expectedTopicMetadataVersion, request, publishGuard);
    }

    @Override
    public CompletableFuture<TopicProjectionRecord> updateProperties(
            String cluster,
            String managedLedgerName,
            ManagedLedgerProjectionIdentity expectedIdentity,
            long expectedVersion,
            Map<String, String> properties) {
        return core.updateProperties(
                cluster, managedLedgerName, expectedIdentity, expectedVersion, properties);
    }

    @Override
    public CompletableFuture<TopicProjectionRecord> mirrorFacadeState(
            String cluster,
            String managedLedgerName,
            ManagedLedgerProjectionIdentity expectedIdentity,
            long expectedVersion,
            ManagedLedgerFacadeState state) {
        return core.mirrorFacadeState(
                cluster, managedLedgerName, expectedIdentity, expectedVersion, state);
    }

    @Override
    public CompletableFuture<ProjectionRepairResult> repairProjectionIndexes(
            String cluster,
            TopicProjectionRecord authoritative) {
        return core.repairProjectionIndexes(cluster, authoritative);
    }

    @Override
    public void close() {
        core.close();
    }
}
