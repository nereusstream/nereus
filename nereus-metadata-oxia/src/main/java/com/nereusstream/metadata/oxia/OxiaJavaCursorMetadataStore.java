/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.records.CursorRetentionRecord;
import com.nereusstream.metadata.oxia.records.CursorStateRecord;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Production F3 cursor metadata adapter using a caller-owned shared Oxia runtime. */
public final class OxiaJavaCursorMetadataStore implements CursorMetadataStore {
    private final CursorMetadataStoreCore core;

    public static OxiaJavaCursorMetadataStore usingSharedRuntime(
            OxiaClientConfiguration clientConfig,
            SharedOxiaClientRuntime runtime,
            CursorMetadataStoreConfig storeConfig) {
        Objects.requireNonNull(clientConfig, "clientConfig");
        Objects.requireNonNull(runtime, "runtime");
        Objects.requireNonNull(storeConfig, "storeConfig");
        runtime.requireCompatible(clientConfig);
        return new OxiaJavaCursorMetadataStore(runtime, storeConfig);
    }

    private OxiaJavaCursorMetadataStore(
            SharedOxiaClientRuntime runtime, CursorMetadataStoreConfig storeConfig) {
        this.core = new CursorMetadataStoreCore(runtime.client(), storeConfig);
    }

    @Override
    public CompletableFuture<Optional<VersionedCursorState>> getCursor(
            String cluster, StreamId streamId, String cursorName) {
        return core.getCursor(cluster, streamId, cursorName);
    }

    @Override
    public CompletableFuture<VersionedCursorState> createCursor(
            String cluster, CursorStateRecord value) {
        return core.createCursor(cluster, value);
    }

    @Override
    public CompletableFuture<VersionedCursorState> compareAndSetCursor(
            String cluster, CursorStateRecord value, long expectedMetadataVersion) {
        return core.compareAndSetCursor(cluster, value, expectedMetadataVersion);
    }

    @Override
    public CompletableFuture<CursorScanPage> scanCursors(
            String cluster,
            StreamId streamId,
            Optional<CursorScanToken> continuation,
            int pageSize) {
        return core.scanCursors(cluster, streamId, continuation, pageSize);
    }

    @Override
    public CompletableFuture<Optional<VersionedCursorRetention>> getRetention(
            String cluster, StreamId streamId) {
        return core.getRetention(cluster, streamId);
    }

    @Override
    public CompletableFuture<VersionedCursorRetention> createRetention(
            String cluster, CursorRetentionRecord value) {
        return core.createRetention(cluster, value);
    }

    @Override
    public CompletableFuture<VersionedCursorRetention> compareAndSetRetention(
            String cluster, CursorRetentionRecord value, long expectedMetadataVersion) {
        return core.compareAndSetRetention(cluster, value, expectedMetadataVersion);
    }

    @Override
    public WatchRegistration watchStreamCursors(
            String cluster, StreamId streamId, Runnable invalidation) {
        return core.watchStreamCursors(cluster, streamId, invalidation);
    }

    @Override
    public void close() {
        core.close();
    }
}
