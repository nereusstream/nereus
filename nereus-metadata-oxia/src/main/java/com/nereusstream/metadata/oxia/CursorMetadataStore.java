/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.records.CursorRetentionRecord;
import com.nereusstream.metadata.oxia.records.CursorStateRecord;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Protocol-neutral authoritative F3 cursor metadata contract. */
public interface CursorMetadataStore extends AutoCloseable {
    static CursorMetadataStore usingSharedRuntime(
            OxiaClientConfiguration clientConfig,
            SharedOxiaClientRuntime runtime,
            CursorMetadataStoreConfig storeConfig) {
        return OxiaJavaCursorMetadataStore.usingSharedRuntime(clientConfig, runtime, storeConfig);
    }

    CompletableFuture<Optional<VersionedCursorState>> getCursor(
            String cluster, StreamId streamId, String cursorName);

    CompletableFuture<VersionedCursorState> createCursor(
            String cluster, CursorStateRecord value);

    CompletableFuture<VersionedCursorState> compareAndSetCursor(
            String cluster, CursorStateRecord value, long expectedMetadataVersion);

    CompletableFuture<CursorScanPage> scanCursors(
            String cluster,
            StreamId streamId,
            Optional<CursorScanToken> continuation,
            int pageSize);

    CompletableFuture<Optional<VersionedCursorRetention>> getRetention(
            String cluster, StreamId streamId);

    CompletableFuture<VersionedCursorRetention> createRetention(
            String cluster, CursorRetentionRecord value);

    CompletableFuture<VersionedCursorRetention> compareAndSetRetention(
            String cluster, CursorRetentionRecord value, long expectedMetadataVersion);

    WatchRegistration watchStreamCursors(
            String cluster, StreamId streamId, Runnable invalidation);

    @Override
    void close();
}
