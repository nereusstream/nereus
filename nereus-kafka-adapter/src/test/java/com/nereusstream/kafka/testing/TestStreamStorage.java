/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.testing;

import com.nereusstream.api.AppendAttemptId;
import com.nereusstream.api.AppendBatch;
import com.nereusstream.api.AppendOptions;
import com.nereusstream.api.AppendRecoveryOptions;
import com.nereusstream.api.AppendResult;
import com.nereusstream.api.AppendSession;
import com.nereusstream.api.AppendSessionOptions;
import com.nereusstream.api.DeleteOptions;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.ReadRequest;
import com.nereusstream.api.ReadResult;
import com.nereusstream.api.ResolveOptions;
import com.nereusstream.api.ResolveResult;
import com.nereusstream.api.SealOptions;
import com.nereusstream.api.SemanticReadResult;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamCreateOptions;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamMetadata;
import com.nereusstream.api.StreamName;
import com.nereusstream.api.StreamState;
import com.nereusstream.api.StreamStorage;
import com.nereusstream.api.TrimOptions;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

/** In-memory stream lifecycle fake for adapter state-machine tests. */
public final class TestStreamStorage implements StreamStorage {
    private final Map<StreamName, StreamMetadata> byName = new LinkedHashMap<>();
    private final Map<StreamId, StreamMetadata> byId = new LinkedHashMap<>();
    private int creates;
    private boolean loseNextCreateResponse;
    private BiFunction<StreamId, ReadRequest, CompletableFuture<SemanticReadResult>>
            semanticReader;

    public synchronized void semanticReader(
            BiFunction<StreamId, ReadRequest, CompletableFuture<SemanticReadResult>> reader) {
        semanticReader = reader;
    }

    public synchronized void loseNextCreateResponseAfterCommit() {
        loseNextCreateResponse = true;
    }

    public synchronized int creates() {
        return creates;
    }

    public synchronized int streamCount() {
        return byName.size();
    }

    @Override
    public synchronized CompletableFuture<StreamMetadata> createOrGetStream(
            StreamName streamName, StreamCreateOptions options) {
        StreamMetadata metadata = byName.get(streamName);
        if (metadata == null) {
            creates++;
            StreamId id = new StreamId("test-stream-" + creates);
            metadata = new StreamMetadata(
                    id, streamName, StreamState.ACTIVE, options.profile().canonical(), options.attributes(),
                    1_000L + creates, 1, 0, 0, 0);
            byName.put(streamName, metadata);
            byId.put(id, metadata);
        }
        if (loseNextCreateResponse) {
            loseNextCreateResponse = false;
            return CompletableFuture.failedFuture(new IllegalStateException("simulated create response loss"));
        }
        return CompletableFuture.completedFuture(metadata);
    }

    @Override
    public synchronized CompletableFuture<StreamMetadata> getStreamMetadata(StreamId streamId) {
        StreamMetadata metadata = byId.get(streamId);
        return metadata == null
                ? CompletableFuture.failedFuture(new IllegalArgumentException("unknown stream"))
                : CompletableFuture.completedFuture(metadata);
    }

    @Override
    public synchronized CompletableFuture<StreamMetadata> seal(StreamId streamId, SealOptions options) {
        return transition(streamId, StreamState.SEALED);
    }

    @Override
    public synchronized CompletableFuture<StreamMetadata> delete(StreamId streamId, DeleteOptions options) {
        return transition(streamId, StreamState.DELETED);
    }

    private CompletableFuture<StreamMetadata> transition(StreamId streamId, StreamState target) {
        StreamMetadata current = byId.get(streamId);
        if (current == null) return CompletableFuture.failedFuture(new IllegalArgumentException("unknown stream"));
        StreamMetadata updated = new StreamMetadata(
                current.streamId(), current.streamName(), target, current.profile(), current.attributes(),
                current.createdAtMillis(), current.metadataVersion() + 1, current.committedEndOffset(),
                current.cumulativeSize(), current.trimOffset());
        byId.put(streamId, updated);
        byName.put(updated.streamName(), updated);
        return CompletableFuture.completedFuture(updated);
    }

    @Override
    public CompletableFuture<AppendSession> acquireAppendSession(
            StreamId streamId, AppendSessionOptions options) {
        return unsupported();
    }

    @Override
    public CompletableFuture<AppendResult> append(
            StreamId streamId, AppendBatch batch, AppendOptions options) {
        return unsupported();
    }

    @Override
    public CompletableFuture<AppendResult> recoverAppend(
            StreamId streamId, AppendAttemptId attemptId, AppendRecoveryOptions options) {
        return unsupported();
    }

    @Override
    public CompletableFuture<ReadResult> read(
            StreamId streamId, long startOffset, ReadOptions options) {
        return unsupported();
    }

    @Override
    public synchronized CompletableFuture<SemanticReadResult> read(
            StreamId streamId, ReadRequest request) {
        return semanticReader == null
                ? unsupported()
                : semanticReader.apply(streamId, request);
    }

    @Override
    public CompletableFuture<ResolveResult> resolve(
            StreamId streamId, long startOffset, ResolveOptions options) {
        return unsupported();
    }

    @Override
    public CompletableFuture<Void> trim(
            StreamId streamId, long beforeOffset, TrimOptions options) {
        return unsupported();
    }

    @Override
    public void close() { }

    private static <T> CompletableFuture<T> unsupported() {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("not needed by lifecycle tests"));
    }
}
