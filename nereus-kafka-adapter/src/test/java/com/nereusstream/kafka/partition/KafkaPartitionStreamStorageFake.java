/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.partition;

import com.nereusstream.api.AppendAttemptId;
import com.nereusstream.api.AppendBatch;
import com.nereusstream.api.AppendOptions;
import com.nereusstream.api.AppendPrecondition;
import com.nereusstream.api.AppendRecoveryOptions;
import com.nereusstream.api.AppendResult;
import com.nereusstream.api.AppendSession;
import com.nereusstream.api.AppendSessionOptions;
import com.nereusstream.api.DeleteOptions;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.ReadRequest;
import com.nereusstream.api.ReadResult;
import com.nereusstream.api.ResolveOptions;
import com.nereusstream.api.ResolveResult;
import com.nereusstream.api.SealOptions;
import com.nereusstream.api.SemanticReadResult;
import com.nereusstream.api.StreamCreateOptions;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamMetadata;
import com.nereusstream.api.StreamName;
import com.nereusstream.api.StreamStorage;
import com.nereusstream.api.TrimOptions;
import com.nereusstream.kafka.codec.KafkaRecordBatchCodec;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

final class KafkaPartitionStreamStorageFake implements StreamStorage {
    private final StreamId streamId;
    private final KafkaRecordBatchCodec codec = new KafkaRecordBatchCodec();
    private final ArrayDeque<PendingAppend> pending = new ArrayDeque<>();
    private final List<ReadBatch> committed = new ArrayList<>();
    private long endOffset;
    private long cumulativeBytes;
    private long commitVersion;
    private int appendCalls;
    private int objectOrdinal;
    private boolean corruptNextResultStream;
    private CompletableFuture<AppendSession> pendingRenewal;
    private int renewalCalls;

    KafkaPartitionStreamStorageFake(StreamId streamId, long endOffset, long commitVersion) {
        this.streamId = streamId;
        this.endOffset = endOffset;
        this.commitVersion = commitVersion;
    }

    synchronized int appendCalls() {
        return appendCalls;
    }

    synchronized AppendPrecondition pendingPrecondition() {
        return pending.element().precondition;
    }

    synchronized AppendOptions pendingOptions() {
        return pending.element().options;
    }

    synchronized AppendBatch pendingBatch() {
        return pending.element().batch;
    }

    synchronized void completeNextSuccess() {
        PendingAppend append = pending.removeFirst();
        long start = append.precondition.expectedStartOffset().orElseThrow();
        long cursor = start;
        long logicalBytes = 0;
        List<ReadBatch> newBatches = new ArrayList<>();
        for (var entry : append.batch.entries()) {
            var decoded = codec.decode(entry.payload());
            if (decoded.baseOffset() != cursor) throw new AssertionError("fake append is not contiguous");
            OffsetRange range = new OffsetRange(decoded.baseOffset(), decoded.nextOffset());
            newBatches.add(KafkaPartitionStorageTestSupport.readBatch(range, entry.payload(), ++objectOrdinal));
            cursor = range.endOffset();
            logicalBytes = Math.addExact(logicalBytes, entry.payload().length);
        }
        if (start != endOffset) throw new AssertionError("fake expected-start mismatch");
        endOffset = cursor;
        cumulativeBytes = Math.addExact(cumulativeBytes, logicalBytes);
        commitVersion++;
        committed.addAll(newBatches);
        ReadBatch first = newBatches.get(0);
        StreamId resultStreamId = corruptNextResultStream
                ? new StreamId("wrong-kafka-partition-stream")
                : streamId;
        corruptNextResultStream = false;
        append.result.complete(new AppendResult(
                resultStreamId,
                new OffsetRange(start, cursor),
                cursor,
                cumulativeBytes,
                0,
                first.source().target(),
                append.batch.payloadFormat(),
                append.batch.recordCount(),
                append.batch.entryCount(),
                logicalBytes,
                List.of(),
                Optional.empty(),
                commitVersion));
    }

    synchronized void corruptNextResultStream() {
        corruptNextResultStream = true;
    }

    synchronized void failNext(Throwable failure) {
        pending.removeFirst().result.completeExceptionally(failure);
    }

    synchronized int renewalCalls() {
        return renewalCalls;
    }

    synchronized void completeRenewal(AppendSession renewed) {
        pendingRenewal.complete(renewed);
    }

    synchronized void failRenewal(Throwable failure) {
        pendingRenewal.completeExceptionally(failure);
    }

    @Override
    public synchronized CompletableFuture<AppendResult> append(
            StreamId requestedStreamId,
            AppendBatch batch,
            AppendOptions options,
            AppendPrecondition precondition) {
        if (!streamId.equals(requestedStreamId)) throw new AssertionError("wrong stream");
        appendCalls++;
        PendingAppend append = new PendingAppend(batch, options, precondition);
        pending.addLast(append);
        return append.result;
    }

    @Override
    public synchronized CompletableFuture<SemanticReadResult> read(
            StreamId requestedStreamId, ReadRequest request) {
        if (!streamId.equals(requestedStreamId)) throw new AssertionError("wrong stream");
        List<ReadBatch> selected = new ArrayList<>();
        int bytes = 0;
        long records = 0;
        for (ReadBatch batch : committed) {
            if (batch.range().endOffset() <= request.startOffset()) continue;
            int nextBytes = Math.addExact(bytes, batch.payload().length);
            long nextRecords = Math.addExact(records, batch.range().recordCount());
            boolean firstOverflow = selected.isEmpty()
                    && request.firstEntryPolicy() == com.nereusstream.api.FirstEntryPolicy.ALLOW_FIRST_ENTRY_OVERFLOW;
            if ((!firstOverflow && nextBytes > request.options().maxBytes())
                    || nextRecords > request.options().maxRecords()) {
                break;
            }
            selected.add(batch);
            bytes = nextBytes;
            records = nextRecords;
            if (bytes >= request.options().maxBytes()) break;
        }
        long next = selected.isEmpty()
                ? request.startOffset()
                : selected.get(selected.size() - 1).range().endOffset();
        ReadResult result = new ReadResult(streamId, request.startOffset(), next, selected, next >= endOffset);
        return CompletableFuture.completedFuture(SemanticReadResult.forRequest(request, result, next));
    }

    @Override
    public CompletableFuture<StreamMetadata> createOrGetStream(StreamName streamName, StreamCreateOptions options) {
        return unsupported();
    }

    @Override
    public CompletableFuture<AppendSession> acquireAppendSession(StreamId streamId, AppendSessionOptions options) {
        return unsupported();
    }

    @Override
    public synchronized CompletableFuture<AppendSession> renewAppendSession(
            AppendSession session, java.time.Duration ttl) {
        renewalCalls++;
        pendingRenewal = new CompletableFuture<>();
        return pendingRenewal;
    }

    @Override
    public CompletableFuture<AppendResult> append(StreamId streamId, AppendBatch batch, AppendOptions options) {
        return unsupported();
    }

    @Override
    public CompletableFuture<AppendResult> recoverAppend(
            StreamId streamId, AppendAttemptId appendAttemptId, AppendRecoveryOptions options) {
        return unsupported();
    }

    @Override
    public CompletableFuture<ReadResult> read(StreamId streamId, long startOffset, ReadOptions options) {
        return unsupported();
    }

    @Override
    public CompletableFuture<ResolveResult> resolve(StreamId streamId, long startOffset, ResolveOptions options) {
        return unsupported();
    }

    @Override
    public CompletableFuture<Void> trim(StreamId streamId, long beforeOffset, TrimOptions options) {
        return unsupported();
    }

    @Override
    public CompletableFuture<StreamMetadata> getStreamMetadata(StreamId streamId) {
        return unsupported();
    }

    @Override
    public CompletableFuture<StreamMetadata> seal(StreamId streamId, SealOptions options) {
        return unsupported();
    }

    @Override
    public CompletableFuture<StreamMetadata> delete(StreamId streamId, DeleteOptions options) {
        return unsupported();
    }

    @Override
    public void close() {}

    private static <T> CompletableFuture<T> unsupported() {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("not used by partition storage test"));
    }

    private static final class PendingAppend {
        private final AppendBatch batch;
        private final AppendOptions options;
        private final AppendPrecondition precondition;
        private final CompletableFuture<AppendResult> result = new CompletableFuture<>();

        private PendingAppend(AppendBatch batch, AppendOptions options, AppendPrecondition precondition) {
            this.batch = batch;
            this.options = options;
            this.precondition = precondition;
        }
    }
}
