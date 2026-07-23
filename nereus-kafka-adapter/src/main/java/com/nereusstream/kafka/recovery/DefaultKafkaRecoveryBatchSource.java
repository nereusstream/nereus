/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.recovery;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.FirstEntryPolicy;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadBoundaryMode;
import com.nereusstream.api.ReadIsolation;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.ReadRequest;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.SemanticReadResult;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamStorage;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Bounded exact-page adapter from the public COMMITTED stream view to Kafka recovery batches. */
public final class DefaultKafkaRecoveryBatchSource implements KafkaRecoveryBatchSource {
    private final StreamStorage streams;
    private final StreamId streamId;
    private final int chunkRecords;
    private final int chunkBytes;

    public DefaultKafkaRecoveryBatchSource(
            StreamStorage streams,
            StreamId streamId,
            int chunkRecords,
            int chunkBytes) {
        this.streams = Objects.requireNonNull(streams, "streams");
        this.streamId = Objects.requireNonNull(streamId, "streamId");
        if (chunkRecords <= 0 || chunkBytes <= 0) {
            throw new IllegalArgumentException("Kafka recovery chunk limits must be positive");
        }
        this.chunkRecords = chunkRecords;
        this.chunkBytes = chunkBytes;
    }

    @Override
    public CompletableFuture<KafkaRecoveryBatchPage> readCommittedPage(
            long startOffset,
            long endOffset,
            Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        if (startOffset < 0 || endOffset <= startOffset) {
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                    "Kafka recovery page must request a non-empty non-negative range"));
        }
        ReadRequest request = new ReadRequest(
                startOffset,
                ReadView.COMMITTED,
                ReadBoundaryMode.EXACT_START,
                FirstEntryPolicy.ALLOW_FIRST_ENTRY_OVERFLOW,
                new ReadOptions(chunkRecords, chunkBytes, ReadIsolation.COMMITTED, timeout));
        return streams.read(streamId, request)
                .thenApply(result -> exactPage(streamId, startOffset, endOffset, result));
    }

    private static KafkaRecoveryBatchPage exactPage(
            StreamId streamId,
            long startOffset,
            long endOffset,
            SemanticReadResult result) {
        Objects.requireNonNull(result, "result");
        if (result.view() != ReadView.COMMITTED
                || result.result().requestedOffset() != startOffset
                || result.sourceCoverageEndOffset() != result.result().nextOffset()
                || !result.result().streamId().equals(streamId)) {
            throw invariant("Kafka recovery read returned mismatched COMMITTED source facts");
        }
        List<ReadBatch> values = result.result().batches();
        if (values.isEmpty()) {
            throw invariant("Kafka recovery read made no progress before the frozen end");
        }
        List<KafkaReplayBatch> batches = new ArrayList<>(values.size());
        long cursor = startOffset;
        for (ReadBatch value : values) {
            if (value.payloadFormat() != PayloadFormat.KAFKA_RECORD_BATCH
                    || value.range().startOffset() != cursor
                    || value.range().endOffset() > endOffset) {
                throw invariant("Kafka recovery read returned a non-Kafka or out-of-range batch");
            }
            batches.add(new KafkaReplayBatch(
                    value.range().startOffset(),
                    value.range().endOffset() - 1,
                    value.payload()));
            cursor = value.range().endOffset();
        }
        if (result.result().nextOffset() != cursor) {
            throw invariant("Kafka recovery read cursor does not match returned batches");
        }
        return new KafkaRecoveryBatchPage(startOffset, cursor, batches);
    }

    private static NereusException invariant(String message) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }
}
