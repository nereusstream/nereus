/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.read;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.FirstEntryPolicy;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.PhysicalReadResult;
import com.nereusstream.api.PhysicalReadStats;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadBoundaryMode;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.ReadRequest;
import com.nereusstream.api.ReadSourceRef;
import com.nereusstream.api.ReadTargetIdentities;
import com.nereusstream.api.ResolvedRange;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.objectstore.wal.WalReadResult;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/** Exact-format physical reader used after generation resolution. */
public interface ReadTargetReader extends AutoCloseable {
    ReadTargetReaderKey key();

    /** Exact target identities accepted by this reader; no physical or logical format wildcard exists. */
    default Set<ReadTargetReaderKey> keys() {
        return Set.of(key());
    }

    long reservationBytes(ResolvedRange range);

    /** Provider-neutral correctness surface used by common read and materialization code. */
    default CompletableFuture<PhysicalReadResult> readPhysicalWithStats(
            StreamId streamId,
            long startOffset,
            List<ResolvedRange> ranges,
            ReadOptions options) {
        return readWithStats(streamId, startOffset, ranges, options)
                .thenApply(result -> fromLegacy(ranges, result));
    }

    /** Request-aware physical read used by ranged-entry callers. */
    default CompletableFuture<PhysicalReadResult> readPhysicalWithStats(
            StreamId streamId,
            ReadRequest request,
            List<ResolvedRange> ranges) {
        if (request.boundaryMode() != ReadBoundaryMode.EXACT_START
                || request.firstEntryPolicy() != FirstEntryPolicy.LEGACY_STRICT_LIMIT) {
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.UNSUPPORTED_READ_SEMANTICS,
                    false,
                    "physical reader does not support ranged-entry semantics"));
        }
        return readPhysicalWithStats(
                streamId,
                request.startOffset(),
                ranges,
                request.options());
    }

    /** Transitional Object-only surface for one milestone; new readers must implement {@link #readPhysicalWithStats}. */
    @Deprecated(forRemoval = true)
    default CompletableFuture<WalReadResult> readWithStats(
            StreamId streamId,
            long startOffset,
            List<ResolvedRange> ranges,
            ReadOptions options) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException(
                "reader implements only the provider-neutral physical result"));
    }

    private static PhysicalReadResult fromLegacy(List<ResolvedRange> ranges, WalReadResult result) {
        List<PhysicalReadStats> stats = result.sliceStats().stream().map(value -> {
            ResolvedRange range = ranges.stream().filter(candidate ->
                            candidate.readTarget() instanceof ObjectSliceReadTarget target
                                    && target.objectId().equals(value.objectId())
                                    && target.objectOffset() == value.objectOffset()
                                    && target.objectLength() == value.fullSlicePayloadBytes()
                                    && target.entryIndexRef().length() == value.entryIndexBytes())
                    .findFirst().orElseThrow(() -> new IllegalArgumentException(
                            "legacy reader reported an unknown Object source"));
            return new PhysicalReadStats(ReadTargetIdentities.sha256(range.readTarget()),
                    value.fullSlicePayloadBytes(), value.entryIndexBytes(), value.downloadedPayloadBytes(),
                    value.downloadedEntryIndexBytes(), value.returnedPayloadBytes());
        }).toList();
        List<ReadBatch> batches = result.batches().stream().map(batch -> {
            ResolvedRange range = ranges.stream().filter(candidate ->
                            candidate.offsetRange().startOffset() <= batch.range().startOffset()
                                    && batch.range().endOffset() <= candidate.offsetRange().endOffset()
                                    && candidate.readTarget() instanceof ObjectSliceReadTarget target
                                    && target.objectId().equals(batch.sourceObjectId()))
                    .findFirst().orElseThrow(() -> new IllegalArgumentException(
                            "legacy reader batch has no exact resolved Object source"));
            return new ReadBatch(batch.range(), batch.payloadFormat(), batch.payload(), batch.schemaRefs(),
                    batch.projectionRef(), new ReadSourceRef(range.offsetRange(), range.generation(),
                            range.commitVersion(), range.readTarget(), ReadTargetIdentities.sha256(range.readTarget())),
                    batch.sourceObjectOffset(), batch.sourceObjectLength());
        }).toList();
        return new PhysicalReadResult(batches, stats);
    }

    @Override
    default void close() {
        // Most adapters borrow a process-owned reader. Stateful implementations may override.
    }
}
