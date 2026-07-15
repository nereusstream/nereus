/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.read;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.ResolvedRange;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.api.target.ReadTargetType;
import com.nereusstream.objectstore.compacted.CompactedObjectFormatException;
import com.nereusstream.objectstore.compacted.CompactedObjectFormatV1;
import com.nereusstream.objectstore.compacted.CompactedObjectMetadata;
import com.nereusstream.objectstore.compacted.CompactedObjectReadRequest;
import com.nereusstream.objectstore.compacted.CompactedObjectReadResult;
import com.nereusstream.objectstore.compacted.CompactedObjectReader;
import com.nereusstream.objectstore.compacted.CompactedObjectRow;
import com.nereusstream.objectstore.wal.WalReadResult;
import com.nereusstream.objectstore.wal.WalSliceReadStats;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Exact NCP1 adapter that maps one dense Parquet row back to one unchanged logical ReadBatch. */
public final class ParquetCompactedTargetReader implements ReadTargetReader {
    public static final ReadTargetReaderKey KEY = new ReadTargetReaderKey(
            ReadTargetType.OBJECT_SLICE,
            1,
            Optional.of(ObjectType.STREAM_COMPACTED_OBJECT),
            Optional.of(CompactedObjectFormatV1.COMMITTED_PHYSICAL_FORMAT));

    private final CompactedObjectReader reader;

    public ParquetCompactedTargetReader(CompactedObjectReader reader) {
        this.reader = Objects.requireNonNull(reader, "reader");
    }

    @Override
    public ReadTargetReaderKey key() {
        return KEY;
    }

    @Override
    public long reservationBytes(ResolvedRange range) {
        ObjectSliceReadTarget target = requireTarget(range);
        try {
            return Math.addExact(target.objectLength(), target.entryIndexRef().length());
        } catch (ArithmeticException failure) {
            throw new NereusException(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    "compacted reader reservation overflows",
                    failure);
        }
    }

    @Override
    public CompletableFuture<WalReadResult> readWithStats(
            StreamId streamId,
            long startOffset,
            List<ResolvedRange> ranges,
            ReadOptions options) {
        try {
            Objects.requireNonNull(streamId, "streamId");
            Objects.requireNonNull(options, "options");
            List<ResolvedRange> exactRanges = List.copyOf(Objects.requireNonNull(ranges, "ranges"));
            validateRanges(startOffset, exactRanges);
            Deadline deadline = new Deadline(options.timeout());
            return readNext(
                    streamId,
                    startOffset,
                    exactRanges,
                    0,
                    options,
                    deadline,
                    new ArrayList<>(),
                    new ArrayList<>(),
                    0,
                    0);
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private CompletableFuture<WalReadResult> readNext(
            StreamId streamId,
            long cursor,
            List<ResolvedRange> ranges,
            int index,
            ReadOptions options,
            Deadline deadline,
            List<ReadBatch> batches,
            List<WalSliceReadStats> stats,
            long returnedRecords,
            long returnedBytes) {
        if (index >= ranges.size()
                || returnedRecords >= options.maxRecords()
                || returnedBytes >= options.maxBytes()) {
            return CompletableFuture.completedFuture(new WalReadResult(batches, stats));
        }
        ResolvedRange range = ranges.get(index);
        if (cursor >= range.offsetRange().endOffset()) {
            return readNext(
                    streamId,
                    cursor,
                    ranges,
                    index + 1,
                    options,
                    deadline,
                    batches,
                    stats,
                    returnedRecords,
                    returnedBytes);
        }
        long rangeStart = Math.max(cursor, range.offsetRange().startOffset());
        int remainingRecords = Math.toIntExact(options.maxRecords() - returnedRecords);
        int remainingBytes = Math.toIntExact(options.maxBytes() - returnedBytes);
        ObjectSliceReadTarget target = requireTarget(range);
        CompactedObjectReadRequest request = new CompactedObjectReadRequest(
                streamId,
                ReadView.COMMITTED,
                range.offsetRange(),
                rangeStart,
                target,
                range.payloadFormat(),
                remainingRecords,
                remainingBytes,
                deadline.remaining());
        CompletableFuture<CompactedObjectReadResult> physical;
        try {
            physical = Objects.requireNonNull(reader.read(request), "compacted-reader future");
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return physical.thenCompose(result -> {
            requireMetadata(range, result.metadata());
            long rangeBytes = 0;
            for (CompactedObjectRow row : result.rows()) {
                byte[] payload = bytes(row.exactPayload());
                rangeBytes = Math.addExact(rangeBytes, payload.length);
                batches.add(new ReadBatch(
                        new OffsetRange(row.streamOffset(), row.streamOffset() + 1),
                        range.payloadFormat(),
                        payload,
                        range.schemaRefs(),
                        target.entryIndexRef(),
                        range.projectionRef(),
                        target.objectId(),
                        target.objectOffset(),
                        target.objectLength()));
            }
            stats.add(new WalSliceReadStats(
                    target.objectId(),
                    target.objectOffset(),
                    target.objectLength(),
                    target.entryIndexRef().length(),
                    rangeBytes));
            long records = Math.addExact(returnedRecords, result.rows().size());
            long payloadBytes = Math.addExact(returnedBytes, rangeBytes);
            long next = result.sourceCoverageEndOffset();
            if (next < range.offsetRange().endOffset()) {
                return CompletableFuture.completedFuture(new WalReadResult(batches, stats));
            }
            return readNext(
                    streamId,
                    next,
                    ranges,
                    index + 1,
                    options,
                    deadline,
                    batches,
                    stats,
                    records,
                    payloadBytes);
        });
    }

    private static void validateRanges(
            long startOffset,
            List<ResolvedRange> ranges) {
        if (startOffset < 0 || ranges.isEmpty()) {
            throw new IllegalArgumentException(
                    "compacted reader requires a non-negative offset and non-empty ranges");
        }
        long previousEnd = -1;
        boolean containsStart = false;
        for (ResolvedRange range : ranges) {
            requireTarget(range);
            if (range.generation() <= 0
                    || (previousEnd >= 0 && range.offsetRange().startOffset() != previousEnd)) {
                throw new CompactedObjectFormatException(
                        "NCP1 resolved ranges must be higher-generation and gap-free");
            }
            containsStart |= range.offsetRange().contains(startOffset);
            previousEnd = range.offsetRange().endOffset();
        }
        if (!containsStart) {
            throw new IllegalArgumentException("startOffset is outside compacted resolved ranges");
        }
    }

    private static ObjectSliceReadTarget requireTarget(ResolvedRange range) {
        Objects.requireNonNull(range, "range");
        if (!(range.readTarget() instanceof ObjectSliceReadTarget target)
                || !ReadTargetReaderKey.from(target).equals(KEY)
                || target.objectOffset() != 0
                || !target.logicalFormat().equals(range.payloadFormat().name())) {
            throw new NereusException(
                    ErrorCode.UNSUPPORTED_READ_TARGET,
                    false,
                    "NCP1 reader received a non-exact compacted target");
        }
        return target;
    }

    private static void requireMetadata(
            ResolvedRange range,
            CompactedObjectMetadata metadata) {
        if (metadata.view() != ReadView.COMMITTED
                || !metadata.sourceCoverage().equals(range.offsetRange())
                || metadata.payloadFormat() != range.payloadFormat()
                || metadata.sourceRecordCount() != range.recordCount()
                || metadata.outputRecordCount() != range.recordCount()
                || metadata.entryCount() != range.entryCount()
                || metadata.logicalBytes() != range.logicalBytes()
                || metadata.projectionIdentitySha256().isPresent()
                        != range.projectionRef().isPresent()) {
            throw new CompactedObjectFormatException(
                    "NCP1 metadata does not match the resolved generation index");
        }
    }

    private static byte[] bytes(ByteBuffer supplied) {
        ByteBuffer value = supplied.asReadOnlyBuffer();
        byte[] result = new byte[value.remaining()];
        value.get(result);
        return result;
    }

    private static final class Deadline {
        private final long deadlineNanos;

        private Deadline(Duration timeout) {
            long now = System.nanoTime();
            long nanos;
            try {
                nanos = timeout.toNanos();
            } catch (ArithmeticException failure) {
                nanos = Long.MAX_VALUE;
            }
            deadlineNanos = nanos >= Long.MAX_VALUE - now ? Long.MAX_VALUE : now + nanos;
        }

        private Duration remaining() {
            long nanos = deadlineNanos == Long.MAX_VALUE
                    ? Long.MAX_VALUE
                    : deadlineNanos - System.nanoTime();
            if (nanos <= 0) {
                throw new NereusException(
                        ErrorCode.TIMEOUT,
                        true,
                        "compacted target read deadline expired");
            }
            return Duration.ofNanos(nanos);
        }
    }
}
