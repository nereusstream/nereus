/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.read;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.FirstEntryPolicy;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.PhysicalReadResult;
import com.nereusstream.api.PhysicalReadStats;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadBoundaryMode;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.ReadRequest;
import com.nereusstream.api.ReadSourceRef;
import com.nereusstream.api.ReadTargetIdentities;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.ResolvedRange;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.api.target.ReadTargetType;
import com.nereusstream.objectstore.compacted.CompactedObjectFormatException;
import com.nereusstream.objectstore.compacted.CompactedObjectFormatV2;
import com.nereusstream.objectstore.compacted.KafkaTopicCompactedObjectReadRequest;
import com.nereusstream.objectstore.compacted.KafkaTopicCompactedObjectReadResult;
import com.nereusstream.objectstore.compacted.KafkaTopicCompactedObjectReader;
import com.nereusstream.objectstore.compacted.KafkaTopicCompactedObjectRow;
import com.nereusstream.objectstore.compacted.RangedCompactedObjectMetadata;
import com.nereusstream.objectstore.compacted.RangedCompactedObjectReadRequest;
import com.nereusstream.objectstore.compacted.RangedCompactedObjectReadResult;
import com.nereusstream.objectstore.compacted.RangedCompactedObjectReader;
import com.nereusstream.objectstore.compacted.RangedCompactedObjectRow;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/** Exact NCP2/NTC2 target adapter; physical and logical format identities are both registry keys. */
public final class ParquetV2CompactedTargetReader implements ReadTargetReader {
    public static final ReadTargetReaderKey NCP2_KEY = key(CompactedObjectFormatV2.COMMITTED_PHYSICAL_FORMAT);
    public static final ReadTargetReaderKey NTC2_KEY = key(CompactedObjectFormatV2.TOPIC_COMPACTED_PHYSICAL_FORMAT);

    private final RangedCompactedObjectReader ncp2Reader;
    private final KafkaTopicCompactedObjectReader ntc2Reader;

    public ParquetV2CompactedTargetReader(
            RangedCompactedObjectReader ncp2Reader,
            KafkaTopicCompactedObjectReader ntc2Reader) {
        this.ncp2Reader = Objects.requireNonNull(ncp2Reader, "ncp2Reader");
        this.ntc2Reader = Objects.requireNonNull(ntc2Reader, "ntc2Reader");
    }

    @Override
    public ReadTargetReaderKey key() {
        return NCP2_KEY;
    }

    @Override
    public Set<ReadTargetReaderKey> keys() {
        return Set.of(NCP2_KEY, NTC2_KEY);
    }

    @Override
    public long reservationBytes(ResolvedRange range) {
        ObjectSliceReadTarget target = requireTarget(range);
        try {
            return Math.addExact(target.objectLength(), target.entryIndexRef().length());
        } catch (ArithmeticException failure) {
            throw invariant("V2 compacted reader reservation overflows", failure);
        }
    }

    @Override
    public CompletableFuture<PhysicalReadResult> readPhysicalWithStats(
            StreamId streamId,
            long startOffset,
            List<ResolvedRange> ranges,
            ReadOptions options) {
        return readPhysicalWithStats(
                streamId,
                new ReadRequest(
                        startOffset,
                        ReadView.COMMITTED,
                        ReadBoundaryMode.EXACT_START,
                        FirstEntryPolicy.LEGACY_STRICT_LIMIT,
                        options),
                ranges);
    }

    @Override
    public CompletableFuture<PhysicalReadResult> readPhysicalWithStats(
            StreamId streamId,
            ReadRequest request,
            List<ResolvedRange> ranges) {
        try {
            Objects.requireNonNull(streamId, "streamId");
            Objects.requireNonNull(request, "request");
            List<ResolvedRange> exactRanges = List.copyOf(Objects.requireNonNull(ranges, "ranges"));
            validateRanges(request, exactRanges);
            return readNext(
                    streamId,
                    request,
                    exactRanges,
                    0,
                    new Deadline(request.options().timeout()),
                    new ArrayList<>(),
                    new ArrayList<>(),
                    0,
                    0,
                    request.startOffset());
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private CompletableFuture<PhysicalReadResult> readNext(
            StreamId streamId,
            ReadRequest request,
            List<ResolvedRange> ranges,
            int index,
            Deadline deadline,
            List<ReadBatch> batches,
            List<PhysicalReadStats> stats,
            long returnedRecords,
            long returnedBytes,
            long coverageEndOffset) {
        ReadOptions options = request.options();
        if (index >= ranges.size()
                || returnedRecords >= options.maxRecords()
                || returnedBytes >= options.maxBytes()) {
            return CompletableFuture.completedFuture(new PhysicalReadResult(
                    batches, stats, OptionalLong.of(coverageEndOffset)));
        }
        ResolvedRange range = ranges.get(index);
        long start = Math.max(coverageEndOffset, range.offsetRange().startOffset());
        if (start >= range.offsetRange().endOffset()) {
            return readNext(
                    streamId, request, ranges, index + 1, deadline, batches, stats,
                    returnedRecords, returnedBytes, coverageEndOffset);
        }
        int remainingRecords = Math.toIntExact(options.maxRecords() - returnedRecords);
        int remainingBytes = Math.toIntExact(options.maxBytes() - returnedBytes);
        FirstEntryPolicy firstPolicy = batches.isEmpty()
                ? request.firstEntryPolicy()
                : FirstEntryPolicy.LEGACY_STRICT_LIMIT;
        ObjectSliceReadTarget target = requireTarget(range);
        if (request.view() == ReadView.COMMITTED) {
            RangedCompactedObjectReadRequest physical = new RangedCompactedObjectReadRequest(
                    streamId, range.offsetRange(), start, target, range.payloadFormat(),
                    request.boundaryMode(), firstPolicy, remainingRecords, remainingBytes, deadline.remaining());
            return ncp2Reader.read(physical).thenCompose(result -> appendNcp2(
                    streamId, request, ranges, index, deadline, batches, stats,
                    returnedRecords, returnedBytes, range, target, result));
        }
        KafkaTopicCompactedObjectReadRequest physical = new KafkaTopicCompactedObjectReadRequest(
                streamId, range.offsetRange(), start, target, request.boundaryMode(), firstPolicy,
                remainingRecords, remainingBytes, deadline.remaining());
        return ntc2Reader.read(physical).thenCompose(result -> appendNtc2(
                streamId, request, ranges, index, deadline, batches, stats,
                returnedRecords, returnedBytes, range, target, result));
    }

    private CompletableFuture<PhysicalReadResult> appendNcp2(
            StreamId streamId,
            ReadRequest request,
            List<ResolvedRange> ranges,
            int index,
            Deadline deadline,
            List<ReadBatch> batches,
            List<PhysicalReadStats> stats,
            long returnedRecords,
            long returnedBytes,
            ResolvedRange range,
            ObjectSliceReadTarget target,
            RangedCompactedObjectReadResult result) {
        requireMetadata(range, result.metadata(), ReadView.COMMITTED);
        long rangeBytes = 0;
        long rangeRecords = 0;
        for (RangedCompactedObjectRow row : result.rows()) {
            byte[] payload = bytes(row.exactPayload());
            rangeBytes = Math.addExact(rangeBytes, payload.length);
            rangeRecords = Math.addExact(rangeRecords, row.recordCount());
            batches.add(batch(range, target, row.streamOffsetStart(), row.endOffset(), payload));
        }
        addStats(stats, target, result.physicalBytesRead(), result.footerBytesRead(), rangeBytes);
        return continueAfter(
                streamId, request, ranges, index, deadline, batches, stats,
                Math.addExact(returnedRecords, rangeRecords), Math.addExact(returnedBytes, rangeBytes),
                result.sourceCoverageEndOffset());
    }

    private CompletableFuture<PhysicalReadResult> appendNtc2(
            StreamId streamId,
            ReadRequest request,
            List<ResolvedRange> ranges,
            int index,
            Deadline deadline,
            List<ReadBatch> batches,
            List<PhysicalReadStats> stats,
            long returnedRecords,
            long returnedBytes,
            ResolvedRange range,
            ObjectSliceReadTarget target,
            KafkaTopicCompactedObjectReadResult result) {
        requireMetadata(range, result.metadata(), ReadView.TOPIC_COMPACTED);
        long rangeBytes = 0;
        long rangeRecords = 0;
        for (KafkaTopicCompactedObjectRow row : result.rows()) {
            byte[] payload = bytes(row.exactPayload());
            rangeBytes = Math.addExact(rangeBytes, payload.length);
            rangeRecords = Math.addExact(rangeRecords, row.recordCount());
            batches.add(batch(range, target, row.streamOffsetStart(), row.endOffset(), payload));
        }
        addStats(stats, target, result.physicalBytesRead(), result.footerBytesRead(), rangeBytes);
        return continueAfter(
                streamId, request, ranges, index, deadline, batches, stats,
                Math.addExact(returnedRecords, rangeRecords), Math.addExact(returnedBytes, rangeBytes),
                result.sourceCoverageEndOffset());
    }

    private CompletableFuture<PhysicalReadResult> continueAfter(
            StreamId streamId,
            ReadRequest request,
            List<ResolvedRange> ranges,
            int index,
            Deadline deadline,
            List<ReadBatch> batches,
            List<PhysicalReadStats> stats,
            long returnedRecords,
            long returnedBytes,
            long coverageEndOffset) {
        if (coverageEndOffset < ranges.get(index).offsetRange().endOffset()) {
            return CompletableFuture.completedFuture(new PhysicalReadResult(
                    batches, stats, OptionalLong.of(coverageEndOffset)));
        }
        return readNext(
                streamId, request, ranges, index + 1, deadline, batches, stats,
                returnedRecords, returnedBytes, coverageEndOffset);
    }

    private static ReadBatch batch(
            ResolvedRange range,
            ObjectSliceReadTarget target,
            long startOffset,
            long endOffset,
            byte[] payload) {
        return new ReadBatch(
                new OffsetRange(startOffset, endOffset),
                range.payloadFormat(),
                payload,
                range.schemaRefs(),
                range.projectionRef(),
                new ReadSourceRef(
                        range.offsetRange(), range.generation(), range.commitVersion(), target,
                        ReadTargetIdentities.sha256(target)),
                target.objectOffset(),
                target.objectLength());
    }

    private static void addStats(
            List<PhysicalReadStats> stats,
            ObjectSliceReadTarget target,
            long physicalBytesRead,
            long footerBytesRead,
            long returnedBytes) {
        stats.add(new PhysicalReadStats(
                ReadTargetIdentities.sha256(target),
                target.objectLength(),
                target.entryIndexRef().length(),
                Math.subtractExact(physicalBytesRead, footerBytesRead),
                footerBytesRead,
                returnedBytes));
    }

    private static void validateRanges(ReadRequest request, List<ResolvedRange> ranges) {
        if (ranges.isEmpty()) {
            throw new IllegalArgumentException("V2 compacted reader requires non-empty ranges");
        }
        long previousEnd = -1;
        boolean containsStart = false;
        for (ResolvedRange range : ranges) {
            ObjectSliceReadTarget target = requireTarget(range);
            ReadView targetView = target.physicalFormat().equals(CompactedObjectFormatV2.COMMITTED_PHYSICAL_FORMAT)
                    ? ReadView.COMMITTED
                    : ReadView.TOPIC_COMPACTED;
            if (range.generation() <= 0
                    || targetView != request.view()
                    || (previousEnd >= 0 && range.offsetRange().startOffset() != previousEnd)) {
                throw new CompactedObjectFormatException(
                        "V2 resolved ranges must match one semantic view and be gap-free");
            }
            containsStart |= range.offsetRange().contains(request.startOffset());
            previousEnd = range.offsetRange().endOffset();
        }
        if (!containsStart) {
            throw new IllegalArgumentException("startOffset is outside V2 resolved ranges");
        }
    }

    private static ObjectSliceReadTarget requireTarget(ResolvedRange range) {
        Objects.requireNonNull(range, "range");
        if (!(range.readTarget() instanceof ObjectSliceReadTarget target)
                || (!ReadTargetReaderKey.from(target).equals(NCP2_KEY)
                        && !ReadTargetReaderKey.from(target).equals(NTC2_KEY))
                || !target.logicalFormat().equals(CompactedObjectFormatV2.KAFKA_LOGICAL_FORMAT)
                || range.payloadFormat() != PayloadFormat.KAFKA_RECORD_BATCH) {
            throw new NereusException(
                    ErrorCode.UNSUPPORTED_READ_TARGET,
                    false,
                    "V2 compacted reader received a non-exact target");
        }
        return target;
    }

    private static void requireMetadata(
            ResolvedRange range,
            RangedCompactedObjectMetadata metadata,
            ReadView view) {
        if (metadata.view() != view
                || !metadata.sourceCoverage().equals(range.offsetRange())
                || metadata.payloadFormat() != range.payloadFormat()
                || metadata.sourceRecordCount() != range.recordCount()
                || metadata.entryCount() != range.entryCount()
                || metadata.logicalBytes() != range.logicalBytes()
                || (view == ReadView.COMMITTED && metadata.outputRecordCount() != range.recordCount())) {
            throw new CompactedObjectFormatException(
                    "V2 metadata does not match resolved generation index");
        }
    }

    private static ReadTargetReaderKey key(String physicalFormat) {
        return new ReadTargetReaderKey(
                ReadTargetType.OBJECT_SLICE,
                1,
                Optional.of(ObjectType.STREAM_COMPACTED_OBJECT),
                Optional.of(physicalFormat),
                Optional.of(CompactedObjectFormatV2.KAFKA_LOGICAL_FORMAT));
    }

    private static byte[] bytes(ByteBuffer supplied) {
        ByteBuffer value = supplied.asReadOnlyBuffer();
        byte[] result = new byte[value.remaining()];
        value.get(result);
        return result;
    }

    private static NereusException invariant(String message, Throwable failure) {
        return new NereusException(
                ErrorCode.METADATA_INVARIANT_VIOLATION, false, message, failure);
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
                throw new NereusException(ErrorCode.TIMEOUT, true, "V2 compacted read deadline expired");
            }
            return Duration.ofNanos(nanos);
        }
    }
}
