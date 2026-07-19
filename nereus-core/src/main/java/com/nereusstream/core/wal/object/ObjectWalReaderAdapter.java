/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.wal.object;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.PhysicalReadResult;
import com.nereusstream.api.PhysicalReadStats;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.ResolvedObjectRange;
import com.nereusstream.api.ResolvedRange;
import com.nereusstream.api.StreamId;
import com.nereusstream.core.read.ReadTargetReaderKey;
import com.nereusstream.core.wal.PrimaryWalReader;
import com.nereusstream.objectstore.wal.WalObjectReader;
import com.nereusstream.objectstore.wal.WalReadResult;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
public final class ObjectWalReaderAdapter implements PrimaryWalReader {
    public static final ReadTargetReaderKey KEY = new ReadTargetReaderKey(
            com.nereusstream.api.target.ReadTargetType.OBJECT_SLICE,
            1,
            Optional.of(ObjectType.MULTI_STREAM_WAL_OBJECT),
            Optional.of("WAL_OBJECT_V1"));

    private final WalObjectReader reader;
    public ObjectWalReaderAdapter(WalObjectReader reader) { this.reader = Objects.requireNonNull(reader); }
    @Override public ReadTargetReaderKey key() { return KEY; }
    @Override public CompletableFuture<PhysicalReadResult> readPhysicalWithStats(
            StreamId streamId, long startOffset, List<ResolvedRange> ranges, ReadOptions options) {
        Objects.requireNonNull(streamId, "streamId");
        try {
            List<ResolvedObjectRange> objectRanges = ranges.stream().map(ResolvedObjectRange::from).toList();
            return reader.readWithStats(startOffset, objectRanges, options)
                    .thenApply(result -> genericResult(objectRanges, result));
        }
        catch (IllegalArgumentException e) { return NereusException.failedFuture(ErrorCode.UNSUPPORTED_READ_TARGET,
                false, "Object WAL reader received a non-object target", e); }
    }

    private static PhysicalReadResult genericResult(
            List<ResolvedObjectRange> ranges,
            WalReadResult result) {
        List<PhysicalReadStats> stats = result.sliceStats().stream().map(value -> {
            ResolvedObjectRange range = ranges.stream()
                    .filter(candidate -> candidate.objectId().equals(value.objectId())
                            && candidate.objectOffset() == value.objectOffset()
                            && candidate.objectLength() == value.fullSlicePayloadBytes()
                            && candidate.entryIndexRef().length() == value.entryIndexBytes())
                    .findFirst()
                    .orElseThrow(() -> new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                            "Object reader reported an unknown resolved source"));
            return new PhysicalReadStats(
                    com.nereusstream.api.ReadTargetIdentities.sha256(range.readTarget()),
                    value.fullSlicePayloadBytes(),
                    value.entryIndexBytes(),
                    value.downloadedPayloadBytes(),
                    value.downloadedEntryIndexBytes(),
                    value.returnedPayloadBytes());
        }).toList();
        return new PhysicalReadResult(result.batches(), stats);
    }
    @Override public long reservationBytes(ResolvedRange range) {
        ResolvedObjectRange object = ResolvedObjectRange.from(range);
        try { return Math.addExact(object.objectLength(), object.entryIndexRef().length()); }
        catch (ArithmeticException e) { throw new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION,
                false, "resolved object reservation overflows", e); }
    }
}
