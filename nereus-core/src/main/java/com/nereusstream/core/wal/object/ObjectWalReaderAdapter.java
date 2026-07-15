/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.wal.object;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.ResolvedObjectRange;
import com.nereusstream.api.ResolvedRange;
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
    @Override public CompletableFuture<WalReadResult> readWithStats(long startOffset, List<ResolvedRange> ranges, ReadOptions options) {
        try { return reader.readWithStats(startOffset, ranges.stream().map(ResolvedObjectRange::from).toList(), options); }
        catch (IllegalArgumentException e) { return NereusException.failedFuture(ErrorCode.UNSUPPORTED_READ_TARGET,
                false, "Object WAL reader received a non-object target", e); }
    }
    @Override public long reservationBytes(ResolvedRange range) {
        ResolvedObjectRange object = ResolvedObjectRange.from(range);
        try { return Math.addExact(object.objectLength(), object.entryIndexRef().length()); }
        catch (ArithmeticException e) { throw new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION,
                false, "resolved object reservation overflows", e); }
    }
}
