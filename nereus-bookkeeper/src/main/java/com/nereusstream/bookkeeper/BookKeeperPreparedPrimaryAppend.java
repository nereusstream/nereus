/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.api.StreamId;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.target.BookKeeperEntryMapping;
import com.nereusstream.core.wal.PreparedPrimaryAppend;
import com.nereusstream.core.wal.PrimaryAppendRequest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/** Exact owned entry bytes prepared before ledger/range allocation; close releases every buffer once. */
public final class BookKeeperPreparedPrimaryAppend implements PreparedPrimaryAppend, AutoCloseable {
    private final PrimaryAppendRequest request;
    private final List<ByteBuf> entries;
    private final BookKeeperEntryMapping entryMapping;
    private final long physicalBytes;
    private final AtomicBoolean closed = new AtomicBoolean();

    public BookKeeperPreparedPrimaryAppend(PrimaryAppendRequest request) {
        this.request = Objects.requireNonNull(request, "request");
        this.entryMapping = request.batch().payloadFormat() == PayloadFormat.KAFKA_RECORD_BATCH
                ? BookKeeperEntryMapping.RANGED_NEREUS_ENTRY_V1
                : BookKeeperEntryMapping.ONE_NEREUS_ENTRY_PER_BOOKKEEPER_ENTRY;
        this.entries = request.batch().entries().stream()
                .map(entry -> entryMapping == BookKeeperEntryMapping.RANGED_NEREUS_ENTRY_V1
                        ? Unpooled.wrappedBuffer(BookKeeperRangedEntryCodecV1.encode(entry))
                        : Unpooled.wrappedBuffer(entry.payload()))
                .toList();
        long total = 0;
        try {
            for (ByteBuf entry : entries) total = Math.addExact(total, entry.readableBytes());
        } catch (Throwable failure) {
            entries.forEach(ByteBuf::release);
            throw failure;
        }
        physicalBytes = total;
    }

    public PrimaryAppendRequest request() { return request; }
    public List<ByteBuf> retainedEntries() {
        requireOpen();
        return entries.stream().map(ByteBuf::retainedDuplicate).toList();
    }
    public long physicalBytes() { return physicalBytes; }
    public BookKeeperEntryMapping entryMapping() { return entryMapping; }
    public com.nereusstream.api.Checksum rangeChecksum(long firstEntryId) {
        requireOpen();
        return BookKeeperRangeChecksums.compute(firstEntryId, entries);
    }
    @Override public StreamId streamId() { return request.streamId(); }
    @Override public int recordCount() { return request.batch().recordCount(); }
    @Override public int entryCount() { return entries.size(); }
    @Override public long logicalBytes() { return request.batch().entries().stream()
            .mapToLong(entry -> entry.payload().length).reduce(0, Math::addExact); }
    @Override public long reservedBytes() { return physicalBytes; }

    @Override public void close() {
        if (closed.compareAndSet(false, true)) entries.forEach(ByteBuf::release);
    }
    private void requireOpen() {
        if (closed.get()) throw new IllegalStateException("prepared BookKeeper append is closed");
    }
}
