/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import com.nereusstream.metadata.oxia.records.BookKeeperWriterLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperWriterStateRecord;

public final class BookKeeperWriterStateRecordCodecV1 extends AbstractF4RecordCodecV1<BookKeeperWriterStateRecord> {
    public BookKeeperWriterStateRecordCodecV1() { super(BookKeeperWriterStateRecord.class); }

    @Override
    public byte[] encode(BookKeeperWriterStateRecord v) {
        try {
            F4Binary.Writer w = writer();
            w.writeString(v.streamId()); w.writeString(v.clusterAlias()); w.writeString(v.configurationBindingSha256());
            w.writeUnsignedShort(v.lifecycle().wireId()); w.writeLong(v.writerStateEpoch()); w.writeString(v.writerId());
            w.writeString(v.writerRunIdHash()); w.writeLong(v.appendSessionEpoch()); w.writeString(v.fencingTokenHash());
            w.writeLong(v.appendSessionLeaseVersion()); w.writeLong(v.nextSegmentSequence()); w.writeString(v.allocationId());
            w.writeLong(v.allocationLedgerId()); w.writeLong(v.activeSegmentSequence()); w.writeLong(v.activeLedgerId());
            w.writeLong(v.activeLedgerRootEpoch()); w.writeLong(v.nextEntryId()); w.writeLong(v.activePhysicalBytes());
            w.writeInt(v.activeAppendRangeCount()); w.writeString(v.activeReservationId()); w.writeLong(v.openedAtMillis());
            w.writeLong(v.updatedAtMillis()); w.writeString(v.stateReason()); w.writeLong(v.metadataVersion());
            return w.toByteArray();
        } catch (RuntimeException failure) { throw malformed(failure); }
    }

    @Override
    public BookKeeperWriterStateRecord decode(byte[] bytes) {
        try {
            F4Binary.Reader r = reader(bytes);
            BookKeeperWriterStateRecord v = new BookKeeperWriterStateRecord(VERSION, r.readString("streamId"),
                    r.readString("clusterAlias"), r.readString("configurationBindingSha256"),
                    BookKeeperWriterLifecycle.fromWireId(r.readUnsignedShort("lifecycle")),
                    r.readLong("writerStateEpoch"), r.readString("writerId"), r.readString("writerRunIdHash"),
                    r.readLong("appendSessionEpoch"), r.readString("fencingTokenHash"),
                    r.readLong("appendSessionLeaseVersion"), r.readLong("nextSegmentSequence"),
                    r.readString("allocationId"), r.readLong("allocationLedgerId"),
                    r.readLong("activeSegmentSequence"), r.readLong("activeLedgerId"),
                    r.readLong("activeLedgerRootEpoch"), r.readLong("nextEntryId"),
                    r.readLong("activePhysicalBytes"), r.readInt("activeAppendRangeCount"),
                    r.readString("activeReservationId"), r.readLong("openedAtMillis"),
                    r.readLong("updatedAtMillis"), r.readString("stateReason"), r.readLong("metadataVersion"));
            r.requireConsumed(); return v;
        } catch (RuntimeException failure) { throw malformed(failure); }
    }
}
