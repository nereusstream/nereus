/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import com.nereusstream.metadata.oxia.records.AllocationSlotLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperAllocationSlotRecord;

public final class BookKeeperAllocationSlotRecordCodecV1 extends AbstractF4RecordCodecV1<BookKeeperAllocationSlotRecord> {
    public BookKeeperAllocationSlotRecordCodecV1() { super(BookKeeperAllocationSlotRecord.class); }

    @Override
    public byte[] encode(BookKeeperAllocationSlotRecord v) {
        try {
            F4Binary.Writer w = writer();
            w.writeInt(v.slot()); w.writeString(v.allocationId()); w.writeString(v.streamId());
            w.writeLong(v.candidateLedgerId()); w.writeString(v.ledgerIdentitySha256());
            w.writeString(v.configurationBindingSha256()); w.writeUnsignedShort(v.lifecycle().wireId());
            w.writeLong(v.createdAtMillis()); w.writeLong(v.updatedAtMillis()); w.writeLong(v.metadataVersion());
            return w.toByteArray();
        } catch (RuntimeException failure) { throw malformed(failure); }
    }

    @Override
    public BookKeeperAllocationSlotRecord decode(byte[] bytes) {
        try {
            F4Binary.Reader r = reader(bytes);
            BookKeeperAllocationSlotRecord v = new BookKeeperAllocationSlotRecord(VERSION, r.readInt("slot"),
                    r.readString("allocationId"), r.readString("streamId"), r.readLong("candidateLedgerId"),
                    r.readString("ledgerIdentitySha256"), r.readString("configurationBindingSha256"),
                    AllocationSlotLifecycle.fromWireId(r.readUnsignedShort("lifecycle")),
                    r.readLong("createdAtMillis"), r.readLong("updatedAtMillis"), r.readLong("metadataVersion"));
            r.requireConsumed(); return v;
        } catch (RuntimeException failure) { throw malformed(failure); }
    }
}
