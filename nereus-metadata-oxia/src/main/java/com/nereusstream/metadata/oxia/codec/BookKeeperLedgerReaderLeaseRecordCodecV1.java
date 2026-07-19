/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import com.nereusstream.metadata.oxia.records.BookKeeperLedgerReaderLeaseRecord;

public final class BookKeeperLedgerReaderLeaseRecordCodecV1
        extends AbstractF4RecordCodecV1<BookKeeperLedgerReaderLeaseRecord> {
    public BookKeeperLedgerReaderLeaseRecordCodecV1() { super(BookKeeperLedgerReaderLeaseRecord.class); }

    @Override
    public byte[] encode(BookKeeperLedgerReaderLeaseRecord v) {
        try {
            F4Binary.Writer w = writer();
            w.writeString(v.ledgerIdentitySha256()); w.writeLong(v.ledgerId()); w.writeLong(v.rootLifecycleEpoch());
            w.writeInt(v.readerSlot()); w.writeString(v.processRunId()); w.writeLong(v.leaseEpoch());
            w.writeLong(v.acquiredAtMillis()); w.writeLong(v.expiresAtMillis()); w.writeLong(v.metadataVersion());
            return w.toByteArray();
        } catch (RuntimeException failure) { throw malformed(failure); }
    }

    @Override
    public BookKeeperLedgerReaderLeaseRecord decode(byte[] bytes) {
        try {
            F4Binary.Reader r = reader(bytes);
            BookKeeperLedgerReaderLeaseRecord v = new BookKeeperLedgerReaderLeaseRecord(VERSION,
                    r.readString("ledgerIdentitySha256"), r.readLong("ledgerId"),
                    r.readLong("rootLifecycleEpoch"), r.readInt("readerSlot"), r.readString("processRunId"),
                    r.readLong("leaseEpoch"), r.readLong("acquiredAtMillis"), r.readLong("expiresAtMillis"),
                    r.readLong("metadataVersion")); r.requireConsumed(); return v;
        } catch (RuntimeException failure) { throw malformed(failure); }
    }
}
