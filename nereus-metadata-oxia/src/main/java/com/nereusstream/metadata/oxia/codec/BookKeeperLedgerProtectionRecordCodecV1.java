/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import com.nereusstream.metadata.oxia.records.BookKeeperLedgerProtectionRecord;
import com.nereusstream.metadata.oxia.records.ProtectionLifecycle;

public final class BookKeeperLedgerProtectionRecordCodecV1
        extends AbstractF4RecordCodecV1<BookKeeperLedgerProtectionRecord> {
    public BookKeeperLedgerProtectionRecordCodecV1() { super(BookKeeperLedgerProtectionRecord.class); }

    @Override
    public byte[] encode(BookKeeperLedgerProtectionRecord v) {
        try {
            F4Binary.Writer w = writer();
            w.writeString(v.ledgerIdentitySha256()); w.writeString(v.clusterAlias()); w.writeLong(v.ledgerId());
            w.writeLong(v.rootLifecycleEpoch()); w.writeInt(v.ledgerRangeSlot()); w.writeInt(v.protectionSlot());
            w.writeUnsignedShort(v.protectionTypeId()); w.writeString(v.referenceId()); w.writeLong(v.firstEntryId());
            w.writeInt(v.entryCount()); w.writeString(v.rangeChecksumSha256()); w.writeString(v.streamId());
            w.writeLong(v.offsetStart()); w.writeLong(v.offsetEnd()); w.writeLong(v.commitVersion());
            w.writeString(v.ownerKey()); w.writeLong(v.ownerMetadataVersion()); w.writeString(v.ownerIdentitySha256());
            w.writeUnsignedShort(v.lifecycle().wireId()); w.writeLong(v.createdAtMillis());
            w.writeLong(v.expiresAtMillis()); w.writeLong(v.metadataVersion()); return w.toByteArray();
        } catch (RuntimeException failure) { throw malformed(failure); }
    }

    @Override
    public BookKeeperLedgerProtectionRecord decode(byte[] bytes) {
        try {
            F4Binary.Reader r = reader(bytes);
            BookKeeperLedgerProtectionRecord v = new BookKeeperLedgerProtectionRecord(VERSION,
                    r.readString("ledgerIdentitySha256"), r.readString("clusterAlias"), r.readLong("ledgerId"),
                    r.readLong("rootLifecycleEpoch"), r.readInt("ledgerRangeSlot"), r.readInt("protectionSlot"),
                    r.readUnsignedShort("protectionTypeId"), r.readString("referenceId"), r.readLong("firstEntryId"),
                    r.readInt("entryCount"), r.readString("rangeChecksumSha256"), r.readString("streamId"),
                    r.readLong("offsetStart"), r.readLong("offsetEnd"), r.readLong("commitVersion"),
                    r.readString("ownerKey"), r.readLong("ownerMetadataVersion"),
                    r.readString("ownerIdentitySha256"),
                    ProtectionLifecycle.fromWireId(r.readUnsignedShort("lifecycle")),
                    r.readLong("createdAtMillis"), r.readLong("expiresAtMillis"), r.readLong("metadataVersion"));
            r.requireConsumed(); return v;
        } catch (RuntimeException failure) { throw malformed(failure); }
    }
}
