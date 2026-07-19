/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import com.nereusstream.metadata.oxia.records.AppendReservationLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperAppendReservationRecord;

public final class BookKeeperAppendReservationRecordCodecV1
        extends AbstractF4RecordCodecV1<BookKeeperAppendReservationRecord> {
    public BookKeeperAppendReservationRecordCodecV1() { super(BookKeeperAppendReservationRecord.class); }

    @Override
    public byte[] encode(BookKeeperAppendReservationRecord v) {
        try {
            F4Binary.Writer w = writer();
            w.writeString(v.reservationId()); w.writeString(v.appendAttemptId()); w.writeString(v.streamId());
            w.writeString(v.writerId()); w.writeString(v.writerRunIdHash()); w.writeLong(v.appendSessionEpoch());
            w.writeString(v.fencingTokenHash()); w.writeLong(v.writerStateEpoch()); w.writeLong(v.ledgerId());
            w.writeLong(v.ledgerRootEpoch()); w.writeInt(v.ledgerRangeSlot()); w.writeLong(v.firstEntryId());
            w.writeInt(v.entryCount()); w.writeString(v.rangeChecksumSha256()); w.writeLong(v.expectedStartOffset());
            w.writeString(v.payloadFormat()); w.writeInt(v.recordCount()); w.writeLong(v.logicalBytes());
            w.writeLong(v.physicalBytes()); F4Binary.writeSchemaRefs(w, v.schemaRefs());
            w.writeString(v.projectionIdentity()); w.writeLong(v.minEventTimeMillis()); w.writeLong(v.maxEventTimeMillis());
            w.writeUnsignedShort(v.lifecycle().wireId()); w.writeString(v.commitId()); w.writeString(v.commitKey());
            w.writeLong(v.commitMetadataVersion()); w.writeString(v.commitRecordSha256()); w.writeLong(v.createdAtMillis());
            w.writeLong(v.updatedAtMillis()); w.writeString(v.stateReason()); w.writeLong(v.metadataVersion());
            return w.toByteArray();
        } catch (RuntimeException failure) { throw malformed(failure); }
    }

    @Override
    public BookKeeperAppendReservationRecord decode(byte[] bytes) {
        try {
            F4Binary.Reader r = reader(bytes);
            BookKeeperAppendReservationRecord v = new BookKeeperAppendReservationRecord(VERSION,
                    r.readString("reservationId"), r.readString("appendAttemptId"), r.readString("streamId"),
                    r.readString("writerId"), r.readString("writerRunIdHash"), r.readLong("appendSessionEpoch"),
                    r.readString("fencingTokenHash"), r.readLong("writerStateEpoch"), r.readLong("ledgerId"),
                    r.readLong("ledgerRootEpoch"), r.readInt("ledgerRangeSlot"), r.readLong("firstEntryId"),
                    r.readInt("entryCount"), r.readString("rangeChecksumSha256"), r.readLong("expectedStartOffset"),
                    r.readString("payloadFormat"), r.readInt("recordCount"), r.readLong("logicalBytes"),
                    r.readLong("physicalBytes"), F4Binary.readSchemaRefs(r, "schemaRefs"),
                    r.readString("projectionIdentity"), r.readLong("minEventTimeMillis"),
                    r.readLong("maxEventTimeMillis"),
                    AppendReservationLifecycle.fromWireId(r.readUnsignedShort("lifecycle")),
                    r.readString("commitId"), r.readString("commitKey"), r.readLong("commitMetadataVersion"),
                    r.readString("commitRecordSha256"), r.readLong("createdAtMillis"), r.readLong("updatedAtMillis"),
                    r.readString("stateReason"), r.readLong("metadataVersion")); r.requireConsumed(); return v;
        } catch (RuntimeException failure) { throw malformed(failure); }
    }
}
