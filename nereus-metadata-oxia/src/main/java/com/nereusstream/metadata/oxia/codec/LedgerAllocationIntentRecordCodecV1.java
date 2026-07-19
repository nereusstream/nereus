/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import com.nereusstream.metadata.oxia.records.LedgerAllocationIntentRecord;
import com.nereusstream.metadata.oxia.records.LedgerAllocationLifecycle;

public final class LedgerAllocationIntentRecordCodecV1 extends AbstractF4RecordCodecV1<LedgerAllocationIntentRecord> {
    public LedgerAllocationIntentRecordCodecV1() { super(LedgerAllocationIntentRecord.class); }

    @Override
    public byte[] encode(LedgerAllocationIntentRecord v) {
        try {
            F4Binary.Writer w = writer();
            w.writeString(v.allocationId()); w.writeString(v.streamId()); w.writeLong(v.segmentSequence());
            w.writeString(v.clusterAlias()); w.writeLong(v.candidateLedgerId()); w.writeInt(v.allocationSlot());
            w.writeString(v.configurationBindingSha256()); w.writeString(v.writerId()); w.writeString(v.writerRunIdHash());
            w.writeLong(v.appendSessionEpoch()); w.writeString(v.fencingTokenHash()); w.writeLong(v.writerStateEpoch());
            w.writeUnsignedShort(v.lifecycle().wireId()); w.writeOptional(v.lateCreateHazard());
            w.writeString(v.bookKeeperMetadataSha256()); w.writeLong(v.createdAtMillis()); w.writeLong(v.updatedAtMillis());
            w.writeString(v.stateReason()); w.writeLong(v.metadataVersion()); return w.toByteArray();
        } catch (RuntimeException failure) { throw malformed(failure); }
    }

    @Override
    public LedgerAllocationIntentRecord decode(byte[] bytes) {
        try {
            F4Binary.Reader r = reader(bytes);
            LedgerAllocationIntentRecord v = new LedgerAllocationIntentRecord(VERSION, r.readString("allocationId"),
                    r.readString("streamId"), r.readLong("segmentSequence"), r.readString("clusterAlias"),
                    r.readLong("candidateLedgerId"), r.readInt("allocationSlot"),
                    r.readString("configurationBindingSha256"), r.readString("writerId"),
                    r.readString("writerRunIdHash"), r.readLong("appendSessionEpoch"),
                    r.readString("fencingTokenHash"), r.readLong("writerStateEpoch"),
                    LedgerAllocationLifecycle.fromWireId(r.readUnsignedShort("lifecycle")),
                    r.readOptional("lateCreateHazard"), r.readString("bookKeeperMetadataSha256"),
                    r.readLong("createdAtMillis"), r.readLong("updatedAtMillis"), r.readString("stateReason"),
                    r.readLong("metadataVersion")); r.requireConsumed(); return v;
        } catch (RuntimeException failure) { throw malformed(failure); }
    }
}
