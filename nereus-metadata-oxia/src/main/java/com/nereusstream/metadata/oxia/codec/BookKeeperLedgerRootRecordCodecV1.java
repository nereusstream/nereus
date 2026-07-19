/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import com.nereusstream.metadata.oxia.records.BookKeeperLedgerLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerRootRecord;

public final class BookKeeperLedgerRootRecordCodecV1 extends AbstractF4RecordCodecV1<BookKeeperLedgerRootRecord> {
    public BookKeeperLedgerRootRecordCodecV1() { super(BookKeeperLedgerRootRecord.class); }

    @Override
    public byte[] encode(BookKeeperLedgerRootRecord v) {
        try {
            F4Binary.Writer w = writer();
            w.writeString(v.ledgerIdentitySha256()); w.writeString(v.clusterAlias()); w.writeString(v.providerScopeSha256());
            w.writeLong(v.ledgerId()); w.writeString(v.streamId()); w.writeLong(v.segmentSequence());
            w.writeString(v.allocationId()); w.writeInt(v.allocationSlot()); w.writeString(v.configurationBindingSha256());
            w.writeString(v.ledgerIdNamespaceSha256()); w.writeOptional(v.lateCreateHazard()); w.writeString(v.writerId());
            w.writeString(v.writerRunIdHash()); w.writeLong(v.appendSessionEpoch()); w.writeString(v.fencingTokenHash());
            w.writeInt(v.ensembleSize()); w.writeInt(v.writeQuorumSize()); w.writeInt(v.ackQuorumSize());
            w.writeString(v.digestType()); w.writeString(v.customMetadataSha256()); w.writeUnsignedShort(v.lifecycle().wireId());
            w.writeLong(v.lifecycleEpoch()); w.writeLong(v.createdAtMillis()); w.writeLong(v.activatedAtMillis());
            w.writeLong(v.sealStartedAtMillis()); w.writeLong(v.sealedAtMillis()); w.writeLong(v.sealedLastEntryId());
            w.writeLong(v.sealedLength()); w.writeString(v.sealReason()); w.writeString(v.gcAttemptId());
            w.writeString(v.referenceSetSha256()); w.writeLong(v.markedAtMillis()); w.writeLong(v.deleteNotBeforeMillis());
            w.writeLong(v.deleteStartedAtMillis()); w.writeLong(v.firstAbsentAtMillis()); w.writeLong(v.deletedAtMillis());
            w.writeString(v.stateReason()); w.writeLong(v.metadataVersion()); return w.toByteArray();
        } catch (RuntimeException failure) { throw malformed(failure); }
    }

    @Override
    public BookKeeperLedgerRootRecord decode(byte[] bytes) {
        try {
            F4Binary.Reader r = reader(bytes);
            BookKeeperLedgerRootRecord v = new BookKeeperLedgerRootRecord(VERSION,
                    r.readString("ledgerIdentitySha256"), r.readString("clusterAlias"),
                    r.readString("providerScopeSha256"), r.readLong("ledgerId"), r.readString("streamId"),
                    r.readLong("segmentSequence"), r.readString("allocationId"), r.readInt("allocationSlot"),
                    r.readString("configurationBindingSha256"), r.readString("ledgerIdNamespaceSha256"),
                    r.readOptional("lateCreateHazard"), r.readString("writerId"), r.readString("writerRunIdHash"),
                    r.readLong("appendSessionEpoch"), r.readString("fencingTokenHash"), r.readInt("ensembleSize"),
                    r.readInt("writeQuorumSize"), r.readInt("ackQuorumSize"), r.readString("digestType"),
                    r.readString("customMetadataSha256"),
                    BookKeeperLedgerLifecycle.fromWireId(r.readUnsignedShort("lifecycle")),
                    r.readLong("lifecycleEpoch"), r.readLong("createdAtMillis"), r.readLong("activatedAtMillis"),
                    r.readLong("sealStartedAtMillis"), r.readLong("sealedAtMillis"),
                    r.readLong("sealedLastEntryId"), r.readLong("sealedLength"), r.readString("sealReason"),
                    r.readString("gcAttemptId"), r.readString("referenceSetSha256"), r.readLong("markedAtMillis"),
                    r.readLong("deleteNotBeforeMillis"), r.readLong("deleteStartedAtMillis"),
                    r.readLong("firstAbsentAtMillis"), r.readLong("deletedAtMillis"), r.readString("stateReason"),
                    r.readLong("metadataVersion")); r.requireConsumed(); return v;
        } catch (RuntimeException failure) { throw malformed(failure); }
    }
}
