/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.records.PhysicalObjectRootRecord;

public final class PhysicalObjectRootRecordCodecV1
        extends AbstractF4RecordCodecV1<PhysicalObjectRootRecord> {
    public PhysicalObjectRootRecordCodecV1() {
        super(PhysicalObjectRootRecord.class);
    }

    @Override
    public byte[] encode(PhysicalObjectRootRecord value) {
        try {
            F4Binary.Writer writer = writer();
            writer.writeString(value.objectKeyHash());
            writer.writeString(value.objectKey());
            writer.writeString(value.objectId());
            writer.writeUnsignedShort(value.objectKindId());
            writer.writeLong(value.objectLength());
            writer.writeString(value.storageChecksumType());
            writer.writeString(value.storageChecksumValue());
            writer.writeString(value.contentSha256());
            writer.writeString(value.etag());
            writer.writeUnsignedShort(value.lifecycle().wireId());
            writer.writeLong(value.lifecycleEpoch());
            writer.writeLong(value.createdAtMillis());
            writer.writeLong(value.orphanNotBeforeMillis());
            writer.writeString(value.gcAttemptId());
            writer.writeString(value.referenceSetSha256());
            writer.writeLong(value.markedAtMillis());
            writer.writeLong(value.deleteNotBeforeMillis());
            writer.writeLong(value.deleteStartedAtMillis());
            writer.writeLong(value.deletedAtMillis());
            writer.writeLong(value.tombstoneFirstAbsentAtMillis());
            writer.writeString(value.tombstoneProofSha256());
            writer.writeString(value.stateReason());
            writer.writeLong(value.metadataVersion());
            return writer.toByteArray();
        } catch (RuntimeException failure) {
            throw malformed(failure);
        }
    }

    @Override
    public PhysicalObjectRootRecord decode(byte[] bytes) {
        try {
            F4Binary.Reader reader = reader(bytes);
            PhysicalObjectRootRecord value = new PhysicalObjectRootRecord(
                    VERSION,
                    reader.readString("objectKeyHash"),
                    reader.readString("objectKey"),
                    reader.readString("objectId"),
                    reader.readUnsignedShort("objectKindId"),
                    reader.readLong("objectLength"),
                    reader.readString("storageChecksumType"),
                    reader.readString("storageChecksumValue"),
                    reader.readString("contentSha256"),
                    reader.readString("etag"),
                    PhysicalObjectLifecycle.fromWireId(reader.readUnsignedShort("lifecycle")),
                    reader.readLong("lifecycleEpoch"),
                    reader.readLong("createdAtMillis"),
                    reader.readLong("orphanNotBeforeMillis"),
                    reader.readString("gcAttemptId"),
                    reader.readString("referenceSetSha256"),
                    reader.readLong("markedAtMillis"),
                    reader.readLong("deleteNotBeforeMillis"),
                    reader.readLong("deleteStartedAtMillis"),
                    reader.readLong("deletedAtMillis"),
                    reader.readLong("tombstoneFirstAbsentAtMillis"),
                    reader.readString("tombstoneProofSha256"),
                    reader.readString("stateReason"),
                    reader.readLong("metadataVersion"));
            reader.requireConsumed();
            return value;
        } catch (RuntimeException failure) {
            throw malformed(failure);
        }
    }
}
