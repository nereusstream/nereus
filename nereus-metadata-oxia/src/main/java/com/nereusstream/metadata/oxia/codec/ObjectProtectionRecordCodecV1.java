/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import com.nereusstream.metadata.oxia.records.ObjectProtectionRecord;

public final class ObjectProtectionRecordCodecV1
        extends AbstractF4RecordCodecV1<ObjectProtectionRecord> {
    public ObjectProtectionRecordCodecV1() {
        super(ObjectProtectionRecord.class);
    }

    @Override
    public byte[] encode(ObjectProtectionRecord value) {
        try {
            F4Binary.Writer writer = writer();
            writer.writeString(value.objectKeyHash());
            writer.writeUnsignedShort(value.protectionTypeId());
            writer.writeString(value.referenceId());
            writer.writeString(value.ownerKey());
            writer.writeLong(value.ownerMetadataVersion());
            writer.writeString(value.ownerIdentitySha256());
            writer.writeLong(value.rootLifecycleEpoch());
            writer.writeLong(value.createdAtMillis());
            writer.writeLong(value.expiresAtMillis());
            writer.writeLong(value.metadataVersion());
            return writer.toByteArray();
        } catch (RuntimeException failure) {
            throw malformed(failure);
        }
    }

    @Override
    public ObjectProtectionRecord decode(byte[] bytes) {
        try {
            F4Binary.Reader reader = reader(bytes);
            ObjectProtectionRecord value = new ObjectProtectionRecord(
                    VERSION,
                    reader.readString("objectKeyHash"),
                    reader.readUnsignedShort("protectionTypeId"),
                    reader.readString("referenceId"),
                    reader.readString("ownerKey"),
                    reader.readLong("ownerMetadataVersion"),
                    reader.readString("ownerIdentitySha256"),
                    reader.readLong("rootLifecycleEpoch"),
                    reader.readLong("createdAtMillis"),
                    reader.readLong("expiresAtMillis"),
                    reader.readLong("metadataVersion"));
            reader.requireConsumed();
            return value;
        } catch (RuntimeException failure) {
            throw malformed(failure);
        }
    }
}
