/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import com.nereusstream.metadata.oxia.records.MaterializationStreamRegistrationRecord;

public final class MaterializationStreamRegistrationRecordCodecV1
        extends AbstractF4RecordCodecV1<MaterializationStreamRegistrationRecord> {
    public MaterializationStreamRegistrationRecordCodecV1() {
        super(MaterializationStreamRegistrationRecord.class);
    }

    @Override
    public byte[] encode(MaterializationStreamRegistrationRecord value) {
        try {
            F4Binary.Writer writer = writer();
            writer.writeString(value.streamId());
            writer.writeString(value.projectionRef());
            writer.writeString(value.projectionIdentitySha256());
            writer.writeString(value.storageProfile());
            writer.writeLong(value.registeredAtMillis());
            writer.writeLong(value.lastHintCommitVersion());
            writer.writeLong(value.updatedAtMillis());
            writer.writeLong(value.metadataVersion());
            return writer.toByteArray();
        } catch (RuntimeException failure) {
            throw malformed(failure);
        }
    }

    @Override
    public MaterializationStreamRegistrationRecord decode(byte[] bytes) {
        try {
            F4Binary.Reader reader = reader(bytes);
            MaterializationStreamRegistrationRecord value = new MaterializationStreamRegistrationRecord(
                    VERSION,
                    reader.readString("streamId"),
                    reader.readString("projectionRef"),
                    reader.readString("projectionIdentitySha256"),
                    reader.readString("storageProfile"),
                    reader.readLong("registeredAtMillis"),
                    reader.readLong("lastHintCommitVersion"),
                    reader.readLong("updatedAtMillis"),
                    reader.readLong("metadataVersion"));
            reader.requireConsumed();
            return value;
        } catch (RuntimeException failure) {
            throw malformed(failure);
        }
    }
}
