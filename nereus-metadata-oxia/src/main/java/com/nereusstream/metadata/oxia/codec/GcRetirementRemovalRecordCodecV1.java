/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import com.nereusstream.metadata.oxia.records.GcRetirementRemovalRecord;

public final class GcRetirementRemovalRecordCodecV1
        extends AbstractF4RecordCodecV1<GcRetirementRemovalRecord> {
    public GcRetirementRemovalRecordCodecV1() {
        super(GcRetirementRemovalRecord.class);
    }

    @Override
    public byte[] encode(GcRetirementRemovalRecord value) {
        try {
            F4Binary.Writer writer = writer();
            writer.writeString(value.objectKeyHash());
            writer.writeString(value.gcAttemptId());
            writer.writeString(value.removalType());
            writer.writeString(value.removalKey());
            writer.writeLong(value.removalMetadataVersion());
            writer.writeString(value.removalDurableValueSha256());
            writer.writeLong(value.metadataVersion());
            return writer.toByteArray();
        } catch (RuntimeException failure) {
            throw malformed(failure);
        }
    }

    @Override
    public GcRetirementRemovalRecord decode(byte[] bytes) {
        try {
            F4Binary.Reader reader = reader(bytes);
            GcRetirementRemovalRecord value = new GcRetirementRemovalRecord(
                    VERSION,
                    reader.readString("objectKeyHash"),
                    reader.readString("gcAttemptId"),
                    reader.readString("removalType"),
                    reader.readString("removalKey"),
                    reader.readLong("removalMetadataVersion"),
                    reader.readString("removalDurableValueSha256"),
                    reader.readLong("metadataVersion"));
            reader.requireConsumed();
            return value;
        } catch (RuntimeException failure) {
            throw malformed(failure);
        }
    }
}
