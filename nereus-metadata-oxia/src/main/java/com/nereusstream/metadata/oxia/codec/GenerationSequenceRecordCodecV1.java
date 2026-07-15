/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import com.nereusstream.metadata.oxia.records.GenerationSequenceRecord;

public final class GenerationSequenceRecordCodecV1 extends AbstractF4RecordCodecV1<GenerationSequenceRecord> {
    public GenerationSequenceRecordCodecV1() {
        super(GenerationSequenceRecord.class);
    }

    @Override
    public byte[] encode(GenerationSequenceRecord value) {
        try {
            F4Binary.Writer writer = writer();
            writer.writeString(value.streamId());
            writer.writeUnsignedShort(value.readViewId());
            writer.writeLong(value.lastAllocatedGeneration());
            writer.writeLong(value.allocationSequence());
            writer.writeString(value.lastPublicationId());
            writer.writeLong(value.updatedAtMillis());
            writer.writeLong(value.metadataVersion());
            return writer.toByteArray();
        } catch (RuntimeException failure) {
            throw malformed(failure);
        }
    }

    @Override
    public GenerationSequenceRecord decode(byte[] bytes) {
        try {
            F4Binary.Reader reader = reader(bytes);
            GenerationSequenceRecord value = new GenerationSequenceRecord(
                    VERSION,
                    reader.readString("streamId"),
                    reader.readUnsignedShort("readViewId"),
                    reader.readLong("lastAllocatedGeneration"),
                    reader.readLong("allocationSequence"),
                    reader.readString("lastPublicationId"),
                    reader.readLong("updatedAtMillis"),
                    reader.readLong("metadataVersion"));
            reader.requireConsumed();
            return value;
        } catch (RuntimeException failure) {
            throw malformed(failure);
        }
    }
}
