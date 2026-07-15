/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import com.nereusstream.metadata.oxia.records.MaterializationCheckpointRecord;

public final class MaterializationCheckpointRecordCodecV1
        extends AbstractF4RecordCodecV1<MaterializationCheckpointRecord> {
    public MaterializationCheckpointRecordCodecV1() {
        super(MaterializationCheckpointRecord.class);
    }

    @Override
    public byte[] encode(MaterializationCheckpointRecord value) {
        try {
            F4Binary.Writer writer = writer();
            writer.writeString(value.streamId());
            writer.writeString(value.policyId());
            writer.writeLong(value.policyVersion());
            writer.writeString(value.policySha256());
            writer.writeLong(value.contiguousCoveredOffset());
            writer.writeLong(value.observedCommitVersion());
            writer.writeLong(value.lastTaskSequence());
            writer.writeString(value.lastTaskId());
            writer.writeLong(value.updatedAtMillis());
            writer.writeLong(value.metadataVersion());
            return writer.toByteArray();
        } catch (RuntimeException failure) {
            throw malformed(failure);
        }
    }

    @Override
    public MaterializationCheckpointRecord decode(byte[] bytes) {
        try {
            F4Binary.Reader reader = reader(bytes);
            MaterializationCheckpointRecord value = new MaterializationCheckpointRecord(
                    VERSION,
                    reader.readString("streamId"),
                    reader.readString("policyId"),
                    reader.readLong("policyVersion"),
                    reader.readString("policySha256"),
                    reader.readLong("contiguousCoveredOffset"),
                    reader.readLong("observedCommitVersion"),
                    reader.readLong("lastTaskSequence"),
                    reader.readString("lastTaskId"),
                    reader.readLong("updatedAtMillis"),
                    reader.readLong("metadataVersion"));
            reader.requireConsumed();
            return value;
        } catch (RuntimeException failure) {
            throw malformed(failure);
        }
    }
}
