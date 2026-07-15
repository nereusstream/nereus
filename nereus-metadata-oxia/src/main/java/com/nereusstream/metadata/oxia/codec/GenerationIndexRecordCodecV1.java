/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;

public final class GenerationIndexRecordCodecV1
        extends AbstractF4RecordCodecV1<GenerationIndexRecord> {
    public GenerationIndexRecordCodecV1() {
        super(GenerationIndexRecord.class);
    }

    @Override
    public byte[] encode(GenerationIndexRecord value) {
        try {
            F4Binary.Writer writer = writer();
            writer.writeString(value.streamId());
            writer.writeUnsignedShort(value.readViewId());
            writer.writeLong(value.offsetStart());
            writer.writeLong(value.offsetEnd());
            writer.writeLong(value.generation());
            writer.writeString(value.publicationId());
            writer.writeString(value.taskId());
            writer.writeUnsignedShort(value.lifecycle().wireId());
            writer.writeString(value.sourceSetSha256());
            writer.writeString(value.policySha256());
            F4Binary.writeReadTarget(writer, value.readTarget());
            writer.writeString(value.targetIdentitySha256());
            writer.writeString(value.materializationPolicySha256());
            writer.writeString(value.payloadFormat());
            writer.writeInt(value.sourceRecordCount());
            writer.writeInt(value.outputRecordCount());
            writer.writeInt(value.entryCount());
            writer.writeLong(value.logicalBytes());
            writer.writeLong(value.cumulativeSizeAtStart());
            writer.writeLong(value.cumulativeSizeAtEnd());
            writer.writeLong(value.firstCommitVersion());
            writer.writeLong(value.lastCommitVersion());
            F4Binary.writeSchemaRefs(writer, value.schemaRefs());
            writer.writeString(value.projectionRef());
            writer.writeLong(value.createdAtMillis());
            writer.writeLong(value.committedAtMillis());
            writer.writeString(value.stateReason());
            writer.writeLong(value.stateChangedAtMillis());
            writer.writeLong(value.metadataVersion());
            return writer.toByteArray();
        } catch (RuntimeException failure) {
            throw malformed(failure);
        }
    }

    @Override
    public GenerationIndexRecord decode(byte[] bytes) {
        try {
            F4Binary.Reader reader = reader(bytes);
            GenerationIndexRecord value = new GenerationIndexRecord(
                    VERSION,
                    reader.readString("streamId"),
                    reader.readUnsignedShort("readViewId"),
                    reader.readLong("offsetStart"),
                    reader.readLong("offsetEnd"),
                    reader.readLong("generation"),
                    reader.readString("publicationId"),
                    reader.readString("taskId"),
                    GenerationLifecycle.fromWireId(reader.readUnsignedShort("lifecycle")),
                    reader.readString("sourceSetSha256"),
                    reader.readString("policySha256"),
                    F4Binary.readReadTarget(reader, "readTarget"),
                    reader.readString("targetIdentitySha256"),
                    reader.readString("materializationPolicySha256"),
                    reader.readString("payloadFormat"),
                    reader.readInt("sourceRecordCount"),
                    reader.readInt("outputRecordCount"),
                    reader.readInt("entryCount"),
                    reader.readLong("logicalBytes"),
                    reader.readLong("cumulativeSizeAtStart"),
                    reader.readLong("cumulativeSizeAtEnd"),
                    reader.readLong("firstCommitVersion"),
                    reader.readLong("lastCommitVersion"),
                    F4Binary.readSchemaRefs(reader, "schemaRefs"),
                    reader.readString("projectionRef"),
                    reader.readLong("createdAtMillis"),
                    reader.readLong("committedAtMillis"),
                    reader.readString("stateReason"),
                    reader.readLong("stateChangedAtMillis"),
                    reader.readLong("metadataVersion"));
            reader.requireConsumed();
            return value;
        } catch (RuntimeException failure) {
            throw malformed(failure);
        }
    }
}
