/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import com.nereusstream.metadata.oxia.records.KafkaCompactionPlanRecord;

/** Closed explicit-field codec for the immutable KCP1 Oxia attachment. */
public final class KafkaCompactionPlanRecordCodecV1
        extends AbstractF4RecordCodecV1<KafkaCompactionPlanRecord> {
    public KafkaCompactionPlanRecordCodecV1() {
        super(KafkaCompactionPlanRecord.class);
    }

    @Override
    public byte[] encode(KafkaCompactionPlanRecord value) {
        try {
            F4Binary.Writer writer = writer();
            writer.writeInt(value.formatVersion());
            writer.writeString(value.kafkaClusterId());
            writer.writeString(value.topicId());
            writer.writeInt(value.partitionId());
            writer.writeString(value.streamId());
            writer.writeString(value.planId());
            writer.writeString(value.materializationTaskId());
            writer.writeLong(value.outputStartOffset());
            writer.writeLong(value.outputEndOffset());
            writer.writeLong(value.decisionEndOffset());
            writer.writeBytes(value.planSha256());
            writer.writeBytes(value.planBytes());
            writer.writeLong(value.createdAtMillis());
            writer.writeLong(value.metadataVersion());
            return writer.toByteArray();
        } catch (RuntimeException failure) {
            throw malformed(failure);
        }
    }

    @Override
    public KafkaCompactionPlanRecord decode(byte[] bytes) {
        try {
            F4Binary.Reader reader = reader(bytes);
            KafkaCompactionPlanRecord value =
                    new KafkaCompactionPlanRecord(
                            reader.readInt("formatVersion"),
                            reader.readString("kafkaClusterId"),
                            reader.readString("topicId"),
                            reader.readInt("partitionId"),
                            reader.readString("streamId"),
                            reader.readString("planId"),
                            reader.readString("materializationTaskId"),
                            reader.readLong("outputStartOffset"),
                            reader.readLong("outputEndOffset"),
                            reader.readLong("decisionEndOffset"),
                            reader.readFixedBytes("planSha256", 32),
                            reader.readBytes("planBytes"),
                            reader.readLong("createdAtMillis"),
                            reader.readLong("metadataVersion"));
            reader.requireConsumed();
            return value;
        } catch (RuntimeException failure) {
            throw malformed(failure);
        }
    }
}
