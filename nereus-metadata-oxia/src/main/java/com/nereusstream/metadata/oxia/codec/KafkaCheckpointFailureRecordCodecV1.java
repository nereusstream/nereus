/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import com.nereusstream.metadata.oxia.records.KafkaCheckpointFailureRecord;

/** Closed explicit-field codec for one immutable NKC1 quarantine audit. */
public final class KafkaCheckpointFailureRecordCodecV1
        extends AbstractF4RecordCodecV1<KafkaCheckpointFailureRecord> {
    public KafkaCheckpointFailureRecordCodecV1() {
        super(KafkaCheckpointFailureRecord.class);
    }

    @Override
    public byte[] encode(KafkaCheckpointFailureRecord value) {
        try {
            F4Binary.Writer writer = writer();
            writer.writeInt(value.formatVersion());
            writer.writeString(value.kafkaClusterId());
            writer.writeString(value.topicId());
            writer.writeInt(value.partitionId());
            writer.writeLong(value.partitionIncarnation());
            writer.writeString(value.objectId());
            writer.writeBytes(value.referenceSha256());
            writer.writeInt(value.sourceId());
            writer.writeString(value.failureCode());
            writer.writeBytes(value.failureSha256());
            writer.writeLong(value.quarantinedAtMillis());
            writer.writeLong(value.metadataVersion());
            return writer.toByteArray();
        } catch (RuntimeException failure) {
            throw malformed(failure);
        }
    }

    @Override
    public KafkaCheckpointFailureRecord decode(byte[] bytes) {
        try {
            F4Binary.Reader reader = reader(bytes);
            KafkaCheckpointFailureRecord value =
                    new KafkaCheckpointFailureRecord(
                            reader.readInt("formatVersion"),
                            reader.readString("kafkaClusterId"),
                            reader.readString("topicId"),
                            reader.readInt("partitionId"),
                            reader.readLong("partitionIncarnation"),
                            reader.readString("objectId"),
                            reader.readFixedBytes("referenceSha256", 32),
                            reader.readInt("sourceId"),
                            reader.readString("failureCode"),
                            reader.readFixedBytes("failureSha256", 32),
                            reader.readLong("quarantinedAtMillis"),
                            reader.readLong("metadataVersion"));
            reader.requireConsumed();
            return value;
        } catch (RuntimeException failure) {
            throw malformed(failure);
        }
    }
}
