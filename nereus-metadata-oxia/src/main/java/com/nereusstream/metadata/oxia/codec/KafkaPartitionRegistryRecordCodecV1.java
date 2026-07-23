/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import com.nereusstream.metadata.oxia.records.KafkaPartitionRegistryRecord;

/** Closed explicit field-order codec for a hint-only Kafka registry entry. */
public final class KafkaPartitionRegistryRecordCodecV1
        extends AbstractF4RecordCodecV1<KafkaPartitionRegistryRecord> {
    public KafkaPartitionRegistryRecordCodecV1() {
        super(KafkaPartitionRegistryRecord.class);
    }

    @Override
    public byte[] encode(KafkaPartitionRegistryRecord value) {
        try {
            F4Binary.Writer writer = writer();
            writer.writeInt(value.formatVersion());
            writer.writeString(value.kafkaClusterId());
            writer.writeString(value.topicId());
            writer.writeInt(value.partitionId());
            writer.writeString(value.bindingRootKey());
            writer.writeBytes(value.bindingRootSha256());
            writer.writeInt(value.lifecycleId());
            writer.writeLong(value.bindingEpoch());
            writer.writeLong(value.updatedAtMillis());
            writer.writeLong(value.metadataVersion());
            return writer.toByteArray();
        } catch (RuntimeException failure) {
            throw malformed(failure);
        }
    }

    @Override
    public KafkaPartitionRegistryRecord decode(byte[] bytes) {
        try {
            F4Binary.Reader reader = reader(bytes);
            KafkaPartitionRegistryRecord value = new KafkaPartitionRegistryRecord(
                    reader.readInt("formatVersion"), reader.readString("kafkaClusterId"),
                    reader.readString("topicId"), reader.readInt("partitionId"),
                    reader.readString("bindingRootKey"), reader.readFixedBytes("bindingRootSha256", 32),
                    reader.readInt("lifecycleId"), reader.readLong("bindingEpoch"),
                    reader.readLong("updatedAtMillis"), reader.readLong("metadataVersion"));
            reader.requireConsumed();
            return value;
        } catch (RuntimeException failure) {
            throw malformed(failure);
        }
    }
}
