/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import com.nereusstream.metadata.oxia.KafkaBrokerIdentity;
import com.nereusstream.metadata.oxia.records.KafkaStorageReadinessRecord;
import java.util.ArrayList;

/** Closed field-order codec for an expiring exact-broker readiness snapshot. */
public final class KafkaStorageReadinessRecordCodecV1
        extends AbstractF4RecordCodecV1<KafkaStorageReadinessRecord> {
    public KafkaStorageReadinessRecordCodecV1() {
        super(KafkaStorageReadinessRecord.class);
    }

    @Override
    public byte[] encode(KafkaStorageReadinessRecord value) {
        try {
            F4Binary.Writer writer = writer();
            writer.writeInt(value.recordVersion());
            writer.writeString(value.kafkaClusterId());
            writer.writeLong(value.readinessEpoch());
            writer.writeLong(value.kraftMetadataOffset());
            writer.writeInt(value.brokers().size());
            for (KafkaBrokerIdentity broker : value.brokers()) {
                writer.writeInt(broker.brokerId());
                writer.writeLong(broker.brokerEpoch());
            }
            writer.writeBytes(value.brokerSetSha256());
            writer.writeBytes(value.capabilitySha256());
            writer.writeBytes(value.providerScopeSha256());
            writer.writeLong(value.createdAtMillis());
            writer.writeLong(value.expiresAtMillis());
            writer.writeLong(value.metadataVersion());
            return writer.toByteArray();
        } catch (RuntimeException failure) {
            throw malformed(failure);
        }
    }

    @Override
    public KafkaStorageReadinessRecord decode(byte[] bytes) {
        try {
            F4Binary.Reader reader = reader(bytes);
            int recordVersion = reader.readInt("recordVersion");
            String kafkaClusterId = reader.readString("kafkaClusterId");
            long readinessEpoch = reader.readLong("readinessEpoch");
            long kraftMetadataOffset = reader.readLong("kraftMetadataOffset");
            int brokerCount = reader.readCount(
                    "brokerCount",
                    Integer.BYTES + Long.BYTES,
                    KafkaStorageReadinessRecord.MAX_BROKERS);
            ArrayList<KafkaBrokerIdentity> brokers = new ArrayList<>(brokerCount);
            for (int index = 0; index < brokerCount; index++) {
                brokers.add(new KafkaBrokerIdentity(
                        reader.readInt("brokerId"), reader.readLong("brokerEpoch")));
            }
            byte[] brokerSetSha256 = reader.readFixedBytes("brokerSetSha256", 32);
            byte[] capabilitySha256 = reader.readFixedBytes("capabilitySha256", 32);
            byte[] providerScopeSha256 = reader.readFixedBytes("providerScopeSha256", 32);
            long createdAtMillis = reader.readLong("createdAtMillis");
            long expiresAtMillis = reader.readLong("expiresAtMillis");
            long metadataVersion = reader.readLong("metadataVersion");
            reader.requireConsumed();
            return new KafkaStorageReadinessRecord(
                    recordVersion,
                    kafkaClusterId,
                    readinessEpoch,
                    kraftMetadataOffset,
                    brokers,
                    brokerSetSha256,
                    capabilitySha256,
                    providerScopeSha256,
                    createdAtMillis,
                    expiresAtMillis,
                    metadataVersion);
        } catch (RuntimeException failure) {
            throw malformed(failure);
        }
    }
}
