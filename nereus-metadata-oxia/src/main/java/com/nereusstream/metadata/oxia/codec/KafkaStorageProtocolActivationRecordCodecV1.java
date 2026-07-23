/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import com.nereusstream.metadata.oxia.records.KafkaStorageProtocolActivationRecord;
import java.util.ArrayList;

/** Closed field-order codec for the cluster-wide Kafka storage activation authority. */
public final class KafkaStorageProtocolActivationRecordCodecV1
        extends AbstractF4RecordCodecV1<KafkaStorageProtocolActivationRecord> {
    public KafkaStorageProtocolActivationRecordCodecV1() {
        super(KafkaStorageProtocolActivationRecord.class);
    }

    @Override
    public byte[] encode(KafkaStorageProtocolActivationRecord value) {
        try {
            F4Binary.Writer writer = writer();
            writer.writeInt(value.recordVersion());
            writer.writeInt(value.lifecycleId());
            writer.writeString(value.kafkaClusterId());
            writer.writeInt(value.protocolVersion());
            writer.writeInt(value.apiVersion());
            writer.writeInt(value.streamHeadSessionVersion());
            writer.writeInt(value.bindingVersion());
            writer.writeInt(value.payloadMappingId());
            writer.writeInt(value.objectWalEntryIndexVersion());
            writer.writeInt(value.ncpVersion());
            writer.writeInt(value.ntcVersion());
            writer.writeInt(value.checkpointVersion());
            writer.writeInt(value.compactionStrategyVersion());
            writer.writeInt(value.allowedStorageProfiles().size());
            for (String profile : value.allowedStorageProfiles()) {
                writer.writeString(profile);
            }
            writer.writeString(value.defaultStorageProfile());
            writer.writeBytes(value.requiredCapabilitySha256());
            writer.writeBytes(value.requiredBrokerSetSha256());
            writer.writeInt(value.kafkaFeatureLevel());
            writer.writeLong(value.preparedAtMetadataOffset());
            writer.writeLong(value.activationEpoch());
            writer.writeLong(value.preparedAtMillis());
            writer.writeLong(value.activatedAtMillis());
            writer.writeLong(value.metadataVersion());
            return writer.toByteArray();
        } catch (RuntimeException failure) {
            throw malformed(failure);
        }
    }

    @Override
    public KafkaStorageProtocolActivationRecord decode(byte[] bytes) {
        try {
            F4Binary.Reader reader = reader(bytes);
            int recordVersion = reader.readInt("recordVersion");
            int lifecycleId = reader.readInt("lifecycleId");
            String kafkaClusterId = reader.readString("kafkaClusterId");
            int protocolVersion = reader.readInt("protocolVersion");
            int apiVersion = reader.readInt("apiVersion");
            int streamHeadSessionVersion = reader.readInt("streamHeadSessionVersion");
            int bindingVersion = reader.readInt("bindingVersion");
            int payloadMappingId = reader.readInt("payloadMappingId");
            int objectWalEntryIndexVersion = reader.readInt("objectWalEntryIndexVersion");
            int ncpVersion = reader.readInt("ncpVersion");
            int ntcVersion = reader.readInt("ntcVersion");
            int checkpointVersion = reader.readInt("checkpointVersion");
            int compactionStrategyVersion = reader.readInt("compactionStrategyVersion");
            int profileCount = reader.readCount(
                    "allowedStorageProfileCount",
                    Integer.BYTES,
                    KafkaStorageProtocolActivationRecord.MAX_STORAGE_PROFILES);
            ArrayList<String> profiles = new ArrayList<>(profileCount);
            for (int index = 0; index < profileCount; index++) {
                profiles.add(reader.readString("allowedStorageProfile"));
            }
            String defaultStorageProfile = reader.readString("defaultStorageProfile");
            byte[] requiredCapabilitySha256 = reader.readFixedBytes("requiredCapabilitySha256", 32);
            byte[] requiredBrokerSetSha256 = reader.readFixedBytes("requiredBrokerSetSha256", 32);
            int kafkaFeatureLevel = reader.readInt("kafkaFeatureLevel");
            long preparedAtMetadataOffset = reader.readLong("preparedAtMetadataOffset");
            long activationEpoch = reader.readLong("activationEpoch");
            long preparedAtMillis = reader.readLong("preparedAtMillis");
            long activatedAtMillis = reader.readLong("activatedAtMillis");
            long metadataVersion = reader.readLong("metadataVersion");
            reader.requireConsumed();
            return new KafkaStorageProtocolActivationRecord(
                    recordVersion,
                    lifecycleId,
                    kafkaClusterId,
                    protocolVersion,
                    apiVersion,
                    streamHeadSessionVersion,
                    bindingVersion,
                    payloadMappingId,
                    objectWalEntryIndexVersion,
                    ncpVersion,
                    ntcVersion,
                    checkpointVersion,
                    compactionStrategyVersion,
                    profiles,
                    defaultStorageProfile,
                    requiredCapabilitySha256,
                    requiredBrokerSetSha256,
                    kafkaFeatureLevel,
                    preparedAtMetadataOffset,
                    activationEpoch,
                    preparedAtMillis,
                    activatedAtMillis,
                    metadataVersion);
        } catch (RuntimeException failure) {
            throw malformed(failure);
        }
    }
}
