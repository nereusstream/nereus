/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import com.nereusstream.metadata.oxia.records.KafkaBrokerCapabilityRecord;
import com.nereusstream.metadata.oxia.records.KafkaStorageProtocolActivationRecord;
import java.util.ArrayList;

/** Closed field-order codec for one epoch-scoped Kafka broker capability. */
public final class KafkaBrokerCapabilityRecordCodecV1
        extends AbstractF4RecordCodecV1<KafkaBrokerCapabilityRecord> {
    public KafkaBrokerCapabilityRecordCodecV1() {
        super(KafkaBrokerCapabilityRecord.class);
    }

    @Override
    public byte[] encode(KafkaBrokerCapabilityRecord value) {
        try {
            F4Binary.Writer writer = writer();
            writer.writeInt(value.recordVersion());
            writer.writeString(value.kafkaClusterId());
            writer.writeInt(value.brokerId());
            writer.writeLong(value.brokerEpoch());
            writer.writeString(value.runtimeInstanceId());
            writer.writeString(value.kafkaVersion());
            writer.writeString(value.nereusBuild());
            writer.writeString(value.javaVersion());
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
            writer.writeInt(value.kafkaFeatureLevel());
            writer.writeInt(value.supportedStorageProfiles().size());
            for (String profile : value.supportedStorageProfiles()) {
                writer.writeString(profile);
            }
            writer.writeBytes(value.configCompatibilitySha256());
            writer.writeBytes(value.codeCapabilitySha256());
            writer.writeBytes(value.providerScopeSha256());
            writer.writeLong(value.startedAtMillis());
            writer.writeLong(value.heartbeatAtMillis());
            writer.writeLong(value.expiresAtMillis());
            writer.writeLong(value.metadataVersion());
            return writer.toByteArray();
        } catch (RuntimeException failure) {
            throw malformed(failure);
        }
    }

    @Override
    public KafkaBrokerCapabilityRecord decode(byte[] bytes) {
        try {
            F4Binary.Reader reader = reader(bytes);
            int recordVersion = reader.readInt("recordVersion");
            String kafkaClusterId = reader.readString("kafkaClusterId");
            int brokerId = reader.readInt("brokerId");
            long brokerEpoch = reader.readLong("brokerEpoch");
            String runtimeInstanceId = reader.readString("runtimeInstanceId");
            String kafkaVersion = reader.readString("kafkaVersion");
            String nereusBuild = reader.readString("nereusBuild");
            String javaVersion = reader.readString("javaVersion");
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
            int kafkaFeatureLevel = reader.readInt("kafkaFeatureLevel");
            int profileCount = reader.readCount(
                    "supportedStorageProfileCount",
                    Integer.BYTES,
                    KafkaStorageProtocolActivationRecord.MAX_STORAGE_PROFILES);
            ArrayList<String> profiles = new ArrayList<>(profileCount);
            for (int index = 0; index < profileCount; index++) {
                profiles.add(reader.readString("supportedStorageProfile"));
            }
            byte[] configCompatibilitySha256 = reader.readFixedBytes(
                    "configCompatibilitySha256", 32);
            byte[] codeCapabilitySha256 = reader.readFixedBytes("codeCapabilitySha256", 32);
            byte[] providerScopeSha256 = reader.readFixedBytes("providerScopeSha256", 32);
            long startedAtMillis = reader.readLong("startedAtMillis");
            long heartbeatAtMillis = reader.readLong("heartbeatAtMillis");
            long expiresAtMillis = reader.readLong("expiresAtMillis");
            long metadataVersion = reader.readLong("metadataVersion");
            reader.requireConsumed();
            return new KafkaBrokerCapabilityRecord(
                    recordVersion,
                    kafkaClusterId,
                    brokerId,
                    brokerEpoch,
                    runtimeInstanceId,
                    kafkaVersion,
                    nereusBuild,
                    javaVersion,
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
                    kafkaFeatureLevel,
                    profiles,
                    configCompatibilitySha256,
                    codeCapabilitySha256,
                    providerScopeSha256,
                    startedAtMillis,
                    heartbeatAtMillis,
                    expiresAtMillis,
                    metadataVersion);
        } catch (RuntimeException failure) {
            throw malformed(failure);
        }
    }
}
