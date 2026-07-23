/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

import com.nereusstream.metadata.oxia.KafkaBrokerIdentity;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Epoch-scoped broker proof for code, configuration, provider scope and protocol support. */
public record KafkaBrokerCapabilityRecord(
        int recordVersion,
        String kafkaClusterId,
        int brokerId,
        long brokerEpoch,
        String runtimeInstanceId,
        String kafkaVersion,
        String nereusBuild,
        String javaVersion,
        int protocolVersion,
        int apiVersion,
        int streamHeadSessionVersion,
        int bindingVersion,
        int payloadMappingId,
        int objectWalEntryIndexVersion,
        int ncpVersion,
        int ntcVersion,
        int checkpointVersion,
        int compactionStrategyVersion,
        int kafkaFeatureLevel,
        List<String> supportedStorageProfiles,
        byte[] configCompatibilitySha256,
        byte[] codeCapabilitySha256,
        byte[] providerScopeSha256,
        long startedAtMillis,
        long heartbeatAtMillis,
        long expiresAtMillis,
        long metadataVersion) {
    public static final int RECORD_VERSION = 1;

    public KafkaBrokerCapabilityRecord {
        if (recordVersion != RECORD_VERSION) {
            throw new IllegalArgumentException("recordVersion must be 1");
        }
        kafkaClusterId = KafkaMetadataValidation.text(kafkaClusterId, "kafkaClusterId");
        new KafkaBrokerIdentity(brokerId, brokerEpoch);
        runtimeInstanceId = KafkaMetadataValidation.text(runtimeInstanceId, "runtimeInstanceId");
        kafkaVersion = KafkaMetadataValidation.text(kafkaVersion, "kafkaVersion");
        nereusBuild = KafkaMetadataValidation.text(nereusBuild, "nereusBuild");
        javaVersion = KafkaMetadataValidation.text(javaVersion, "javaVersion");
        KafkaStorageProtocolActivationRecord.requireExactProtocolVersions(
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
                kafkaFeatureLevel);
        supportedStorageProfiles = KafkaStorageProtocolActivationRecord.canonicalProfiles(
                supportedStorageProfiles, "supportedStorageProfiles");
        configCompatibilitySha256 = KafkaMetadataValidation.sha256(
                configCompatibilitySha256, "configCompatibilitySha256", false);
        codeCapabilitySha256 = KafkaMetadataValidation.sha256(
                codeCapabilitySha256, "codeCapabilitySha256", false);
        providerScopeSha256 = KafkaMetadataValidation.sha256(
                providerScopeSha256, "providerScopeSha256", false);
        if (startedAtMillis <= 0 || heartbeatAtMillis < startedAtMillis
                || expiresAtMillis <= heartbeatAtMillis || metadataVersion < 0) {
            throw new IllegalArgumentException("invalid Kafka broker capability timing/version fields");
        }
    }

    public KafkaBrokerIdentity identity() {
        return new KafkaBrokerIdentity(brokerId, brokerEpoch);
    }

    public KafkaBrokerCapabilityRecord withMetadataVersion(long version) {
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
                supportedStorageProfiles,
                configCompatibilitySha256,
                codeCapabilitySha256,
                providerScopeSha256,
                startedAtMillis,
                heartbeatAtMillis,
                expiresAtMillis,
                version);
    }

    @Override public byte[] configCompatibilitySha256() { return configCompatibilitySha256.clone(); }
    @Override public byte[] codeCapabilitySha256() { return codeCapabilitySha256.clone(); }
    @Override public byte[] providerScopeSha256() { return providerScopeSha256.clone(); }

    @Override
    public boolean equals(Object other) {
        return this == other || other instanceof KafkaBrokerCapabilityRecord that
                && recordVersion == that.recordVersion && brokerId == that.brokerId
                && brokerEpoch == that.brokerEpoch && protocolVersion == that.protocolVersion
                && apiVersion == that.apiVersion
                && streamHeadSessionVersion == that.streamHeadSessionVersion
                && bindingVersion == that.bindingVersion && payloadMappingId == that.payloadMappingId
                && objectWalEntryIndexVersion == that.objectWalEntryIndexVersion
                && ncpVersion == that.ncpVersion && ntcVersion == that.ntcVersion
                && checkpointVersion == that.checkpointVersion
                && compactionStrategyVersion == that.compactionStrategyVersion
                && kafkaFeatureLevel == that.kafkaFeatureLevel && startedAtMillis == that.startedAtMillis
                && heartbeatAtMillis == that.heartbeatAtMillis && expiresAtMillis == that.expiresAtMillis
                && metadataVersion == that.metadataVersion && kafkaClusterId.equals(that.kafkaClusterId)
                && runtimeInstanceId.equals(that.runtimeInstanceId) && kafkaVersion.equals(that.kafkaVersion)
                && nereusBuild.equals(that.nereusBuild) && javaVersion.equals(that.javaVersion)
                && supportedStorageProfiles.equals(that.supportedStorageProfiles)
                && Arrays.equals(configCompatibilitySha256, that.configCompatibilitySha256)
                && Arrays.equals(codeCapabilitySha256, that.codeCapabilitySha256)
                && Arrays.equals(providerScopeSha256, that.providerScopeSha256);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(
                recordVersion, kafkaClusterId, brokerId, brokerEpoch, runtimeInstanceId, kafkaVersion,
                nereusBuild, javaVersion, protocolVersion, apiVersion, streamHeadSessionVersion,
                bindingVersion, payloadMappingId, objectWalEntryIndexVersion, ncpVersion, ntcVersion,
                checkpointVersion, compactionStrategyVersion, kafkaFeatureLevel,
                supportedStorageProfiles, startedAtMillis, heartbeatAtMillis, expiresAtMillis,
                metadataVersion);
        result = 31 * result + Arrays.hashCode(configCompatibilitySha256);
        result = 31 * result + Arrays.hashCode(codeCapabilitySha256);
        return 31 * result + Arrays.hashCode(providerScopeSha256);
    }
}
