/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

import com.nereusstream.api.StorageProfile;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** One-way cluster authority for the exact native-Kafka storage protocol and broker set. */
public record KafkaStorageProtocolActivationRecord(
        int recordVersion,
        int lifecycleId,
        String kafkaClusterId,
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
        List<String> allowedStorageProfiles,
        String defaultStorageProfile,
        byte[] requiredCapabilitySha256,
        byte[] requiredBrokerSetSha256,
        int kafkaFeatureLevel,
        long preparedAtMetadataOffset,
        long activationEpoch,
        long preparedAtMillis,
        long activatedAtMillis,
        long metadataVersion) {
    public static final int RECORD_VERSION = 1;
    public static final int PROTOCOL_VERSION = 1;
    public static final int API_VERSION = 1;
    public static final int STREAM_HEAD_SESSION_VERSION = 2;
    public static final int BINDING_VERSION = 1;
    public static final int OBJECT_WAL_ENTRY_INDEX_VERSION = 1;
    public static final int NCP_VERSION = 2;
    public static final int NTC_VERSION = 2;
    public static final int CHECKPOINT_VERSION = 1;
    public static final int COMPACTION_STRATEGY_VERSION = 1;
    public static final int KAFKA_FEATURE_LEVEL = 1;
    public static final int MAX_STORAGE_PROFILES = 5;

    public KafkaStorageProtocolActivationRecord {
        if (recordVersion != RECORD_VERSION) {
            throw new IllegalArgumentException("recordVersion must be 1");
        }
        KafkaStorageActivationLifecycle lifecycle = KafkaStorageActivationLifecycle.fromWireId(lifecycleId);
        kafkaClusterId = KafkaMetadataValidation.text(kafkaClusterId, "kafkaClusterId");
        requireExactProtocolVersions(
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
        allowedStorageProfiles = canonicalProfiles(allowedStorageProfiles, "allowedStorageProfiles");
        defaultStorageProfile = canonicalProfile(defaultStorageProfile, "defaultStorageProfile");
        if (!allowedStorageProfiles.contains(defaultStorageProfile)) {
            throw new IllegalArgumentException("defaultStorageProfile must be allowed");
        }
        requiredCapabilitySha256 = KafkaMetadataValidation.sha256(
                requiredCapabilitySha256, "requiredCapabilitySha256", false);
        requiredBrokerSetSha256 = KafkaMetadataValidation.sha256(
                requiredBrokerSetSha256, "requiredBrokerSetSha256", false);
        if (preparedAtMetadataOffset < 0 || activationEpoch <= 0 || preparedAtMillis <= 0
                || metadataVersion < 0) {
            throw new IllegalArgumentException("invalid Kafka storage activation numeric fields");
        }
        if (lifecycle == KafkaStorageActivationLifecycle.PREPARED && activatedAtMillis != 0) {
            throw new IllegalArgumentException("PREPARED activation cannot carry activatedAtMillis");
        }
        if (lifecycle == KafkaStorageActivationLifecycle.ACTIVE
                && activatedAtMillis < preparedAtMillis) {
            throw new IllegalArgumentException("ACTIVE activation requires a valid activation time");
        }
    }

    public KafkaStorageActivationLifecycle lifecycle() {
        return KafkaStorageActivationLifecycle.fromWireId(lifecycleId);
    }

    public KafkaStorageProtocolActivationRecord withMetadataVersion(long version) {
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
                allowedStorageProfiles,
                defaultStorageProfile,
                requiredCapabilitySha256,
                requiredBrokerSetSha256,
                kafkaFeatureLevel,
                preparedAtMetadataOffset,
                activationEpoch,
                preparedAtMillis,
                activatedAtMillis,
                version);
    }

    @Override public byte[] requiredCapabilitySha256() { return requiredCapabilitySha256.clone(); }
    @Override public byte[] requiredBrokerSetSha256() { return requiredBrokerSetSha256.clone(); }

    @Override
    public boolean equals(Object other) {
        return this == other || other instanceof KafkaStorageProtocolActivationRecord that
                && recordVersion == that.recordVersion && lifecycleId == that.lifecycleId
                && protocolVersion == that.protocolVersion && apiVersion == that.apiVersion
                && streamHeadSessionVersion == that.streamHeadSessionVersion
                && bindingVersion == that.bindingVersion && payloadMappingId == that.payloadMappingId
                && objectWalEntryIndexVersion == that.objectWalEntryIndexVersion
                && ncpVersion == that.ncpVersion && ntcVersion == that.ntcVersion
                && checkpointVersion == that.checkpointVersion
                && compactionStrategyVersion == that.compactionStrategyVersion
                && kafkaFeatureLevel == that.kafkaFeatureLevel
                && preparedAtMetadataOffset == that.preparedAtMetadataOffset
                && activationEpoch == that.activationEpoch && preparedAtMillis == that.preparedAtMillis
                && activatedAtMillis == that.activatedAtMillis && metadataVersion == that.metadataVersion
                && kafkaClusterId.equals(that.kafkaClusterId)
                && allowedStorageProfiles.equals(that.allowedStorageProfiles)
                && defaultStorageProfile.equals(that.defaultStorageProfile)
                && Arrays.equals(requiredCapabilitySha256, that.requiredCapabilitySha256)
                && Arrays.equals(requiredBrokerSetSha256, that.requiredBrokerSetSha256);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(
                recordVersion, lifecycleId, kafkaClusterId, protocolVersion, apiVersion,
                streamHeadSessionVersion, bindingVersion, payloadMappingId,
                objectWalEntryIndexVersion, ncpVersion, ntcVersion, checkpointVersion,
                compactionStrategyVersion, allowedStorageProfiles, defaultStorageProfile,
                kafkaFeatureLevel, preparedAtMetadataOffset, activationEpoch, preparedAtMillis,
                activatedAtMillis, metadataVersion);
        result = 31 * result + Arrays.hashCode(requiredCapabilitySha256);
        return 31 * result + Arrays.hashCode(requiredBrokerSetSha256);
    }

    static List<String> canonicalProfiles(List<String> supplied, String name) {
        List<String> profiles = KafkaMetadataValidation.list(supplied, MAX_STORAGE_PROFILES, name);
        if (profiles.isEmpty()) {
            throw new IllegalArgumentException(name + " must be non-empty");
        }
        String previous = null;
        for (String profile : profiles) {
            String exact = canonicalProfile(profile, name);
            if (previous != null && previous.compareTo(exact) >= 0) {
                throw new IllegalArgumentException(name + " must be strictly sorted and unique");
            }
            previous = exact;
        }
        return profiles;
    }

    static String canonicalProfile(String supplied, String name) {
        String profile = KafkaMetadataValidation.text(supplied, name);
        final StorageProfile parsed;
        try {
            parsed = StorageProfile.valueOf(profile);
        } catch (IllegalArgumentException failure) {
            throw new IllegalArgumentException(name + " contains an unknown storage profile", failure);
        }
        if (parsed.canonical() != parsed) {
            throw new IllegalArgumentException(name + " cannot use a legacy storage profile alias");
        }
        return profile;
    }

    static void requireExactProtocolVersions(
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
            int kafkaFeatureLevel) {
        if (protocolVersion != PROTOCOL_VERSION
                || apiVersion != API_VERSION
                || streamHeadSessionVersion != STREAM_HEAD_SESSION_VERSION
                || bindingVersion != BINDING_VERSION
                || payloadMappingId != KafkaPayloadMapping.KAFKA_RECORD_BATCH_V1.wireId()
                || objectWalEntryIndexVersion != OBJECT_WAL_ENTRY_INDEX_VERSION
                || ncpVersion != NCP_VERSION
                || ntcVersion != NTC_VERSION
                || checkpointVersion != CHECKPOINT_VERSION
                || compactionStrategyVersion != COMPACTION_STRATEGY_VERSION
                || kafkaFeatureLevel != KAFKA_FEATURE_LEVEL) {
            throw new IllegalArgumentException("unsupported Kafka storage protocol version tuple");
        }
    }
}
