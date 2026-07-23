/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

import com.nereusstream.metadata.oxia.KafkaPartitionId;
import java.util.Arrays;

/** Hint-only registry entry used to discover authoritative Kafka binding roots. */
public record KafkaPartitionRegistryRecord(
        int formatVersion,
        String kafkaClusterId,
        String topicId,
        int partitionId,
        String bindingRootKey,
        byte[] bindingRootSha256,
        int lifecycleId,
        long bindingEpoch,
        long updatedAtMillis,
        long metadataVersion) {
    public KafkaPartitionRegistryRecord {
        if (formatVersion != 1) throw new IllegalArgumentException("formatVersion must be 1");
        new KafkaPartitionId(kafkaClusterId, topicId, partitionId);
        bindingRootKey = KafkaMetadataValidation.text(bindingRootKey, "bindingRootKey");
        bindingRootSha256 = KafkaMetadataValidation.sha256(
                bindingRootSha256, "bindingRootSha256", false);
        KafkaPartitionLifecycle.fromWireId(lifecycleId);
        if (bindingEpoch <= 0 || updatedAtMillis <= 0 || metadataVersion < 0) {
            throw new IllegalArgumentException("invalid Kafka registry fields");
        }
    }

    public KafkaPartitionId identity() {
        return new KafkaPartitionId(kafkaClusterId, topicId, partitionId);
    }

    public KafkaPartitionLifecycle lifecycle() {
        return KafkaPartitionLifecycle.fromWireId(lifecycleId);
    }

    public KafkaPartitionRegistryRecord withMetadataVersion(long version) {
        return new KafkaPartitionRegistryRecord(
                formatVersion, kafkaClusterId, topicId, partitionId, bindingRootKey,
                bindingRootSha256, lifecycleId, bindingEpoch, updatedAtMillis, version);
    }

    @Override public byte[] bindingRootSha256() { return bindingRootSha256.clone(); }

    @Override
    public boolean equals(Object other) {
        return this == other || other instanceof KafkaPartitionRegistryRecord that
                && formatVersion == that.formatVersion && partitionId == that.partitionId
                && lifecycleId == that.lifecycleId && bindingEpoch == that.bindingEpoch
                && updatedAtMillis == that.updatedAtMillis && metadataVersion == that.metadataVersion
                && kafkaClusterId.equals(that.kafkaClusterId) && topicId.equals(that.topicId)
                && bindingRootKey.equals(that.bindingRootKey)
                && Arrays.equals(bindingRootSha256, that.bindingRootSha256);
    }

    @Override
    public int hashCode() {
        int result = java.util.Objects.hash(formatVersion, kafkaClusterId, topicId, partitionId,
                bindingRootKey, lifecycleId, bindingEpoch, updatedAtMillis, metadataVersion);
        return 31 * result + Arrays.hashCode(bindingRootSha256);
    }
}
