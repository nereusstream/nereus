/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

import com.nereusstream.metadata.oxia.KafkaPartitionId;

import java.util.Arrays;
import java.util.Objects;

/**
 * Immutable durable quarantine and first-failure audit for one exact rooted NKC1 reference.
 *
 * <p>The raw failure message is never persisted. {@code failureSha256} identifies a canonical,
 * redacted failure signature.
 */
public record KafkaCheckpointFailureRecord(
        int formatVersion,
        String kafkaClusterId,
        String topicId,
        int partitionId,
        long partitionIncarnation,
        String objectId,
        byte[] referenceSha256,
        int sourceId,
        String failureCode,
        byte[] failureSha256,
        long quarantinedAtMillis,
        long metadataVersion) {
    public KafkaCheckpointFailureRecord {
        if (formatVersion != 1) {
            throw new IllegalArgumentException("formatVersion must be 1");
        }
        new KafkaPartitionId(kafkaClusterId, topicId, partitionId);
        objectId = KafkaMetadataValidation.text(objectId, "objectId");
        referenceSha256 = KafkaMetadataValidation.sha256(referenceSha256, "referenceSha256", false);
        KafkaCheckpointFailureSource.fromWireId(sourceId);
        failureCode = KafkaMetadataValidation.text(failureCode, "failureCode");
        failureSha256 = KafkaMetadataValidation.sha256(failureSha256, "failureSha256", false);
        if (partitionIncarnation <= 0 || quarantinedAtMillis <= 0 || metadataVersion < 0) {
            throw new IllegalArgumentException("invalid Kafka checkpoint failure numeric fields");
        }
    }

    public KafkaPartitionId identity() {
        return new KafkaPartitionId(kafkaClusterId, topicId, partitionId);
    }

    public KafkaCheckpointFailureSource source() {
        return KafkaCheckpointFailureSource.fromWireId(sourceId);
    }

    public KafkaCheckpointFailureRecord withMetadataVersion(long version) {
        return new KafkaCheckpointFailureRecord(
                formatVersion,
                kafkaClusterId,
                topicId,
                partitionId,
                partitionIncarnation,
                objectId,
                referenceSha256,
                sourceId,
                failureCode,
                failureSha256,
                quarantinedAtMillis,
                version);
    }

    @Override
    public byte[] referenceSha256() {
        return referenceSha256.clone();
    }

    @Override
    public byte[] failureSha256() {
        return failureSha256.clone();
    }

    @Override
    public boolean equals(Object other) {
        return this == other
                || other instanceof KafkaCheckpointFailureRecord that
                        && formatVersion == that.formatVersion
                        && partitionId == that.partitionId
                        && partitionIncarnation == that.partitionIncarnation
                        && sourceId == that.sourceId
                        && quarantinedAtMillis == that.quarantinedAtMillis
                        && metadataVersion == that.metadataVersion
                        && kafkaClusterId.equals(that.kafkaClusterId)
                        && topicId.equals(that.topicId)
                        && objectId.equals(that.objectId)
                        && failureCode.equals(that.failureCode)
                        && Arrays.equals(referenceSha256, that.referenceSha256)
                        && Arrays.equals(failureSha256, that.failureSha256);
    }

    @Override
    public int hashCode() {
        int result =
                Objects.hash(
                        formatVersion,
                        kafkaClusterId,
                        topicId,
                        partitionId,
                        partitionIncarnation,
                        objectId,
                        sourceId,
                        failureCode,
                        quarantinedAtMillis,
                        metadataVersion);
        result = 31 * result + Arrays.hashCode(referenceSha256);
        return 31 * result + Arrays.hashCode(failureSha256);
    }
}
