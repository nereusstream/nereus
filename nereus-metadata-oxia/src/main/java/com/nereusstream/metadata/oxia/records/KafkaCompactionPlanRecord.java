/* Licensed under the Apache License, Version 2.0 */

package com.nereusstream.metadata.oxia.records;

import com.nereusstream.metadata.oxia.KafkaPartitionId;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Immutable Oxia attachment for one bounded canonical KCP1 compaction-plan image.
 */
public record KafkaCompactionPlanRecord(
        int formatVersion,
        String kafkaClusterId,
        String topicId,
        int partitionId,
        String streamId,
        String planId,
        String materializationTaskId,
        long outputStartOffset,
        long outputEndOffset,
        long decisionEndOffset,
        byte[] planSha256,
        byte[] planBytes,
        long createdAtMillis,
        long metadataVersion) {
    public static final int FORMAT_VERSION = 1;
    public static final int MAX_PLAN_BYTES = 60 << 10;

    public KafkaCompactionPlanRecord {
        if (formatVersion != FORMAT_VERSION) {
            throw new IllegalArgumentException("Kafka compaction plan formatVersion must be 1");
        }
        KafkaPartitionId identity = new KafkaPartitionId(kafkaClusterId, topicId, partitionId);
        kafkaClusterId = identity.kafkaClusterId();
        topicId = identity.topicId();
        streamId = KafkaMetadataValidation.text(streamId, "streamId");
        planId = KafkaMetadataValidation.text(planId, "planId");
        materializationTaskId = KafkaMetadataValidation.text(materializationTaskId, "materializationTaskId");
        requirePlanId(planId);
        requireTaskId(materializationTaskId);
        if (outputStartOffset < 0 || outputEndOffset <= outputStartOffset || decisionEndOffset < outputEndOffset) {
            throw new IllegalArgumentException("invalid Kafka compaction plan ranges");
        }
        planSha256 = KafkaMetadataValidation.sha256(planSha256, "planSha256", false);
        planBytes = Objects.requireNonNull(planBytes, "planBytes").clone();
        if (planBytes.length == 0 || planBytes.length > MAX_PLAN_BYTES) {
            throw new IllegalArgumentException("Kafka compaction plan bytes are empty or too large");
        }
        if (!MessageDigest.isEqual(planSha256, sha256(planBytes))) {
            throw new IllegalArgumentException("Kafka compaction plan SHA does not match its bytes");
        }
        if (createdAtMillis < 0 || metadataVersion < 0) {
            throw new IllegalArgumentException("invalid Kafka compaction plan timestamps/version");
        }
    }

    public KafkaPartitionId identity() {
        return new KafkaPartitionId(kafkaClusterId, topicId, partitionId);
    }

    public KafkaCompactionPlanRecord withMetadataVersion(long version) {
        return new KafkaCompactionPlanRecord(
                formatVersion,
                kafkaClusterId,
                topicId,
                partitionId,
                streamId,
                planId,
                materializationTaskId,
                outputStartOffset,
                outputEndOffset,
                decisionEndOffset,
                planSha256,
                planBytes,
                createdAtMillis,
                version);
    }

    @Override
    public byte[] planSha256() {
        return planSha256.clone();
    }

    @Override
    public byte[] planBytes() {
        return planBytes.clone();
    }

    @Override
    public boolean equals(Object other) {
        return this == other
                || other instanceof KafkaCompactionPlanRecord that
                        && formatVersion == that.formatVersion
                        && partitionId == that.partitionId
                        && outputStartOffset == that.outputStartOffset
                        && outputEndOffset == that.outputEndOffset
                        && decisionEndOffset == that.decisionEndOffset
                        && createdAtMillis == that.createdAtMillis
                        && metadataVersion == that.metadataVersion
                        && kafkaClusterId.equals(that.kafkaClusterId)
                        && topicId.equals(that.topicId)
                        && streamId.equals(that.streamId)
                        && planId.equals(that.planId)
                        && materializationTaskId.equals(that.materializationTaskId)
                        && Arrays.equals(planSha256, that.planSha256)
                        && Arrays.equals(planBytes, that.planBytes);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(
                formatVersion,
                kafkaClusterId,
                topicId,
                partitionId,
                streamId,
                planId,
                materializationTaskId,
                outputStartOffset,
                outputEndOffset,
                decisionEndOffset,
                createdAtMillis,
                metadataVersion);
        result = 31 * result + Arrays.hashCode(planSha256);
        return 31 * result + Arrays.hashCode(planBytes);
    }

    private static void requirePlanId(String value) {
        requireBase32Id(value, "kcp1-", "Kafka compaction planId");
    }

    private static void requireTaskId(String value) {
        requireBase32Id(value, "mat1-", "Kafka materialization taskId");
    }

    private static void requireBase32Id(String value, String prefix, String field) {
        if (!value.startsWith(prefix) || value.length() != prefix.length() + 52) {
            throw new IllegalArgumentException(field + " is not canonical");
        }
        for (int index = prefix.length(); index < value.length(); index++) {
            char character = value.charAt(index);
            if (!((character >= 'a' && character <= 'z') || (character >= '2' && character <= '7'))) {
                throw new IllegalArgumentException(field + " is not base32lower");
            }
        }
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }
}
