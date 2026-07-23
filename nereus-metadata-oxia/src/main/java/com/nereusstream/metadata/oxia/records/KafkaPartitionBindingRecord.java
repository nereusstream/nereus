/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

import com.nereusstream.metadata.oxia.KafkaPartitionId;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

/** Authoritative single-key Kafka partition binding root. */
public record KafkaPartitionBindingRecord(
        int formatVersion,
        String kafkaClusterId,
        String topicId,
        int partitionId,
        String observedTopicName,
        long incarnation,
        String streamName,
        String streamId,
        int payloadMappingId,
        String storageProfile,
        int lifecycleId,
        long bindingEpoch,
        long createdMetadataOffset,
        long lastAppliedMetadataOffset,
        int observedLeaderId,
        int observedLeaderEpoch,
        long observedBrokerEpoch,
        long observedLogStartOffset,
        long observedStableEndOffset,
        KafkaCompactionCoverageRecord compactionCoverage,
        List<KafkaCheckpointReferenceRecord> checkpointReferences,
        KafkaPartitionPendingOperationRecord pendingOperation,
        long createdAtMillis,
        long updatedAtMillis,
        long metadataVersion) {
    public KafkaPartitionBindingRecord {
        if (formatVersion != 1) throw new IllegalArgumentException("formatVersion must be 1");
        new KafkaPartitionId(kafkaClusterId, topicId, partitionId);
        observedTopicName = KafkaMetadataValidation.text(observedTopicName, "observedTopicName");
        Objects.requireNonNull(streamName, "streamName");
        Objects.requireNonNull(streamId, "streamId");
        KafkaPayloadMapping.fromWireId(payloadMappingId);
        storageProfile = KafkaMetadataValidation.text(storageProfile, "storageProfile");
        KafkaPartitionLifecycle lifecycle = KafkaPartitionLifecycle.fromWireId(lifecycleId);
        compactionCoverage = Objects.requireNonNull(compactionCoverage, "compactionCoverage");
        checkpointReferences = KafkaMetadataValidation.list(checkpointReferences, 3, "checkpointReferences");
        pendingOperation = Objects.requireNonNull(pendingOperation, "pendingOperation");
        if (incarnation <= 0 || bindingEpoch <= 0 || createdMetadataOffset < 0
                || lastAppliedMetadataOffset < createdMetadataOffset || observedLeaderId < -1
                || observedLeaderEpoch < -1 || observedBrokerEpoch < -1 || observedLogStartOffset < 0
                || observedStableEndOffset < observedLogStartOffset || createdAtMillis <= 0
                || updatedAtMillis < createdAtMillis || metadataVersion < 0) {
            throw new IllegalArgumentException("invalid Kafka partition binding numeric fields");
        }
        if (lifecycle == KafkaPartitionLifecycle.CREATING) {
            if (!streamName.isEmpty() || !streamId.isEmpty()) {
                throw new IllegalArgumentException("CREATING binding cannot publish stream identity");
            }
        } else {
            KafkaMetadataValidation.text(streamName, "streamName");
            KafkaMetadataValidation.text(streamId, "streamId");
        }
        validateCheckpoints(checkpointReferences);
        validatePending(lifecycle, pendingOperation);
    }

    public KafkaPartitionId identity() {
        return new KafkaPartitionId(kafkaClusterId, topicId, partitionId);
    }

    public KafkaPayloadMapping payloadMapping() {
        return KafkaPayloadMapping.fromWireId(payloadMappingId);
    }

    public KafkaPartitionLifecycle lifecycle() {
        return KafkaPartitionLifecycle.fromWireId(lifecycleId);
    }

    public KafkaPartitionBindingRecord withMetadataVersion(long version) {
        return new KafkaPartitionBindingRecord(
                formatVersion, kafkaClusterId, topicId, partitionId, observedTopicName, incarnation,
                streamName, streamId, payloadMappingId, storageProfile, lifecycleId, bindingEpoch,
                createdMetadataOffset, lastAppliedMetadataOffset, observedLeaderId, observedLeaderEpoch,
                observedBrokerEpoch, observedLogStartOffset, observedStableEndOffset, compactionCoverage,
                checkpointReferences, pendingOperation, createdAtMillis, updatedAtMillis, version);
    }

    private static void validateCheckpoints(List<KafkaCheckpointReferenceRecord> values) {
        long previous = Long.MAX_VALUE;
        HashSet<String> ids = new HashSet<>();
        for (KafkaCheckpointReferenceRecord value : values) {
            if (value.checkpointOffset() >= previous || !ids.add(value.objectId())) {
                throw new IllegalArgumentException(
                        "checkpoint references must strictly descend and use distinct object IDs");
            }
            previous = value.checkpointOffset();
        }
    }

    private static void validatePending(
            KafkaPartitionLifecycle lifecycle, KafkaPartitionPendingOperationRecord operation) {
        if (operation.isEmpty()) return;
        boolean compatible = switch (operation.operationType()) {
            case CREATE -> lifecycle == KafkaPartitionLifecycle.CREATING;
            case DELETE -> lifecycle == KafkaPartitionLifecycle.DELETING;
            case REPAIR -> lifecycle == KafkaPartitionLifecycle.CORRUPT;
            case NONE -> false;
        };
        if (!compatible) throw new IllegalArgumentException("pending operation is incompatible with lifecycle");
    }
}
