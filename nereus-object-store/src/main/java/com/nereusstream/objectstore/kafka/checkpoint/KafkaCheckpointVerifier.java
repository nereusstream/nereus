/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.kafka.checkpoint;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKey;
import java.util.Objects;

/** Exact NKC1 object/key/source-fact verifier. */
public final class KafkaCheckpointVerifier {
    public void verifyExpected(
            KafkaCheckpointObject object,
            String nereusCluster,
            KafkaCheckpointHeader expectedHeader,
            Checksum contentPolicySha256) {
        Objects.requireNonNull(object, "object");
        Objects.requireNonNull(expectedHeader, "expectedHeader");
        ObjectKey expectedKey = KafkaCheckpointFormatV1.objectKey(
                nereusCluster, expectedHeader, contentPolicySha256);
        if (!object.objectKey().equals(expectedKey)
                || !object.objectId().equals(KafkaCheckpointFormatV1.objectId(expectedKey))
                || !object.header().equals(expectedHeader)) {
            throw new NereusException(
                    ErrorCode.OBJECT_CHECKSUM_MISMATCH, false,
                    "NKC1 object does not match its deterministic key or captured source facts");
        }
    }

    public void verifyRecoveryWindow(
            KafkaCheckpointObject object,
            String kafkaClusterId,
            String topicId,
            int partitionId,
            long incarnation,
            String streamId,
            int payloadMappingId,
            long currentTrimOffset,
            long currentEndOffset) {
        KafkaCheckpointHeader header = Objects.requireNonNull(object, "object").header();
        if (!header.kafkaClusterId().equals(kafkaClusterId)
                || !header.topicId().equals(topicId)
                || header.partitionId() != partitionId
                || header.incarnation() != incarnation
                || !header.streamId().value().equals(streamId)
                || header.payloadMappingId() != payloadMappingId
                || currentTrimOffset < 0
                || currentEndOffset < currentTrimOffset
                || header.logStartOffset() > currentTrimOffset
                || header.checkpointOffset() < currentTrimOffset
                || header.checkpointOffset() > currentEndOffset) {
            throw new NereusException(
                    ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                    "NKC1 identity or recovery coverage does not match the authoritative partition window");
        }
    }
}
