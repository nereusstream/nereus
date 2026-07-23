/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.kafka.checkpoint;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.StreamId;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** Canonical NKC1 source snapshot facts. */
public record KafkaCheckpointHeader(
        int flags,
        String kafkaClusterId,
        String topicId,
        int partitionId,
        long incarnation,
        StreamId streamId,
        int payloadMappingId,
        int leaderEpoch,
        long checkpointOffset,
        long logStartOffset,
        long stableEndOffset,
        long sourceCommitVersion,
        String sourceLastCommitId,
        Checksum sourceHeadSha256) {
    public KafkaCheckpointHeader {
        if ((flags & ~KafkaCheckpointFormatV1.HEADER_ALLOW_OPTIONAL_SECTIONS_FLAG) != 0) {
            throw new IllegalArgumentException("unknown Kafka checkpoint header flags");
        }
        kafkaClusterId = text(kafkaClusterId, "kafkaClusterId");
        topicId = KafkaCheckpointFormatV1.canonicalTopicId(topicId);
        Objects.requireNonNull(streamId, "streamId");
        sourceLastCommitId = text(sourceLastCommitId, "sourceLastCommitId");
        Objects.requireNonNull(sourceHeadSha256, "sourceHeadSha256");
        if (sourceHeadSha256.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException("sourceHeadSha256 must use SHA256");
        }
        if (partitionId < 0 || incarnation <= 0 || payloadMappingId != 1 || leaderEpoch < 0
                || checkpointOffset < 0 || logStartOffset < 0 || stableEndOffset < checkpointOffset
                || checkpointOffset < logStartOffset || sourceCommitVersion <= 0) {
            throw new IllegalArgumentException("invalid Kafka checkpoint header numeric fields");
        }
    }

    private static String text(String value, String name) {
        Objects.requireNonNull(value, name);
        int bytes = value.getBytes(StandardCharsets.UTF_8).length;
        if (value.isBlank() || bytes > KafkaCheckpointFormatV1.MAX_STRING_BYTES) {
            throw new IllegalArgumentException(name + " must be nonblank and bounded");
        }
        return value;
    }
}
