/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.compacted;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.StreamId;
import java.util.Objects;

/** Immutable NTC2 writer facts copied into the closed Kafka topic-compacted metadata map. */
public record KafkaTopicCompactedObjectWriteRequest(
        String cluster,
        StreamId streamId,
        OffsetRange sourceCoverage,
        String outputAttemptId,
        Checksum sourceSetSha256,
        Checksum policySha256,
        long outputRecordCount,
        int entryCount,
        long logicalBytes,
        long cumulativeSizeAtEnd,
        int targetRowGroupRecords,
        String compression,
        String writerBuild,
        KafkaTopicCompactedFormatSpecV2 topicCompaction) {
    public KafkaTopicCompactedObjectWriteRequest {
        cluster = CompactedObjectFormatV2.requireText(cluster, "cluster");
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(sourceCoverage, "sourceCoverage");
        if (sourceCoverage.isEmpty()) {
            throw new IllegalArgumentException("sourceCoverage cannot be empty");
        }
        outputAttemptId = CompactedObjectFormatV2.requireBase32(outputAttemptId, "outputAttemptId");
        requireSha256(sourceSetSha256, "sourceSetSha256");
        requireSha256(policySha256, "policySha256");
        if (outputRecordCount < 0
                || outputRecordCount > sourceCoverage.recordCount()
                || entryCount < 0
                || logicalBytes < 0
                || cumulativeSizeAtEnd < logicalBytes) {
            throw new IllegalArgumentException("NTC2 accounting is invalid");
        }
        if ((outputRecordCount == 0) != (entryCount == 0)) {
            throw new IllegalArgumentException("NTC2 empty row/count accounting is inconsistent");
        }
        if (targetRowGroupRecords <= 0 || targetRowGroupRecords > CompactedObjectFormatV2.MAX_ROW_GROUPS) {
            throw new IllegalArgumentException("targetRowGroupRecords must be in [1, 65536]");
        }
        compression = CompactedObjectFormatV2.requireCompression(compression);
        writerBuild = CompactedObjectFormatV2.requireText(writerBuild, "writerBuild");
        Objects.requireNonNull(topicCompaction, "topicCompaction");
        if (topicCompaction.outputBatchCount() != entryCount) {
            throw new IllegalArgumentException("NTC2 output batch count must equal entry count");
        }
    }

    private static void requireSha256(Checksum value, String field) {
        Objects.requireNonNull(value, field);
        if (value.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException(field + " must use SHA256");
        }
    }
}
