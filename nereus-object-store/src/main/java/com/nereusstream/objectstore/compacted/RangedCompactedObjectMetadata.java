/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.compacted;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import java.util.Objects;
import java.util.Optional;

/** Strictly decoded NCP2/NTC2 file metadata. */
public record RangedCompactedObjectMetadata(
        ReadView view,
        StreamId streamId,
        OffsetRange sourceCoverage,
        Checksum sourceSetSha256,
        Checksum policySha256,
        String outputAttemptId,
        PayloadFormat payloadFormat,
        String logicalFormat,
        String rangeModel,
        long sourceRecordCount,
        long outputRecordCount,
        int entryCount,
        long logicalBytes,
        long cumulativeSizeAtEnd,
        String writerBuild,
        String compression,
        int targetRowGroupRecords,
        Optional<KafkaTopicCompactedFormatSpecV2> topicCompaction) {
    public RangedCompactedObjectMetadata {
        Objects.requireNonNull(view, "view");
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(sourceCoverage, "sourceCoverage");
        Objects.requireNonNull(sourceSetSha256, "sourceSetSha256");
        Objects.requireNonNull(policySha256, "policySha256");
        Objects.requireNonNull(outputAttemptId, "outputAttemptId");
        Objects.requireNonNull(payloadFormat, "payloadFormat");
        Objects.requireNonNull(logicalFormat, "logicalFormat");
        Objects.requireNonNull(rangeModel, "rangeModel");
        Objects.requireNonNull(writerBuild, "writerBuild");
        Objects.requireNonNull(compression, "compression");
        topicCompaction = Objects.requireNonNull(topicCompaction, "topicCompaction");
    }
}
