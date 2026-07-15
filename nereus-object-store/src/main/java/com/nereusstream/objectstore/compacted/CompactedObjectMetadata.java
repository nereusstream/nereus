/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.compacted;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import java.util.Objects;
import java.util.Optional;

/** Strictly decoded required NCP1/NTC1 file metadata. */
public record CompactedObjectMetadata(
        ReadView view,
        StreamId streamId,
        OffsetRange sourceCoverage,
        Checksum sourceSetSha256,
        Checksum policySha256,
        String outputAttemptId,
        PayloadFormat payloadFormat,
        String logicalFormat,
        Optional<Checksum> projectionIdentitySha256,
        int sourceRecordCount,
        int outputRecordCount,
        int entryCount,
        long logicalBytes,
        long cumulativeSizeAtEnd,
        String writerBuild,
        String compression,
        int targetRowGroupRecords,
        Optional<TopicCompactionFormatSpec> topicCompaction) {
    public CompactedObjectMetadata {
        Objects.requireNonNull(view, "view");
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(sourceCoverage, "sourceCoverage");
        Objects.requireNonNull(sourceSetSha256, "sourceSetSha256");
        Objects.requireNonNull(policySha256, "policySha256");
        Objects.requireNonNull(outputAttemptId, "outputAttemptId");
        Objects.requireNonNull(payloadFormat, "payloadFormat");
        Objects.requireNonNull(logicalFormat, "logicalFormat");
        projectionIdentitySha256 = Objects.requireNonNull(
                projectionIdentitySha256, "projectionIdentitySha256");
        Objects.requireNonNull(writerBuild, "writerBuild");
        Objects.requireNonNull(compression, "compression");
        topicCompaction = Objects.requireNonNull(topicCompaction, "topicCompaction");
    }
}
