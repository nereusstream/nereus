/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.compacted;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.StreamId;
import java.util.Objects;

/** Immutable NCP2 writer facts copied into the closed Parquet metadata map. */
public record RangedCompactedObjectWriteRequest(
        String cluster,
        StreamId streamId,
        OffsetRange sourceCoverage,
        String outputAttemptId,
        Checksum sourceSetSha256,
        Checksum policySha256,
        PayloadFormat payloadFormat,
        String logicalFormat,
        long sourceRecordCount,
        int entryCount,
        long logicalBytes,
        long cumulativeSizeAtEnd,
        int targetRowGroupRecords,
        String compression,
        String writerBuild) {
    public RangedCompactedObjectWriteRequest {
        cluster = CompactedObjectFormatV2.requireText(cluster, "cluster");
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(sourceCoverage, "sourceCoverage");
        if (sourceCoverage.isEmpty()) {
            throw new IllegalArgumentException("sourceCoverage cannot be empty");
        }
        outputAttemptId = CompactedObjectFormatV2.requireBase32(outputAttemptId, "outputAttemptId");
        requireSha256(sourceSetSha256, "sourceSetSha256");
        requireSha256(policySha256, "policySha256");
        Objects.requireNonNull(payloadFormat, "payloadFormat");
        logicalFormat = CompactedObjectFormatV2.requireText(logicalFormat, "logicalFormat");
        if (sourceRecordCount != sourceCoverage.recordCount()
                || entryCount <= 0
                || logicalBytes < 0
                || cumulativeSizeAtEnd < logicalBytes) {
            throw new IllegalArgumentException("NCP2 accounting is invalid");
        }
        if (targetRowGroupRecords <= 0 || targetRowGroupRecords > CompactedObjectFormatV2.MAX_ROW_GROUPS) {
            throw new IllegalArgumentException("targetRowGroupRecords must be in [1, 65536]");
        }
        compression = CompactedObjectFormatV2.requireCompression(compression);
        writerBuild = CompactedObjectFormatV2.requireText(writerBuild, "writerBuild");
    }

    private static void requireSha256(Checksum value, String field) {
        Objects.requireNonNull(value, field);
        if (value.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException(field + " must use SHA256");
        }
    }
}
