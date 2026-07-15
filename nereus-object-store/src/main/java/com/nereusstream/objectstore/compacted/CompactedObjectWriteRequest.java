/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.compacted;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.MetadataCanonicalizer;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.SchemaRef;
import com.nereusstream.api.StreamId;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Immutable NCP1/NTC1 writer facts; all values are copied into Parquet metadata. */
public record CompactedObjectWriteRequest(
        String cluster,
        ReadView view,
        StreamId streamId,
        OffsetRange sourceCoverage,
        String outputAttemptId,
        Checksum sourceSetSha256,
        Checksum policySha256,
        PayloadFormat payloadFormat,
        String logicalFormat,
        Optional<Checksum> projectionIdentitySha256,
        int sourceRecordCount,
        int expectedOutputRecordCount,
        int entryCount,
        long logicalBytes,
        List<SchemaRef> schemaRefs,
        long cumulativeSizeAtStart,
        long cumulativeSizeAtEnd,
        int targetRowGroupRecords,
        String compression,
        String writerBuild,
        Optional<TopicCompactionFormatSpec> topicCompaction) {
    public CompactedObjectWriteRequest {
        cluster = requireText(cluster, "cluster");
        Objects.requireNonNull(view, "view");
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(sourceCoverage, "sourceCoverage");
        if (sourceCoverage.isEmpty()) {
            throw new IllegalArgumentException("sourceCoverage cannot be empty");
        }
        outputAttemptId = requireBase32(outputAttemptId, "outputAttemptId");
        requireSha256(sourceSetSha256, "sourceSetSha256");
        requireSha256(policySha256, "policySha256");
        Objects.requireNonNull(payloadFormat, "payloadFormat");
        logicalFormat = requireText(logicalFormat, "logicalFormat");
        projectionIdentitySha256 = Objects.requireNonNull(
                        projectionIdentitySha256, "projectionIdentitySha256")
                .map(value -> requireSha256(value, "projectionIdentitySha256"));
        long coverageRecords = sourceCoverage.recordCount();
        if (sourceRecordCount <= 0
                || sourceRecordCount != coverageRecords
                || expectedOutputRecordCount < 0
                || expectedOutputRecordCount > sourceRecordCount
                || entryCount <= 0
                || logicalBytes < 0
                || cumulativeSizeAtStart < 0
                || cumulativeSizeAtEnd < cumulativeSizeAtStart) {
            throw new IllegalArgumentException("compacted object accounting is invalid");
        }
        schemaRefs = MetadataCanonicalizer.canonicalSchemaRefs(schemaRefs);
        if (targetRowGroupRecords <= 0 || targetRowGroupRecords > 65_536) {
            throw new IllegalArgumentException("targetRowGroupRecords must be in [1, 65536]");
        }
        compression = requireText(compression, "compression");
        if (!compression.equals("ZSTD") && !compression.equals("UNCOMPRESSED")) {
            throw new IllegalArgumentException("compression must be ZSTD or UNCOMPRESSED");
        }
        writerBuild = requireText(writerBuild, "writerBuild");
        topicCompaction = Objects.requireNonNull(topicCompaction, "topicCompaction");
        if (view == ReadView.COMMITTED) {
            if (expectedOutputRecordCount != sourceRecordCount
                    || topicCompaction.isPresent()
                    || Math.subtractExact(cumulativeSizeAtEnd, cumulativeSizeAtStart) != logicalBytes) {
                throw new IllegalArgumentException("committed compacted output must be dense and lossless");
            }
        } else if (topicCompaction.isEmpty()) {
            throw new IllegalArgumentException("topic-compacted output requires strategy metadata");
        }
    }

    private static Checksum requireSha256(Checksum checksum, String field) {
        Objects.requireNonNull(checksum, field);
        if (checksum.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException(field + " must be SHA256");
        }
        return checksum;
    }

    private static String requireBase32(String value, String field) {
        value = requireText(value, field);
        if (value.length() < 26 || value.length() > 128 || !value.matches("[a-z2-7]+")) {
            throw new IllegalArgumentException(field + " must be lowercase base32 with at least 128 bits");
        }
        return value;
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }
}
