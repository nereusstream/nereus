/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.compacted;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import java.time.Duration;
import java.util.Objects;

/** Immutable full-object checksum and exact NCP2/NTC2 selection used before publication. */
public record RangedCompactedObjectVerificationRequest(
        StreamId streamId,
        ReadView view,
        OffsetRange sourceCoverage,
        ObjectSliceReadTarget target,
        PayloadFormat payloadFormat,
        Checksum storageCrc32c,
        Checksum contentSha256,
        Duration timeout) {
    public RangedCompactedObjectVerificationRequest {
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(view, "view");
        Objects.requireNonNull(sourceCoverage, "sourceCoverage");
        if (sourceCoverage.isEmpty()) {
            throw new IllegalArgumentException("verification sourceCoverage cannot be empty");
        }
        Objects.requireNonNull(target, "target");
        Objects.requireNonNull(payloadFormat, "payloadFormat");
        requireChecksum(storageCrc32c, ChecksumType.CRC32C, "storageCrc32c");
        requireChecksum(contentSha256, ChecksumType.SHA256, "contentSha256");
        if (!target.sliceChecksum().equals(storageCrc32c)
                || !target.physicalFormat().equals(CompactedObjectFormatV2.physicalFormat(view))) {
            throw new IllegalArgumentException("verification target does not match V2 format/checksums");
        }
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("verification timeout must be positive");
        }
        if (view == ReadView.COMMITTED) {
            new RangedCompactedObjectReadRequest(
                    streamId, sourceCoverage, sourceCoverage.startOffset(), target, payloadFormat,
                    com.nereusstream.api.ReadBoundaryMode.EXACT_START,
                    com.nereusstream.api.FirstEntryPolicy.ALLOW_FIRST_ENTRY_OVERFLOW,
                    1, 1, timeout);
        } else {
            new KafkaTopicCompactedObjectReadRequest(
                    streamId, sourceCoverage, sourceCoverage.startOffset(), target,
                    com.nereusstream.api.ReadBoundaryMode.EXACT_START,
                    com.nereusstream.api.FirstEntryPolicy.ALLOW_FIRST_ENTRY_OVERFLOW,
                    1, 1, timeout);
        }
    }

    public static RangedCompactedObjectVerificationRequest from(
            RangedCompactedObjectWriteRequest request,
            RangedCompactedObjectWriteResult result,
            Duration timeout) {
        return from(
                request.streamId(), ReadView.COMMITTED, request.sourceCoverage(),
                request.payloadFormat(), request.logicalFormat(), result, timeout);
    }

    public static RangedCompactedObjectVerificationRequest from(
            KafkaTopicCompactedObjectWriteRequest request,
            RangedCompactedObjectWriteResult result,
            Duration timeout) {
        return from(
                request.streamId(), ReadView.TOPIC_COMPACTED, request.sourceCoverage(),
                PayloadFormat.KAFKA_RECORD_BATCH, CompactedObjectFormatV2.KAFKA_LOGICAL_FORMAT,
                result, timeout);
    }

    private static RangedCompactedObjectVerificationRequest from(
            StreamId streamId,
            ReadView view,
            OffsetRange coverage,
            PayloadFormat payloadFormat,
            String logicalFormat,
            RangedCompactedObjectWriteResult result,
            Duration timeout) {
        Objects.requireNonNull(result, "result");
        ObjectSliceReadTarget target = new ObjectSliceReadTarget(
                1,
                result.objectId(),
                result.objectKey(),
                com.nereusstream.api.ObjectType.STREAM_COMPACTED_OBJECT,
                result.physicalFormat(),
                logicalFormat,
                coverage.startOffset() + "-" + coverage.endOffset(),
                0,
                result.objectLength(),
                result.storageCrc32c(),
                result.entryIndexRef());
        return new RangedCompactedObjectVerificationRequest(
                streamId, view, coverage, target, payloadFormat,
                result.storageCrc32c(), result.contentSha256(), timeout);
    }

    private static void requireChecksum(Checksum value, ChecksumType expected, String field) {
        Objects.requireNonNull(value, field);
        if (value.type() != expected) {
            throw new IllegalArgumentException(field + " must use " + expected);
        }
    }
}
