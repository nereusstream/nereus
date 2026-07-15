/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.compacted;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import java.time.Duration;
import java.util.Objects;

/** Immutable physical identity and logical selection required for full compacted-object verification. */
public record CompactedObjectVerificationRequest(
        StreamId streamId,
        ReadView view,
        OffsetRange sourceCoverage,
        ObjectSliceReadTarget target,
        PayloadFormat payloadFormat,
        Checksum storageCrc32c,
        Checksum contentSha256,
        Duration timeout) {
    public CompactedObjectVerificationRequest {
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
        if (target.objectType() != ObjectType.STREAM_COMPACTED_OBJECT
                || target.objectOffset() != 0
                || target.objectLength() > CompactedObjectFormatV1.MAX_OBJECT_BYTES
                || !target.sliceChecksum().equals(storageCrc32c)
                || !target.physicalFormat().equals(CompactedObjectFormatV1.physicalFormat(view))
                || !target.logicalFormat().equals(payloadFormat.name())) {
            throw new IllegalArgumentException(
                    "verification target does not match the frozen compacted identity");
        }
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("verification timeout must be positive");
        }
        // Reuse the strict read request constructor for all footer/reference invariants.
        new CompactedObjectReadRequest(
                streamId,
                view,
                sourceCoverage,
                sourceCoverage.startOffset(),
                target,
                payloadFormat,
                1,
                1,
                timeout);
    }

    public static CompactedObjectVerificationRequest from(
            CompactedObjectWriteRequest request,
            CompactedObjectWriteResult result,
            Duration timeout) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(result, "result");
        ObjectSliceReadTarget target = new ObjectSliceReadTarget(
                1,
                result.objectId(),
                result.objectKey(),
                ObjectType.STREAM_COMPACTED_OBJECT,
                result.physicalFormat(),
                request.logicalFormat(),
                request.sourceCoverage().startOffset() + "-" + request.sourceCoverage().endOffset(),
                0,
                result.objectLength(),
                result.storageCrc32c(),
                result.entryIndexRef());
        return new CompactedObjectVerificationRequest(
                request.streamId(),
                request.view(),
                request.sourceCoverage(),
                target,
                request.payloadFormat(),
                result.storageCrc32c(),
                result.contentSha256(),
                timeout);
    }

    private static void requireChecksum(
            Checksum checksum,
            ChecksumType expected,
            String field) {
        Objects.requireNonNull(checksum, field);
        if (checksum.type() != expected) {
            throw new IllegalArgumentException(field + " must use " + expected);
        }
    }
}
