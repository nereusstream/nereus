/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.compacted;

import com.nereusstream.api.EntryIndexLocation;
import com.nereusstream.api.FirstEntryPolicy;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ReadBoundaryMode;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import java.time.Duration;
import java.util.Objects;

/** Exact immutable NCP2 target, boundary mode, and logical read limits. */
public record RangedCompactedObjectReadRequest(
        StreamId streamId,
        OffsetRange sourceCoverage,
        long startOffset,
        ObjectSliceReadTarget target,
        PayloadFormat payloadFormat,
        ReadBoundaryMode boundaryMode,
        FirstEntryPolicy firstEntryPolicy,
        int maxRecords,
        int maxBytes,
        Duration timeout) {
    public RangedCompactedObjectReadRequest {
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(sourceCoverage, "sourceCoverage");
        if (sourceCoverage.isEmpty() || !sourceCoverage.contains(startOffset)) {
            throw new IllegalArgumentException("startOffset must be inside non-empty sourceCoverage");
        }
        Objects.requireNonNull(target, "target");
        Objects.requireNonNull(payloadFormat, "payloadFormat");
        Objects.requireNonNull(boundaryMode, "boundaryMode");
        Objects.requireNonNull(firstEntryPolicy, "firstEntryPolicy");
        String logicalFormat = payloadFormat == PayloadFormat.KAFKA_RECORD_BATCH
                ? CompactedObjectFormatV2.KAFKA_LOGICAL_FORMAT
                : payloadFormat.name();
        requireTarget(target, CompactedObjectFormatV2.COMMITTED_PHYSICAL_FORMAT, logicalFormat);
        if (maxRecords <= 0 || maxBytes <= 0) {
            throw new IllegalArgumentException("NCP2 read limits must be positive");
        }
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
    }

    static void requireTarget(ObjectSliceReadTarget target, String physicalFormat, String logicalFormat) {
        if (target.objectType() != ObjectType.STREAM_COMPACTED_OBJECT
                || target.objectOffset() != 0
                || target.objectLength() > CompactedObjectFormatV2.MAX_OBJECT_BYTES
                || !target.physicalFormat().equals(physicalFormat)
                || !target.logicalFormat().equals(logicalFormat)
                || target.entryIndexRef().location() != EntryIndexLocation.OBJECT_FOOTER
                || target.entryIndexRef().length() > CompactedObjectFormatV2.MAX_FOOTER_BYTES
                || !target.entryIndexRef().objectId().equals(java.util.Optional.of(target.objectId()))
                || !target.entryIndexRef().objectKey().equals(java.util.Optional.of(target.objectKey()))
                || Math.addExact(target.entryIndexRef().offset(), target.entryIndexRef().length())
                        != target.objectLength()) {
            throw new IllegalArgumentException("V2 compacted read target identity/footer is inconsistent");
        }
    }
}
