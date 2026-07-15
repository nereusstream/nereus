/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.compacted;

import com.nereusstream.api.EntryIndexLocation;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import java.time.Duration;
import java.util.Objects;

/** Exact immutable target plus logical limits for one compacted-object read. */
public record CompactedObjectReadRequest(
        StreamId streamId,
        ReadView view,
        OffsetRange sourceCoverage,
        long startOffset,
        ObjectSliceReadTarget target,
        PayloadFormat payloadFormat,
        int maxRecords,
        int maxBytes,
        Duration timeout) {
    public CompactedObjectReadRequest {
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(view, "view");
        Objects.requireNonNull(sourceCoverage, "sourceCoverage");
        if (sourceCoverage.isEmpty() || !sourceCoverage.contains(startOffset)) {
            throw new IllegalArgumentException("startOffset must be inside non-empty sourceCoverage");
        }
        Objects.requireNonNull(target, "target");
        Objects.requireNonNull(payloadFormat, "payloadFormat");
        if (target.objectType() != ObjectType.STREAM_COMPACTED_OBJECT
                || target.objectOffset() != 0
                || !target.physicalFormat().equals(CompactedObjectFormatV1.physicalFormat(view))
                || !target.logicalFormat().equals(payloadFormat.name())
                || target.entryIndexRef().location() != EntryIndexLocation.OBJECT_FOOTER
                || !target.entryIndexRef().objectId().equals(java.util.Optional.of(target.objectId()))
                || !target.entryIndexRef().objectKey().equals(java.util.Optional.of(target.objectKey()))
                || target.entryIndexRef().offset() + target.entryIndexRef().length()
                        != target.objectLength()) {
            throw new IllegalArgumentException("compacted read target identity/footer is inconsistent");
        }
        if (maxRecords <= 0 || maxBytes <= 0) {
            throw new IllegalArgumentException("compacted read limits must be positive");
        }
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
    }
}
