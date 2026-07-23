/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.compacted;

import com.nereusstream.api.FirstEntryPolicy;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.ReadBoundaryMode;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import java.time.Duration;
import java.util.Objects;

/** Exact immutable NTC2 target, boundary mode, and logical read limits. */
public record KafkaTopicCompactedObjectReadRequest(
        StreamId streamId,
        OffsetRange sourceCoverage,
        long startOffset,
        ObjectSliceReadTarget target,
        ReadBoundaryMode boundaryMode,
        FirstEntryPolicy firstEntryPolicy,
        int maxRecords,
        int maxBytes,
        Duration timeout) {
    public KafkaTopicCompactedObjectReadRequest {
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(sourceCoverage, "sourceCoverage");
        if (sourceCoverage.isEmpty() || !sourceCoverage.contains(startOffset)) {
            throw new IllegalArgumentException("startOffset must be inside non-empty sourceCoverage");
        }
        Objects.requireNonNull(target, "target");
        Objects.requireNonNull(boundaryMode, "boundaryMode");
        Objects.requireNonNull(firstEntryPolicy, "firstEntryPolicy");
        RangedCompactedObjectReadRequest.requireTarget(
                target,
                CompactedObjectFormatV2.TOPIC_COMPACTED_PHYSICAL_FORMAT,
                CompactedObjectFormatV2.KAFKA_LOGICAL_FORMAT);
        if (maxRecords <= 0 || maxBytes <= 0) {
            throw new IllegalArgumentException("NTC2 read limits must be positive");
        }
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
    }
}
