/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.backpressure;

import com.nereusstream.api.StreamId;
import java.util.Objects;

/** One exact, bounded materialization-lag observation derived from authoritative committed coverage. */
public record MaterializationLagSnapshot(
        StreamId streamId,
        long verifiedCoveredOffset,
        long committedEndOffset,
        long lagRecords,
        long lagBytes,
        long oldestLagMillis,
        long observedHeadMetadataVersion,
        long observedAtMillis) {
    public MaterializationLagSnapshot {
        Objects.requireNonNull(streamId, "streamId");
        if (verifiedCoveredOffset < 0
                || committedEndOffset < verifiedCoveredOffset
                || lagRecords != committedEndOffset - verifiedCoveredOffset
                || lagBytes < 0
                || oldestLagMillis < 0
                || observedHeadMetadataVersion < 0
                || observedAtMillis < 0) {
            throw new IllegalArgumentException(
                    "materialization lag snapshot fields are inconsistent");
        }
        if (lagRecords == 0
                && (lagBytes != 0 || oldestLagMillis != 0)) {
            throw new IllegalArgumentException(
                    "zero materialization lag must have zero byte and age lag");
        }
    }
}
