/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.compacted;

import java.util.List;
import java.util.Objects;

/** Strictly verified sparse NTC2 rows and physical/source-coverage accounting. */
public record KafkaTopicCompactedObjectReadResult(
        RangedCompactedObjectMetadata metadata,
        List<KafkaTopicCompactedObjectRow> rows,
        long sourceCoverageEndOffset,
        long physicalBytesRead,
        long footerBytesRead) {
    public KafkaTopicCompactedObjectReadResult {
        Objects.requireNonNull(metadata, "metadata");
        rows = List.copyOf(Objects.requireNonNull(rows, "rows"));
        if (sourceCoverageEndOffset < metadata.sourceCoverage().startOffset()
                || sourceCoverageEndOffset > metadata.sourceCoverage().endOffset()
                || physicalBytesRead < 0
                || footerBytesRead <= 0
                || footerBytesRead > physicalBytesRead) {
            throw new IllegalArgumentException("NTC2 read accounting is invalid");
        }
    }
}
