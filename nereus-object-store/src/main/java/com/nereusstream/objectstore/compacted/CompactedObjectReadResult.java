/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.compacted;

import java.util.List;
import java.util.Objects;

/** Strictly verified compacted rows and exact source-coverage/read-amplification accounting. */
public record CompactedObjectReadResult(
        CompactedObjectMetadata metadata,
        List<CompactedObjectRow> rows,
        long sourceCoverageEndOffset,
        long physicalBytesRead,
        long footerBytesRead) {
    public CompactedObjectReadResult {
        Objects.requireNonNull(metadata, "metadata");
        rows = List.copyOf(Objects.requireNonNull(rows, "rows"));
        if (sourceCoverageEndOffset < metadata.sourceCoverage().startOffset()
                || sourceCoverageEndOffset > metadata.sourceCoverage().endOffset()
                || physicalBytesRead < 0
                || footerBytesRead <= 0
                || footerBytesRead > physicalBytesRead) {
            throw new IllegalArgumentException("compacted read accounting is invalid");
        }
    }
}
