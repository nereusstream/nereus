/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.OffsetRange;
import java.util.Objects;

/** Exact accounting proved after a complete dense source read. */
public record ExactSourceReadSummary(
        OffsetRange coverage,
        int recordCount,
        int entryCount,
        long logicalBytes,
        Checksum orderedPayloadSha256) {
    public ExactSourceReadSummary {
        Objects.requireNonNull(coverage, "coverage");
        if (coverage.isEmpty()
                || recordCount <= 0
                || recordCount != coverage.recordCount()
                || entryCount <= 0
                || logicalBytes < 0) {
            throw new IllegalArgumentException("exact source summary accounting is invalid");
        }
        Objects.requireNonNull(orderedPayloadSha256, "orderedPayloadSha256");
        if (orderedPayloadSha256.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException("orderedPayloadSha256 must use SHA256");
        }
    }
}
