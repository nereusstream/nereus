/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public record RangeRetentionStatsScanPage(
        List<VersionedRangeRetentionStats> values,
        Optional<F4ScanToken> continuation) {
    public RangeRetentionStatsScanPage {
        values = F4ValueValidation.orderedPage(values, VersionedRangeRetentionStats::key, 1_000, "values");
        continuation = Objects.requireNonNull(continuation, "continuation");
        if (values.isEmpty() && continuation.isPresent()) {
            throw new IllegalArgumentException("empty retention-stats page cannot carry a continuation");
        }
    }
}
