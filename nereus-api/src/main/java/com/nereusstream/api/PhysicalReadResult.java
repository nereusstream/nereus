/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.api;

import java.util.List;
import java.util.Objects;

/** Provider-neutral batches and exact accounting returned by a physical target reader. */
public record PhysicalReadResult(
        List<ReadBatch> batches,
        List<PhysicalReadStats> rangeStats) {
    public PhysicalReadResult {
        batches = List.copyOf(Objects.requireNonNull(batches, "batches"));
        rangeStats = List.copyOf(Objects.requireNonNull(rangeStats, "rangeStats"));
    }
}
