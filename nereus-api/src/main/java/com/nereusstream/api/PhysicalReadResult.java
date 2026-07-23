/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.api;

import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;

/** Provider-neutral batches and exact accounting returned by a physical target reader. */
public record PhysicalReadResult(
        List<ReadBatch> batches,
        List<PhysicalReadStats> rangeStats,
        OptionalLong sourceCoverageEndOffset) {
    public PhysicalReadResult {
        batches = List.copyOf(Objects.requireNonNull(batches, "batches"));
        rangeStats = List.copyOf(Objects.requireNonNull(rangeStats, "rangeStats"));
        sourceCoverageEndOffset = Objects.requireNonNull(
                sourceCoverageEndOffset, "sourceCoverageEndOffset");
        if (sourceCoverageEndOffset.isPresent() && sourceCoverageEndOffset.getAsLong() < 0) {
            throw new IllegalArgumentException("sourceCoverageEndOffset must be non-negative");
        }
        if (sourceCoverageEndOffset.isPresent()
                && !batches.isEmpty()
                && sourceCoverageEndOffset.getAsLong()
                        < batches.get(batches.size() - 1).range().endOffset()) {
            throw new IllegalArgumentException("source coverage cannot precede returned batches");
        }
    }

    public PhysicalReadResult(List<ReadBatch> batches, List<PhysicalReadStats> rangeStats) {
        this(batches, rangeStats, OptionalLong.empty());
    }
}
