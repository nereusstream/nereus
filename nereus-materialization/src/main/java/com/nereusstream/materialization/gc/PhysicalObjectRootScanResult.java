/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

/** Exact lifecycle counts from one complete 256-shard root pass. */
public record PhysicalObjectRootScanResult(
        long activeRoots,
        long markedRoots,
        long deletingRoots,
        long deletedRoots,
        long quarantinedRoots) {
    public PhysicalObjectRootScanResult {
        if (activeRoots < 0 || markedRoots < 0 || deletingRoots < 0
                || deletedRoots < 0 || quarantinedRoots < 0) {
            throw new IllegalArgumentException("root scan counts must be non-negative");
        }
    }

    public long totalRoots() {
        return Math.addExact(
                Math.addExact(activeRoots, markedRoots),
                Math.addExact(
                        deletingRoots,
                        Math.addExact(deletedRoots, quarantinedRoots)));
    }
}
