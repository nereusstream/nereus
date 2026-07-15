/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/** Reloads every typed metadata removal from authoritative stores for exact list comparison. */
@FunctionalInterface
public interface GcPlanMetadataRevalidator {
    CompletableFuture<List<GcPlannedMetadataRemoval>> reload(
            GcCandidate candidate,
            List<GcPlannedMetadataRemoval> expectedRemovals);
}
