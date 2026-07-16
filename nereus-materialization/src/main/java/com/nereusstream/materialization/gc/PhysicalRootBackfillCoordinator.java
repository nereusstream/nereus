/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import java.util.concurrent.CompletableFuture;

/** Cluster-wide live-reference root/protection backfill contract. */
public interface PhysicalRootBackfillCoordinator {
    CompletableFuture<PhysicalRootBackfillReport> run(
            PhysicalRootBackfillRequest request);
}
