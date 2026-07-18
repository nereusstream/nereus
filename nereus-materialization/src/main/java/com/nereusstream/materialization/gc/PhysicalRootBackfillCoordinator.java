/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.metadata.oxia.VersionedGenerationProtocolActivation;
import java.util.concurrent.CompletableFuture;

/** Cluster-wide live-reference root/protection backfill contract. */
public interface PhysicalRootBackfillCoordinator {
    CompletableFuture<PhysicalRootBackfillReport> run(
            PhysicalRootBackfillRequest request);

    /**
     * Scans and protects the next readiness epoch without publishing partial
     * proofs. Only the rollover coordinator may atomically install the report.
     */
    default CompletableFuture<PhysicalRootBackfillReport> runRollover(
            PhysicalRootBackfillRequest request,
            VersionedGenerationProtocolActivation current) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException(
                "physical-root backfill does not implement deletion-active readiness rollover"));
    }
}
