/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import java.util.concurrent.CompletableFuture;

/** Slow root-last retirement boundary for one exact DELETED physical-object root. */
@FunctionalInterface
public interface PhysicalRootTombstoneRetirementCoordinator {
    CompletableFuture<TombstoneRetirementResult> retire(
            VersionedPhysicalObjectRoot deletedRoot);
}
