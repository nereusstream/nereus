/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.metadata.oxia.VersionedMaterializationTask;
import java.util.concurrent.CompletableFuture;

/** Admission seam from recovery/scanning into the bounded worker scheduler. */
@FunctionalInterface
public interface MaterializationTaskDispatcher {
    CompletableFuture<Void> dispatch(
            VersionedMaterializationTask durableTask,
            MaterializationTask task);
}
