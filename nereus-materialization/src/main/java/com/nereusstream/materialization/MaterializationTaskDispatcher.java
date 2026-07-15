/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.metadata.oxia.VersionedMaterializationTask;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/** Admission seam from recovery/scanning into the bounded worker scheduler. */
@FunctionalInterface
public interface MaterializationTaskDispatcher {
    CompletableFuture<Void> dispatch(
            VersionedMaterializationTask durableTask,
            MaterializationTask task);

    /** Rejects new tasks, cancels queued work and drains/cancels active work by the deadline. */
    default CompletableFuture<Void> closeAsync(
            Duration timeout,
            ScheduledExecutorService scheduler) {
        return CompletableFuture.completedFuture(null);
    }

    default int activeTaskCount() {
        return 0;
    }

    default int queuedTaskCount() {
        return 0;
    }
}
