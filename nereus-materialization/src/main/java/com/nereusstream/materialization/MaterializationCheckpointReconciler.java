/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.VersionedMaterializationCheckpoint;
import java.util.concurrent.CompletableFuture;

/** Rebuilds advisory progress only from authoritative committed-generation coverage. */
@FunctionalInterface
public interface MaterializationCheckpointReconciler {
    CompletableFuture<VersionedMaterializationCheckpoint> reconcile(
            StreamId streamId,
            MaterializationPolicy policy,
            MaterializationTaskMutationGuard mutationGuard);
}
