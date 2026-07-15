/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.metadata.oxia.VersionedMaterializationTask;
import java.util.concurrent.CompletableFuture;

/** Reconstructs and converges every physical protection required by an active durable task. */
@FunctionalInterface
public interface MaterializationTaskProtectionReconciler {
    CompletableFuture<MaterializationTaskProtections> reconcile(
            VersionedMaterializationTask task);
}
