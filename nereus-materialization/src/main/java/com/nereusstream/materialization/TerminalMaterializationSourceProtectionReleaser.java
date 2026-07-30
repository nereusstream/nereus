/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.metadata.oxia.VersionedMaterializationTask;
import java.util.concurrent.CompletableFuture;

/** Releases every exact task-owned source protection before a terminal task root is deleted. */
@FunctionalInterface
public interface TerminalMaterializationSourceProtectionReleaser {
    CompletableFuture<Integer> release(
            VersionedMaterializationTask terminalTask,
            MaterializationTaskMutationGuard mutationGuard);
}
