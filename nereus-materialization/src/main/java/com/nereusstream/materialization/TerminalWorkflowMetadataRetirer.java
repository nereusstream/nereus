/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.StreamId;
import java.util.concurrent.CompletableFuture;

/** Bounded proof-driven cleanup of advisory terminal workflow metadata. */
public interface TerminalWorkflowMetadataRetirer {
    CompletableFuture<TerminalWorkflowMetadataRetirementResult> retire(
            StreamId streamId,
            MaterializationPolicy currentPolicy,
            long stableCompletedTrimOffset,
            MaterializationTaskMutationGuard mutationGuard);
}
