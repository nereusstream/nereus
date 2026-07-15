/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.StreamId;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** Produces advisory deterministic tasks; persistence remains an explicit task-store operation. */
public interface MaterializationPlanner {
    CompletableFuture<List<MaterializationTask>> plan(
            StreamId streamId,
            OffsetRange requestedRange,
            MaterializationPolicy policy,
            int maxTasks);
}
