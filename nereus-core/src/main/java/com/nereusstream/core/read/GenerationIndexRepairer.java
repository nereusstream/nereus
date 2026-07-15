/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.read;

import com.nereusstream.api.StreamId;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/** Bounded terminal repair seam for live-tail and recovery-checkpoint evidence. */
@FunctionalInterface
public interface GenerationIndexRepairer {
    CompletableFuture<GenerationIndexRepairResult> repair(
            StreamId streamId, long targetOffset, Duration timeout);
}
