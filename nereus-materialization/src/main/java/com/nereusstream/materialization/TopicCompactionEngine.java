/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.ReadOptions;
import java.util.concurrent.CompletableFuture;

/** Bounded deterministic two-pass builder for one sparse TOPIC_COMPACTED output. */
public interface TopicCompactionEngine {
    CompletableFuture<TopicCompactionPlan> prepare(
            MaterializationTask task,
            ExactSourceRangeReader sourceReader,
            ReadOptions readOptions,
            TopicCompactionRegistry.Binding binding,
            long planningTimeMillis);
}
