/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/** Sole publication surface that may transition a higher generation to COMMITTED. */
public interface GenerationCommitter {
    CompletableFuture<GenerationCommitResult> publish(
            MaterializationTask task,
            MaterializationOutput output,
            Duration timeout);

    default CompletableFuture<GenerationCommitResult> publish(
            MaterializationTask task,
            MaterializationOutput output) {
        return publish(task, output, Duration.ofSeconds(30));
    }
}
