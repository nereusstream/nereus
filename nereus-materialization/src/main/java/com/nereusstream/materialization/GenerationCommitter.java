/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import java.util.concurrent.CompletableFuture;

/** Sole publication surface that may transition a higher generation to COMMITTED. */
public interface GenerationCommitter {
    CompletableFuture<GenerationCommitResult> publish(
            MaterializationTask task,
            MaterializationOutput output);
}
