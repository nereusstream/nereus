/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Sole publication surface that may transition a higher generation to COMMITTED. */
public interface GenerationCommitter {
    CompletableFuture<GenerationCommitResult> publish(
            MaterializationTask task, MaterializationOutput output);

    /**
     * Publishes with an additional caller-owned authority fence.
     *
     * <p>Implementations that own the COMMITTED CAS should override this method and revalidate the
     * guard immediately before that CAS. The default preserves compatibility while still checking
     * the guard before delegating.
     */
    default CompletableFuture<GenerationCommitResult> publish(
            MaterializationTask task,
            MaterializationOutput output,
            MaterializationTaskMutationGuard authorityGuard) {
        try {
            MaterializationTaskMutationGuard exact =
                    Objects.requireNonNull(authorityGuard, "authorityGuard");
            return Objects.requireNonNull(
                            exact.revalidate(), "generation publication authority future")
                    .thenCompose(ignored -> publish(task, output));
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }
}
