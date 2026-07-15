/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Re-enters the idempotent publication state machine for a recovered durable task/output pair. */
public final class GenerationPublicationReconciler {
    private final GenerationCommitter committer;

    public GenerationPublicationReconciler(GenerationCommitter committer) {
        this.committer = Objects.requireNonNull(committer, "committer");
    }

    public CompletableFuture<GenerationCommitResult> reconcile(
            MaterializationTask task,
            MaterializationOutput output) {
        try {
            return Objects.requireNonNull(
                    committer.publish(
                            Objects.requireNonNull(task, "task"),
                            Objects.requireNonNull(output, "output")),
                    "generation publication reconciliation future");
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }
}
