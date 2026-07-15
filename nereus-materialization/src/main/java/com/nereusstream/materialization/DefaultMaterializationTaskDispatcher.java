/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.metadata.oxia.VersionedMaterializationTask;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/** Admits one recovered task onto the worker executor and continues through generation publication. */
public final class DefaultMaterializationTaskDispatcher implements MaterializationTaskDispatcher {
    private final MaterializationWorker worker;
    private final GenerationCommitter committer;
    private final Executor workerExecutor;

    public DefaultMaterializationTaskDispatcher(
            MaterializationWorker worker,
            GenerationCommitter committer,
            Executor workerExecutor) {
        this.worker = Objects.requireNonNull(worker, "worker");
        this.committer = Objects.requireNonNull(committer, "committer");
        this.workerExecutor = Objects.requireNonNull(workerExecutor, "workerExecutor");
    }

    @Override
    public CompletableFuture<Void> dispatch(
            VersionedMaterializationTask durable,
            MaterializationTask task) {
        Objects.requireNonNull(durable, "durable");
        Objects.requireNonNull(task, "task");
        try {
            return CompletableFuture.runAsync(() -> { }, workerExecutor)
                    .thenCompose(ignored -> worker.execute(task))
                    .thenCompose(output -> committer.publish(task, output))
                    .thenApply(ignored -> null);
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }
}
