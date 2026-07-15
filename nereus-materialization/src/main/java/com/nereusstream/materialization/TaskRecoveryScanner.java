/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.F4ScanToken;
import com.nereusstream.metadata.oxia.TaskScanPage;
import com.nereusstream.metadata.oxia.VersionedMaterializationTask;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Bounded per-stream scanner; global discovery remains owned only by the registered-stream scanner. */
public final class TaskRecoveryScanner {
    private final MaterializationTaskStore tasks;
    private final MaterializationTaskRecovery recovery;
    private final int pageSize;

    public TaskRecoveryScanner(
            MaterializationTaskStore tasks,
            MaterializationTaskRecovery recovery,
            int pageSize) {
        this.tasks = Objects.requireNonNull(tasks, "tasks");
        this.recovery = Objects.requireNonNull(recovery, "recovery");
        if (pageSize <= 0 || pageSize > 1_000) {
            throw new IllegalArgumentException("pageSize must be in [1, 1000]");
        }
        this.pageSize = pageSize;
    }

    public CompletableFuture<TaskRecoveryScanResult> scan(StreamId streamId) {
        return scan(streamId, MaterializationTaskMutationGuard.noOp());
    }

    public CompletableFuture<TaskRecoveryScanResult> scan(
            StreamId streamId,
            MaterializationTaskMutationGuard mutationGuard) {
        try {
            return scanPage(
                    Objects.requireNonNull(streamId, "streamId"),
                    Objects.requireNonNull(mutationGuard, "mutationGuard"),
                    Optional.empty(),
                    0,
                    new EnumMap<>(MaterializationTaskRecoveryAction.class));
        } catch (RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private CompletableFuture<TaskRecoveryScanResult> scanPage(
            StreamId streamId,
            MaterializationTaskMutationGuard mutationGuard,
            Optional<F4ScanToken> continuation,
            int scanned,
            EnumMap<MaterializationTaskRecoveryAction, Integer> actions) {
        return tasks.scan(streamId, continuation, pageSize).thenCompose(page ->
                recoverPage(page, mutationGuard, 0, scanned, actions).thenCompose(nextScanned -> {
                    if (page.continuation().isPresent()) {
                        return scanPage(
                                streamId, mutationGuard, page.continuation(), nextScanned, actions);
                    }
                    return CompletableFuture.completedFuture(new TaskRecoveryScanResult(
                            nextScanned, Map.copyOf(actions)));
                }));
    }

    private CompletableFuture<Integer> recoverPage(
            TaskScanPage page,
            MaterializationTaskMutationGuard mutationGuard,
            int index,
            int scanned,
            EnumMap<MaterializationTaskRecoveryAction, Integer> actions) {
        if (index == page.values().size()) {
            return CompletableFuture.completedFuture(scanned);
        }
        VersionedMaterializationTask task = page.values().get(index);
        return recovery.recover(task, mutationGuard).thenCompose(action -> {
            actions.merge(action, 1, Integer::sum);
            return recoverPage(
                    page, mutationGuard, index + 1, Math.addExact(scanned, 1), actions);
        });
    }
}
