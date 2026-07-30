/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.core.physical.ObjectProtectionOwner;
import com.nereusstream.metadata.oxia.VersionedMaterializationTask;
import com.nereusstream.metadata.oxia.records.TaskLifecycle;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Exact, response-loss-safe source-protection cleanup for one terminal materialization task.
 *
 * <p>The terminal task remains the durable removal authority until every deterministic source
 * reference is absent. Each provider release must revalidate both that exact task root and the
 * caller's external mutation fence. A caller may delete the task only after this operation
 * succeeds.
 */
public final class DefaultTerminalMaterializationSourceProtectionReleaser
        implements TerminalMaterializationSourceProtectionReleaser {
    private final String cluster;
    private final MaterializationTaskStore tasks;
    private final MaterializationSourceProtectionRegistry sourceProtections;
    private final Duration operationTimeout;
    private final ScheduledExecutorService scheduler;

    public DefaultTerminalMaterializationSourceProtectionReleaser(
            String cluster,
            MaterializationTaskStore tasks,
            MaterializationSourceProtectionRegistry sourceProtections,
            Duration operationTimeout,
            ScheduledExecutorService scheduler) {
        this.cluster = requireText(cluster, "cluster");
        this.tasks = Objects.requireNonNull(tasks, "tasks");
        this.sourceProtections = Objects.requireNonNull(sourceProtections, "sourceProtections");
        this.operationTimeout = requirePositive(operationTimeout, "operationTimeout");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
    }

    @Override
    public CompletableFuture<Integer> release(
            VersionedMaterializationTask terminalTask,
            MaterializationTaskMutationGuard mutationGuard) {
        try {
            VersionedMaterializationTask exact =
                    Objects.requireNonNull(terminalTask, "terminalTask");
            requireTerminal(exact.value().lifecycle());
            Operation operation = new Operation(
                    exact,
                    tasks.requireTask(exact),
                    Objects.requireNonNull(mutationGuard, "mutationGuard"));
            CompletableFuture<Integer> result = operation.releaseAt(0, 0);
            result.whenComplete((ignored, failure) -> operation.close());
            return result;
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private final class Operation implements AutoCloseable {
        private final VersionedMaterializationTask terminalTask;
        private final MaterializationTask task;
        private final MaterializationTaskMutationGuard mutationGuard;
        private final MaterializationDeadline deadline;

        private Operation(
                VersionedMaterializationTask terminalTask,
                MaterializationTask task,
                MaterializationTaskMutationGuard mutationGuard) {
            this.terminalTask = terminalTask;
            this.task = task;
            this.mutationGuard = mutationGuard;
            this.deadline = new MaterializationDeadline(operationTimeout, scheduler);
        }

        private CompletableFuture<Integer> releaseAt(int index, int released) {
            if (index == task.sources().size()) {
                return revalidateTerminal().thenApply(ignored -> released);
            }
            SourceGeneration source = task.sources().get(index);
            String referenceId = MaterializationProtectionIdentities.sourceReferenceId(
                    cluster, task, source);
            return revalidateTerminal()
                    .thenCompose(ignored -> deadline.bound(
                            () -> sourceProtections.findExisting(
                                    task.streamId(), source, referenceId),
                            "find terminal materialization source protection"))
                    .thenCompose(optional -> releaseIfPresent(
                            optional, referenceId, index, released));
        }

        private CompletableFuture<Integer> releaseIfPresent(
                Optional<MaterializationSourceProtection> optional,
                String referenceId,
                int index,
                int released) {
            if (optional.isEmpty()) {
                return releaseAt(index + 1, released);
            }
            MaterializationSourceProtection protection = optional.orElseThrow();
            requireOwnedProtection(protection, referenceId);
            return deadline.bound(
                            () -> sourceProtections.release(
                                    protection,
                                    current -> authorizeRelease(current, referenceId)),
                            "release terminal materialization source protection")
                    .thenCompose(ignored -> releaseAt(
                            index + 1, Math.addExact(released, 1)));
        }

        private CompletableFuture<Void> authorizeRelease(
                MaterializationSourceProtection current,
                String referenceId) {
            requireOwnedProtection(current, referenceId);
            return revalidateTerminal();
        }

        private CompletableFuture<Void> revalidateTerminal() {
            return deadline.bound(
                            mutationGuard::revalidate,
                            "revalidate terminal materialization source release authority")
                    .thenCompose(ignored -> deadline.bound(
                            () -> tasks.get(task.streamId(), task.taskId()),
                            "reload terminal materialization task before source release"))
                    .thenAccept(optional -> {
                        if (optional.isEmpty()
                                || !sameVersioned(terminalTask, optional.orElseThrow())
                                || !isTerminal(optional.orElseThrow().value().lifecycle())) {
                            throw condition(
                                    "terminal materialization task changed before source release");
                        }
                    });
        }

        private void requireOwnedProtection(
                MaterializationSourceProtection protection,
                String referenceId) {
            ObjectProtectionOwner owner = protection.owner();
            if (!protection.referenceId().equals(referenceId)
                    || !owner.ownerKey().equals(terminalTask.key())
                    || owner.metadataVersion() > terminalTask.metadataVersion()) {
                throw invariant(
                        "terminal materialization source protection has an invalid owner");
            }
        }

        @Override
        public void close() {
            deadline.close();
        }
    }

    private static boolean sameVersioned(
            VersionedMaterializationTask expected,
            VersionedMaterializationTask actual) {
        return expected.key().equals(actual.key())
                && expected.metadataVersion() == actual.metadataVersion()
                && expected.durableValueSha256().equals(actual.durableValueSha256())
                && expected.value().equals(actual.value());
    }

    private static void requireTerminal(TaskLifecycle lifecycle) {
        if (!isTerminal(lifecycle)) {
            throw new IllegalArgumentException(
                    "source-protection release requires a terminal task: " + lifecycle);
        }
    }

    private static boolean isTerminal(TaskLifecycle lifecycle) {
        return lifecycle == TaskLifecycle.PUBLISHED
                || lifecycle == TaskLifecycle.CANCELLED
                || lifecycle == TaskLifecycle.TERMINAL_FAILED;
    }

    private static Duration requirePositive(Duration value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(field + " must be positive");
        }
        return value;
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }

    private static NereusException condition(String message) {
        return new NereusException(ErrorCode.METADATA_CONDITION_FAILED, true, message);
    }

    private static NereusException invariant(String message) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }
}
