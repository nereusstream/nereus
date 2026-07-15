/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.core.physical.ObjectProtection;
import com.nereusstream.core.physical.ObjectProtectionManager;
import com.nereusstream.core.physical.ObjectProtectionOwner;
import com.nereusstream.core.physical.ObjectProtectionRequest;
import com.nereusstream.core.read.PhysicalObjectIdentityResolver;
import com.nereusstream.metadata.oxia.F4MetadataConditionFailedException;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.VersionedMaterializationTask;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import com.nereusstream.metadata.oxia.records.TaskLifecycle;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/** Exact source/output protection reconstruction before publication becomes terminal. */
public final class DefaultMaterializationTaskProtectionReconciler
        implements MaterializationTaskProtectionReconciler {
    private final String cluster;
    private final MaterializationTaskStore tasks;
    private final GenerationMetadataStore generations;
    private final PhysicalObjectIdentityResolver identities;
    private final ObjectProtectionManager protections;
    private final Duration operationTimeout;
    private final ScheduledExecutorService scheduler;

    public DefaultMaterializationTaskProtectionReconciler(
            String cluster,
            MaterializationTaskStore tasks,
            GenerationMetadataStore generations,
            PhysicalObjectIdentityResolver identities,
            ObjectProtectionManager protections,
            Duration operationTimeout,
            ScheduledExecutorService scheduler) {
        this.cluster = requireText(cluster, "cluster");
        this.tasks = Objects.requireNonNull(tasks, "tasks");
        this.generations = Objects.requireNonNull(generations, "generations");
        this.identities = Objects.requireNonNull(identities, "identities");
        this.protections = Objects.requireNonNull(protections, "protections");
        this.operationTimeout = requirePositive(operationTimeout, "operationTimeout");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
    }

    @Override
    public CompletableFuture<MaterializationTaskProtections> reconcile(
            VersionedMaterializationTask durable) {
        try {
            VersionedMaterializationTask expected = Objects.requireNonNull(durable, "durable");
            MaterializationTask task = tasks.requireTask(expected);
            requireProtectedLifecycle(expected.value().lifecycle());
            return new Operation(expected, task).run();
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private final class Operation {
        private final VersionedMaterializationTask durable;
        private final MaterializationTask task;
        private final MaterializationDeadline deadline;
        private final ObjectProtectionOwner owner;
        private final List<ObjectProtection> sourceProtections = new ArrayList<>();

        private Operation(
                VersionedMaterializationTask durable,
                MaterializationTask task) {
            this.durable = durable;
            this.task = task;
            this.deadline = new MaterializationDeadline(operationTimeout, scheduler);
            this.owner = MaterializationProtectionIdentities.taskOwner(durable);
        }

        private CompletableFuture<MaterializationTaskProtections> run() {
            return revalidateTaskOwner(owner)
                    .thenCompose(ignored -> reconcileSource(0))
                    .thenCompose(ignored -> reconcileOutput())
                    .thenCompose(output -> revalidateTaskOwner(owner)
                            .thenApply(ignored -> new MaterializationTaskProtections(
                                    durable, sourceProtections, output)));
        }

        private CompletableFuture<Void> reconcileSource(int index) {
            if (index == task.sources().size()) {
                return CompletableFuture.completedFuture(null);
            }
            SourceGeneration source = task.sources().get(index);
            if (!(source.readTarget() instanceof ObjectSliceReadTarget target)) {
                return CompletableFuture.failedFuture(new NereusException(
                        ErrorCode.UNSUPPORTED_READ_TARGET,
                        false,
                        "materialization source protection requires an object-slice target"));
            }
            return deadline.bound(
                            () -> identities.resolve(target, source.view()),
                            "resolve materialization source protection identity")
                    .thenCompose(identity -> deadline.bound(
                            () -> protections.acquireOrTransfer(
                                    new ObjectProtectionRequest(
                                            identity,
                                            ObjectProtectionType.MATERIALIZATION_SOURCE,
                                            MaterializationProtectionIdentities.sourceReferenceId(
                                                    cluster, task, source),
                                            owner,
                                            0),
                                    this::revalidateTaskOwner),
                            "reconcile materialization source protection"))
                    .thenCompose(protection -> revalidateSource(source)
                            .thenApply(ignored -> {
                                sourceProtections.add(protection);
                                return null;
                            }))
                    .thenCompose(ignored -> reconcileSource(index + 1));
        }

        private CompletableFuture<Optional<ObjectProtection>> reconcileOutput() {
            if (durable.value().lifecycle() == TaskLifecycle.CLAIMED) {
                if (durable.value().output().isPresent()) {
                    return CompletableFuture.failedFuture(invariant(
                            "CLAIMED materialization task cannot carry an output"));
                }
                return CompletableFuture.completedFuture(Optional.empty());
            }
            MaterializationOutput output = MaterializationRecordMapper.domainOutput(
                    task, durable.value().output().orElseThrow(() -> invariant(
                            "ready/publishing/published task is missing output")));
            return deadline.bound(
                            () -> protections.acquireOrTransfer(
                                    new ObjectProtectionRequest(
                                            MaterializationRecordMapper.physicalIdentity(output),
                                            ObjectProtectionType.MATERIALIZATION_OUTPUT,
                                            MaterializationProtectionIdentities.outputReferenceId(
                                                    cluster, task, output),
                                            owner,
                                            0),
                                    this::revalidateTaskOwner),
                            "reconcile materialization output protection")
                    .thenApply(Optional::of);
        }

        private CompletableFuture<Void> revalidateSource(SourceGeneration source) {
            return deadline.bound(
                            () -> generations.getCandidate(
                                    cluster,
                                    task.streamId(),
                                    source.view(),
                                    source.range().endOffset(),
                                    source.generation()),
                            "revalidate protected materialization source")
                    .thenAccept(actual -> {
                        if (actual.isEmpty()
                                || !MaterializationSourceMapper.matchesExactSource(
                                        actual.orElseThrow(), task.streamId(), source)) {
                            throw new F4MetadataConditionFailedException(
                                    "materialization source changed during protection recovery");
                        }
                    });
        }

        private CompletableFuture<Void> revalidateTaskOwner(
                ObjectProtectionOwner expectedOwner) {
            if (!owner.equals(expectedOwner)) {
                return CompletableFuture.failedFuture(condition(
                        "materialization protection owner differs from recovered task"));
            }
            return deadline.bound(
                            () -> tasks.get(task.streamId(), task.taskId()),
                            "revalidate materialization task protection owner")
                    .thenAccept(actual -> {
                        if (actual.isEmpty()
                                || !sameVersioned(durable, actual.orElseThrow())) {
                            throw condition("materialization task changed during protection recovery");
                        }
                    });
        }
    }

    private static void requireProtectedLifecycle(TaskLifecycle lifecycle) {
        if (lifecycle != TaskLifecycle.CLAIMED
                && lifecycle != TaskLifecycle.OUTPUT_READY
                && lifecycle != TaskLifecycle.PUBLISHING) {
            throw new IllegalArgumentException(
                    "task lifecycle does not own materialization protections: " + lifecycle);
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
