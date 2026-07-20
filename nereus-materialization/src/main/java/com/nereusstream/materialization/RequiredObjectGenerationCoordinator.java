/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.AppendOutcome;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ProjectionRef;
import com.nereusstream.api.ReadTargetIdentities;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamState;
import com.nereusstream.api.target.BookKeeperEntryRangeReadTarget;
import com.nereusstream.core.append.RequiredObjectGenerationCompletion;
import com.nereusstream.core.append.RequiredObjectGenerationProof;
import com.nereusstream.core.append.RequiredObjectGenerationRequest;
import com.nereusstream.core.capability.GenerationActivationProof;
import com.nereusstream.core.capability.GenerationOperation;
import com.nereusstream.core.capability.GenerationProtocolActivationGuard;
import com.nereusstream.core.capability.LiveProjectionSubject;
import com.nereusstream.metadata.oxia.F4ScanToken;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.ProjectionIdentity;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.VersionedMaterializationStreamRegistration;
import com.nereusstream.metadata.oxia.VersionedMaterializationTask;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * BK-M4 producer barrier built on the one F4 planner/task/worker/publication pipeline.
 *
 * <p>The coordinator first converges one deterministic task for the exact committed append, then waits for the
 * ordinary worker and checkpoint path, and finally requires the same normal-read proof used by source retirement.
 */
public final class RequiredObjectGenerationCoordinator
        implements RequiredObjectGenerationCompletion {
    private static final int MAX_SCANNED_TASKS = 4_096;

    private final String cluster;
    private final OxiaMetadataStore l0;
    private final GenerationMetadataStore generations;
    private final GenerationProtocolActivationGuard activation;
    private final MaterializationPlanner planner;
    private final MaterializationTaskStore tasks;
    private final MaterializationTaskRecovery recovery;
    private final MaterializationService service;
    private final CommittedObjectGenerationAuthority authority;
    private final MaterializationPolicy policy;
    private final int taskScanPageSize;
    private final ScheduledExecutorService scheduler;

    public RequiredObjectGenerationCoordinator(
            String cluster,
            OxiaMetadataStore l0,
            GenerationMetadataStore generations,
            GenerationProtocolActivationGuard activation,
            MaterializationPlanner planner,
            MaterializationTaskStore tasks,
            MaterializationTaskRecovery recovery,
            MaterializationService service,
            CommittedObjectGenerationAuthority authority,
            MaterializationPolicy policy,
            int taskScanPageSize,
            ScheduledExecutorService scheduler) {
        this.cluster = text(cluster, "cluster");
        this.l0 = Objects.requireNonNull(l0, "l0");
        this.generations = Objects.requireNonNull(generations, "generations");
        this.activation = Objects.requireNonNull(activation, "activation");
        this.planner = Objects.requireNonNull(planner, "planner");
        this.tasks = Objects.requireNonNull(tasks, "tasks");
        this.recovery = Objects.requireNonNull(recovery, "recovery");
        this.service = Objects.requireNonNull(service, "service");
        this.authority = Objects.requireNonNull(authority, "authority");
        this.policy = Objects.requireNonNull(policy, "policy");
        if (taskScanPageSize <= 0 || taskScanPageSize > 1_000) {
            throw new IllegalArgumentException("taskScanPageSize must be in [1, 1000]");
        }
        this.taskScanPageSize = taskScanPageSize;
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
    }

    @Override
    public CompletableFuture<RequiredObjectGenerationProof> complete(
            RequiredObjectGenerationRequest request,
            Duration timeout) {
        try {
            RequiredObjectGenerationRequest exact = Objects.requireNonNull(request, "request");
            MaterializationDeadline deadline = new MaterializationDeadline(
                    Objects.requireNonNull(timeout, "timeout"), scheduler);
            CompletableFuture<RequiredObjectGenerationProof> result = proveCurrent(exact, deadline)
                    .thenCompose(existing -> existing.isPresent()
                            ? CompletableFuture.completedFuture(existing.orElseThrow())
                            : admit(exact, deadline)
                                    .thenCompose(admission -> convergeTask(exact, admission, deadline))
                                    .thenCompose(ignored -> deadline.bound(
                                            service::scanNow,
                                            "reconcile required Object-generation checkpoint"))
                                    .thenCompose(ignored -> proveCurrent(exact, deadline))
                                    .thenApply(proof -> proof.orElseThrow(() -> failure(
                                            ErrorCode.METADATA_CONDITION_FAILED,
                                            true,
                                            "required Object generation did not become readable"))));
            result.whenComplete((ignored, failure) -> deadline.close());
            return result;
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private CompletableFuture<Admission> admit(
            RequiredObjectGenerationRequest request,
            MaterializationDeadline deadline) {
        CompletableFuture<StreamMetadataSnapshot> snapshot = deadline.bound(
                () -> l0.getStreamSnapshot(cluster, request.streamId()),
                "load sync-profile stream snapshot");
        CompletableFuture<Optional<VersionedMaterializationStreamRegistration>> registration = deadline.bound(
                () -> generations.getStreamRegistration(cluster, request.streamId()),
                "load sync-profile materialization registration");
        return snapshot.thenCombine(registration, (stream, optional) -> {
                    VersionedMaterializationStreamRegistration registered = optional.orElseThrow(() -> failure(
                            ErrorCode.METADATA_CONDITION_FAILED,
                            true,
                            "sync-profile materialization registration is absent"));
                    ProjectionRef projection = requireAdmission(request, stream, registered);
                    return new LiveProjectionSubject(
                            request.streamId(),
                            projection,
                            new Checksum(
                                    ChecksumType.SHA256,
                                    registered.value().projectionIdentitySha256()));
                })
                .thenCompose(subject -> deadline.bound(
                        () -> activation.requireReady(
                                GenerationOperation.GENERATION_PUBLISH,
                                subject,
                                true),
                        "admit required Object-generation publication"))
                .thenApply(Admission::new);
    }

    private ProjectionRef requireAdmission(
            RequiredObjectGenerationRequest request,
            StreamMetadataSnapshot snapshot,
            VersionedMaterializationStreamRegistration registration) {
        if (!snapshot.metadata().streamId().equals(request.streamId().value())
                || snapshot.committedEnd().committedEndOffset() < request.sourceRange().endOffset()
                || snapshot.committedEnd().commitVersion() < request.sourceCommitVersion()) {
            throw invariant("required Object-generation stream head does not contain the source append");
        }
        StreamState state;
        StorageProfile profile;
        StorageProfile registeredProfile;
        Optional<ProjectionRef> projection;
        try {
            state = StreamState.valueOf(snapshot.metadata().state());
            profile = StorageProfile.valueOf(snapshot.metadata().profile()).canonical();
            registeredProfile = StorageProfile.valueOf(
                    registration.value().storageProfile()).canonical();
            projection = ProjectionIdentity.decode(registration.value().projectionRef());
        } catch (RuntimeException failure) {
            throw invariant("sync-profile materialization admission identity is invalid", failure);
        }
        if (state != StreamState.ACTIVE
                || profile != StorageProfile.BOOKKEEPER_WAL_SYNC_OBJECT
                || registeredProfile != profile
                || !registration.value().streamId().equals(request.streamId().value())
                || projection.isEmpty()) {
            throw failure(
                    ErrorCode.METADATA_CONDITION_FAILED,
                    true,
                    "stream no longer admits synchronous BookKeeper Object publication");
        }
        return projection.orElseThrow();
    }

    private CompletableFuture<Void> convergeTask(
            RequiredObjectGenerationRequest request,
            Admission admission,
            MaterializationDeadline deadline) {
        MaterializationTaskMutationGuard guard = () -> activation.revalidate(admission.proof());
        return deadline.bound(
                        () -> planner.plan(request.streamId(), request.sourceRange(), policy, 1),
                        "plan exact required Object-generation task")
                .thenCompose(planned -> {
                    if (!planned.isEmpty()) {
                        MaterializationTask task = requireExactTask(request, planned.get(0));
                        return deadline.bound(
                                        () -> tasks.create(task, guard),
                                        "create exact required Object-generation task")
                                .thenCompose(durable -> recover(durable, guard, deadline));
                    }
                    return findExistingTask(request, Optional.empty(), new ArrayList<>(), 0, deadline)
                            .thenCompose(existing -> existing.isPresent()
                                    ? recover(existing.orElseThrow(), guard, deadline)
                                    : deadline.bound(
                                            service::scanNow,
                                            "recover already-published required Object generation")
                                            .thenApply(ignored -> null));
                });
    }

    private CompletableFuture<Void> recover(
            VersionedMaterializationTask durable,
            MaterializationTaskMutationGuard guard,
            MaterializationDeadline deadline) {
        tasks.requireTask(durable, policy);
        return deadline.bound(
                        () -> recovery.recover(durable, guard),
                        "execute or recover exact required Object-generation task")
                .thenApply(ignored -> null);
    }

    private CompletableFuture<Optional<VersionedMaterializationTask>> findExistingTask(
            RequiredObjectGenerationRequest request,
            Optional<F4ScanToken> continuation,
            List<VersionedMaterializationTask> matches,
            int observed,
            MaterializationDeadline deadline) {
        return deadline.bound(
                        () -> tasks.scan(request.streamId(), continuation, taskScanPageSize),
                        "scan deterministic required Object-generation task")
                .thenCompose(page -> {
                    int nextObserved = Math.addExact(observed, page.values().size());
                    if (nextObserved > MAX_SCANNED_TASKS) {
                        throw failure(
                                ErrorCode.METADATA_LIMIT_EXCEEDED,
                                false,
                                "required Object-generation task scan exceeds 4096");
                    }
                    for (VersionedMaterializationTask durable : page.values()) {
                        MaterializationTask task = tasks.requireTask(durable);
                        if (task.policyDigestSha256().equals(policy.digestSha256())
                                && matches(request, task)) {
                            matches.add(durable);
                        }
                    }
                    if (matches.size() > 1) {
                        throw invariant("multiple deterministic tasks own one required append range");
                    }
                    if (page.continuation().isPresent()) {
                        return findExistingTask(
                                request, page.continuation(), matches, nextObserved, deadline);
                    }
                    return CompletableFuture.completedFuture(matches.stream().findFirst());
                });
    }

    private MaterializationTask requireExactTask(
            RequiredObjectGenerationRequest request,
            MaterializationTask task) {
        if (!matches(request, task)) {
            throw invariant("planner did not return the exact single-source required task");
        }
        return task;
    }

    private boolean matches(
            RequiredObjectGenerationRequest request,
            MaterializationTask task) {
        return task.streamId().equals(request.streamId())
                && task.coverage().equals(request.sourceRange())
                && task.policyDigestSha256().equals(policy.digestSha256())
                && task.sources().size() == 1
                && task.sources().get(0).generation() == 0
                && task.sources().get(0).commitVersion() == request.sourceCommitVersion()
                && task.sources().get(0).range().equals(request.sourceRange())
                && task.sources().get(0).readTarget() instanceof BookKeeperEntryRangeReadTarget;
    }

    private CompletableFuture<Optional<RequiredObjectGenerationProof>> proveCurrent(
            RequiredObjectGenerationRequest request,
            MaterializationDeadline deadline) {
        return deadline.bound(
                        () -> authority.prove(
                                request.streamId(),
                                request.sourceRange(),
                                request.sourceCommitVersion()),
                        "prove required Object generation through normal read path")
                .thenApply(optional -> optional
                        .filter(this::usesCurrentPolicy)
                        .map(proof -> toProof(request, proof)));
    }

    private boolean usesCurrentPolicy(CommittedObjectGenerationProof proof) {
        return proof.index().value().generation() > 0
                && proof.index().value().policySha256().equals(policy.digestSha256().value())
                && proof.checkpoint().value().policyId().equals(policy.policyId())
                && proof.checkpoint().value().policyVersion() == policy.policyVersion();
    }

    private static RequiredObjectGenerationProof toProof(
            RequiredObjectGenerationRequest request,
            CommittedObjectGenerationProof proof) {
        return new RequiredObjectGenerationProof(
                request,
                proof.index().value().taskId(),
                proof.index().value().generation(),
                proof.index().key(),
                proof.index().metadataVersion(),
                proof.index().durableValueSha256(),
                ReadTargetIdentities.sha256(proof.target()));
    }

    private static NereusException failure(
            ErrorCode code,
            boolean retriable,
            String message) {
        return new NereusException(
                code,
                retriable,
                message,
                AppendOutcome.KNOWN_COMMITTED);
    }

    private static NereusException invariant(String message) {
        return invariant(message, null);
    }

    private static NereusException invariant(String message, Throwable cause) {
        return new NereusException(
                ErrorCode.METADATA_INVARIANT_VIOLATION,
                false,
                message,
                cause,
                AppendOutcome.KNOWN_COMMITTED);
    }

    private static String text(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }

    private record Admission(GenerationActivationProof proof) {
        private Admission {
            Objects.requireNonNull(proof, "proof");
        }
    }
}
