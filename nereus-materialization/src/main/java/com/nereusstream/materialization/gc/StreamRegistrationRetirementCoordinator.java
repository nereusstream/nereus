/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.ProjectionRef;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamState;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.api.target.ReadTarget;
import com.nereusstream.core.capability.GenerationProjectionAuthorityReader;
import com.nereusstream.core.capability.GenerationProjectionAuthoritySnapshot;
import com.nereusstream.core.capability.LiveProjectionSubject;
import com.nereusstream.core.capability.StreamRetirementReferenceAuthorityReader;
import com.nereusstream.core.capability.StreamRetirementReferenceAuthoritySnapshot;
import com.nereusstream.materialization.MaterializationDeadline;
import com.nereusstream.metadata.oxia.AppendRecoveryAnchor;
import com.nereusstream.metadata.oxia.F4Keyspace;
import com.nereusstream.metadata.oxia.F4ScanToken;
import com.nereusstream.metadata.oxia.GenerationIndexDigests;
import com.nereusstream.metadata.oxia.GenerationIndexIdentity;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.ObjectProtectionIdentity;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.PhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.ProjectionIdentity;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.metadata.oxia.VersionedGenerationSequence;
import com.nereusstream.metadata.oxia.VersionedGenerationZeroIndex;
import com.nereusstream.metadata.oxia.VersionedMaterializationCheckpoint;
import com.nereusstream.metadata.oxia.VersionedMaterializationStreamRegistration;
import com.nereusstream.metadata.oxia.VersionedMaterializationTask;
import com.nereusstream.metadata.oxia.VersionedObjectProtection;
import com.nereusstream.metadata.oxia.VersionedRangeRetentionStats;
import com.nereusstream.metadata.oxia.VersionedRecoveryCheckpointRoot;
import com.nereusstream.metadata.oxia.codec.GenerationIndexRecordCodecV1;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import com.nereusstream.metadata.oxia.records.ObjectProtectionRecord;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import com.nereusstream.metadata.oxia.records.RecoveryCheckpointReferenceRecord;
import com.nereusstream.metadata.oxia.records.SourceGenerationRecord;
import com.nereusstream.metadata.oxia.records.TaskLifecycle;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointCodecV1;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointFormatV1;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointObject;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointPublication;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

/**
 * Final, proof-driven retirement of one deleted stream incarnation from the 64-shard registry.
 *
 * <p>Registration removal is always last. Every destructive predecessor is conditional, bounded,
 * and remains safe if the process stops before the final registration delete: generation/task/root
 * reference domains continue to expose any owner record that still exists.
 */
public final class StreamRegistrationRetirementCoordinator {
    private static final List<ReadView> VIEWS =
            List.of(ReadView.COMMITTED, ReadView.TOPIC_COMPACTED);
    private static final EnumSet<TaskLifecycle> TERMINAL_TASKS = EnumSet.of(
            TaskLifecycle.PUBLISHED,
            TaskLifecycle.CANCELLED,
            TaskLifecycle.TERMINAL_FAILED);
    private static final EnumSet<GenerationLifecycle> TERMINAL_INDEXES = EnumSet.of(
            GenerationLifecycle.RETIRED,
            GenerationLifecycle.ABORTED);
    private static final int MAX_DELETE_RECOVERY_ATTEMPTS = 8;

    private final String cluster;
    private final OxiaMetadataStore l0;
    private final GenerationMetadataStore generations;
    private final PhysicalObjectMetadataStore physical;
    private final GenerationProjectionAuthorityReader projections;
    private final StreamRetirementReferenceAuthorityReader externalReferences;
    private final RecoveryCheckpointCodecV1 checkpoints;
    private final PhysicalGcConfig config;
    private final long metadataAuditGraceMillis;
    private final Clock clock;
    private final ScheduledExecutorService scheduler;
    private final F4Keyspace keys;
    private final ReadTargetCodecRegistry targetCodecs = ReadTargetCodecRegistry.phase15();
    private final GenerationIndexRecordCodecV1 generationCodec =
            new GenerationIndexRecordCodecV1();
    private final RecoveryReplacementVerifier recoveryVerifier;

    public StreamRegistrationRetirementCoordinator(
            String cluster,
            OxiaMetadataStore l0,
            GenerationMetadataStore generations,
            PhysicalObjectMetadataStore physical,
            GenerationProjectionAuthorityReader projections,
            StreamRetirementReferenceAuthorityReader externalReferences,
            RecoveryCheckpointCodecV1 checkpoints,
            PhysicalGcConfig config,
            Duration metadataAuditGrace,
            Clock clock,
            ScheduledExecutorService scheduler) {
        this.cluster = requireText(cluster, "cluster");
        this.l0 = Objects.requireNonNull(l0, "l0");
        this.generations = Objects.requireNonNull(generations, "generations");
        this.physical = Objects.requireNonNull(physical, "physical");
        this.projections = Objects.requireNonNull(projections, "projections");
        this.externalReferences = Objects.requireNonNull(
                externalReferences, "externalReferences");
        this.checkpoints = Objects.requireNonNull(checkpoints, "checkpoints");
        this.config = Objects.requireNonNull(config, "config");
        this.metadataAuditGraceMillis = requirePositiveMillis(
                metadataAuditGrace, "metadataAuditGrace");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.keys = new F4Keyspace(cluster);
        this.recoveryVerifier = new RecoveryReplacementVerifier(
                cluster, generations, physical, checkpoints, config);
    }

    public CompletableFuture<StreamRegistrationRetirementResult> retire(StreamId streamId) {
        StreamId exactStream = Objects.requireNonNull(streamId, "streamId");
        if (!config.enabled()) {
            return CompletableFuture.completedFuture(simple(
                    exactStream,
                    0,
                    StreamRegistrationRetirementStatus.DISABLED));
        }
        Operation operation = new Operation(exactStream);
        CompletableFuture<StreamRegistrationRetirementResult> result;
        try {
            result = operation.run();
        } catch (Throwable failure) {
            operation.close();
            return CompletableFuture.failedFuture(failure);
        }
        result.whenComplete((ignored, failure) -> operation.close());
        return result;
    }

    private final class Operation implements AutoCloseable {
        private final StreamId stream;
        private final MaterializationDeadline deadline;
        private int protectionsRetired;
        private int indexesRetired;
        private int tasksRetired;
        private int checkpointsRetired;
        private int retentionStatsRetired;
        private int sequencesRetired;
        private boolean recoveryRootRetired;

        private Operation(StreamId stream) {
            this.stream = stream;
            this.deadline = new MaterializationDeadline(
                    config.operationTimeout(), scheduler);
        }

        private CompletableFuture<StreamRegistrationRetirementResult> run() {
            return bound(
                            () -> generations.getStreamRegistration(cluster, stream),
                            "load stream registration for retirement")
                    .thenCompose(optional -> optional
                            .<CompletableFuture<StreamRegistrationRetirementResult>>map(
                                    this::runRegistered)
                            .orElseGet(() -> CompletableFuture.completedFuture(simple(
                                    stream,
                                    0,
                                    StreamRegistrationRetirementStatus.ALREADY_ABSENT))));
        }

        private CompletableFuture<StreamRegistrationRetirementResult> runRegistered(
                VersionedMaterializationStreamRegistration registration) {
            LiveProjectionSubject subject = subject(registration);
            return captureBasis(registration, subject).thenCompose(capture -> {
                StreamRegistrationRetirementStatus blocked = validateCapture(capture);
                if (blocked != null) {
                    return completed(registration, blocked);
                }
                return scanWorkflow().thenCompose(workflow -> {
                    StreamRegistrationRetirementStatus workflowBlock = classify(workflow);
                    if (workflowBlock != null) {
                        return completed(registration, workflowBlock);
                    }
                    return bound(
                                    () -> generations.getRecoveryRoot(cluster, stream),
                                    "load recovery root for registration retirement")
                            .thenCompose(root -> requireEmptyRecoveryTail(root)
                                    .thenCompose(tailEmpty -> {
                                        if (!tailEmpty) {
                                            return completed(
                                                    registration,
                                                    StreamRegistrationRetirementStatus
                                                            .RECOVERY_TAIL_PRESENT);
                                        }
                                        return buildOwnerScope(workflow, root)
                                                .thenCompose(scope -> {
                                                    if (scope.limitExceeded()) {
                                                        return completed(
                                                                registration,
                                                                StreamRegistrationRetirementStatus
                                                                        .LIMIT_EXCEEDED);
                                                    }
                                                    if (config.dryRun()) {
                                                        return completed(
                                                                registration,
                                                                StreamRegistrationRetirementStatus
                                                                        .DRY_RUN);
                                                    }
                                                    return mutate(
                                                            capture,
                                                            workflow,
                                                            root,
                                                            scope);
                                                });
                                    }));
                });
            });
        }

        private CompletableFuture<StreamRegistrationRetirementResult> mutate(
                RetirementCapture capture,
                WorkflowSnapshot workflow,
                Optional<VersionedRecoveryCheckpointRoot> recoveryRoot,
                OwnerScope scope) {
            return retireProtectionKind(capture, scope, OwnerKind.INDEX, 0)
                    .thenCompose(exact -> exact
                            ? retireIndexes(workflow.indexes(), 0)
                            : CompletableFuture.completedFuture(false))
                    .thenCompose(exact -> exact
                            ? requireIndexesEmpty()
                            : CompletableFuture.completedFuture(false))
                    .thenCompose(exact -> exact
                            ? retireProtectionKind(capture, scope, OwnerKind.TASK, 0)
                            : CompletableFuture.completedFuture(false))
                    .thenCompose(exact -> exact
                            ? retireTasks(workflow.tasks(), 0)
                            : CompletableFuture.completedFuture(false))
                    .thenCompose(exact -> exact
                            ? requireTasksEmpty()
                            : CompletableFuture.completedFuture(false))
                    .thenCompose(exact -> exact
                            ? retireProtectionKind(capture, scope, OwnerKind.RECOVERY_ROOT, 0)
                            : CompletableFuture.completedFuture(false))
                    .thenCompose(exact -> exact
                            ? deleteRecoveryRoot(recoveryRoot, 0)
                            : CompletableFuture.completedFuture(false))
                    .thenCompose(exact -> exact
                            ? retireCheckpoints(workflow.checkpoints(), 0)
                            : CompletableFuture.completedFuture(false))
                    .thenCompose(exact -> exact
                            ? retireStats(workflow.stats(), 0)
                            : CompletableFuture.completedFuture(false))
                    .thenCompose(exact -> exact
                            ? retireSequences(workflow.sequences(), 0)
                            : CompletableFuture.completedFuture(false))
                    .thenCompose(exact -> exact
                            ? requireAllStreamMetadataEmpty()
                            : CompletableFuture.completedFuture(false))
                    .thenCompose(exact -> {
                        if (!exact) {
                            return completed(
                                    capture.registration(),
                                    StreamRegistrationRetirementStatus.VERSION_CHANGED);
                        }
                        return revalidateCapture(capture).thenCompose(matches -> {
                            if (!matches) {
                                return completed(
                                        capture.registration(),
                                        StreamRegistrationRetirementStatus.VERSION_CHANGED);
                            }
                            return deleteRegistration(capture.registration(), 0)
                                    .thenCompose(retired -> retired
                                            ? CompletableFuture.completedFuture(result(
                                                    capture.registration(),
                                                    StreamRegistrationRetirementStatus.RETIRED,
                                                    true))
                                            : completed(
                                                    capture.registration(),
                                                    StreamRegistrationRetirementStatus
                                                            .VERSION_CHANGED));
                        });
                    });
        }

        private CompletableFuture<RetirementCapture> captureBasis(
                VersionedMaterializationStreamRegistration registration,
                LiveProjectionSubject subject) {
            return bound(
                            () -> l0.getStreamSnapshot(cluster, stream),
                            "capture deleted L0 stream authority")
                    .thenCompose(streamSnapshot -> bound(
                                    () -> projections.capture(subject),
                                    "capture projection retirement authority")
                            .thenCompose(projection -> bound(
                                            () -> externalReferences.capture(subject),
                                            "capture external stream references")
                                    .thenApply(external -> new RetirementCapture(
                                            registration,
                                            subject,
                                            streamSnapshot,
                                            projection,
                                            external))));
        }

        private StreamRegistrationRetirementStatus validateCapture(
                RetirementCapture capture) {
            if (!capture.streamSnapshot().metadata().streamId().equals(stream.value())) {
                throw invariant("L0 stream snapshot belongs to another stream");
            }
            StreamState state;
            StorageProfile registeredProfile;
            StorageProfile actualProfile;
            try {
                state = StreamState.valueOf(
                        capture.streamSnapshot().metadata().state());
                registeredProfile = StorageProfile.valueOf(
                                capture.registration().value().storageProfile())
                        .canonical();
                actualProfile = StorageProfile.valueOf(
                                capture.streamSnapshot().metadata().profile())
                        .canonical();
            } catch (RuntimeException failure) {
                throw invariant("retirement authority contains an unknown stream profile/state", failure);
            }
            if (registeredProfile != actualProfile
                    || !actualProfile.objectMaterializationEnabled()
                    || capture.registration().value().lastHintCommitVersion()
                            > capture.streamSnapshot().committedEnd().commitVersion()) {
                return StreamRegistrationRetirementStatus.VERSION_CHANGED;
            }
            if (state != StreamState.DELETED) {
                return StreamRegistrationRetirementStatus.STREAM_NOT_DELETED;
            }
            if (!capture.projection().subject().equals(capture.subject())) {
                throw invariant("projection authority returned another subject");
            }
            if (capture.projection().live()) {
                return StreamRegistrationRetirementStatus.PROJECTION_LIVE;
            }
            if (!capture.external().subject().equals(capture.subject())) {
                throw invariant("external retirement authority returned another subject");
            }
            if (!capture.external().complete()) {
                return StreamRegistrationRetirementStatus.LIMIT_EXCEEDED;
            }
            if (!capture.external().referenceFree()) {
                return StreamRegistrationRetirementStatus.EXTERNAL_REFERENCE;
            }
            return null;
        }

        private CompletableFuture<Boolean> revalidateCapture(RetirementCapture expected) {
            return bound(
                            () -> generations.getStreamRegistration(cluster, stream),
                            "revalidate stream registration")
                    .thenCompose(registration -> {
                        if (!registration.equals(Optional.of(expected.registration()))) {
                            return CompletableFuture.completedFuture(false);
                        }
                        return bound(
                                        () -> l0.getStreamSnapshot(cluster, stream),
                                        "revalidate deleted L0 stream authority")
                                .thenCompose(streamSnapshot -> {
                                    if (!streamSnapshot.sameVersionedAuthority(
                                            expected.streamSnapshot())) {
                                        return CompletableFuture.completedFuture(false);
                                    }
                                    return bound(
                                                    () -> projections.capture(expected.subject()),
                                                    "revalidate projection retirement authority")
                                            .thenCompose(projection -> {
                                                if (!projection.equals(expected.projection())) {
                                                    return CompletableFuture.completedFuture(false);
                                                }
                                                return bound(
                                                                () -> externalReferences.capture(
                                                                        expected.subject()),
                                                                "revalidate external stream references")
                                                        .thenApply(external ->
                                                                external.equals(expected.external()));
                                            });
                                });
                    });
        }

        private CompletableFuture<WorkflowSnapshot> scanWorkflow() {
            WorkflowAccumulator accumulator = new WorkflowAccumulator(
                    config.maxAuthoritiesPerDomainSnapshot());
            return scanTasks(Optional.empty(), null, accumulator)
                    .thenCompose(ignored -> scanIndexes(
                            0, Optional.empty(), null, accumulator))
                    .thenCompose(ignored -> scanCheckpoints(
                            Optional.empty(), null, accumulator))
                    .thenCompose(ignored -> scanStats(
                            Optional.empty(), null, accumulator))
                    .thenCompose(ignored -> loadSequences(0, accumulator))
                    .thenApply(ignored -> accumulator.snapshot());
        }

        private CompletableFuture<Void> scanTasks(
                Optional<F4ScanToken> continuation,
                String previousKey,
                WorkflowAccumulator accumulator) {
            if (accumulator.limitExceeded) {
                return CompletableFuture.completedFuture(null);
            }
            return bound(
                            () -> generations.scanTasks(
                                    cluster,
                                    stream,
                                    continuation,
                                    config.metadataScanPageSize()),
                            "scan stream tasks for registration retirement")
                    .thenCompose(page -> {
                        requireProgress(page.values(), previousKey, VersionedMaterializationTask::key, "task");
                        for (VersionedMaterializationTask task : page.values()) {
                            if (!task.value().streamId().equals(stream.value())) {
                                throw invariant("task scan escaped the retiring stream");
                            }
                            if (!accumulator.addTask(task)) {
                                return CompletableFuture.completedFuture(null);
                            }
                        }
                        if (page.continuation().isEmpty()) {
                            return CompletableFuture.completedFuture(null);
                        }
                        return scanTasks(
                                page.continuation(),
                                page.values().get(page.values().size() - 1).key(),
                                accumulator);
                    });
        }

        private CompletableFuture<Void> scanIndexes(
                int viewIndex,
                Optional<F4ScanToken> continuation,
                String previousKey,
                WorkflowAccumulator accumulator) {
            if (accumulator.limitExceeded || viewIndex == VIEWS.size()) {
                return CompletableFuture.completedFuture(null);
            }
            ReadView view = VIEWS.get(viewIndex);
            return bound(
                            () -> generations.scanIndex(
                                    cluster,
                                    stream,
                                    view,
                                    0,
                                    Long.MAX_VALUE,
                                    continuation,
                                    config.metadataScanPageSize()),
                            "scan stream generation indexes for registration retirement")
                    .thenCompose(page -> {
                        requireProgress(page.values(), previousKey, VersionedGenerationCandidate::key, "index");
                        for (VersionedGenerationCandidate candidate : page.values()) {
                            requireIndexIdentity(stream, view, candidate);
                            if (!accumulator.addIndex(candidate)) {
                                return CompletableFuture.completedFuture(null);
                            }
                        }
                        if (page.continuation().isPresent()) {
                            return scanIndexes(
                                    viewIndex,
                                    page.continuation(),
                                    page.values().get(page.values().size() - 1).key(),
                                    accumulator);
                        }
                        return scanIndexes(
                                viewIndex + 1,
                                Optional.empty(),
                                null,
                                accumulator);
                    });
        }

        private CompletableFuture<Void> scanCheckpoints(
                Optional<F4ScanToken> continuation,
                String previousKey,
                WorkflowAccumulator accumulator) {
            if (accumulator.limitExceeded) {
                return CompletableFuture.completedFuture(null);
            }
            return bound(
                            () -> generations.scanMaterializationCheckpoints(
                                    cluster,
                                    stream,
                                    continuation,
                                    config.metadataScanPageSize()),
                            "scan stream checkpoints for registration retirement")
                    .thenCompose(page -> {
                        requireProgress(
                                page.values(),
                                previousKey,
                                VersionedMaterializationCheckpoint::key,
                                "checkpoint");
                        for (VersionedMaterializationCheckpoint checkpoint : page.values()) {
                            if (!checkpoint.value().streamId().equals(stream.value())) {
                                throw invariant("checkpoint scan escaped the retiring stream");
                            }
                            if (!accumulator.addCheckpoint(checkpoint)) {
                                return CompletableFuture.completedFuture(null);
                            }
                        }
                        if (page.continuation().isEmpty()) {
                            return CompletableFuture.completedFuture(null);
                        }
                        return scanCheckpoints(
                                page.continuation(),
                                page.values().get(page.values().size() - 1).key(),
                                accumulator);
                    });
        }

        private CompletableFuture<Void> scanStats(
                Optional<F4ScanToken> continuation,
                String previousKey,
                WorkflowAccumulator accumulator) {
            if (accumulator.limitExceeded) {
                return CompletableFuture.completedFuture(null);
            }
            return bound(
                            () -> generations.scanRangeRetentionStats(
                                    cluster,
                                    stream,
                                    0,
                                    Long.MAX_VALUE,
                                    continuation,
                                    config.metadataScanPageSize()),
                            "scan stream retention stats for registration retirement")
                    .thenCompose(page -> {
                        requireProgress(
                                page.values(),
                                previousKey,
                                VersionedRangeRetentionStats::key,
                                "retention-stats");
                        for (VersionedRangeRetentionStats stats : page.values()) {
                            if (!stats.value().streamId().equals(stream.value())) {
                                throw invariant("retention-stats scan escaped the retiring stream");
                            }
                            if (!accumulator.addStats(stats)) {
                                return CompletableFuture.completedFuture(null);
                            }
                        }
                        if (page.continuation().isEmpty()) {
                            return CompletableFuture.completedFuture(null);
                        }
                        return scanStats(
                                page.continuation(),
                                page.values().get(page.values().size() - 1).key(),
                                accumulator);
                    });
        }

        private CompletableFuture<Void> loadSequences(
                int viewIndex,
                WorkflowAccumulator accumulator) {
            if (accumulator.limitExceeded || viewIndex == VIEWS.size()) {
                return CompletableFuture.completedFuture(null);
            }
            ReadView view = VIEWS.get(viewIndex);
            return bound(
                            () -> generations.getSequence(cluster, stream, view),
                            "load stream generation sequence for registration retirement")
                    .thenCompose(optional -> {
                        optional.ifPresent(sequence -> accumulator.addSequence(view, sequence));
                        return loadSequences(viewIndex + 1, accumulator);
                    });
        }

        private StreamRegistrationRetirementStatus classify(WorkflowSnapshot workflow) {
            if (workflow.limitExceeded()) {
                return StreamRegistrationRetirementStatus.LIMIT_EXCEEDED;
            }
            for (VersionedMaterializationTask task : workflow.tasks()) {
                if (!TERMINAL_TASKS.contains(task.value().lifecycle())) {
                    return StreamRegistrationRetirementStatus.TASK_NOT_TERMINAL;
                }
                if (!auditGraceElapsed(task.value().updatedAtMillis())) {
                    return StreamRegistrationRetirementStatus.AUDIT_GRACE_PENDING;
                }
            }
            for (VersionedGenerationCandidate candidate : workflow.indexes()) {
                if (candidate instanceof VersionedGenerationZeroIndex) {
                    return StreamRegistrationRetirementStatus.INDEX_STILL_LIVE;
                }
                VersionedGenerationIndex higher = (VersionedGenerationIndex) candidate;
                if (!TERMINAL_INDEXES.contains(higher.value().lifecycle())) {
                    return StreamRegistrationRetirementStatus.INDEX_STILL_LIVE;
                }
                if (!auditGraceElapsed(higher.value().stateChangedAtMillis())) {
                    return StreamRegistrationRetirementStatus.AUDIT_GRACE_PENDING;
                }
            }
            return null;
        }

        private CompletableFuture<Boolean> requireEmptyRecoveryTail(
                Optional<VersionedRecoveryCheckpointRoot> optionalRoot) {
            if (optionalRoot.isEmpty()) {
                return CompletableFuture.completedFuture(true);
            }
            VersionedRecoveryCheckpointRoot root = optionalRoot.orElseThrow();
            AppendRecoveryAnchor anchor = recoveryAnchor(root);
            return bound(
                            () -> l0.readAppendRecoveryTail(
                                    cluster,
                                    stream,
                                    anchor,
                                    Optional.empty(),
                                    1),
                            "prove recovery tail empty before root retirement")
                    .thenApply(page -> page.anchorReached()
                            && page.continuation().isEmpty()
                            && page.commitsNewestFirst().isEmpty());
        }

        private CompletableFuture<OwnerScope> buildOwnerScope(
                WorkflowSnapshot workflow,
                Optional<VersionedRecoveryCheckpointRoot> recoveryRoot) {
            OwnerScopeBuilder builder = new OwnerScopeBuilder(
                    config.maxReferencesPerDomainSnapshot());
            for (VersionedMaterializationTask task : workflow.tasks()) {
                OwnerFact owner = OwnerFact.task(task);
                for (ObjectKeyHash object : taskObjects(task)) {
                    builder.add(owner, object);
                }
            }
            for (VersionedGenerationCandidate candidate : workflow.indexes()) {
                if (candidate instanceof VersionedGenerationIndex higher) {
                    OwnerFact owner = OwnerFact.index(higher);
                    builder.add(owner, targetObject(higher.value().readTarget()));
                }
            }
            if (recoveryRoot.isEmpty()) {
                return CompletableFuture.completedFuture(builder.build());
            }
            VersionedRecoveryCheckpointRoot root = recoveryRoot.orElseThrow();
            OwnerFact owner = OwnerFact.recoveryRoot(root);
            return addRecoveryReferences(
                            root,
                            owner,
                            builder,
                            0)
                    .thenApply(ignored -> builder.build());
        }

        private CompletableFuture<Void> addRecoveryReferences(
                VersionedRecoveryCheckpointRoot root,
                OwnerFact owner,
                OwnerScopeBuilder builder,
                int index) {
            if (builder.limitExceeded
                    || index == root.value().checkpoints().size()) {
                return CompletableFuture.completedFuture(null);
            }
            RecoveryCheckpointReferenceRecord reference =
                    root.value().checkpoints().get(index);
            builder.add(owner, new ObjectKeyHash(reference.objectKeyHash()));
            if (builder.limitExceeded) {
                return CompletableFuture.completedFuture(null);
            }
            Checksum content = new Checksum(
                    ChecksumType.SHA256, reference.contentSha256());
            return bound(
                            () -> checkpoints.openAndVerify(
                                    new ObjectKey(reference.objectKey()),
                                    reference.objectLength(),
                                    content,
                                    deadline.remaining()),
                            "open recovery checkpoint for root-owner retirement")
                    .thenCompose(checkpoint -> {
                        recoveryVerifier.requireCheckpointIdentity(
                                stream, reference, checkpoint);
                        return scanRecoveryPublications(
                                root,
                                checkpoint,
                                owner,
                                builder,
                                OptionalInt.empty());
                    })
                    .thenCompose(ignored -> addRecoveryReferences(
                            root, owner, builder, index + 1));
        }

        private CompletableFuture<Void> scanRecoveryPublications(
                VersionedRecoveryCheckpointRoot root,
                RecoveryCheckpointObject checkpoint,
                OwnerFact owner,
                OwnerScopeBuilder builder,
                OptionalInt continuation) {
            if (builder.limitExceeded) {
                return CompletableFuture.completedFuture(null);
            }
            return bound(
                            () -> checkpoints.scanPublications(
                                    checkpoint,
                                    continuation,
                                    RecoveryCheckpointFormatV1
                                            .MAX_PUBLICATION_SCAN_PAGE_SIZE,
                                    deadline.remaining()),
                            "scan recovery checkpoint publications for owner retirement")
                    .thenCompose(page -> {
                        for (RecoveryCheckpointPublication publication : page.values()) {
                            GenerationIndexRecord embedded = decodePublication(
                                    root, publication);
                            builder.add(owner, targetObject(embedded.readTarget()));
                            if (builder.limitExceeded) {
                                return CompletableFuture.completedFuture(null);
                            }
                        }
                        return page.continuation().isPresent()
                                ? scanRecoveryPublications(
                                        root,
                                        checkpoint,
                                        owner,
                                        builder,
                                        page.continuation())
                                : CompletableFuture.completedFuture(null);
                    });
        }

        private GenerationIndexRecord decodePublication(
                VersionedRecoveryCheckpointRoot root,
                RecoveryCheckpointPublication publication) {
            byte[] canonical = bytes(publication.canonicalGenerationIndexRecord());
            GenerationIndexRecord record;
            try {
                record = generationCodec.decode(canonical);
            } catch (RuntimeException failure) {
                throw invariant("cannot decode recovery checkpoint publication", failure);
            }
            if (!Arrays.equals(canonical, generationCodec.encode(record))
                    || record.metadataVersion() != 0
                    || record.lifecycle() != GenerationLifecycle.COMMITTED
                    || record.readViewId() != ReadView.COMMITTED.wireId()
                    || !record.streamId().equals(stream.value())
                    || record.generation() != publication.generation()
                    || !record.publicationId().equals(
                            publication.publicationId().value())
                    || record.offsetStart() != publication.coverage().startOffset()
                    || record.offsetEnd() != publication.coverage().endOffset()
                    || !GenerationIndexDigests.canonicalRecordSha256(record)
                            .equals(publication.generationIndexRecordSha256())
                    || !root.value().streamId().equals(record.streamId())) {
                throw invariant("recovery checkpoint publication is not canonical");
            }
            return record;
        }

        private CompletableFuture<Boolean> retireProtectionKind(
                RetirementCapture capture,
                OwnerScope scope,
                OwnerKind kind,
                int attempt) {
            if (!scope.hasKind(kind)) {
                return CompletableFuture.completedFuture(true);
            }
            if (attempt >= MAX_DELETE_RECOVERY_ATTEMPTS) {
                return CompletableFuture.failedFuture(invariant(
                        "stream-owner protection retirement did not converge"));
            }
            return revalidateCapture(capture).thenCompose(matches -> {
                if (!matches) {
                    return CompletableFuture.completedFuture(false);
                }
                return requireOwnersExact(scope, kind, 0).thenCompose(exact -> {
                    if (!exact) {
                        return CompletableFuture.completedFuture(false);
                    }
                    return scanOwnerProtections(scope, kind)
                            .thenCompose(scan -> {
                                if (scan.limitExceeded()) {
                                    return CompletableFuture.failedFuture(invariant(
                                            "owner protection scan exceeded its configured bound"));
                                }
                                if (scan.values().isEmpty()) {
                                    return CompletableFuture.completedFuture(true);
                                }
                                return deleteProtections(scan.values(), 0)
                                        .thenCompose(deleted -> deleted
                                                ? retireProtectionKind(
                                                        capture,
                                                        scope,
                                                        kind,
                                                        attempt + 1)
                                                : CompletableFuture.completedFuture(false));
                            });
                });
            });
        }

        private CompletableFuture<Boolean> requireOwnersExact(
                OwnerScope scope,
                OwnerKind kind,
                int index) {
            List<OwnerFact> owners = scope.owners(kind);
            if (index == owners.size()) {
                return CompletableFuture.completedFuture(true);
            }
            OwnerFact owner = owners.get(index);
            CompletableFuture<Boolean> exact = switch (kind) {
                case INDEX -> {
                    VersionedGenerationIndex expected = owner.index().orElseThrow();
                    GenerationIndexIdentity identity = indexIdentity(expected);
                    yield bound(
                                    () -> generations.getIndex(cluster, identity),
                                    "revalidate terminal generation owner")
                            .thenApply(current -> current.equals(Optional.of(expected)));
                }
                case TASK -> {
                    VersionedMaterializationTask expected = owner.task().orElseThrow();
                    yield bound(
                                    () -> generations.getTask(
                                            cluster,
                                            stream,
                                            expected.value().taskId()),
                                    "revalidate terminal task owner")
                            .thenApply(current -> current.equals(Optional.of(expected)));
                }
                case RECOVERY_ROOT -> {
                    VersionedRecoveryCheckpointRoot expected =
                            owner.recoveryRoot().orElseThrow();
                    yield bound(
                                    () -> generations.getRecoveryRoot(cluster, stream),
                                    "revalidate recovery-root owner")
                            .thenApply(current -> current.equals(Optional.of(expected)));
                }
            };
            return exact.thenCompose(matches -> matches
                    ? requireOwnersExact(scope, kind, index + 1)
                    : CompletableFuture.completedFuture(false));
        }

        private CompletableFuture<ProtectionScan> scanOwnerProtections(
                OwnerScope scope,
                OwnerKind kind) {
            ProtectionScanAccumulator accumulator = new ProtectionScanAccumulator(
                    config.maxReferencesPerDomainSnapshot());
            return scanOwnerObject(
                            scope,
                            kind,
                            scope.objects(kind),
                            0,
                            Optional.empty(),
                            accumulator)
                    .thenApply(ignored -> accumulator.snapshot());
        }

        private CompletableFuture<Void> scanOwnerObject(
                OwnerScope scope,
                OwnerKind kind,
                List<ObjectKeyHash> objects,
                int objectIndex,
                Optional<F4ScanToken> continuation,
                ProtectionScanAccumulator accumulator) {
            if (accumulator.limitExceeded || objectIndex == objects.size()) {
                return CompletableFuture.completedFuture(null);
            }
            ObjectKeyHash object = objects.get(objectIndex);
            return bound(
                            () -> physical.scanProtections(
                                    cluster,
                                    object,
                                    continuation,
                                    config.metadataScanPageSize()),
                            "scan protections owned by retiring stream metadata")
                    .thenCompose(page -> {
                        for (VersionedObjectProtection protection : page.values()) {
                            if (!protection.value().objectKeyHash().equals(object.value())) {
                                throw invariant("protection scan escaped its physical object");
                            }
                            Optional<OwnerFact> owner = scope.owner(
                                    kind, protection.value().ownerKey());
                            if (owner.isPresent()) {
                                validateProtectionOwner(owner.orElseThrow(), protection);
                                if (!accumulator.add(protection)) {
                                    return CompletableFuture.completedFuture(null);
                                }
                            }
                        }
                        if (page.continuation().isPresent()) {
                            return scanOwnerObject(
                                    scope,
                                    kind,
                                    objects,
                                    objectIndex,
                                    page.continuation(),
                                    accumulator);
                        }
                        return scanOwnerObject(
                                scope,
                                kind,
                                objects,
                                objectIndex + 1,
                                Optional.empty(),
                                accumulator);
                    });
        }

        private void validateProtectionOwner(
                OwnerFact owner,
                VersionedObjectProtection protection) {
            ObjectProtectionRecord value = protection.value();
            if (value.ownerMetadataVersion() > owner.metadataVersion()) {
                throw invariant("protection owner version is ahead of its durable owner");
            }
            if (value.ownerMetadataVersion() == owner.metadataVersion()
                    && !value.ownerIdentitySha256().equals(
                            owner.identitySha256().value())) {
                throw invariant("one protection owner version has conflicting identities");
            }
            ObjectProtectionType type = ObjectProtectionType.fromWireId(
                    value.protectionTypeId());
            boolean allowed = switch (owner.kind()) {
                case INDEX -> type == ObjectProtectionType.VISIBLE_GENERATION;
                case TASK -> type == ObjectProtectionType.MATERIALIZATION_SOURCE
                        || type == ObjectProtectionType.MATERIALIZATION_OUTPUT
                        || type == ObjectProtectionType.VISIBLE_GENERATION;
                case RECOVERY_ROOT -> type == ObjectProtectionType.RECOVERY_CHECKPOINT_OBJECT
                        || type == ObjectProtectionType.RECOVERY_CHECKPOINT_TARGET
                        || type == ObjectProtectionType.RECOVERY_CHECKPOINT_PENDING;
            };
            if (!allowed) {
                throw invariant("durable owner holds an unexpected protection type");
            }
        }

        private CompletableFuture<Boolean> deleteProtections(
                List<VersionedObjectProtection> values,
                int index) {
            if (index == values.size()) {
                return CompletableFuture.completedFuture(true);
            }
            return deleteProtection(values.get(index), 0).thenCompose(deleted -> {
                if (!deleted) {
                    return CompletableFuture.completedFuture(false);
                }
                protectionsRetired = Math.addExact(protectionsRetired, 1);
                return deleteProtections(values, index + 1);
            });
        }

        private CompletableFuture<Boolean> deleteProtection(
                VersionedObjectProtection expected,
                int attempt) {
            ObjectProtectionIdentity identity = protectionIdentity(expected.value());
            return bound(
                            () -> physical.deleteProtection(
                                    cluster,
                                    identity,
                                    expected.metadataVersion()),
                            "conditionally delete retiring-owner protection")
                    .handle((ignored, failure) -> {
                        if (failure == null) {
                            return CompletableFuture.completedFuture(true);
                        }
                        Throwable original = unwrap(failure);
                        return findProtection(identity, Optional.empty())
                                .thenCompose(current -> {
                                    if (current.isEmpty()) {
                                        return CompletableFuture.completedFuture(true);
                                    }
                                    if (!current.orElseThrow().equals(expected)) {
                                        return CompletableFuture.completedFuture(false);
                                    }
                                    if (attempt + 1 >= MAX_DELETE_RECOVERY_ATTEMPTS) {
                                        return CompletableFuture.failedFuture(original);
                                    }
                                    return deleteProtection(
                                            current.orElseThrow(), attempt + 1);
                                });
                    })
                    .thenCompose(Function.identity());
        }

        private CompletableFuture<Optional<VersionedObjectProtection>> findProtection(
                ObjectProtectionIdentity identity,
                Optional<F4ScanToken> continuation) {
            return bound(
                            () -> physical.scanProtections(
                                    cluster,
                                    identity.object(),
                                    continuation,
                                    config.metadataScanPageSize()),
                            "reload protection after uncertain delete")
                    .thenCompose(page -> {
                        Optional<VersionedObjectProtection> found = page.values().stream()
                                .filter(value -> protectionIdentity(value.value())
                                        .equals(identity))
                                .findFirst();
                        if (found.isPresent() || page.continuation().isEmpty()) {
                            return CompletableFuture.completedFuture(found);
                        }
                        return findProtection(identity, page.continuation());
                    });
        }

        private CompletableFuture<Boolean> retireIndexes(
                List<VersionedGenerationCandidate> indexes,
                int index) {
            if (index == indexes.size()) {
                return CompletableFuture.completedFuture(true);
            }
            VersionedGenerationIndex expected =
                    (VersionedGenerationIndex) indexes.get(index);
            return deleteIndex(expected, 0).thenCompose(deleted -> {
                if (!deleted) {
                    return CompletableFuture.completedFuture(false);
                }
                indexesRetired = Math.addExact(indexesRetired, 1);
                return retireIndexes(indexes, index + 1);
            });
        }

        private CompletableFuture<Boolean> deleteIndex(
                VersionedGenerationIndex expected,
                int attempt) {
            GenerationIndexIdentity identity = indexIdentity(expected);
            return bound(
                            () -> generations.deleteIndex(
                                    cluster,
                                    identity,
                                    expected.metadataVersion()),
                            "conditionally delete terminal generation index")
                    .handle((ignored, failure) -> {
                        if (failure == null) {
                            return CompletableFuture.completedFuture(true);
                        }
                        Throwable original = unwrap(failure);
                        return bound(
                                        () -> generations.getIndex(cluster, identity),
                                        "reload terminal index after uncertain delete")
                                .thenCompose(current -> {
                                    if (current.isEmpty()) {
                                        return CompletableFuture.completedFuture(true);
                                    }
                                    if (!current.orElseThrow().equals(expected)) {
                                        return CompletableFuture.completedFuture(false);
                                    }
                                    if (attempt + 1 >= MAX_DELETE_RECOVERY_ATTEMPTS) {
                                        return CompletableFuture.failedFuture(original);
                                    }
                                    return deleteIndex(
                                            current.orElseThrow(), attempt + 1);
                                });
                    })
                    .thenCompose(Function.identity());
        }

        private CompletableFuture<Boolean> requireIndexesEmpty() {
            WorkflowAccumulator accumulator = new WorkflowAccumulator(
                    config.maxAuthoritiesPerDomainSnapshot());
            return scanIndexes(0, Optional.empty(), null, accumulator)
                    .thenApply(ignored -> !accumulator.limitExceeded
                            && accumulator.indexes.isEmpty());
        }

        private CompletableFuture<Boolean> retireTasks(
                List<VersionedMaterializationTask> tasks,
                int index) {
            if (index == tasks.size()) {
                return CompletableFuture.completedFuture(true);
            }
            VersionedMaterializationTask expected = tasks.get(index);
            return deleteTask(expected, 0).thenCompose(deleted -> {
                if (!deleted) {
                    return CompletableFuture.completedFuture(false);
                }
                tasksRetired = Math.addExact(tasksRetired, 1);
                return retireTasks(tasks, index + 1);
            });
        }

        private CompletableFuture<Boolean> deleteTask(
                VersionedMaterializationTask expected,
                int attempt) {
            return bound(
                            () -> generations.deleteTask(
                                    cluster,
                                    stream,
                                    expected.value().taskId(),
                                    expected.metadataVersion()),
                            "conditionally delete terminal task")
                    .handle((ignored, failure) -> {
                        if (failure == null) {
                            return CompletableFuture.completedFuture(true);
                        }
                        Throwable original = unwrap(failure);
                        return bound(
                                        () -> generations.getTask(
                                                cluster,
                                                stream,
                                                expected.value().taskId()),
                                        "reload terminal task after uncertain delete")
                                .thenCompose(current -> {
                                    if (current.isEmpty()) {
                                        return CompletableFuture.completedFuture(true);
                                    }
                                    if (!current.orElseThrow().equals(expected)) {
                                        return CompletableFuture.completedFuture(false);
                                    }
                                    if (attempt + 1 >= MAX_DELETE_RECOVERY_ATTEMPTS) {
                                        return CompletableFuture.failedFuture(original);
                                    }
                                    return deleteTask(
                                            current.orElseThrow(), attempt + 1);
                                });
                    })
                    .thenCompose(Function.identity());
        }

        private CompletableFuture<Boolean> requireTasksEmpty() {
            WorkflowAccumulator accumulator = new WorkflowAccumulator(
                    config.maxAuthoritiesPerDomainSnapshot());
            return scanTasks(Optional.empty(), null, accumulator)
                    .thenApply(ignored -> !accumulator.limitExceeded
                            && accumulator.tasks.isEmpty());
        }

        private CompletableFuture<Boolean> deleteRecoveryRoot(
                Optional<VersionedRecoveryCheckpointRoot> expected,
                int attempt) {
            if (expected.isEmpty()) {
                return CompletableFuture.completedFuture(true);
            }
            VersionedRecoveryCheckpointRoot root = expected.orElseThrow();
            return bound(
                            () -> generations.deleteRecoveryRoot(
                                    cluster, stream, root.metadataVersion()),
                            "conditionally delete drained recovery root")
                    .handle((ignored, failure) -> {
                        if (failure == null) {
                            recoveryRootRetired = true;
                            return CompletableFuture.completedFuture(true);
                        }
                        Throwable original = unwrap(failure);
                        return bound(
                                        () -> generations.getRecoveryRoot(cluster, stream),
                                        "reload recovery root after uncertain delete")
                                .thenCompose(current -> {
                                    if (current.isEmpty()) {
                                        recoveryRootRetired = true;
                                        return CompletableFuture.completedFuture(true);
                                    }
                                    if (!current.orElseThrow().equals(root)) {
                                        return CompletableFuture.completedFuture(false);
                                    }
                                    if (attempt + 1 >= MAX_DELETE_RECOVERY_ATTEMPTS) {
                                        return CompletableFuture.failedFuture(original);
                                    }
                                    return deleteRecoveryRoot(current, attempt + 1);
                                });
                    })
                    .thenCompose(Function.identity());
        }

        private CompletableFuture<Boolean> retireCheckpoints(
                List<VersionedMaterializationCheckpoint> values,
                int index) {
            if (index == values.size()) {
                return CompletableFuture.completedFuture(true);
            }
            VersionedMaterializationCheckpoint expected = values.get(index);
            return deleteCheckpoint(expected, 0).thenCompose(deleted -> {
                if (!deleted) {
                    return CompletableFuture.completedFuture(false);
                }
                checkpointsRetired = Math.addExact(checkpointsRetired, 1);
                return retireCheckpoints(values, index + 1);
            });
        }

        private CompletableFuture<Boolean> deleteCheckpoint(
                VersionedMaterializationCheckpoint expected,
                int attempt) {
            return bound(
                            () -> generations.deleteMaterializationCheckpoint(
                                    cluster,
                                    stream,
                                    expected.value().policyId(),
                                    expected.value().policyVersion(),
                                    expected.metadataVersion()),
                            "conditionally delete deleted-stream checkpoint")
                    .handle((ignored, failure) -> {
                        if (failure == null) {
                            return CompletableFuture.completedFuture(true);
                        }
                        Throwable original = unwrap(failure);
                        return bound(
                                        () -> generations.getMaterializationCheckpoint(
                                                cluster,
                                                stream,
                                                expected.value().policyId(),
                                                expected.value().policyVersion()),
                                        "reload checkpoint after uncertain delete")
                                .thenCompose(current -> {
                                    if (current.isEmpty()) {
                                        return CompletableFuture.completedFuture(true);
                                    }
                                    if (!current.orElseThrow().equals(expected)) {
                                        return CompletableFuture.completedFuture(false);
                                    }
                                    if (attempt + 1 >= MAX_DELETE_RECOVERY_ATTEMPTS) {
                                        return CompletableFuture.failedFuture(original);
                                    }
                                    return deleteCheckpoint(
                                            current.orElseThrow(), attempt + 1);
                                });
                    })
                    .thenCompose(Function.identity());
        }

        private CompletableFuture<Boolean> retireStats(
                List<VersionedRangeRetentionStats> values,
                int index) {
            if (index == values.size()) {
                return CompletableFuture.completedFuture(true);
            }
            VersionedRangeRetentionStats expected = values.get(index);
            return deleteStats(expected, 0).thenCompose(deleted -> {
                if (!deleted) {
                    return CompletableFuture.completedFuture(false);
                }
                retentionStatsRetired = Math.addExact(retentionStatsRetired, 1);
                return retireStats(values, index + 1);
            });
        }

        private CompletableFuture<Boolean> deleteStats(
                VersionedRangeRetentionStats expected,
                int attempt) {
            return bound(
                            () -> generations.deleteRangeRetentionStats(
                                    cluster,
                                    stream,
                                    expected.value().offsetEnd(),
                                    expected.value().commitVersion(),
                                    expected.metadataVersion()),
                            "conditionally delete deleted-stream retention stats")
                    .handle((ignored, failure) -> {
                        if (failure == null) {
                            return CompletableFuture.completedFuture(true);
                        }
                        Throwable original = unwrap(failure);
                        return bound(
                                        () -> generations.getRangeRetentionStats(
                                                cluster,
                                                stream,
                                                expected.value().offsetEnd(),
                                                expected.value().commitVersion()),
                                        "reload retention stats after uncertain delete")
                                .thenCompose(current -> {
                                    if (current.isEmpty()) {
                                        return CompletableFuture.completedFuture(true);
                                    }
                                    if (!current.orElseThrow().equals(expected)) {
                                        return CompletableFuture.completedFuture(false);
                                    }
                                    if (attempt + 1 >= MAX_DELETE_RECOVERY_ATTEMPTS) {
                                        return CompletableFuture.failedFuture(original);
                                    }
                                    return deleteStats(
                                            current.orElseThrow(), attempt + 1);
                                });
                    })
                    .thenCompose(Function.identity());
        }

        private CompletableFuture<Boolean> retireSequences(
                List<ViewSequence> values,
                int index) {
            if (index == values.size()) {
                return CompletableFuture.completedFuture(true);
            }
            ViewSequence expected = values.get(index);
            return deleteSequence(expected, 0).thenCompose(deleted -> {
                if (!deleted) {
                    return CompletableFuture.completedFuture(false);
                }
                sequencesRetired = Math.addExact(sequencesRetired, 1);
                return retireSequences(values, index + 1);
            });
        }

        private CompletableFuture<Boolean> deleteSequence(
                ViewSequence expected,
                int attempt) {
            return bound(
                            () -> generations.deleteSequence(
                                    cluster,
                                    stream,
                                    expected.view(),
                                    expected.value().metadataVersion()),
                            "conditionally delete deleted-stream generation sequence")
                    .handle((ignored, failure) -> {
                        if (failure == null) {
                            return CompletableFuture.completedFuture(true);
                        }
                        Throwable original = unwrap(failure);
                        return bound(
                                        () -> generations.getSequence(
                                                cluster, stream, expected.view()),
                                        "reload generation sequence after uncertain delete")
                                .thenCompose(current -> {
                                    if (current.isEmpty()) {
                                        return CompletableFuture.completedFuture(true);
                                    }
                                    if (!current.orElseThrow().equals(expected.value())) {
                                        return CompletableFuture.completedFuture(false);
                                    }
                                    if (attempt + 1 >= MAX_DELETE_RECOVERY_ATTEMPTS) {
                                        return CompletableFuture.failedFuture(original);
                                    }
                                    return deleteSequence(expected, attempt + 1);
                                });
                    })
                    .thenCompose(Function.identity());
        }

        private CompletableFuture<Boolean> requireAllStreamMetadataEmpty() {
            return scanWorkflow().thenCompose(workflow -> {
                if (workflow.limitExceeded()
                        || !workflow.tasks().isEmpty()
                        || !workflow.indexes().isEmpty()
                        || !workflow.checkpoints().isEmpty()
                        || !workflow.stats().isEmpty()
                        || !workflow.sequences().isEmpty()) {
                    return CompletableFuture.completedFuture(false);
                }
                return bound(
                                () -> generations.getRecoveryRoot(cluster, stream),
                                "revalidate recovery-root absence")
                        .thenApply(Optional::isEmpty);
            });
        }

        private CompletableFuture<Boolean> deleteRegistration(
                VersionedMaterializationStreamRegistration expected,
                int attempt) {
            return bound(
                            () -> generations.deleteStreamRegistration(
                                    cluster,
                                    stream,
                                    expected.metadataVersion()),
                            "conditionally delete final stream registration")
                    .handle((ignored, failure) -> {
                        if (failure == null) {
                            return CompletableFuture.completedFuture(true);
                        }
                        Throwable original = unwrap(failure);
                        return bound(
                                        () -> generations.getStreamRegistration(
                                                cluster, stream),
                                        "reload registration after uncertain delete")
                                .thenCompose(current -> {
                                    if (current.isEmpty()) {
                                        return CompletableFuture.completedFuture(true);
                                    }
                                    if (!current.orElseThrow().equals(expected)) {
                                        return CompletableFuture.completedFuture(false);
                                    }
                                    if (attempt + 1 >= MAX_DELETE_RECOVERY_ATTEMPTS) {
                                        return CompletableFuture.failedFuture(original);
                                    }
                                    return deleteRegistration(
                                            current.orElseThrow(), attempt + 1);
                                });
                    })
                    .thenCompose(Function.identity());
        }

        private <T> CompletableFuture<T> bound(
                java.util.function.Supplier<CompletableFuture<T>> operation,
                String stage) {
            return deadline.bound(operation, stage);
        }

        private CompletableFuture<StreamRegistrationRetirementResult> completed(
                VersionedMaterializationStreamRegistration registration,
                StreamRegistrationRetirementStatus status) {
            return CompletableFuture.completedFuture(result(registration, status, false));
        }

        private StreamRegistrationRetirementResult result(
                VersionedMaterializationStreamRegistration registration,
                StreamRegistrationRetirementStatus status,
                boolean registrationRetired) {
            return new StreamRegistrationRetirementResult(
                    stream,
                    registration.metadataVersion(),
                    status,
                    protectionsRetired,
                    indexesRetired,
                    tasksRetired,
                    checkpointsRetired,
                    retentionStatsRetired,
                    sequencesRetired,
                    recoveryRootRetired,
                    registrationRetired);
        }

        @Override
        public void close() {
            deadline.close();
        }
    }

    private LiveProjectionSubject subject(
            VersionedMaterializationStreamRegistration registration) {
        StreamId stream = new StreamId(registration.value().streamId());
        if (!registration.key().equals(keys.materializationRegistryKey(stream))) {
            throw invariant("registration key/value identity mismatch");
        }
        Optional<ProjectionRef> projection;
        try {
            projection = ProjectionIdentity.decode(registration.value().projectionRef());
        } catch (RuntimeException failure) {
            throw invariant("registration projection reference is malformed", failure);
        }
        return new LiveProjectionSubject(
                stream,
                projection.orElseThrow(() -> invariant(
                        "registration has no projection reference")),
                new Checksum(
                        ChecksumType.SHA256,
                        registration.value().projectionIdentitySha256()));
    }

    private void requireIndexIdentity(
            StreamId expectedStream,
            ReadView expectedView,
            VersionedGenerationCandidate candidate) {
        if (candidate instanceof VersionedGenerationZeroIndex zero) {
            if (expectedView != ReadView.COMMITTED
                    || !zero.value().streamId().equals(expectedStream)
                    || !zero.key().equals(keys.generationIndexKey(
                            expectedStream,
                            expectedView,
                            zero.value().offsetEnd(),
                            0))) {
                throw invariant("generation-zero index has a mismatched stream/view identity");
            }
            return;
        }
        VersionedGenerationIndex higher = (VersionedGenerationIndex) candidate;
        if (!higher.value().streamId().equals(expectedStream.value())
                || ReadView.fromWireId(higher.value().readViewId()) != expectedView
                || !higher.key().equals(keys.generationIndexKey(
                        expectedStream,
                        expectedView,
                        higher.value().offsetEnd(),
                        higher.value().generation()))) {
            throw invariant("higher-generation index has a mismatched stream/view identity");
        }
    }

    private List<ObjectKeyHash> taskObjects(VersionedMaterializationTask task) {
        LinkedHashSet<ObjectKeyHash> objects = new LinkedHashSet<>();
        for (SourceGenerationRecord source : task.value().sources()) {
            objects.add(targetObject(source.readTarget()));
        }
        task.value().output().ifPresent(output ->
                objects.add(new ObjectKeyHash(output.objectKeyHash())));
        return List.copyOf(objects);
    }

    private ObjectKeyHash targetObject(
            com.nereusstream.metadata.oxia.records.ReadTargetRecord encoded) {
        ReadTarget target;
        try {
            target = targetCodecs.decode(encoded);
        } catch (RuntimeException failure) {
            throw invariant("stream-retirement read target cannot be decoded", failure);
        }
        if (!(target instanceof ObjectSliceReadTarget object)) {
            throw invariant("stream-retirement owner does not reference an object slice");
        }
        return ObjectKeyHash.from(object.objectKey());
    }

    private GenerationIndexIdentity indexIdentity(VersionedGenerationIndex index) {
        return new GenerationIndexIdentity(
                new StreamId(index.value().streamId()),
                ReadView.fromWireId(index.value().readViewId()),
                index.value().offsetEnd(),
                index.value().generation());
    }

    private AppendRecoveryAnchor recoveryAnchor(VersionedRecoveryCheckpointRoot root) {
        if (root.value().checkpoints().isEmpty()) {
            return AppendRecoveryAnchor.genesis(
                    new StreamId(root.value().streamId()));
        }
        return new AppendRecoveryAnchor(
                new StreamId(root.value().streamId()),
                root.value().lastCommitId(),
                root.value().coveredEndOffset(),
                root.value().cumulativeSizeAtEnd(),
                root.value().lastCommitVersion());
    }

    private boolean auditGraceElapsed(long changedAtMillis) {
        long safeGrace;
        long notBefore;
        try {
            safeGrace = Math.addExact(
                    metadataAuditGraceMillis,
                    config.maximumClockSkew().toMillis());
            notBefore = Math.addExact(changedAtMillis, safeGrace);
        } catch (ArithmeticException overflow) {
            return false;
        }
        return notBefore < clock.millis();
    }

    private static ObjectProtectionIdentity protectionIdentity(
            ObjectProtectionRecord value) {
        return new ObjectProtectionIdentity(
                new ObjectKeyHash(value.objectKeyHash()),
                ObjectProtectionType.fromWireId(value.protectionTypeId()),
                value.referenceId());
    }

    private static byte[] bytes(ByteBuffer value) {
        ByteBuffer copy = value.asReadOnlyBuffer();
        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return bytes;
    }

    private static <T> void requireProgress(
            List<T> values,
            String previousKey,
            Function<T, String> key,
            String label) {
        if (previousKey != null
                && !values.isEmpty()
                && key.apply(values.get(0)).compareTo(previousKey) <= 0) {
            throw invariant(label + " scan did not advance");
        }
    }

    private static StreamRegistrationRetirementResult simple(
            StreamId stream,
            long version,
            StreamRegistrationRetirementStatus status) {
        return StreamRegistrationRetirementResult.simple(stream, version, status);
    }

    private static Duration requirePositive(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    private static long requirePositiveMillis(Duration value, String name) {
        Duration exact = requirePositive(value, name);
        long millis;
        try {
            millis = exact.toMillis();
        } catch (ArithmeticException failure) {
            throw new IllegalArgumentException(name + " must fit milliseconds", failure);
        }
        if (millis <= 0 || !exact.equals(Duration.ofMillis(millis))) {
            throw new IllegalArgumentException(
                    name + " must be positive and exactly millisecond-representable");
        }
        return millis;
    }

    private static String requireText(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must be non-blank");
        }
        return value;
    }

    private static NereusException invariant(String message) {
        return invariant(message, null);
    }

    private static NereusException invariant(String message, Throwable cause) {
        return new NereusException(
                ErrorCode.METADATA_INVARIANT_VIOLATION,
                false,
                message,
                cause);
    }

    private static Throwable unwrap(Throwable supplied) {
        Throwable current = supplied;
        while ((current instanceof CompletionException
                        || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private record RetirementCapture(
            VersionedMaterializationStreamRegistration registration,
            LiveProjectionSubject subject,
            StreamMetadataSnapshot streamSnapshot,
            GenerationProjectionAuthoritySnapshot projection,
            StreamRetirementReferenceAuthoritySnapshot external) {
    }

    private record ViewSequence(
            ReadView view,
            VersionedGenerationSequence value) {
    }

    private record WorkflowSnapshot(
            List<VersionedMaterializationTask> tasks,
            List<VersionedGenerationCandidate> indexes,
            List<VersionedMaterializationCheckpoint> checkpoints,
            List<VersionedRangeRetentionStats> stats,
            List<ViewSequence> sequences,
            boolean limitExceeded) {
    }

    private static final class WorkflowAccumulator {
        private final int maximum;
        private final ArrayList<VersionedMaterializationTask> tasks = new ArrayList<>();
        private final ArrayList<VersionedGenerationCandidate> indexes = new ArrayList<>();
        private final ArrayList<VersionedMaterializationCheckpoint> checkpoints = new ArrayList<>();
        private final ArrayList<VersionedRangeRetentionStats> stats = new ArrayList<>();
        private final ArrayList<ViewSequence> sequences = new ArrayList<>();
        private int observed;
        private boolean limitExceeded;

        private WorkflowAccumulator(int maximum) {
            this.maximum = maximum;
        }

        private boolean addTask(VersionedMaterializationTask value) {
            return add(tasks, value);
        }

        private boolean addIndex(VersionedGenerationCandidate value) {
            return add(indexes, value);
        }

        private boolean addCheckpoint(VersionedMaterializationCheckpoint value) {
            return add(checkpoints, value);
        }

        private boolean addStats(VersionedRangeRetentionStats value) {
            return add(stats, value);
        }

        private void addSequence(ReadView view, VersionedGenerationSequence value) {
            add(sequences, new ViewSequence(view, value));
        }

        private <T> boolean add(List<T> target, T value) {
            observed = Math.addExact(observed, 1);
            if (observed > maximum) {
                limitExceeded = true;
                return false;
            }
            target.add(value);
            return true;
        }

        private WorkflowSnapshot snapshot() {
            return new WorkflowSnapshot(
                    List.copyOf(tasks),
                    List.copyOf(indexes),
                    List.copyOf(checkpoints),
                    List.copyOf(stats),
                    List.copyOf(sequences),
                    limitExceeded);
        }
    }

    private enum OwnerKind {
        INDEX,
        TASK,
        RECOVERY_ROOT
    }

    private record OwnerFact(
            OwnerKind kind,
            String ownerKey,
            long metadataVersion,
            Checksum identitySha256,
            Optional<VersionedGenerationIndex> index,
            Optional<VersionedMaterializationTask> task,
            Optional<VersionedRecoveryCheckpointRoot> recoveryRoot) {
        private OwnerFact {
            Objects.requireNonNull(kind, "kind");
            ownerKey = requireText(ownerKey, "ownerKey");
            if (metadataVersion < 0) {
                throw new IllegalArgumentException("owner metadataVersion must be non-negative");
            }
            Objects.requireNonNull(identitySha256, "identitySha256");
            Objects.requireNonNull(index, "index");
            Objects.requireNonNull(task, "task");
            Objects.requireNonNull(recoveryRoot, "recoveryRoot");
            int values = (index.isPresent() ? 1 : 0)
                    + (task.isPresent() ? 1 : 0)
                    + (recoveryRoot.isPresent() ? 1 : 0);
            if (values != 1) {
                throw new IllegalArgumentException("owner fact must carry exactly one durable value");
            }
        }

        private static OwnerFact index(VersionedGenerationIndex value) {
            return new OwnerFact(
                    OwnerKind.INDEX,
                    value.key(),
                    value.metadataVersion(),
                    value.durableValueSha256(),
                    Optional.of(value),
                    Optional.empty(),
                    Optional.empty());
        }

        private static OwnerFact task(VersionedMaterializationTask value) {
            return new OwnerFact(
                    OwnerKind.TASK,
                    value.key(),
                    value.metadataVersion(),
                    value.durableValueSha256(),
                    Optional.empty(),
                    Optional.of(value),
                    Optional.empty());
        }

        private static OwnerFact recoveryRoot(VersionedRecoveryCheckpointRoot value) {
            return new OwnerFact(
                    OwnerKind.RECOVERY_ROOT,
                    value.key(),
                    value.metadataVersion(),
                    value.durableValueSha256(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.of(value));
        }
    }

    private record OwnerScope(
            Map<String, OwnerFact> owners,
            Map<String, Set<ObjectKeyHash>> objectsByOwner,
            boolean limitExceeded) {
        private OwnerScope {
            owners = Map.copyOf(owners);
            LinkedHashMap<String, Set<ObjectKeyHash>> canonical = new LinkedHashMap<>();
            objectsByOwner.forEach((key, values) -> canonical.put(
                    key, Set.copyOf(values)));
            objectsByOwner = Map.copyOf(canonical);
        }

        private Optional<OwnerFact> owner(OwnerKind kind, String key) {
            OwnerFact owner = owners.get(key);
            return owner != null && owner.kind() == kind
                    ? Optional.of(owner)
                    : Optional.empty();
        }

        private List<OwnerFact> owners(OwnerKind kind) {
            return owners.values().stream()
                    .filter(value -> value.kind() == kind)
                    .sorted(Comparator.comparing(OwnerFact::ownerKey))
                    .toList();
        }

        private boolean hasKind(OwnerKind kind) {
            return owners.values().stream().anyMatch(value -> value.kind() == kind);
        }

        private List<ObjectKeyHash> objects(OwnerKind kind) {
            return owners(kind).stream()
                    .flatMap(owner -> objectsByOwner.getOrDefault(
                                    owner.ownerKey(), Set.of())
                            .stream())
                    .distinct()
                    .sorted(Comparator.comparing(ObjectKeyHash::value))
                    .toList();
        }
    }

    private static final class OwnerScopeBuilder {
        private final int maximum;
        private final LinkedHashMap<String, OwnerFact> owners = new LinkedHashMap<>();
        private final LinkedHashMap<String, LinkedHashSet<ObjectKeyHash>> objects =
                new LinkedHashMap<>();
        private int observed;
        private boolean limitExceeded;

        private OwnerScopeBuilder(int maximum) {
            this.maximum = maximum;
        }

        private void add(OwnerFact owner, ObjectKeyHash object) {
            if (limitExceeded) {
                return;
            }
            OwnerFact existing = owners.putIfAbsent(owner.ownerKey(), owner);
            if (existing != null && !existing.equals(owner)) {
                throw invariant("one retirement owner key has conflicting durable facts");
            }
            LinkedHashSet<ObjectKeyHash> ownerObjects = objects.computeIfAbsent(
                    owner.ownerKey(), ignored -> new LinkedHashSet<>());
            if (ownerObjects.add(Objects.requireNonNull(object, "object"))) {
                observed = Math.addExact(observed, 1);
                if (observed > maximum) {
                    limitExceeded = true;
                }
            }
        }

        private OwnerScope build() {
            LinkedHashMap<String, Set<ObjectKeyHash>> exact = new LinkedHashMap<>();
            objects.forEach((key, value) -> exact.put(key, Set.copyOf(value)));
            return new OwnerScope(owners, exact, limitExceeded);
        }
    }

    private record ProtectionScan(
            List<VersionedObjectProtection> values,
            boolean limitExceeded) {
    }

    private static final class ProtectionScanAccumulator {
        private final int maximum;
        private final ArrayList<VersionedObjectProtection> values = new ArrayList<>();
        private boolean limitExceeded;

        private ProtectionScanAccumulator(int maximum) {
            this.maximum = maximum;
        }

        private boolean add(VersionedObjectProtection value) {
            if (values.size() == maximum) {
                limitExceeded = true;
                return false;
            }
            values.add(value);
            return true;
        }

        private ProtectionScan snapshot() {
            return new ProtectionScan(List.copyOf(values), limitExceeded);
        }
    }
}
