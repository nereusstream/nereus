/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.GenerationId;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.ProjectionRef;
import com.nereusstream.api.PublicationId;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamState;
import com.nereusstream.core.capability.GenerationActivationProof;
import com.nereusstream.core.capability.GenerationOperation;
import com.nereusstream.core.capability.GenerationProtocolActivationGuard;
import com.nereusstream.core.capability.LiveProjectionSubject;
import com.nereusstream.core.physical.ObjectProtection;
import com.nereusstream.core.physical.ObjectProtectionManager;
import com.nereusstream.core.physical.ObjectProtectionOwner;
import com.nereusstream.core.physical.ObjectProtectionRequest;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.core.read.MetadataPhysicalObjectIdentityResolver;
import com.nereusstream.metadata.oxia.AllocatedGeneration;
import com.nereusstream.metadata.oxia.F4Keyspace;
import com.nereusstream.metadata.oxia.F4MetadataConditionFailedException;
import com.nereusstream.metadata.oxia.GenerationAllocator;
import com.nereusstream.metadata.oxia.GenerationIndexIdentity;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.PhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.ProjectionIdentity;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.metadata.oxia.VersionedGenerationZeroIndex;
import com.nereusstream.metadata.oxia.VersionedMaterializationStreamRegistration;
import com.nereusstream.metadata.oxia.VersionedMaterializationTask;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import com.nereusstream.metadata.oxia.records.MaterializationStreamRegistrationRecord;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.records.TaskLifecycle;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Restart-safe higher-generation publisher whose only visibility point is the exact index lifecycle CAS.
 *
 * <p>Every task/index/object-store cut is resolved by reloading authoritative metadata. Allocation gaps are legal,
 * while a generation may proceed only after it is attached to the durable task.
 */
public final class DefaultGenerationCommitter implements GenerationCommitter {
    private static final int MAX_RECOVERY_ATTEMPTS = 32;

    private final String cluster;
    private final OxiaMetadataStore l0Store;
    private final GenerationMetadataStore generationStore;
    private final GenerationAllocator allocator;
    private final PhysicalObjectMetadataStore physicalStore;
    private final ObjectProtectionManager protectionManager;
    private final MaterializationTaskProtectionReconciler taskProtectionReconciler;
    private final MaterializationSourceProtectionRegistry sourceProtectionAdapters;
    private final GenerationProtocolActivationGuard activationGuard;
    private final MaterializationOutputVerifier outputVerifier;
    private final PublicationIdGenerator publicationIds;
    private final Duration operationTimeout;
    private final ScheduledExecutorService scheduler;
    private final Clock clock;

    public DefaultGenerationCommitter(
            String cluster,
            OxiaMetadataStore l0Store,
            GenerationMetadataStore generationStore,
            PhysicalObjectMetadataStore physicalStore,
            ObjectProtectionManager protectionManager,
            GenerationProtocolActivationGuard activationGuard,
            MaterializationOutputVerifier outputVerifier,
            Duration operationTimeout,
            ScheduledExecutorService scheduler,
            Clock clock) {
        this(
                cluster,
                l0Store,
                generationStore,
                physicalStore,
                protectionManager,
                activationGuard,
                outputVerifier,
                new SecurePublicationIdGenerator(),
                operationTimeout,
                scheduler,
                clock);
    }

    public DefaultGenerationCommitter(
            String cluster,
            OxiaMetadataStore l0Store,
            GenerationMetadataStore generationStore,
            PhysicalObjectMetadataStore physicalStore,
            ObjectProtectionManager protectionManager,
            MaterializationSourceProtectionRegistry sourceProtectionAdapters,
            GenerationProtocolActivationGuard activationGuard,
            MaterializationOutputVerifier outputVerifier,
            Duration operationTimeout,
            ScheduledExecutorService scheduler,
            Clock clock) {
        this(
                cluster,
                l0Store,
                generationStore,
                physicalStore,
                protectionManager,
                activationGuard,
                outputVerifier,
                new SecurePublicationIdGenerator(),
                operationTimeout,
                scheduler,
                clock,
                sourceProtectionAdapters);
    }

    public DefaultGenerationCommitter(
            String cluster,
            OxiaMetadataStore l0Store,
            GenerationMetadataStore generationStore,
            PhysicalObjectMetadataStore physicalStore,
            ObjectProtectionManager protectionManager,
            GenerationProtocolActivationGuard activationGuard,
            MaterializationOutputVerifier outputVerifier,
            PublicationIdGenerator publicationIds,
            Duration operationTimeout,
            ScheduledExecutorService scheduler,
            Clock clock) {
        this(
                cluster,
                l0Store,
                generationStore,
                physicalStore,
                protectionManager,
                activationGuard,
                outputVerifier,
                publicationIds,
                operationTimeout,
                scheduler,
                clock,
                new MaterializationSourceProtectionRegistry(List.of(
                        new ObjectMaterializationSourceProtectionAdapter(
                                new MetadataPhysicalObjectIdentityResolver(
                                        cluster, l0Store, physicalStore),
                                protectionManager))));
    }

    private DefaultGenerationCommitter(
            String cluster,
            OxiaMetadataStore l0Store,
            GenerationMetadataStore generationStore,
            PhysicalObjectMetadataStore physicalStore,
            ObjectProtectionManager protectionManager,
            GenerationProtocolActivationGuard activationGuard,
            MaterializationOutputVerifier outputVerifier,
            PublicationIdGenerator publicationIds,
            Duration operationTimeout,
            ScheduledExecutorService scheduler,
            Clock clock,
            MaterializationSourceProtectionRegistry sourceProtectionAdapters) {
        this.cluster = requireText(cluster, "cluster");
        this.l0Store = Objects.requireNonNull(l0Store, "l0Store");
        this.generationStore = Objects.requireNonNull(generationStore, "generationStore");
        this.allocator = new GenerationAllocator(cluster, generationStore);
        this.physicalStore = Objects.requireNonNull(physicalStore, "physicalStore");
        this.protectionManager = Objects.requireNonNull(protectionManager, "protectionManager");
        this.activationGuard = Objects.requireNonNull(activationGuard, "activationGuard");
        this.outputVerifier = Objects.requireNonNull(outputVerifier, "outputVerifier");
        this.publicationIds = Objects.requireNonNull(publicationIds, "publicationIds");
        this.operationTimeout = requirePositive(operationTimeout, "operationTimeout");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.sourceProtectionAdapters = Objects.requireNonNull(
                sourceProtectionAdapters, "sourceProtectionAdapters");
        this.taskProtectionReconciler = new DefaultMaterializationTaskProtectionReconciler(
                this.cluster,
                new MaterializationTaskStore(this.cluster, this.generationStore, this.clock),
                this.generationStore,
                new MetadataPhysicalObjectIdentityResolver(
                        this.cluster, this.l0Store, this.physicalStore),
                this.protectionManager,
                this.sourceProtectionAdapters,
                this.operationTimeout,
                this.scheduler);
    }

    @Override
    public CompletableFuture<GenerationCommitResult> publish(
            MaterializationTask task,
            MaterializationOutput output) {
        try {
            return new PublicationOperation(
                    Objects.requireNonNull(task, "task"),
                    Objects.requireNonNull(output, "output"))
                    .run();
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private final class PublicationOperation {
        private final MaterializationTask task;
        private final MaterializationOutput output;
        private final MaterializationDeadline deadline;

        private PublicationOperation(
                MaterializationTask task,
                MaterializationOutput output) {
            this.task = task;
            this.output = output;
            this.deadline = new MaterializationDeadline(operationTimeout, scheduler);
            requireDomainAgreement(task, output);
        }

        private CompletableFuture<GenerationCommitResult> run() {
            return verifyOutput("verify output before publication admission")
                    .thenCompose(ignored -> admit())
                    .thenCompose(admission -> loadTask()
                            .thenCompose(current -> freezePublication(current, null, 0))
                            .thenCompose(current -> attachGeneration(current, 0))
                            .thenCompose(current -> continuePublication(current, admission)));
        }

        private CompletableFuture<Admission> admit() {
            return deadline.bound(
                            () -> generationStore.getStreamRegistration(cluster, task.streamId()),
                            "load materialization stream registration")
                    .thenCompose(optional -> {
                        VersionedMaterializationStreamRegistration registration = optional.orElseThrow(() ->
                                condition("materialization stream registration is absent"));
                        AdmissionSubject subject = validateRegistration(registration);
                        return loadSnapshot()
                                .thenApply(snapshot -> {
                                    validateSnapshot(snapshot, subject.profile());
                                    return subject;
                                });
                    })
                    .thenCompose(subject -> deadline.bound(
                                    () -> activationGuard.requireReady(
                                            operationFor(task.view()), subject.subject(), false),
                                    "admit generation publication")
                            .thenApply(proof -> new Admission(subject.profile(), proof)));
        }

        private CompletableFuture<VersionedMaterializationTask> freezePublication(
                VersionedMaterializationTask current,
                PublicationId candidatePublication,
                int attempt) {
            requireTaskOutput(current);
            TaskLifecycle lifecycle = current.value().lifecycle();
            if (lifecycle == TaskLifecycle.PUBLISHING || lifecycle == TaskLifecycle.PUBLISHED) {
                return CompletableFuture.completedFuture(current);
            }
            if (lifecycle != TaskLifecycle.OUTPUT_READY) {
                return CompletableFuture.failedFuture(condition(
                        "materialization task is not ready for publication: " + lifecycle));
            }
            if (attempt >= MAX_RECOVERY_ATTEMPTS) {
                return recoveryExhausted("freeze publication id");
            }
            PublicationId publication = candidatePublication == null
                    ? Objects.requireNonNull(publicationIds.next(), "publication id")
                    : candidatePublication;
            return deadline.bound(
                            () -> generationStore.compareAndSetTask(
                                    cluster,
                                    MaterializationRecordMapper.publishing(
                                            current.value(), publication, clock.millis()),
                                    current.metadataVersion()),
                            "freeze task publication id")
                    .handle((updated, failure) -> {
                        if (failure == null) {
                            return CompletableFuture.completedFuture(updated);
                        }
                        Throwable original = unwrap(failure);
                        return loadTask().thenCompose(reloaded -> {
                            requireTaskOutput(reloaded);
                            if (reloaded.value().lifecycle() == TaskLifecycle.OUTPUT_READY) {
                                return freezePublication(reloaded, publication, attempt + 1);
                            }
                            if (reloaded.value().lifecycle() == TaskLifecycle.PUBLISHING
                                    || reloaded.value().lifecycle() == TaskLifecycle.PUBLISHED) {
                                return CompletableFuture.completedFuture(reloaded);
                            }
                            return CompletableFuture.failedFuture(original);
                        });
                    })
                    .thenCompose(value -> value);
        }

        private CompletableFuture<VersionedMaterializationTask> attachGeneration(
                VersionedMaterializationTask current,
                int attempt) {
            requireTaskOutput(current);
            if (current.value().lifecycle() == TaskLifecycle.PUBLISHED
                    || current.value().allocatedGeneration().isPresent()) {
                requireFrozenAllocation(current);
                return CompletableFuture.completedFuture(current);
            }
            if (current.value().lifecycle() != TaskLifecycle.PUBLISHING) {
                return CompletableFuture.failedFuture(condition(
                        "generation can only be attached to a PUBLISHING task"));
            }
            if (attempt >= MAX_RECOVERY_ATTEMPTS) {
                return recoveryExhausted("attach allocated generation");
            }
            PublicationId publication = new PublicationId(current.value().publicationId());
            return deadline.bound(
                            () -> allocator.allocate(task.streamId(), task.view(), publication),
                            "allocate view-scoped generation")
                    .thenCompose(allocation -> attachAllocated(current, allocation, attempt));
        }

        private CompletableFuture<VersionedMaterializationTask> attachAllocated(
                VersionedMaterializationTask current,
                AllocatedGeneration allocation,
                int attempt) {
            return deadline.bound(
                            () -> generationStore.compareAndSetTask(
                                    cluster,
                                    MaterializationRecordMapper.attachGeneration(
                                            current.value(), allocation.generation().value(), clock.millis()),
                                    current.metadataVersion()),
                            "attach allocated generation to task")
                    .handle((updated, failure) -> {
                        if (failure == null) {
                            return CompletableFuture.completedFuture(updated);
                        }
                        Throwable original = unwrap(failure);
                        return loadTask().thenCompose(reloaded -> {
                            requireTaskOutput(reloaded);
                            if (reloaded.value().lifecycle() == TaskLifecycle.PUBLISHED
                                    || reloaded.value().allocatedGeneration().isPresent()) {
                                requireFrozenAllocation(reloaded);
                                return CompletableFuture.completedFuture(reloaded);
                            }
                            if (reloaded.value().lifecycle() == TaskLifecycle.PUBLISHING) {
                                return attachGeneration(reloaded, attempt + 1);
                            }
                            return CompletableFuture.failedFuture(original);
                        });
                    })
                    .thenCompose(value -> value);
        }

        private CompletableFuture<GenerationCommitResult> continuePublication(
                VersionedMaterializationTask publishingTask,
                Admission admission) {
            requireFrozenAllocation(publishingTask);
            CompletableFuture<?> temporaryProtections =
                    publishingTask.value().lifecycle() == TaskLifecycle.PUBLISHED
                            ? CompletableFuture.completedFuture(null)
                            : reconcileTaskProtections(publishingTask);
            return temporaryProtections
                    .thenCompose(ignored -> loadIndex(publishingTask))
                    .thenCompose(optional -> {
                if (optional.isPresent()) {
                    VersionedGenerationIndex current = optional.orElseThrow();
                    requireIndexIdentity(publishingTask, current);
                    if (current.value().lifecycle() == GenerationLifecycle.COMMITTED) {
                        return finalizeCommitted(
                                publishingTask, current, admission, Optional.empty(), false);
                    }
                    if (current.value().lifecycle() == GenerationLifecycle.ABORTED) {
                        return recoverAborted(
                                publishingTask, current, admission, Optional.empty());
                    }
                    if (current.value().lifecycle() != GenerationLifecycle.PREPARED) {
                        return CompletableFuture.failedFuture(unrecoverableIndex(current));
                    }
                }
                return acquireTaskProtection(publishingTask)
                        .thenCompose(protection -> createOrLoadPrepared(
                                publishingTask, admission, protection))
                        .exceptionallyCompose(protectionFailure -> loadIndex(publishingTask)
                                .thenCompose(reloaded -> {
                                    if (reloaded.isPresent()) {
                                        VersionedGenerationIndex current = reloaded.orElseThrow();
                                        requireIndexIdentity(publishingTask, current);
                                        if (current.value().lifecycle()
                                                == GenerationLifecycle.COMMITTED) {
                                            return finalizeCommitted(
                                                    publishingTask,
                                                    current,
                                                    admission,
                                                    Optional.empty(),
                                                    false);
                                        }
                                    }
                                    return CompletableFuture.failedFuture(
                                            unwrap(protectionFailure));
                                }));
                    });
        }

        private CompletableFuture<GenerationCommitResult> createOrLoadPrepared(
                VersionedMaterializationTask publishingTask,
                Admission admission,
                ObjectProtection taskProtection) {
            return loadIndex(publishingTask).thenCompose(optional -> {
                if (optional.isPresent()) {
                    return continueWithIndex(
                            publishingTask,
                            optional.orElseThrow(),
                            admission,
                            taskProtection);
                }
                long generation = publishingTask.value().allocatedGeneration().orElseThrow();
                GenerationIndexRecord prepared = MaterializationRecordMapper.preparedIndex(
                        task,
                        output,
                        generation,
                        publishingTask.value().publicationId(),
                        publishingTask.value().updatedAtMillis());
                return loadExactPublishingTask(publishingTask)
                        .thenCompose(ignored -> deadline.bound(
                                () -> generationStore.createPrepared(cluster, prepared),
                                "create prepared generation index"))
                        .thenCompose(created -> continueWithIndex(
                                publishingTask, created, admission, taskProtection));
            });
        }

        private CompletableFuture<GenerationCommitResult> continueWithIndex(
                VersionedMaterializationTask publishingTask,
                VersionedGenerationIndex index,
                Admission admission,
                ObjectProtection taskProtection) {
            requireIndexIdentity(publishingTask, index);
            if (index.value().lifecycle() == GenerationLifecycle.COMMITTED) {
                return finalizeCommitted(
                        publishingTask, index, admission, Optional.of(taskProtection), false);
            }
            if (index.value().lifecycle() == GenerationLifecycle.ABORTED) {
                return recoverAborted(
                        publishingTask, index, admission, Optional.of(taskProtection));
            }
            if (index.value().lifecycle() != GenerationLifecycle.PREPARED) {
                return CompletableFuture.failedFuture(unrecoverableIndex(index));
            }
            return publishPrepared(publishingTask, index, admission, taskProtection, 0);
        }

        private CompletableFuture<GenerationCommitResult> recoverAborted(
                VersionedMaterializationTask publishingTask,
                VersionedGenerationIndex aborted,
                Admission admission,
                Optional<ObjectProtection> taskProtection) {
            requireIndexIdentity(publishingTask, aborted);
            if (aborted.value().lifecycle() != GenerationLifecycle.ABORTED) {
                return CompletableFuture.failedFuture(condition(
                        "aborted-publication recovery loaded another lifecycle"));
            }
            CompletableFuture<ObjectProtection> protection = taskProtection.isPresent()
                    ? CompletableFuture.completedFuture(taskProtection.orElseThrow())
                    : acquireTaskProtection(publishingTask);
            return protection
                    .thenCompose(handle -> deadline.bound(
                            () -> protectionManager.release(
                                    handle,
                                    ignored -> authorizeAbortedProtectionRemoval(
                                            publishingTask, aborted)),
                            "release aborted task-owned visible-generation protection"))
                    .thenCompose(ignored -> resetAbortedTask(publishingTask, aborted, 0))
                    .thenCompose(reset -> freezePublication(reset, null, 0))
                    .thenCompose(current -> attachGeneration(current, 0))
                    .thenCompose(current -> continuePublication(current, admission));
        }

        private CompletableFuture<Void> authorizeAbortedProtectionRemoval(
                VersionedMaterializationTask expectedTask,
                VersionedGenerationIndex expectedIndex) {
            return loadIndexByIdentity(expectedIndex).thenCompose(actualIndex -> {
                if (!sameVersionedIndex(expectedIndex, actualIndex)
                        || actualIndex.value().lifecycle() != GenerationLifecycle.ABORTED) {
                    return CompletableFuture.failedFuture(condition(
                            "aborted index changed while protection removal was authorized"));
                }
                return loadTask().thenApply(actualTask -> {
                    if (!sameVersionedTask(expectedTask, actualTask)
                            || actualTask.value().lifecycle() != TaskLifecycle.PUBLISHING) {
                        throw condition(
                                "publishing task changed while aborted protection removal was authorized");
                    }
                    return null;
                });
            });
        }

        private CompletableFuture<VersionedMaterializationTask> resetAbortedTask(
                VersionedMaterializationTask current,
                VersionedGenerationIndex aborted,
                int attempt) {
            requireTaskMatchesIndex(current, aborted);
            if (attempt >= MAX_RECOVERY_ATTEMPTS) {
                return recoveryExhausted("reset aborted materialization publication");
            }
            return deadline.bound(
                            () -> generationStore.compareAndSetTask(
                                    cluster,
                                    MaterializationRecordMapper.outputReadyAfterAbort(
                                            current.value(), clock.millis()),
                                    current.metadataVersion()),
                            "reset aborted materialization publication")
                    .handle((reset, failure) -> {
                        if (failure == null) {
                            return CompletableFuture.completedFuture(reset);
                        }
                        Throwable original = unwrap(failure);
                        return loadTask().thenCompose(reloaded -> {
                            requireTaskOutput(reloaded);
                            if (reloaded.value().lifecycle() == TaskLifecycle.OUTPUT_READY) {
                                return CompletableFuture.completedFuture(reloaded);
                            }
                            if (reloaded.value().lifecycle() == TaskLifecycle.PUBLISHING
                                    && reloaded.value().publicationId()
                                            .equals(aborted.value().publicationId())
                                    && reloaded.value().allocatedGeneration().isPresent()
                                    && reloaded.value().allocatedGeneration().orElseThrow()
                                            == aborted.value().generation()) {
                                return resetAbortedTask(reloaded, aborted, attempt + 1);
                            }
                            if (reloaded.value().lifecycle() == TaskLifecycle.PUBLISHING
                                    || reloaded.value().lifecycle() == TaskLifecycle.PUBLISHED) {
                                return CompletableFuture.completedFuture(reloaded);
                            }
                            return CompletableFuture.failedFuture(original);
                        });
                    })
                    .thenCompose(value -> value);
        }

        private CompletableFuture<GenerationCommitResult> publishPrepared(
                VersionedMaterializationTask publishingTask,
                VersionedGenerationIndex prepared,
                Admission admission,
                ObjectProtection taskProtection,
                int attempt) {
            if (attempt >= MAX_RECOVERY_ATTEMPTS) {
                return recoveryExhausted("commit prepared generation index");
            }
            requirePreparedExact(publishingTask, prepared);
            return revalidateBeforeCommit(publishingTask, prepared, admission, taskProtection)
                    .thenCompose(ignored -> commitPreparedCas(
                            publishingTask,
                            prepared,
                            admission,
                            taskProtection,
                            attempt));
        }

        private CompletableFuture<GenerationCommitResult> commitPreparedCas(
                VersionedMaterializationTask publishingTask,
                VersionedGenerationIndex prepared,
                Admission admission,
                ObjectProtection taskProtection,
                int attempt) {
            return deadline.bound(
                            () -> generationStore.compareAndSetIndex(
                                    cluster,
                                    MaterializationRecordMapper.committedIndex(
                                            prepared.value(), clock.millis()),
                                    prepared.metadataVersion()),
                            "commit prepared generation index")
                    .handle((committed, failure) -> {
                        if (failure == null) {
                            return finalizeCommitted(
                                    publishingTask,
                                    committed,
                                    admission,
                                    Optional.of(taskProtection),
                                    true);
                        }
                        Throwable original = unwrap(failure);
                        return loadIndex(publishingTask).thenCompose(optional -> {
                            if (optional.isEmpty()) {
                                return CompletableFuture.failedFuture(original);
                            }
                            VersionedGenerationIndex current = optional.orElseThrow();
                            requireIndexIdentity(publishingTask, current);
                            if (current.value().lifecycle() == GenerationLifecycle.COMMITTED) {
                                return finalizeCommitted(
                                        publishingTask,
                                        current,
                                        admission,
                                        Optional.of(taskProtection),
                                        false);
                            }
                            if (current.value().lifecycle() == GenerationLifecycle.PREPARED) {
                                return publishPrepared(
                                        publishingTask,
                                        current,
                                        admission,
                                        taskProtection,
                                        attempt + 1);
                            }
                            return CompletableFuture.failedFuture(unrecoverableIndex(current));
                        });
                    })
                    .thenCompose(value -> value);
        }

        private CompletableFuture<Void> revalidateBeforeCommit(
                VersionedMaterializationTask publishingTask,
                VersionedGenerationIndex prepared,
                Admission admission,
                ObjectProtection taskProtection) {
            return verifyOutput("verify output immediately before generation commit")
                    .thenCompose(ignored -> loadExactPublishingTask(publishingTask))
                    .thenCompose(ignored -> loadSnapshot())
                    .thenCompose(snapshot -> {
                        validateSnapshot(snapshot, admission.profile());
                        return revalidateSources(snapshot, publishingTask);
                    })
                    .thenCompose(ignored -> loadExactPrepared(prepared))
                    .thenCompose(ignored -> deadline.bound(
                            () -> protectionManager.revalidate(
                                    taskProtection,
                                    expected -> revalidateTaskOwner(publishingTask, expected)),
                            "revalidate task-owned visible-generation protection"))
                    .thenCompose(ignored -> deadline.bound(
                            () -> activationGuard.revalidate(admission.proof()),
                            "revalidate generation activation proof"));
        }

        private CompletableFuture<GenerationCommitResult> finalizeCommitted(
                VersionedMaterializationTask publishingTask,
                VersionedGenerationIndex committed,
                Admission admission,
                Optional<ObjectProtection> taskProtection,
                boolean committedByThisCall) {
            requireCommittedExact(publishingTask, committed);
            return ensureIndexProtection(publishingTask, committed, taskProtection)
                    .thenCompose(protection -> markPublished(publishingTask, committed, 0)
                            .thenCompose(ignored -> deadline.bound(
                                    () -> protectionManager.revalidate(
                                            protection,
                                            expected -> revalidateIndexOwner(committed, expected)),
                                    "revalidate committed-index-owned protection")))
                    .thenCompose(ignored -> loadExactCommitted(committed))
                    .thenApply(exact -> commitResult(exact, committedByThisCall));
        }

        private CompletableFuture<MaterializationTaskProtections> reconcileTaskProtections(
                VersionedMaterializationTask durable) {
            return deadline.bound(
                    () -> taskProtectionReconciler.reconcile(durable),
                    "reconcile source/output task protections before publication");
        }

        private CompletableFuture<ObjectProtection> ensureIndexProtection(
                VersionedMaterializationTask publishingTask,
                VersionedGenerationIndex committed,
                Optional<ObjectProtection> taskProtection) {
            ObjectProtectionOwner owner = indexOwner(committed);
            if (taskProtection.isPresent()) {
                return deadline.bound(
                                () -> protectionManager.transfer(
                                        taskProtection.orElseThrow(),
                                        owner,
                                        expected -> revalidateIndexOwner(committed, expected)),
                                "transfer visible-generation protection to committed index")
                        .exceptionallyCompose(transferFailure -> reconcileIndexProtection(
                                publishingTask, committed, 0, unwrap(transferFailure)));
            }
            return reconcileIndexProtection(publishingTask, committed, 0, null);
        }

        private CompletableFuture<ObjectProtection> reconcileIndexProtection(
                VersionedMaterializationTask publishingTask,
                VersionedGenerationIndex committed,
                int attempt,
                Throwable previousFailure) {
            if (attempt >= MAX_RECOVERY_ATTEMPTS) {
                Throwable exhausted = recoveryExhaustedFailure(
                        "reconcile visible-generation protection owner");
                if (previousFailure != null) {
                    exhausted.addSuppressed(previousFailure);
                }
                return CompletableFuture.failedFuture(exhausted);
            }
            ObjectProtectionOwner owner = indexOwner(committed);
            ObjectProtectionRequest indexRequest = protectionRequest(publishingTask, owner);
            return deadline.bound(
                            () -> protectionManager.acquire(
                                    indexRequest,
                                    expected -> revalidateIndexOwner(committed, expected)),
                            "recover committed-index-owned protection")
                    .exceptionallyCompose(indexAcquireFailure -> acquireTaskProtection(publishingTask)
                            .thenCompose(taskOwned -> deadline.bound(
                                    () -> protectionManager.transfer(
                                            taskOwned,
                                            owner,
                                            expected -> revalidateIndexOwner(committed, expected)),
                                    "repair visible-generation protection owner"))
                            .exceptionallyCompose(repairFailure -> {
                                Throwable exact = unwrap(indexAcquireFailure);
                                exact.addSuppressed(unwrap(repairFailure));
                                if (previousFailure != null) {
                                    exact.addSuppressed(previousFailure);
                                }
                                return reconcileIndexProtection(
                                        publishingTask, committed, attempt + 1, exact);
                            }));
        }

        private CompletableFuture<VersionedMaterializationTask> markPublished(
                VersionedMaterializationTask current,
                VersionedGenerationIndex committed,
                int attempt) {
            requireTaskOutput(current);
            if (current.value().lifecycle() == TaskLifecycle.PUBLISHED) {
                requireTaskMatchesIndex(current, committed);
                return CompletableFuture.completedFuture(current);
            }
            if (current.value().lifecycle() != TaskLifecycle.PUBLISHING) {
                return CompletableFuture.failedFuture(condition(
                        "committed generation belongs to a non-publishing task"));
            }
            requireTaskMatchesIndex(current, committed);
            if (attempt >= MAX_RECOVERY_ATTEMPTS) {
                return recoveryExhausted("mark materialization task published");
            }
            return deadline.bound(
                            () -> generationStore.compareAndSetTask(
                                    cluster,
                                    MaterializationRecordMapper.published(
                                            current.value(), clock.millis()),
                                    current.metadataVersion()),
                            "mark materialization task published")
                    .handle((published, failure) -> {
                        if (failure == null) {
                            return CompletableFuture.completedFuture(published);
                        }
                        Throwable original = unwrap(failure);
                        return loadTask().thenCompose(reloaded -> {
                            requireTaskOutput(reloaded);
                            if (reloaded.value().lifecycle() == TaskLifecycle.PUBLISHED) {
                                requireTaskMatchesIndex(reloaded, committed);
                                return CompletableFuture.completedFuture(reloaded);
                            }
                            if (reloaded.value().lifecycle() == TaskLifecycle.PUBLISHING) {
                                return markPublished(reloaded, committed, attempt + 1);
                            }
                            return CompletableFuture.failedFuture(original);
                        });
                    })
                    .thenCompose(value -> value);
        }

        private CompletableFuture<ObjectProtection> acquireTaskProtection(
                VersionedMaterializationTask publishingTask) {
            ObjectProtectionOwner owner = taskOwner(publishingTask);
            return deadline.bound(
                    () -> protectionManager.acquire(
                            protectionRequest(publishingTask, owner),
                            expected -> revalidateTaskOwner(publishingTask, expected)),
                    "acquire task-owned visible-generation protection");
        }

        private ObjectProtectionRequest protectionRequest(
                VersionedMaterializationTask publishingTask,
                ObjectProtectionOwner owner) {
            return new ObjectProtectionRequest(
                    MaterializationRecordMapper.physicalIdentity(output),
                    ObjectProtectionType.VISIBLE_GENERATION,
                    visibleReferenceId(publishingTask),
                    owner,
                    0);
        }

        private CompletableFuture<Void> revalidateTaskOwner(
                VersionedMaterializationTask expectedTask,
                ObjectProtectionOwner expectedOwner) {
            return loadTask().thenApply(actual -> {
                requireTaskOutput(actual);
                requireSameOwner(expectedOwner, taskOwner(actual), "task protection owner changed");
                if (!sameVersionedTask(expectedTask, actual)
                        || actual.value().lifecycle() != TaskLifecycle.PUBLISHING) {
                    throw condition("task changed during visible-generation protection handshake");
                }
                return null;
            });
        }

        private CompletableFuture<Void> revalidateIndexOwner(
                VersionedGenerationIndex expectedIndex,
                ObjectProtectionOwner expectedOwner) {
            return loadIndexByIdentity(expectedIndex).thenApply(actual -> {
                requireSameOwner(expectedOwner, indexOwner(actual), "index protection owner changed");
                if (!sameVersionedIndex(expectedIndex, actual)
                        || actual.value().lifecycle() != GenerationLifecycle.COMMITTED) {
                    throw condition("committed index changed during protection handshake");
                }
                return null;
            });
        }

        private CompletableFuture<Void> revalidateSources(
                StreamMetadataSnapshot snapshot,
                VersionedMaterializationTask publishingTask) {
            List<CompletableFuture<Void>> checks = new ArrayList<>(task.sources().size());
            for (SourceGeneration source : task.sources()) {
                checks.add(revalidateSource(source, snapshot, publishingTask));
            }
            return CompletableFuture.allOf(checks.toArray(CompletableFuture[]::new));
        }

        private CompletableFuture<Void> revalidateSource(
                SourceGeneration source,
                StreamMetadataSnapshot snapshot,
                VersionedMaterializationTask publishingTask) {
            if (source.commitVersion() > snapshot.committedEnd().commitVersion()) {
                return CompletableFuture.failedFuture(condition(
                        "materialization source is newer than committed head"));
            }
            return deadline.bound(
                            () -> generationStore.getCandidate(
                                    cluster,
                                    task.streamId(),
                                    source.view(),
                                    source.range().endOffset(),
                                    source.generation()),
                            "reload exact materialization source index")
                    .thenCompose(optional -> {
                        VersionedGenerationCandidate candidate = optional.orElseThrow(() ->
                                condition("materialization source index disappeared"));
                        requireSourceIndex(source, candidate);
                        return deadline.bound(
                                        () -> sourceProtectionAdapters.acquireOrTransfer(
                                                task.streamId(),
                                                source,
                                                MaterializationProtectionIdentities.sourceReferenceId(
                                                        cluster, task, source),
                                                taskOwner(publishingTask),
                                                expected -> revalidateTaskOwner(
                                                        publishingTask, expected)),
                                        "revalidate materialization source protection")
                                .thenApply(ignored -> null);
                    });
        }

        private CompletableFuture<Void> loadExactPublishingTask(
                VersionedMaterializationTask expected) {
            return loadTask().thenApply(actual -> {
                if (!sameVersionedTask(expected, actual)
                        || actual.value().lifecycle() != TaskLifecycle.PUBLISHING) {
                    throw condition("publishing task changed before generation commit");
                }
                requireTaskOutput(actual);
                return null;
            });
        }

        private CompletableFuture<Void> loadExactPrepared(
                VersionedGenerationIndex expected) {
            return loadIndexByIdentity(expected).thenApply(actual -> {
                if (!sameVersionedIndex(expected, actual)
                        || actual.value().lifecycle() != GenerationLifecycle.PREPARED) {
                    throw condition("prepared generation index changed before commit");
                }
                return null;
            });
        }

        private CompletableFuture<VersionedGenerationIndex> loadExactCommitted(
                VersionedGenerationIndex expected) {
            return loadIndexByIdentity(expected).thenApply(actual -> {
                if (!sameVersionedIndex(expected, actual)
                        || actual.value().lifecycle() != GenerationLifecycle.COMMITTED) {
                    throw condition("committed generation index changed before strict success");
                }
                return actual;
            });
        }

        private CompletableFuture<VersionedGenerationIndex> loadIndexByIdentity(
                VersionedGenerationIndex expected) {
            GenerationIndexRecord value = expected.value();
            GenerationIndexIdentity identity = new GenerationIndexIdentity(
                    task.streamId(),
                    task.view(),
                    value.offsetEnd(),
                    value.generation());
            return deadline.bound(
                            () -> generationStore.getIndex(cluster, identity),
                            "reload exact generation index")
                    .thenApply(optional -> optional.orElseThrow(() ->
                            condition("generation index disappeared")));
        }

        private CompletableFuture<Optional<VersionedGenerationIndex>> loadIndex(
                VersionedMaterializationTask publishingTask) {
            long generation = publishingTask.value().allocatedGeneration().orElseThrow();
            GenerationIndexIdentity identity = new GenerationIndexIdentity(
                    task.streamId(), task.view(), task.coverage().endOffset(), generation);
            return deadline.bound(
                    () -> generationStore.getIndex(cluster, identity),
                    "load task generation index");
        }

        private CompletableFuture<VersionedMaterializationTask> loadTask() {
            return deadline.bound(
                            () -> generationStore.getTask(cluster, task.streamId(), task.taskId()),
                            "load materialization task")
                    .thenApply(optional -> optional.orElseThrow(() ->
                            condition("materialization task is absent")));
        }

        private CompletableFuture<StreamMetadataSnapshot> loadSnapshot() {
            return deadline.bound(
                    () -> l0Store.getStreamSnapshot(cluster, task.streamId()),
                    "load committed stream head");
        }

        private CompletableFuture<Void> verifyOutput(String stage) {
            return deadline.bound(
                    () -> outputVerifier.verify(task, output, deadline.remaining()),
                    stage);
        }

        private AdmissionSubject validateRegistration(
                VersionedMaterializationStreamRegistration registration) {
            MaterializationStreamRegistrationRecord value = registration.value();
            String expectedKey = new F4Keyspace(cluster).materializationRegistryKey(task.streamId());
            if (!registration.key().equals(expectedKey)
                    || !value.streamId().equals(task.streamId().value())) {
                throw invariant("materialization registration key/stream identity is inconsistent");
            }
            StorageProfile profile;
            try {
                profile = StorageProfile.valueOf(value.storageProfile()).canonical();
            } catch (IllegalArgumentException failure) {
                throw invariant("materialization registration has an unknown storage profile", failure);
            }
            if (!profile.objectMaterializationEnabled()) {
                throw new NereusException(
                        ErrorCode.UNSUPPORTED_STORAGE_PROFILE,
                        false,
                        "generation publication requires an object-materialization profile");
            }
            Optional<ProjectionRef> decoded;
            try {
                decoded = ProjectionIdentity.decode(value.projectionRef());
            } catch (IllegalArgumentException failure) {
                throw invariant("materialization registration projection identity is malformed", failure);
            }
            ProjectionRef projection = decoded.orElseThrow(() ->
                    invariant("generation publication requires a non-empty projection identity"));
            if (!output.projectionRef().equals(Optional.of(projection))) {
                throw invariant("materialization output projection differs from stream registration");
            }
            for (SourceGeneration source : task.sources()) {
                if (source.projectionRef().isPresent()
                        && !source.projectionRef().orElseThrow().equals(projection)) {
                    throw invariant("materialization source projection differs from stream registration");
                }
                if (source.generation() > 0 && source.projectionRef().isEmpty()) {
                    throw invariant("higher-generation source is missing its effective projection identity");
                }
            }
            LiveProjectionSubject subject = new LiveProjectionSubject(
                    task.streamId(),
                    projection,
                    new Checksum(ChecksumType.SHA256, value.projectionIdentitySha256()));
            return new AdmissionSubject(profile, subject);
        }

        private void validateSnapshot(
                StreamMetadataSnapshot snapshot,
                StorageProfile admittedProfile) {
            if (!snapshot.metadata().streamId().equals(task.streamId().value())) {
                throw invariant("stream snapshot belongs to another stream");
            }
            StreamState state;
            StorageProfile profile;
            try {
                state = StreamState.valueOf(snapshot.metadata().state());
                profile = StorageProfile.valueOf(snapshot.metadata().profile()).canonical();
            } catch (IllegalArgumentException failure) {
                throw invariant("stream snapshot contains an unknown state/profile", failure);
            }
            if (state != StreamState.ACTIVE && state != StreamState.SEALED) {
                throw new NereusException(
                        state == StreamState.DELETED
                                ? ErrorCode.STREAM_NOT_FOUND
                                : ErrorCode.STREAM_NOT_ACTIVE,
                        state == StreamState.CREATING,
                        "stream state does not admit generation publication");
            }
            if (profile != admittedProfile
                    || task.coverage().endOffset()
                            > snapshot.committedEnd().committedEndOffset()
                    || task.taskSequence() > snapshot.committedEnd().commitVersion()
                    || output.cumulativeSizeAtEnd() > snapshot.committedEnd().cumulativeSize()) {
                throw condition("materialization publication no longer fits committed head truth");
            }
        }

        private void requireTaskOutput(VersionedMaterializationTask durable) {
            try {
                MaterializationRecordMapper.requireTaskAndOutput(durable, task, output);
            } catch (IllegalArgumentException failure) {
                throw invariant("durable task/output identity does not match publication input", failure);
            }
        }

        private void requireFrozenAllocation(VersionedMaterializationTask durable) {
            requireTaskOutput(durable);
            if ((durable.value().lifecycle() != TaskLifecycle.PUBLISHING
                            && durable.value().lifecycle() != TaskLifecycle.PUBLISHED)
                    || durable.value().allocatedGeneration().isEmpty()
                    || durable.value().publicationId().isEmpty()) {
                throw condition("task does not contain a frozen publication allocation");
            }
        }

        private void requireIndexIdentity(
                VersionedMaterializationTask publishingTask,
                VersionedGenerationIndex index) {
            requireFrozenAllocation(publishingTask);
            GenerationIndexRecord expected = MaterializationRecordMapper.preparedIndex(
                    task,
                    output,
                    publishingTask.value().allocatedGeneration().orElseThrow(),
                    publishingTask.value().publicationId(),
                    index.value().createdAtMillis());
            String expectedKey = new F4Keyspace(cluster).generationIndexKey(
                    task.streamId(),
                    task.view(),
                    task.coverage().endOffset(),
                    publishingTask.value().allocatedGeneration().orElseThrow());
            if (!index.key().equals(expectedKey)
                    || !MaterializationRecordMapper.sameGenerationPublicationIdentity(
                            expected, index.value())) {
                throw invariant("generation index does not match the frozen task publication identity");
            }
        }

        private void requirePreparedExact(
                VersionedMaterializationTask publishingTask,
                VersionedGenerationIndex index) {
            requireIndexIdentity(publishingTask, index);
            if (index.value().lifecycle() != GenerationLifecycle.PREPARED) {
                throw condition("generation index is not PREPARED");
            }
        }

        private void requireCommittedExact(
                VersionedMaterializationTask publishingTask,
                VersionedGenerationIndex index) {
            requireIndexIdentity(publishingTask, index);
            if (index.value().lifecycle() != GenerationLifecycle.COMMITTED) {
                throw condition("generation index is not COMMITTED");
            }
        }

        private void requireTaskMatchesIndex(
                VersionedMaterializationTask durable,
                VersionedGenerationIndex committed) {
            requireFrozenAllocation(durable);
            if (durable.value().allocatedGeneration().orElseThrow() != committed.value().generation()
                    || !durable.value().publicationId().equals(committed.value().publicationId())) {
                throw invariant("task publication allocation differs from committed index");
            }
        }

        private void requireSourceIndex(
                SourceGeneration source,
                VersionedGenerationCandidate actual) {
            if (!actual.key().equals(source.indexKey())
                    || actual.metadataVersion() != source.indexMetadataVersion()
                    || !actual.durableValueSha256().equals(source.indexRecordSha256())) {
                throw condition("materialization source index identity changed");
            }
            if (actual instanceof VersionedGenerationZeroIndex zero) {
                if (source.generation() != 0 || zero.value().tombstoned()) {
                    throw condition("generation-zero materialization source is no longer readable");
                }
                return;
            }
            if (actual instanceof VersionedGenerationIndex higher) {
                if (source.generation() <= 0
                        || higher.value().lifecycle() != GenerationLifecycle.COMMITTED) {
                    throw condition("higher-generation materialization source is not COMMITTED");
                }
                return;
            }
            throw invariant("unknown materialization source index wrapper");
        }

        private ObjectProtectionOwner taskOwner(VersionedMaterializationTask value) {
            return new ObjectProtectionOwner(
                    value.key(), value.metadataVersion(), value.durableValueSha256());
        }

        private ObjectProtectionOwner indexOwner(VersionedGenerationIndex value) {
            return MaterializationProtectionIdentities.indexOwner(value);
        }

        private String visibleReferenceId(VersionedMaterializationTask publishingTask) {
            return MaterializationProtectionIdentities.visibleReferenceId(
                    cluster,
                    task,
                    publishingTask.value().allocatedGeneration().orElseThrow(),
                    publishingTask.value().publicationId());
        }

        private GenerationCommitResult commitResult(
                VersionedGenerationIndex committed,
                boolean committedByThisCall) {
            return new GenerationCommitResult(
                    task.streamId(),
                    task.view(),
                    task.coverage(),
                    new GenerationId(committed.value().generation()),
                    new PublicationId(committed.value().publicationId()),
                    committed.key(),
                    committed.metadataVersion(),
                    committed.durableValueSha256(),
                    committedByThisCall);
        }

        private NereusException unrecoverableIndex(VersionedGenerationIndex current) {
            return new NereusException(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    "task generation index is in unrecoverable lifecycle "
                            + current.value().lifecycle());
        }

        private <T> CompletableFuture<T> recoveryExhausted(String stage) {
            return CompletableFuture.failedFuture(recoveryExhaustedFailure(stage));
        }

        private NereusException recoveryExhaustedFailure(String stage) {
            return new NereusException(
                    ErrorCode.METADATA_CONDITION_FAILED,
                    true,
                    stage + " exhausted bounded CAS recovery");
        }
    }

    private record AdmissionSubject(
            StorageProfile profile,
            LiveProjectionSubject subject) {
    }

    private record Admission(
            StorageProfile profile,
            GenerationActivationProof proof) {
    }

    private static GenerationOperation operationFor(ReadView view) {
        return view == ReadView.COMMITTED
                ? GenerationOperation.GENERATION_PUBLISH
                : GenerationOperation.TOPIC_COMPACTED_PUBLISH;
    }

    private static void requireDomainAgreement(
            MaterializationTask task,
            MaterializationOutput output) {
        if (!output.taskId().equals(task.taskId())
                || !output.streamId().equals(task.streamId())
                || output.view() != task.view()
                || !output.coverage().equals(task.coverage())
                || !output.sourceSetSha256().equals(task.sourceSetSha256())) {
            throw new IllegalArgumentException("materialization task/output identity does not agree");
        }
        long records = 0;
        long entries = 0;
        long logicalBytes = 0;
        long cumulativeCursor = task.sources().get(0).cumulativeSizeAtStart();
        for (SourceGeneration source : task.sources()) {
            if (source.cumulativeSizeAtStart() != cumulativeCursor) {
                throw new IllegalArgumentException("materialization source cumulative intervals are not contiguous");
            }
            records = Math.addExact(records, source.recordCount());
            entries = Math.addExact(entries, source.entryCount());
            logicalBytes = Math.addExact(logicalBytes, source.logicalBytes());
            cumulativeCursor = source.cumulativeSizeAtEnd();
        }
        if (records != output.sourceRecordCount()
                || output.cumulativeSizeAtStart()
                        != task.sources().get(0).cumulativeSizeAtStart()
                || output.cumulativeSizeAtEnd() != cumulativeCursor) {
            throw new IllegalArgumentException("materialization output/source accounting does not agree");
        }
        if (task.view() == ReadView.COMMITTED
                && (entries != output.entryCount() || logicalBytes != output.logicalBytes())) {
            throw new IllegalArgumentException("lossless materialization output accounting is not exact");
        }
    }

    private static boolean sameVersionedTask(
            VersionedMaterializationTask left,
            VersionedMaterializationTask right) {
        return left.key().equals(right.key())
                && left.metadataVersion() == right.metadataVersion()
                && left.durableValueSha256().equals(right.durableValueSha256())
                && left.value().equals(right.value());
    }

    private static boolean sameVersionedIndex(
            VersionedGenerationIndex left,
            VersionedGenerationIndex right) {
        return left.key().equals(right.key())
                && left.metadataVersion() == right.metadataVersion()
                && left.durableValueSha256().equals(right.durableValueSha256())
                && left.value().equals(right.value());
    }

    private static void requireSameOwner(
            ObjectProtectionOwner expected,
            ObjectProtectionOwner actual,
            String message) {
        if (!expected.equals(actual)) {
            throw condition(message);
        }
    }

    private static F4MetadataConditionFailedException condition(String message) {
        return new F4MetadataConditionFailedException(message);
    }

    private static NereusException invariant(String message) {
        return invariant(message, null);
    }

    private static NereusException invariant(String message, Throwable cause) {
        return new NereusException(
                ErrorCode.METADATA_INVARIANT_VIOLATION, false, message, cause);
    }

    private static Throwable unwrap(Throwable supplied) {
        Throwable current = supplied;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
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
}
