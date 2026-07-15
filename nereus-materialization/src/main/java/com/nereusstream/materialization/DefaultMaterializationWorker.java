/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ProjectionRef;
import com.nereusstream.api.ReadIsolation;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.SchemaRef;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.core.physical.ObjectProtection;
import com.nereusstream.core.physical.ObjectProtectionManager;
import com.nereusstream.core.physical.ObjectProtectionOwner;
import com.nereusstream.core.physical.ObjectProtectionRequest;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.read.PhysicalObjectIdentityResolver;
import com.nereusstream.metadata.oxia.F4Keyspace;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.ProjectionIdentity;
import com.nereusstream.metadata.oxia.VersionedMaterializationStreamRegistration;
import com.nereusstream.metadata.oxia.VersionedMaterializationTask;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.metadata.oxia.records.MaterializationStreamRegistrationRecord;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import com.nereusstream.metadata.oxia.records.TaskFailureClass;
import com.nereusstream.metadata.oxia.records.TaskLifecycle;
import com.nereusstream.objectstore.HeadObjectOptions;
import com.nereusstream.objectstore.HeadObjectResult;
import com.nereusstream.objectstore.ObjectAlreadyExistsException;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.PutObjectOptions;
import com.nereusstream.objectstore.compacted.CompactedObjectWriteRequest;
import com.nereusstream.objectstore.compacted.CompactedObjectWriteResult;
import com.nereusstream.objectstore.compacted.CompactedObjectWriter;
import com.nereusstream.objectstore.compacted.TopicCompactionFormatSpec;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * F4-M3 NCP1/NTC1 worker from durable claim through verified OUTPUT_READY.
 *
 * <p>Visibility remains exclusively owned by {@link GenerationCommitter}; worker success only freezes one immutable
 * output and its durable physical protections.
 */
public final class DefaultMaterializationWorker implements MaterializationWorker {
    private static final int MAX_CLAIM_RECOVERY_ATTEMPTS = 16;

    private final String cluster;
    private final String processRunId;
    private final MaterializationTaskStore tasks;
    private final GenerationMetadataStore generations;
    private final PhysicalObjectIdentityResolver identities;
    private final ObjectProtectionManager protections;
    private final ExactSourceRangeReaderFactory sourceReaders;
    private final CompactedObjectWriter writer;
    private final ObjectStore objectStore;
    private final MaterializationOutputVerifier outputVerifier;
    private final Optional<TopicCompactionEngine> topicCompactionEngine;
    private final TopicCompactionRegistry topicCompactionRegistry;
    private final WorkerClaimIdGenerator claimIds;
    private final int sourceReadPageRecords;
    private final int sourceReadPageBytes;
    private final long claimDurationMillis;
    private final long claimRenewIntervalMillis;
    private final long retryDelayMillis;
    private final int maxTaskAttempts;
    private final Duration operationTimeout;
    private final String writerBuild;
    private final ScheduledExecutorService scheduler;
    private final Executor callbackExecutor;
    private final Clock clock;
    private final F4Keyspace keyspace;
    private final ConcurrentMap<String, Operation> activeOperations = new ConcurrentHashMap<>();

    public DefaultMaterializationWorker(
            String cluster,
            String processRunId,
            MaterializationTaskStore tasks,
            GenerationMetadataStore generations,
            PhysicalObjectIdentityResolver identities,
            ObjectProtectionManager protections,
            ExactSourceRangeReaderFactory sourceReaders,
            CompactedObjectWriter writer,
            ObjectStore objectStore,
            MaterializationOutputVerifier outputVerifier,
            int sourceReadPageRecords,
            int sourceReadPageBytes,
            Duration claimDuration,
            Duration claimRenewInterval,
            Duration retryDelay,
            int maxTaskAttempts,
            Duration operationTimeout,
            String writerBuild,
            ScheduledExecutorService scheduler,
            Executor callbackExecutor,
            Clock clock) {
        this(
                cluster,
                processRunId,
                tasks,
                generations,
                identities,
                protections,
                sourceReaders,
                writer,
                objectStore,
                outputVerifier,
                new SecureWorkerClaimIdGenerator(),
                sourceReadPageRecords,
                sourceReadPageBytes,
                claimDuration,
                claimRenewInterval,
                retryDelay,
                maxTaskAttempts,
                operationTimeout,
                writerBuild,
                scheduler,
                callbackExecutor,
                clock);
    }

    public DefaultMaterializationWorker(
            String cluster,
            String processRunId,
            MaterializationTaskStore tasks,
            GenerationMetadataStore generations,
            PhysicalObjectIdentityResolver identities,
            ObjectProtectionManager protections,
            ExactSourceRangeReaderFactory sourceReaders,
            CompactedObjectWriter writer,
            ObjectStore objectStore,
            MaterializationOutputVerifier outputVerifier,
            TopicCompactionEngine topicCompactionEngine,
            TopicCompactionRegistry topicCompactionRegistry,
            int sourceReadPageRecords,
            int sourceReadPageBytes,
            Duration claimDuration,
            Duration claimRenewInterval,
            Duration retryDelay,
            int maxTaskAttempts,
            Duration operationTimeout,
            String writerBuild,
            ScheduledExecutorService scheduler,
            Executor callbackExecutor,
            Clock clock) {
        this(
                cluster,
                processRunId,
                tasks,
                generations,
                identities,
                protections,
                sourceReaders,
                writer,
                objectStore,
                outputVerifier,
                new SecureWorkerClaimIdGenerator(),
                sourceReadPageRecords,
                sourceReadPageBytes,
                claimDuration,
                claimRenewInterval,
                retryDelay,
                maxTaskAttempts,
                operationTimeout,
                writerBuild,
                scheduler,
                callbackExecutor,
                clock,
                TopicSupport.enabled(topicCompactionEngine, topicCompactionRegistry));
    }

    DefaultMaterializationWorker(
            String cluster,
            String processRunId,
            MaterializationTaskStore tasks,
            GenerationMetadataStore generations,
            PhysicalObjectIdentityResolver identities,
            ObjectProtectionManager protections,
            ExactSourceRangeReaderFactory sourceReaders,
            CompactedObjectWriter writer,
            ObjectStore objectStore,
            MaterializationOutputVerifier outputVerifier,
            WorkerClaimIdGenerator claimIds,
            int sourceReadPageRecords,
            int sourceReadPageBytes,
            Duration claimDuration,
            Duration claimRenewInterval,
            Duration retryDelay,
            int maxTaskAttempts,
            Duration operationTimeout,
            String writerBuild,
            ScheduledExecutorService scheduler,
            Executor callbackExecutor,
            Clock clock) {
        this(
                cluster,
                processRunId,
                tasks,
                generations,
                identities,
                protections,
                sourceReaders,
                writer,
                objectStore,
                outputVerifier,
                claimIds,
                sourceReadPageRecords,
                sourceReadPageBytes,
                claimDuration,
                claimRenewInterval,
                retryDelay,
                maxTaskAttempts,
                operationTimeout,
                writerBuild,
                scheduler,
                callbackExecutor,
                clock,
                TopicSupport.disabled());
    }

    DefaultMaterializationWorker(
            String cluster,
            String processRunId,
            MaterializationTaskStore tasks,
            GenerationMetadataStore generations,
            PhysicalObjectIdentityResolver identities,
            ObjectProtectionManager protections,
            ExactSourceRangeReaderFactory sourceReaders,
            CompactedObjectWriter writer,
            ObjectStore objectStore,
            MaterializationOutputVerifier outputVerifier,
            TopicCompactionEngine topicCompactionEngine,
            TopicCompactionRegistry topicCompactionRegistry,
            WorkerClaimIdGenerator claimIds,
            int sourceReadPageRecords,
            int sourceReadPageBytes,
            Duration claimDuration,
            Duration claimRenewInterval,
            Duration retryDelay,
            int maxTaskAttempts,
            Duration operationTimeout,
            String writerBuild,
            ScheduledExecutorService scheduler,
            Executor callbackExecutor,
            Clock clock) {
        this(
                cluster,
                processRunId,
                tasks,
                generations,
                identities,
                protections,
                sourceReaders,
                writer,
                objectStore,
                outputVerifier,
                claimIds,
                sourceReadPageRecords,
                sourceReadPageBytes,
                claimDuration,
                claimRenewInterval,
                retryDelay,
                maxTaskAttempts,
                operationTimeout,
                writerBuild,
                scheduler,
                callbackExecutor,
                clock,
                TopicSupport.enabled(topicCompactionEngine, topicCompactionRegistry));
    }

    private DefaultMaterializationWorker(
            String cluster,
            String processRunId,
            MaterializationTaskStore tasks,
            GenerationMetadataStore generations,
            PhysicalObjectIdentityResolver identities,
            ObjectProtectionManager protections,
            ExactSourceRangeReaderFactory sourceReaders,
            CompactedObjectWriter writer,
            ObjectStore objectStore,
            MaterializationOutputVerifier outputVerifier,
            WorkerClaimIdGenerator claimIds,
            int sourceReadPageRecords,
            int sourceReadPageBytes,
            Duration claimDuration,
            Duration claimRenewInterval,
            Duration retryDelay,
            int maxTaskAttempts,
            Duration operationTimeout,
            String writerBuild,
            ScheduledExecutorService scheduler,
            Executor callbackExecutor,
            Clock clock,
            TopicSupport topicSupport) {
        this.cluster = requireText(cluster, "cluster");
        this.processRunId = requireBase32(processRunId, "processRunId");
        this.tasks = Objects.requireNonNull(tasks, "tasks");
        this.generations = Objects.requireNonNull(generations, "generations");
        this.identities = Objects.requireNonNull(identities, "identities");
        this.protections = Objects.requireNonNull(protections, "protections");
        this.sourceReaders = Objects.requireNonNull(sourceReaders, "sourceReaders");
        this.writer = Objects.requireNonNull(writer, "writer");
        this.objectStore = Objects.requireNonNull(objectStore, "objectStore");
        this.outputVerifier = Objects.requireNonNull(outputVerifier, "outputVerifier");
        TopicSupport exactTopicSupport = Objects.requireNonNull(topicSupport, "topicSupport");
        this.topicCompactionEngine = exactTopicSupport.engine();
        this.topicCompactionRegistry = exactTopicSupport.registry();
        this.claimIds = Objects.requireNonNull(claimIds, "claimIds");
        if (sourceReadPageRecords <= 0 || sourceReadPageRecords > 65_536) {
            throw new IllegalArgumentException("sourceReadPageRecords must be in [1, 65536]");
        }
        if (sourceReadPageBytes < 64 * 1024 || sourceReadPageBytes > 64 * 1024 * 1024) {
            throw new IllegalArgumentException("sourceReadPageBytes must be in [64 KiB, 64 MiB]");
        }
        this.sourceReadPageRecords = sourceReadPageRecords;
        this.sourceReadPageBytes = sourceReadPageBytes;
        this.claimDurationMillis = requirePositiveMillis(claimDuration, "claimDuration");
        this.claimRenewIntervalMillis = requirePositiveMillis(
                claimRenewInterval, "claimRenewInterval");
        if (claimRenewIntervalMillis > claimDurationMillis / 3) {
            throw new IllegalArgumentException(
                    "claimRenewInterval must be at most one third of claimDuration");
        }
        this.retryDelayMillis = requirePositiveMillis(retryDelay, "retryDelay");
        if (maxTaskAttempts <= 0) {
            throw new IllegalArgumentException("maxTaskAttempts must be positive");
        }
        this.maxTaskAttempts = maxTaskAttempts;
        this.operationTimeout = requirePositive(operationTimeout, "operationTimeout");
        if (operationTimeout.toMillis() >= claimDurationMillis) {
            throw new IllegalArgumentException("operationTimeout must be shorter than claimDuration");
        }
        this.writerBuild = requireText(writerBuild, "writerBuild");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.callbackExecutor = Objects.requireNonNull(callbackExecutor, "callbackExecutor");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.keyspace = new F4Keyspace(cluster);
    }

    @Override
    public CompletableFuture<MaterializationOutput> execute(MaterializationTask task) {
        try {
            MaterializationTask exactTask = Objects.requireNonNull(task, "task");
            Operation operation = new Operation(exactTask);
            Operation existing = activeOperations.putIfAbsent(exactTask.taskId(), operation);
            if (existing != null) {
                return CompletableFuture.failedFuture(new NereusException(
                        ErrorCode.BACKPRESSURE_REJECTED,
                        true,
                        "materialization task is already executing in this process"));
            }
            return operation.run().whenComplete((ignored, failure) ->
                    activeOperations.remove(exactTask.taskId(), operation));
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    @Override
    public void cancel(MaterializationTask task) {
        Objects.requireNonNull(task, "task");
        Operation operation = activeOperations.get(task.taskId());
        if (operation != null) {
            operation.cancel();
        }
    }

    private final class Operation {
        private final MaterializationTask task;
        private final MaterializationDeadline deadline;
        private final List<ObjectProtection> sourceProtections = new ArrayList<>();
        private final Object heartbeatLock = new Object();
        private volatile VersionedMaterializationTask claimed;
        private ObjectProtection outputProtection;
        private Runnable rowsCloser;
        private CompactedObjectWriteResult written;
        private ScheduledFuture<?> heartbeatSchedule;
        private CompletableFuture<Void> heartbeatTail = CompletableFuture.completedFuture(null);
        private boolean heartbeatStopped;
        private final AtomicBoolean cancelled = new AtomicBoolean();
        private final AtomicBoolean cleaned = new AtomicBoolean();

        private Operation(MaterializationTask task) {
            this.task = task;
            this.deadline = new MaterializationDeadline(operationTimeout, scheduler);
            boolean lossless = task.taskKind() == TaskKind.LOSSLESS_REWRITE
                    && task.policy().targetPhysicalFormat()
                            .equals(MaterializationPolicy.COMMITTED_FORMAT);
            boolean topic = task.taskKind() == TaskKind.TOPIC_KEY_COMPACTION
                    && task.policy().targetPhysicalFormat()
                            .equals(MaterializationPolicy.TOPIC_COMPACTED_FORMAT)
                    && task.policy().topicCompaction().isPresent()
                    && topicCompactionEngine.isPresent();
            if (!lossless && !topic) {
                throw execution(
                        TaskFailureClass.UNSUPPORTED_MAPPING,
                        ErrorCode.UNSUPPORTED_FORMAT,
                        false,
                        "F4-M3 worker has no execution engine for the durable task policy",
                        null);
            }
        }

        private CompletableFuture<MaterializationOutput> run() {
            String candidateClaimId = requireBase32(claimIds.next(), "claimId");
            return claim(candidateClaimId, 0)
                    .thenCompose(this::reuseOrExecute)
                    .whenComplete((ignored, failure) -> cleanupLocal());
        }

        private CompletableFuture<MaterializationOutput> reuseOrExecute(
                VersionedMaterializationTask durable) {
            if (durable.value().output().isPresent()
                    && (durable.value().lifecycle() == TaskLifecycle.OUTPUT_READY
                            || durable.value().lifecycle() == TaskLifecycle.PUBLISHING
                            || durable.value().lifecycle() == TaskLifecycle.PUBLISHED)) {
                return CompletableFuture.completedFuture(MaterializationRecordMapper.domainOutput(
                        task, durable.value().output().orElseThrow()));
            }
            claimed = durable;
            TaskFacts facts = taskFacts();
            return loadRegistration(facts)
                    .thenCompose(admission -> acquireSources(0)
                            .thenApply(ignored -> {
                                startHeartbeat();
                                return admission;
                            })
                            .thenCompose(exactAdmission -> write(facts, exactAdmission)))
                    .thenCompose(result -> upload(result).thenApply(head -> output(facts, result, head)))
                    .thenCompose(output -> verify(output).thenApply(ignored -> output))
                    .thenCompose(output -> stopHeartbeat(true).thenApply(ignored -> output))
                    .thenCompose(this::protectOutput)
                    .thenCompose(this::freezeOutput)
                    .exceptionallyCompose(this::failClaim);
        }

        private CompletableFuture<VersionedMaterializationTask> claim(
                String candidateClaimId,
                int recoveryAttempt) {
            if (recoveryAttempt >= MAX_CLAIM_RECOVERY_ATTEMPTS) {
                return CompletableFuture.failedFuture(condition("worker claim recovery exhausted"));
            }
            return deadline.bound(
                            () -> tasks.get(task.streamId(), task.taskId()),
                            "load materialization task for claim")
                    .thenCompose(optional -> {
                        VersionedMaterializationTask current = optional.orElseThrow(() ->
                                condition("materialization task is absent before worker claim"));
                        requireTask(current);
                        TaskLifecycle lifecycle = current.value().lifecycle();
                        if (lifecycle == TaskLifecycle.OUTPUT_READY
                                || lifecycle == TaskLifecycle.PUBLISHING
                                || lifecycle == TaskLifecycle.PUBLISHED) {
                            return CompletableFuture.completedFuture(current);
                        }
                        if (lifecycle == TaskLifecycle.CLAIMED) {
                            var claim = current.value().workerClaim().orElseThrow();
                            if (claim.claimId().equals(candidateClaimId)
                                    && claim.processRunId().equals(processRunId)) {
                                return CompletableFuture.completedFuture(current);
                            }
                            return CompletableFuture.failedFuture(condition(
                                    "materialization task is already claimed by another worker"));
                        }
                        if (lifecycle != TaskLifecycle.PLANNED
                                && (lifecycle != TaskLifecycle.RETRY_WAIT
                                        || clock.millis() < current.value().retryNotBeforeMillis())) {
                            return CompletableFuture.failedFuture(condition(
                                    "materialization task lifecycle does not admit a worker claim: "
                                            + lifecycle));
                        }
                        long now = clock.millis();
                        long expiresAt = addTime(now, claimDurationMillis);
                        var replacement = MaterializationRecordMapper.claimed(
                                current.value(), candidateClaimId, processRunId, now, expiresAt);
                        return deadline.bound(
                                        () -> tasks.compareAndSet(
                                                replacement, current.metadataVersion()),
                                        "claim materialization task")
                                .handle((updated, failure) -> failure == null
                                        ? CompletableFuture.completedFuture(updated)
                                        : claim(candidateClaimId, recoveryAttempt + 1))
                                .thenCompose(value -> value);
                    });
        }

        private CompletableFuture<Admission> loadRegistration(TaskFacts facts) {
            return deadline.bound(
                            () -> generations.getStreamRegistration(cluster, task.streamId()),
                            "load materialization registration for worker")
                    .thenApply(optional -> validateRegistration(optional.orElseThrow(() ->
                            condition("materialization stream registration is absent")), facts));
        }

        private Admission validateRegistration(
                VersionedMaterializationStreamRegistration registration,
                TaskFacts facts) {
            MaterializationStreamRegistrationRecord value = registration.value();
            Optional<ProjectionRef> projection;
            try {
                projection = ProjectionIdentity.decode(value.projectionRef());
            } catch (RuntimeException failure) {
                throw invariant("materialization registration projection is malformed", failure);
            }
            if (!registration.key().equals(keyspace.materializationRegistryKey(task.streamId()))
                    || !value.streamId().equals(task.streamId().value())
                    || projection.isEmpty()
                    || !projection.equals(facts.projectionRef())) {
                throw condition("materialization registration no longer matches the task projection");
            }
            return new Admission(new Checksum(
                    ChecksumType.SHA256, value.projectionIdentitySha256()));
        }

        private CompletableFuture<Void> acquireSources(int index) {
            if (index == task.sources().size()) {
                return CompletableFuture.completedFuture(null);
            }
            SourceGeneration source = task.sources().get(index);
            if (!(source.readTarget() instanceof ObjectSliceReadTarget target)) {
                return CompletableFuture.failedFuture(execution(
                        TaskFailureClass.UNSUPPORTED_MAPPING,
                        ErrorCode.UNSUPPORTED_READ_TARGET,
                        false,
                        "materialization source is not an object-slice target",
                        null));
            }
            return deadline.bound(
                            () -> identities.resolve(target, source.view()),
                            "resolve materialization source identity")
                    .thenCompose(identity -> deadline.bound(
                            () -> protections.acquireOrTransfer(
                                    new ObjectProtectionRequest(
                                            identity,
                                            ObjectProtectionType.MATERIALIZATION_SOURCE,
                                            MaterializationProtectionIdentities.sourceReferenceId(
                                                    cluster, task, source),
                                            MaterializationProtectionIdentities.taskOwner(claimed),
                                            0),
                                    this::revalidateClaimOwner),
                            "acquire materialization source protection"))
                    .thenCompose(protection -> {
                        sourceProtections.add(protection);
                        return revalidateSource(source)
                                .thenCompose(ignored -> acquireSources(index + 1));
                    });
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
                            throw execution(
                                    TaskFailureClass.SOURCE_CHANGED,
                                    ErrorCode.METADATA_CONDITION_FAILED,
                                    true,
                                    "materialization source changed after protection",
                                    null);
                        }
                    });
        }

        private CompletableFuture<CompactedObjectWriteResult> write(
                TaskFacts facts,
                Admission admission) {
            ExactSourceRangeReader exactReader = Objects.requireNonNull(
                    sourceReaders.forStream(task.streamId()), "exact source reader");
            ReadOptions options = new ReadOptions(
                    sourceReadPageRecords,
                    sourceReadPageBytes,
                    ReadIsolation.COMMITTED,
                    deadline.remaining());
            if (task.taskKind() == TaskKind.LOSSLESS_REWRITE) {
                LosslessMaterializationRowPublisher lossless =
                        new LosslessMaterializationRowPublisher(
                                task, exactReader, options, callbackExecutor);
                rowsCloser = lossless::close;
                return writeRows(
                        writeRequest(
                                facts,
                                admission,
                                Math.toIntExact(task.coverage().recordCount())),
                        lossless);
            }
            TopicCompactionRegistry.Binding binding = resolveTopicBinding();
            return deadline.bound(
                            () -> topicCompactionEngine.orElseThrow().prepare(
                                    task,
                                    exactReader,
                                    options,
                                    binding,
                                    claimed.value().createdAtMillis()),
                            "prepare two-pass topic compaction")
                    .thenCompose(plan -> {
                        rowsCloser = plan::close;
                        return writeRows(
                                writeRequest(facts, admission, plan.outputRecordCount()),
                                plan);
                    });
        }

        private CompletableFuture<CompactedObjectWriteResult> writeRows(
                CompactedObjectWriteRequest request,
                java.util.concurrent.Flow.Publisher<com.nereusstream.objectstore.compacted.CompactedObjectRow>
                        publisher) {
            return deadline.bound(
                            () -> writer.write(request, publisher),
                            "write compacted Parquet output")
                    .thenApply(result -> {
                        written = result;
                        return result;
                    });
        }

        private TopicCompactionRegistry.Binding resolveTopicBinding() {
            try {
                return topicCompactionRegistry.resolve(
                        task.policy().topicCompaction().orElseThrow());
            } catch (Throwable failure) {
                throw execution(
                        TaskFailureClass.UNSUPPORTED_MAPPING,
                        ErrorCode.UNSUPPORTED_FORMAT,
                        false,
                        "topic-compaction registry cannot resolve the durable task policy",
                        failure);
            }
        }

        private CompactedObjectWriteRequest writeRequest(
                TaskFacts facts,
                Admission admission,
                int expectedOutputRecordCount) {
            return new CompactedObjectWriteRequest(
                    cluster,
                    task.view(),
                    task.streamId(),
                    task.coverage(),
                    claimed.value().workerClaim().orElseThrow().claimId(),
                    task.sourceSetSha256(),
                    task.policyDigestSha256(),
                    facts.payloadFormat(),
                    facts.payloadFormat().name(),
                    Optional.of(admission.projectionIdentitySha256()),
                    Math.toIntExact(task.coverage().recordCount()),
                    expectedOutputRecordCount,
                    facts.entryCount(),
                    facts.logicalBytes(),
                    facts.schemaRefs(),
                    facts.cumulativeSizeAtStart(),
                    facts.cumulativeSizeAtEnd(),
                    task.policy().targetRowGroupRecords(),
                    task.policy().compression(),
                    writerBuild,
                    task.policy().topicCompaction().map(spec ->
                            new TopicCompactionFormatSpec(
                                    spec.strategyId(),
                                    spec.strategyVersion(),
                                    spec.keyCodecId())));
        }

        private CompletableFuture<HeadObjectResult> upload(CompactedObjectWriteResult result) {
            PutObjectOptions options = new PutObjectOptions(
                    "application/vnd.apache.parquet",
                    result.storageCrc32c(),
                    true,
                    Map.of(
                            "nereus-task-id", task.taskId(),
                            "nereus-content-sha256", result.contentSha256().value()),
                    deadline.remaining());
            return deadline.bound(
                            () -> objectStore.putObject(
                                    result.objectKey(),
                                    result.stagingFile(),
                                    options,
                                    (key, attempt) -> {
                                        if (!key.equals(result.objectKey()) || attempt <= 0) {
                                            return CompletableFuture.failedFuture(invariant(
                                                    "guarded compacted upload identity is invalid", null));
                                        }
                                        return revalidateClaim(claimed);
                                    }),
                            "upload compacted Parquet output")
                    .handle((put, failure) -> {
                        if (failure == null || unwrap(failure) instanceof ObjectAlreadyExistsException) {
                            return head(result);
                        }
                        return CompletableFuture.<HeadObjectResult>failedFuture(unwrap(failure));
                    })
                    .thenCompose(value -> value);
        }

        private CompletableFuture<HeadObjectResult> head(CompactedObjectWriteResult result) {
            return deadline.bound(
                            () -> objectStore.headObject(
                                    result.objectKey(),
                                    new HeadObjectOptions(deadline.remaining())),
                            "HEAD compacted Parquet output")
                    .thenApply(head -> {
                        if (!head.key().equals(result.objectKey())
                                || head.objectLength() != result.objectLength()
                                || !head.checksum().equals(result.storageCrc32c())) {
                            throw execution(
                                    TaskFailureClass.OUTPUT_INVARIANT,
                                    ErrorCode.OBJECT_CHECKSUM_MISMATCH,
                                    false,
                                    "uploaded compacted output HEAD differs from staged bytes",
                                    null);
                        }
                        return head;
                    });
        }

        private MaterializationOutput output(
                TaskFacts facts,
                CompactedObjectWriteResult result,
                HeadObjectResult head) {
            ObjectSliceReadTarget target = new ObjectSliceReadTarget(
                    1,
                    result.objectId(),
                    result.objectKey(),
                    ObjectType.STREAM_COMPACTED_OBJECT,
                    result.physicalFormat(),
                    facts.payloadFormat().name(),
                    task.taskId(),
                    0,
                    result.objectLength(),
                    result.storageCrc32c(),
                    result.entryIndexRef());
            Checksum targetIdentity = new Checksum(
                    ChecksumType.SHA256,
                    ReadTargetCodecRegistry.phase15().encode(target).identityChecksumValue());
            return new MaterializationOutput(
                    task.taskId(),
                    task.streamId(),
                    task.view(),
                    task.coverage(),
                    claimed.value().workerClaim().orElseThrow().claimId(),
                    result.objectId(),
                    result.objectKey(),
                    result.objectKeyHash(),
                    result.objectLength(),
                    result.storageCrc32c(),
                    result.contentSha256(),
                    head.etag().orElse(""),
                    result.physicalFormat(),
                    facts.payloadFormat().name(),
                    target,
                    targetIdentity,
                    result.entryIndexRef(),
                    Math.toIntExact(task.coverage().recordCount()),
                    result.outputRecordCount(),
                    facts.entryCount(),
                    facts.logicalBytes(),
                    facts.schemaRefs(),
                    facts.cumulativeSizeAtStart(),
                    facts.cumulativeSizeAtEnd(),
                    task.sourceSetSha256(),
                    facts.projectionRef());
        }

        private CompletableFuture<Void> verify(MaterializationOutput output) {
            return deadline.bound(
                    () -> outputVerifier.verify(task, output, deadline.remaining()),
                    "strictly verify compacted materialization output");
        }

        private CompletableFuture<MaterializationOutput> protectOutput(MaterializationOutput output) {
            PhysicalObjectIdentity identity = MaterializationRecordMapper.physicalIdentity(output);
            return deadline.bound(
                            () -> protections.acquireOrTransfer(
                                    new ObjectProtectionRequest(
                                            identity,
                                            ObjectProtectionType.MATERIALIZATION_OUTPUT,
                                            MaterializationProtectionIdentities.outputReferenceId(
                                                    cluster, task, output),
                                            MaterializationProtectionIdentities.taskOwner(claimed),
                                            0),
                                    this::revalidateClaimOwner),
                            "acquire materialization output protection")
                    .thenApply(protection -> {
                        outputProtection = protection;
                        return output;
                    });
        }

        private CompletableFuture<MaterializationOutput> freezeOutput(MaterializationOutput output) {
            var ready = MaterializationRecordMapper.outputReady(
                    claimed.value(), output, clock.millis());
            return deadline.bound(
                            () -> tasks.compareAndSet(ready, claimed.metadataVersion()),
                            "freeze materialization OUTPUT_READY")
                    .handle((updated, failure) -> {
                        if (failure == null) {
                            return CompletableFuture.completedFuture(updated);
                        }
                        Throwable original = unwrap(failure);
                        return tasks.get(task.streamId(), task.taskId()).thenCompose(optional -> {
                            if (optional.isPresent()
                                    && exactOutputReady(optional.orElseThrow(), output)) {
                                return CompletableFuture.completedFuture(optional.orElseThrow());
                            }
                            return CompletableFuture.failedFuture(original);
                        });
                    })
                    .thenCompose(value -> value)
                    .thenCompose(outputReady -> transferProtections(outputReady, output, 0)
                            .thenApply(ignored -> output));
        }

        private CompletableFuture<Void> transferProtections(
                VersionedMaterializationTask outputReady,
                MaterializationOutput output,
                int index) {
            int total = sourceProtections.size() + 1;
            if (index == total) {
                claimed = outputReady;
                return CompletableFuture.completedFuture(null);
            }
            ObjectProtection current = index < sourceProtections.size()
                    ? sourceProtections.get(index)
                    : outputProtection;
            return deadline.bound(
                            () -> protections.transfer(
                                    current,
                                    MaterializationProtectionIdentities.taskOwner(outputReady),
                                    expected -> revalidateOutputOwner(
                                            expected, outputReady, output)),
                            "transfer materialization protection to OUTPUT_READY")
                    .thenCompose(transferred -> {
                        if (index < sourceProtections.size()) {
                            sourceProtections.set(index, transferred);
                        } else {
                            outputProtection = transferred;
                        }
                        return transferProtections(outputReady, output, index + 1);
                    });
        }

        private CompletableFuture<MaterializationOutput> failClaim(Throwable failure) {
            Throwable exact = unwrap(failure);
            if (cancelled.get()) {
                return CompletableFuture.failedFuture(new NereusException(
                        ErrorCode.STORAGE_CLOSED,
                        false,
                        "materialization worker operation was cancelled during close",
                        exact));
            }
            if (claimed == null || claimed.value().lifecycle() != TaskLifecycle.CLAIMED) {
                return CompletableFuture.failedFuture(exact);
            }
            return stopHeartbeat(false)
                    .exceptionally(heartbeatFailure -> {
                        exact.addSuppressed(unwrap(heartbeatFailure));
                        return null;
                    })
                    .thenCompose(ignored -> releaseProtections(0))
                    .exceptionally(releaseFailure -> {
                        exact.addSuppressed(unwrap(releaseFailure));
                        return null;
                    })
                    .thenCompose(ignored -> persistFailure(exact))
                    .handle((ignored, transitionFailure) -> {
                        if (transitionFailure != null) {
                            exact.addSuppressed(unwrap(transitionFailure));
                        }
                        return CompletableFuture.<MaterializationOutput>failedFuture(exact);
                    })
                    .thenCompose(value -> value);
        }

        private CompletableFuture<Void> releaseProtections(int index) {
            List<ObjectProtection> all = new ArrayList<>(sourceProtections);
            if (outputProtection != null) {
                all.add(outputProtection);
            }
            if (index == all.size()) {
                return CompletableFuture.completedFuture(null);
            }
            ObjectProtection protection = all.get(index);
            return protections.release(
                            protection,
                            exact -> revalidateClaim(claimed))
                    .thenCompose(ignored -> releaseProtections(index + 1));
        }

        private CompletableFuture<Void> persistFailure(Throwable failure) {
            FailureDecision decision = failureDecision(failure, claimed.value().attempt());
            long now = clock.millis();
            long retryAt = decision.lifecycle() == TaskLifecycle.RETRY_WAIT
                    ? addTime(now, retryDelayMillis)
                    : 0;
            var replacement = MaterializationRecordMapper.failedClaim(
                    claimed.value(),
                    decision.lifecycle(),
                    decision.failureClass(),
                    failureMessage(failure),
                    retryAt,
                    now);
            return tasks.compareAndSet(replacement, claimed.metadataVersion())
                    .handle((updated, transitionFailure) -> {
                        if (transitionFailure == null) {
                            return CompletableFuture.<Void>completedFuture(null);
                        }
                        return tasks.get(task.streamId(), task.taskId()).thenCompose(optional -> {
                            if (optional.isPresent()
                                    && optional.orElseThrow().value().lifecycle()
                                            == decision.lifecycle()
                                    && optional.orElseThrow().value().failureClassId()
                                            == decision.failureClass().wireId()) {
                                return CompletableFuture.<Void>completedFuture(null);
                            }
                            return CompletableFuture.<Void>failedFuture(unwrap(transitionFailure));
                        });
                    })
                    .thenCompose(value -> value);
        }

        private CompletableFuture<Void> revalidateClaimOwner(ObjectProtectionOwner expected) {
            if (!expected.equals(MaterializationProtectionIdentities.taskOwner(claimed))) {
                return CompletableFuture.failedFuture(condition(
                        "materialization protection owner differs from the active claim"));
            }
            return revalidateClaim(claimed);
        }

        private void startHeartbeat() {
            synchronized (heartbeatLock) {
                if (heartbeatSchedule != null || heartbeatStopped) {
                    throw new IllegalStateException("materialization heartbeat already started/stopped");
                }
                try {
                    heartbeatSchedule = scheduler.scheduleAtFixedRate(
                            this::enqueueHeartbeat,
                            claimRenewIntervalMillis,
                            claimRenewIntervalMillis,
                            TimeUnit.MILLISECONDS);
                } catch (RejectedExecutionException failure) {
                    throw new NereusException(
                            ErrorCode.STORAGE_CLOSED,
                            false,
                            "materialization heartbeat scheduler rejected admitted work",
                            failure);
                }
            }
        }

        private void enqueueHeartbeat() {
            synchronized (heartbeatLock) {
                if (heartbeatStopped) {
                    return;
                }
                heartbeatTail = heartbeatTail.thenCompose(ignored -> heartbeatOnce());
            }
        }

        private CompletableFuture<Void> stopHeartbeat(boolean finalRenewal) {
            synchronized (heartbeatLock) {
                if (heartbeatStopped) {
                    return heartbeatTail;
                }
                heartbeatStopped = true;
                if (heartbeatSchedule != null) {
                    heartbeatSchedule.cancel(false);
                    heartbeatSchedule = null;
                }
                if (finalRenewal) {
                    heartbeatTail = heartbeatTail.thenCompose(ignored -> heartbeatOnce());
                }
                return heartbeatTail;
            }
        }

        private CompletableFuture<Void> heartbeatOnce() {
            VersionedMaterializationTask expected = claimed;
            if (expected == null
                    || expected.value().lifecycle() != TaskLifecycle.CLAIMED
                    || expected.value().workerClaim().isEmpty()) {
                return CompletableFuture.failedFuture(condition(
                        "materialization heartbeat lost its active claim"));
            }
            long now = clock.millis();
            long priorExpiry = expected.value().workerClaim().orElseThrow().expiresAtMillis();
            long requestedExpiry = addTime(now, claimDurationMillis);
            long expiresAt = Math.max(addTime(priorExpiry, 1), requestedExpiry);
            var replacement = MaterializationRecordMapper.heartbeat(
                    expected.value(), expiresAt, now);
            return deadline.bound(
                            () -> tasks.compareAndSet(
                                    replacement, expected.metadataVersion()),
                            "renew materialization worker claim")
                    .thenCompose(updated -> {
                        claimed = updated;
                        return transferHeartbeatProtections(updated, 0);
                    });
        }

        private CompletableFuture<Void> transferHeartbeatProtections(
                VersionedMaterializationTask updated,
                int index) {
            if (index == sourceProtections.size()) {
                return CompletableFuture.completedFuture(null);
            }
            ObjectProtection current = sourceProtections.get(index);
            return deadline.bound(
                            () -> protections.transfer(
                                    current,
                                    MaterializationProtectionIdentities.taskOwner(updated),
                                    expectedOwner -> revalidateHeartbeatOwner(
                                            expectedOwner, updated)),
                            "transfer source protection to renewed claim")
                    .thenCompose(transferred -> {
                        sourceProtections.set(index, transferred);
                        return transferHeartbeatProtections(updated, index + 1);
                    });
        }

        private CompletableFuture<Void> revalidateHeartbeatOwner(
                ObjectProtectionOwner expectedOwner,
                VersionedMaterializationTask updated) {
            if (!expectedOwner.equals(MaterializationProtectionIdentities.taskOwner(updated))) {
                return CompletableFuture.failedFuture(condition(
                        "renewed source protection owner is inconsistent"));
            }
            return revalidateClaim(updated);
        }

        private CompletableFuture<Void> revalidateOutputOwner(
                ObjectProtectionOwner expected,
                VersionedMaterializationTask outputReady,
                MaterializationOutput output) {
            if (!expected.equals(MaterializationProtectionIdentities.taskOwner(outputReady))) {
                return CompletableFuture.failedFuture(condition(
                        "materialization protection owner differs from OUTPUT_READY task"));
            }
            return tasks.get(task.streamId(), task.taskId()).thenAccept(optional -> {
                if (optional.isEmpty() || !sameVersioned(outputReady, optional.orElseThrow())
                        || !exactOutputReady(optional.orElseThrow(), output)) {
                    throw condition("OUTPUT_READY task changed during protection transfer");
                }
            });
        }

        private CompletableFuture<Void> revalidateClaim(
                VersionedMaterializationTask expected) {
            return tasks.get(task.streamId(), task.taskId()).thenAccept(optional -> {
                if (optional.isEmpty()
                        || !sameVersioned(expected, optional.orElseThrow())
                        || optional.orElseThrow().value().lifecycle() != TaskLifecycle.CLAIMED
                        || optional.orElseThrow().value().workerClaim().isEmpty()) {
                    throw condition("worker claim changed during protected materialization IO");
                }
            });
        }

        private TaskFacts taskFacts() {
            SourceGeneration first = task.sources().get(0);
            long cumulative = first.cumulativeSizeAtStart();
            long logicalBytes = 0;
            int entries = 0;
            for (SourceGeneration source : task.sources()) {
                if (source.payloadFormat() != first.payloadFormat()
                        || !source.schemaRefs().equals(first.schemaRefs())
                        || !source.projectionRef().equals(first.projectionRef())
                        || source.cumulativeSizeAtStart() != cumulative) {
                    throw execution(
                            TaskFailureClass.OUTPUT_INVARIANT,
                            ErrorCode.METADATA_INVARIANT_VIOLATION,
                            false,
                            "materialization task sources are format/accounting incompatible",
                            null);
                }
                entries = Math.addExact(entries, source.entryCount());
                logicalBytes = Math.addExact(logicalBytes, source.logicalBytes());
                cumulative = source.cumulativeSizeAtEnd();
            }
            if (first.projectionRef().isEmpty()) {
                throw execution(
                        TaskFailureClass.UNSUPPORTED_MAPPING,
                        ErrorCode.UNSUPPORTED_FORMAT,
                        false,
                        "F4-M3 worker requires a registered projection identity",
                        null);
            }
            return new TaskFacts(
                    first.payloadFormat(),
                    first.schemaRefs(),
                    first.projectionRef(),
                    entries,
                    logicalBytes,
                    first.cumulativeSizeAtStart(),
                    cumulative);
        }

        private void requireTask(VersionedMaterializationTask durable) {
            MaterializationTask recovered = tasks.requireTask(durable);
            if (!recovered.equals(task)) {
                throw invariant("durable task differs from worker input", null);
            }
        }

        private void cleanupLocal() {
            if (!cleaned.compareAndSet(false, true)) {
                return;
            }
            deadline.close();
            synchronized (heartbeatLock) {
                heartbeatStopped = true;
                if (heartbeatSchedule != null) {
                    heartbeatSchedule.cancel(false);
                    heartbeatSchedule = null;
                }
            }
            if (rowsCloser != null) {
                rowsCloser.run();
            }
            if (written != null) {
                written.close();
            }
        }

        private void cancel() {
            if (!cancelled.compareAndSet(false, true)) {
                return;
            }
            cleanupLocal();
        }

    }

    private static boolean sameVersioned(
            VersionedMaterializationTask left,
            VersionedMaterializationTask right) {
        return left.key().equals(right.key())
                && left.metadataVersion() == right.metadataVersion()
                && left.durableValueSha256().equals(right.durableValueSha256())
                && left.value().equals(right.value());
    }

    private static boolean exactOutputReady(
            VersionedMaterializationTask durable,
            MaterializationOutput output) {
        return durable.value().lifecycle() == TaskLifecycle.OUTPUT_READY
                && durable.value().output().isPresent()
                && durable.value().output().orElseThrow()
                        .equals(MaterializationRecordMapper.outputRecord(output));
    }

    private FailureDecision failureDecision(Throwable failure, long attempt) {
        TaskFailureClass failureClass;
        if (failure instanceof MaterializationExecutionException execution) {
            failureClass = execution.failureClass();
        } else if (failure instanceof NereusException nereus) {
            failureClass = switch (nereus.code()) {
                case OBJECT_UPLOAD_FAILED, OBJECT_READ_FAILED, OBJECT_NOT_FOUND, TIMEOUT ->
                        TaskFailureClass.RETRYABLE_OBJECT_STORE;
                case METADATA_UNAVAILABLE, METADATA_CONDITION_FAILED ->
                        TaskFailureClass.RETRYABLE_METADATA;
                case BACKPRESSURE_REJECTED, METADATA_LIMIT_EXCEEDED, READ_LIMIT_TOO_SMALL ->
                        TaskFailureClass.RETRYABLE_RESOURCE_LIMIT;
                case OBJECT_CHECKSUM_MISMATCH, PRIMARY_WAL_CHECKSUM_MISMATCH ->
                        TaskFailureClass.CORRUPT_SOURCE;
                case UNSUPPORTED_FORMAT, UNSUPPORTED_READ_TARGET, UNSUPPORTED_STORAGE_PROFILE ->
                        TaskFailureClass.UNSUPPORTED_MAPPING;
                default -> TaskFailureClass.OUTPUT_INVARIANT;
            };
        } else {
            failureClass = TaskFailureClass.OUTPUT_INVARIANT;
        }
        TaskLifecycle lifecycle;
        if (failureClass == TaskFailureClass.SOURCE_CHANGED
                || failureClass == TaskFailureClass.SOURCE_RETIRED) {
            lifecycle = TaskLifecycle.CANCELLED;
        } else if (failureClass == TaskFailureClass.UNSUPPORTED_MAPPING
                || failureClass == TaskFailureClass.OUTPUT_INVARIANT
                || failureClass == TaskFailureClass.CORRUPT_SOURCE
                || attempt >= maxTaskAttempts) {
            lifecycle = TaskLifecycle.TERMINAL_FAILED;
        } else {
            lifecycle = TaskLifecycle.RETRY_WAIT;
        }
        return new FailureDecision(lifecycle, failureClass);
    }

    private static String failureMessage(Throwable failure) {
        String message = failure.getMessage();
        if (message == null || message.isBlank()) {
            message = failure.getClass().getSimpleName();
        }
        return message.length() <= 1_024 ? message : message.substring(0, 1_024);
    }

    private static long addTime(long value, long delta) {
        if (value < 0 || delta <= 0 || value > Long.MAX_VALUE - delta) {
            throw new IllegalArgumentException("materialization time window overflows");
        }
        return value + delta;
    }

    private static long requirePositiveMillis(Duration value, String field) {
        Duration exact = requirePositive(value, field);
        try {
            long millis = exact.toMillis();
            if (millis <= 0) {
                throw new IllegalArgumentException(field + " must be at least one millisecond");
            }
            return millis;
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException(field + " is too large", overflow);
        }
    }

    private static Duration requirePositive(Duration value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(field + " must be positive");
        }
        return value;
    }

    private static String requireBase32(String value, String field) {
        value = requireText(value, field);
        if (value.length() < 26 || value.length() > 128 || !value.matches("[a-z2-7]+")) {
            throw new IllegalArgumentException(field + " must be lowercase base32 with at least 128 bits");
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

    private static NereusException invariant(String message, Throwable cause) {
        return new NereusException(
                ErrorCode.METADATA_INVARIANT_VIOLATION, false, message, cause);
    }

    private static MaterializationExecutionException execution(
            TaskFailureClass failureClass,
            ErrorCode code,
            boolean retriable,
            String message,
            Throwable cause) {
        return new MaterializationExecutionException(
                failureClass, code, retriable, message, cause);
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException
                        || current instanceof java.util.concurrent.ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private record Admission(Checksum projectionIdentitySha256) { }

    private record TaskFacts(
            PayloadFormat payloadFormat,
            List<SchemaRef> schemaRefs,
            Optional<ProjectionRef> projectionRef,
            int entryCount,
            long logicalBytes,
            long cumulativeSizeAtStart,
            long cumulativeSizeAtEnd) {
        private TaskFacts {
            schemaRefs = List.copyOf(schemaRefs);
        }
    }

    private record FailureDecision(
            TaskLifecycle lifecycle,
            TaskFailureClass failureClass) { }

    private record TopicSupport(
            Optional<TopicCompactionEngine> engine,
            TopicCompactionRegistry registry) {
        private TopicSupport {
            engine = Objects.requireNonNull(engine, "engine");
            registry = Objects.requireNonNull(registry, "registry");
        }

        private static TopicSupport disabled() {
            return new TopicSupport(Optional.empty(), TopicCompactionRegistry.empty());
        }

        private static TopicSupport enabled(
                TopicCompactionEngine engine,
                TopicCompactionRegistry registry) {
            return new TopicSupport(
                    Optional.of(Objects.requireNonNull(engine, "engine")),
                    Objects.requireNonNull(registry, "registry"));
        }
    }
}
