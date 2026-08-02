/* Licensed under the Apache License, Version 2.0 */

package com.nereusstream.kafka.runtime;

import com.nereusstream.core.append.RequiredObjectGenerationCompletion;
import com.nereusstream.core.capability.GenerationProtocolActivationGuard;
import com.nereusstream.core.physical.ObjectProtectionManager;
import com.nereusstream.core.physical.ObjectReadPinManager;
import com.nereusstream.core.read.MetadataPhysicalObjectIdentityResolver;
import com.nereusstream.core.read.Phase4ReadComponents;
import com.nereusstream.core.read.ReadTargetDispatcher;
import com.nereusstream.materialization.CommittedGenerationRetirementAuthority;
import com.nereusstream.materialization.CommittedObjectGenerationAuthority;
import com.nereusstream.materialization.DefaultExactSourceRangeReader;
import com.nereusstream.materialization.DefaultGenerationCommitter;
import com.nereusstream.materialization.DefaultMaterializationCheckpointReconciler;
import com.nereusstream.materialization.DefaultMaterializationLagSnapshotReader;
import com.nereusstream.materialization.DefaultMaterializationOutputVerifier;
import com.nereusstream.materialization.DefaultMaterializationPlanner;
import com.nereusstream.materialization.DefaultMaterializationService;
import com.nereusstream.materialization.DefaultMaterializationTaskDispatcher;
import com.nereusstream.materialization.DefaultMaterializationTaskProtectionReconciler;
import com.nereusstream.materialization.DefaultMaterializationWorker;
import com.nereusstream.materialization.DefaultTerminalWorkflowMetadataRetirer;
import com.nereusstream.materialization.ExactSourceRangeReaderFactory;
import com.nereusstream.materialization.GenerationPublicationReconciler;
import com.nereusstream.materialization.MaterializationConfig;
import com.nereusstream.materialization.MaterializationMetricsObserver;
import com.nereusstream.materialization.MaterializationService;
import com.nereusstream.materialization.MaterializationSourceProtectionAdapter;
import com.nereusstream.materialization.MaterializationSourceProtectionRegistry;
import com.nereusstream.materialization.MaterializationSourceProvider;
import com.nereusstream.materialization.MaterializationStreamAuthorityMode;
import com.nereusstream.materialization.MaterializationStreamTrigger;
import com.nereusstream.materialization.MaterializationTaskRecovery;
import com.nereusstream.materialization.MaterializationTaskStore;
import com.nereusstream.materialization.NormalPathCommittedObjectGenerationReadVerifier;
import com.nereusstream.materialization.ObjectMaterializationSourceProtectionAdapter;
import com.nereusstream.materialization.RangedMaterializationFormatVerifier;
import com.nereusstream.materialization.RegisteredMaterializationStreamScanner;
import com.nereusstream.materialization.RequiredObjectGenerationCoordinator;
import com.nereusstream.materialization.TaskRecoveryScanner;
import com.nereusstream.materialization.recovery.RecoveryCheckpointBuildStatus;
import com.nereusstream.materialization.recovery.RecoveryCheckpointPublisher;
import com.nereusstream.materialization.recovery.RecoveryCheckpointRunResult;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.PhysicalObjectMetadataStore;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.compacted.ParquetKafkaTopicCompactedReader;
import com.nereusstream.objectstore.compacted.ParquetRangedCompactedObjectReader;
import com.nereusstream.objectstore.compacted.ParquetRangedCompactedObjectWriter;
import com.nereusstream.objectstore.compacted.RangedCompactedObjectVerifier;
import com.nereusstream.objectstore.staging.StagingFileManager;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * One process-shared Kafka NCP2 materialization graph.
 *
 * <p>The graph is projection-free and protocol-neutral below this adapter boundary. It owns only
 * its staging directory and dedicated worker executor; all metadata stores, object storage,
 * scheduler, read components and source providers are borrowed from the enclosing Kafka runtime.
 */
public final class KafkaObjectMaterializationRuntime implements KafkaRuntimeBackgroundService, AutoCloseable {
    private static final String WRITER_BUILD = "nereus-kafka-f9-ncp2";

    private final MaterializationService service;
    private final StagingFileManager stagingFiles;
    private final ExecutorService workerExecutor;
    private final Duration closeTimeout;
    private final Executor callbackExecutor;
    private final DefaultMaterializationLagSnapshotReader lagSnapshotReader;
    private final CommittedObjectGenerationAuthority committedObjectAuthority;
    private final RequiredObjectGenerationCompletion requiredObjectGeneration;
    private final AtomicBoolean closeStarted = new AtomicBoolean();
    private volatile CompletableFuture<Void> closeFuture;

    public KafkaObjectMaterializationRuntime(
            String cluster,
            String processRunId,
            int maxCommitChainScan,
            MaterializationConfig config,
            OxiaMetadataStore l0,
            GenerationMetadataStore generations,
            PhysicalObjectMetadataStore physical,
            ObjectStore objectStore,
            MetadataPhysicalObjectIdentityResolver identities,
            ObjectProtectionManager protections,
            ObjectReadPinManager readPins,
            Phase4ReadComponents readComponents,
            GenerationProtocolActivationGuard activation,
            List<MaterializationSourceProvider> additionalPrimarySources,
            ScheduledExecutorService scheduler,
            ExecutorService workerExecutor,
            Executor callbackExecutor,
            Clock clock) {
        String exactCluster = requireText(cluster, "cluster");
        String exactProcessRunId = requireText(processRunId, "processRunId");
        if (maxCommitChainScan <= 0) {
            throw new IllegalArgumentException("maxCommitChainScan must be positive");
        }
        MaterializationConfig exactConfig = Objects.requireNonNull(config, "config");
        if (!exactConfig
                .committedPolicy()
                .targetPhysicalFormat()
                .equals(com.nereusstream.materialization.MaterializationPolicy.KAFKA_COMMITTED_FORMAT)) {
            throw new IllegalArgumentException(
                    "Kafka materialization runtime requires the closed NCP2 committed policy");
        }
        OxiaMetadataStore exactL0 = Objects.requireNonNull(l0, "l0");
        GenerationMetadataStore exactGenerations = Objects.requireNonNull(generations, "generations");
        PhysicalObjectMetadataStore exactPhysical = Objects.requireNonNull(physical, "physical");
        ObjectStore exactObjectStore = Objects.requireNonNull(objectStore, "objectStore");
        MetadataPhysicalObjectIdentityResolver exactIdentities = Objects.requireNonNull(identities, "identities");
        ObjectProtectionManager exactProtections = Objects.requireNonNull(protections, "protections");
        ObjectReadPinManager exactReadPins = Objects.requireNonNull(readPins, "readPins");
        Phase4ReadComponents exactReads = Objects.requireNonNull(readComponents, "readComponents");
        GenerationProtocolActivationGuard exactActivation = Objects.requireNonNull(activation, "activation");
        List<MaterializationSourceProvider> exactAdditionalSources =
                List.copyOf(Objects.requireNonNull(additionalPrimarySources, "additionalPrimarySources"));
        ScheduledExecutorService exactScheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.workerExecutor = Objects.requireNonNull(workerExecutor, "workerExecutor");
        this.callbackExecutor = Objects.requireNonNull(callbackExecutor, "callbackExecutor");
        Clock exactClock = Objects.requireNonNull(clock, "clock");
        this.closeTimeout = exactConfig.closeTimeout();

        this.stagingFiles = new StagingFileManager(
                exactConfig.stagingDirectory(),
                exactConfig.maxStagingBytes(),
                exactConfig.uploadChunkBytes(),
                exactConfig.metadataAuditGrace(),
                this.workerExecutor);

        List<MaterializationSourceProtectionAdapter<?>> sourceProtectionAdapters = new ArrayList<>();
        sourceProtectionAdapters.add(
                new ObjectMaterializationSourceProtectionAdapter(exactIdentities, exactProtections));
        exactAdditionalSources.stream()
                .map(MaterializationSourceProvider::protectionAdapter)
                .forEach(sourceProtectionAdapters::add);
        MaterializationSourceProtectionRegistry sourceProtections =
                new MaterializationSourceProtectionRegistry(sourceProtectionAdapters);

        ParquetRangedCompactedObjectReader rangedReader =
                new ParquetRangedCompactedObjectReader(exactObjectStore, this.workerExecutor);
        DefaultMaterializationOutputVerifier outputVerifier = new DefaultMaterializationOutputVerifier(
                exactObjectStore,
                new RangedMaterializationFormatVerifier(new RangedCompactedObjectVerifier(
                        exactObjectStore,
                        rangedReader,
                        new ParquetKafkaTopicCompactedReader(exactObjectStore, this.workerExecutor))));
        ReadTargetDispatcher targetDispatcher = new ReadTargetDispatcher(exactReads.readers());
        ExactSourceRangeReaderFactory sourceReaders = streamId -> new DefaultExactSourceRangeReader(
                exactCluster,
                streamId,
                exactGenerations,
                exactIdentities,
                exactReadPins,
                targetDispatcher,
                exactConfig.sourceReadPageRecords(),
                Math.toIntExact(exactConfig.sourceReadPageBytes()),
                exactClock,
                this.callbackExecutor);
        MaterializationTaskStore tasks = new MaterializationTaskStore(exactCluster, exactGenerations, exactClock);
        DefaultMaterializationWorker worker = new DefaultMaterializationWorker(
                exactCluster,
                exactProcessRunId,
                tasks,
                exactGenerations,
                exactIdentities,
                exactProtections,
                sourceProtections,
                sourceReaders,
                new ParquetRangedCompactedObjectWriter(stagingFiles, this.workerExecutor),
                exactObjectStore,
                outputVerifier,
                exactConfig.sourceReadPageRecords(),
                Math.toIntExact(exactConfig.sourceReadPageBytes()),
                exactConfig.workerClaimDuration(),
                exactConfig.workerClaimRenewInterval(),
                exactConfig.retryMinBackoff(),
                exactConfig.maxTaskAttempts(),
                exactConfig.operationTimeout(),
                WRITER_BUILD,
                exactScheduler,
                this.callbackExecutor,
                exactClock);
        DefaultGenerationCommitter committer = new DefaultGenerationCommitter(
                exactCluster,
                exactL0,
                exactGenerations,
                exactPhysical,
                exactProtections,
                sourceProtections,
                exactActivation,
                outputVerifier,
                MaterializationStreamAuthorityMode.DIRECT_STREAM,
                exactConfig.operationTimeout(),
                exactScheduler,
                exactClock);
        DefaultMaterializationTaskDispatcher dispatcher = new DefaultMaterializationTaskDispatcher(
                worker,
                committer,
                this.workerExecutor,
                exactConfig.maxConcurrentWorkers(),
                exactConfig.maxConcurrentWorkersPerStream());
        DefaultMaterializationTaskProtectionReconciler taskProtections =
                new DefaultMaterializationTaskProtectionReconciler(
                        exactCluster,
                        tasks,
                        exactGenerations,
                        exactIdentities,
                        exactProtections,
                        sourceProtections,
                        exactConfig.operationTimeout(),
                        exactScheduler);
        MaterializationTaskRecovery recovery = new MaterializationTaskRecovery(
                tasks,
                taskProtections,
                new GenerationPublicationReconciler(committer),
                dispatcher,
                exactClock,
                exactConfig.maximumClockSkew(),
                exactConfig.retryMinBackoff());
        DefaultMaterializationPlanner planner = new DefaultMaterializationPlanner(
                exactCluster,
                exactL0,
                exactGenerations,
                exactConfig.plannerPageSize(),
                MaterializationStreamAuthorityMode.DIRECT_STREAM);
        RecoveryCheckpointPublisher noRecoveryCheckpoints = ignored -> CompletableFuture.completedFuture(
                RecoveryCheckpointRunResult.skipped(RecoveryCheckpointBuildStatus.NO_LIVE_TAIL));
        RegisteredMaterializationStreamScanner scanner = new RegisteredMaterializationStreamScanner(
                exactCluster,
                exactL0,
                exactGenerations,
                exactActivation,
                com.nereusstream.materialization.MaterializationSourceRepairer.noOp(),
                planner,
                tasks,
                recovery,
                new TaskRecoveryScanner(tasks, recovery, exactConfig.taskScanPageSize()),
                noRecoveryCheckpoints,
                new DefaultMaterializationCheckpointReconciler(
                        exactCluster,
                        exactL0,
                        exactGenerations,
                        exactConfig.plannerPageSize(),
                        exactConfig.operationTimeout(),
                        exactScheduler,
                        exactClock),
                new DefaultTerminalWorkflowMetadataRetirer(
                        exactCluster,
                        tasks,
                        exactGenerations,
                        exactPhysical,
                        sourceProtections,
                        exactConfig.metadataAuditGrace(),
                        exactConfig.taskScanPageSize(),
                        exactConfig.operationTimeout(),
                        exactScheduler,
                        exactClock),
                exactConfig.committedPolicy(),
                exactConfig.registryScanPageSize(),
                exactConfig.maxTasksPerPlan(),
                MaterializationStreamAuthorityMode.DIRECT_STREAM);
        this.service = new DefaultMaterializationService(
                scanner,
                dispatcher,
                exactConfig,
                exactScheduler,
                this.callbackExecutor,
                MaterializationMetricsObserver.noop());
        this.committedObjectAuthority = new CommittedObjectGenerationAuthority(
                exactCluster,
                exactGenerations,
                exactPhysical,
                new NormalPathCommittedObjectGenerationReadVerifier(
                        exactReads.resolver(),
                        exactReads.readers(),
                        exactConfig.sourceReadPageRecords(),
                        Math.toIntExact(exactConfig.sourceReadPageBytes()),
                        exactScheduler),
                exactConfig.plannerPageSize(),
                exactConfig.operationTimeout(),
                exactScheduler);
        this.lagSnapshotReader = new DefaultMaterializationLagSnapshotReader(
                exactCluster,
                exactL0,
                exactGenerations,
                exactConfig.committedPolicy(),
                exactConfig.plannerPageSize(),
                maxCommitChainScan,
                exactScheduler,
                exactClock);
        this.requiredObjectGeneration = new RequiredObjectGenerationCoordinator(
                exactCluster,
                exactL0,
                exactGenerations,
                exactActivation,
                planner,
                tasks,
                recovery,
                service,
                committedObjectAuthority,
                exactConfig.committedPolicy(),
                exactConfig.taskScanPageSize(),
                exactScheduler,
                MaterializationStreamAuthorityMode.DIRECT_STREAM);
    }

    public DefaultMaterializationLagSnapshotReader lagSnapshotReader() {
        requireOpen();
        return lagSnapshotReader;
    }

    public RequiredObjectGenerationCompletion requiredObjectGenerationCompletion() {
        requireOpen();
        return requiredObjectGeneration;
    }

    public CommittedGenerationRetirementAuthority committedGenerationRetirementAuthority() {
        requireOpen();
        return committedObjectAuthority;
    }

    public MaterializationStreamTrigger streamTrigger() {
        requireOpen();
        return streamId -> {
            Objects.requireNonNull(streamId, "streamId");
            return service.scanNow().thenApply(ignored -> null);
        };
    }

    @Override
    public CompletionStage<Void> start() {
        if (closeStarted.get()) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Kafka Object materialization runtime is closed"));
        }
        return service.start();
    }

    @Override
    public CompletionStage<Void> closeAsync() {
        CompletableFuture<Void> existing = closeFuture;
        if (existing != null) {
            return existing;
        }
        synchronized (this) {
            if (closeFuture != null) {
                return closeFuture;
            }
            closeStarted.set(true);
            closeFuture = service.closeAsync()
                    .handleAsync(
                            (ignored, serviceFailure) -> {
                                Throwable failure = serviceFailure == null ? null : unwrap(serviceFailure);
                                try {
                                    stagingFiles.close();
                                } catch (Throwable stagingFailure) {
                                    failure = merge(failure, stagingFailure);
                                }
                                workerExecutor.shutdown();
                                try {
                                    if (!workerExecutor.awaitTermination(
                                            closeTimeout.toNanos(), TimeUnit.NANOSECONDS)) {
                                        workerExecutor.shutdownNow();
                                        failure = merge(
                                                failure,
                                                new IllegalStateException(
                                                        "Kafka NCP2 worker executor close deadline " + "expired"));
                                    }
                                } catch (ArithmeticException | InterruptedException closeFailure) {
                                    if (closeFailure instanceof InterruptedException) {
                                        Thread.currentThread().interrupt();
                                    }
                                    workerExecutor.shutdownNow();
                                    failure = merge(failure, closeFailure);
                                }
                                if (failure != null) {
                                    throw new CompletionException(failure);
                                }
                                return null;
                            },
                            callbackExecutor);
            return closeFuture;
        }
    }

    @Override
    public void close() {
        closeAsync().toCompletableFuture().join();
    }

    private void requireOpen() {
        if (closeStarted.get()) {
            throw new IllegalStateException("Kafka Object materialization runtime is closed");
        }
    }

    private static Throwable merge(Throwable first, Throwable next) {
        if (first == null) {
            return next;
        }
        first.addSuppressed(next);
        return first;
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }
}
