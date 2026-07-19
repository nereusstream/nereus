/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import com.nereusstream.core.StreamStorageConfig;
import com.nereusstream.core.append.GenerationZeroPhysicalReferencePublisher;
import com.nereusstream.core.backpressure.MaterializationLagSnapshotReader;
import com.nereusstream.core.capability.GenerationProtocolActivationGuard;
import com.nereusstream.core.physical.ObjectProtectionManager;
import com.nereusstream.core.physical.ObjectReadPinManager;
import com.nereusstream.core.profile.Phase4StorageProfileResolver;
import com.nereusstream.core.profile.StorageProfileResolver;
import com.nereusstream.core.read.GenerationReadResolver;
import com.nereusstream.core.read.MetadataGenerationReadFailureHandler;
import com.nereusstream.core.read.MetadataPhysicalObjectIdentityResolver;
import com.nereusstream.core.read.ParquetCompactedTargetReader;
import com.nereusstream.core.read.Phase4ReadComponents;
import com.nereusstream.core.read.ReadAfterStableCommitRepair;
import com.nereusstream.core.read.ReadTargetDispatcher;
import com.nereusstream.core.read.ReadTargetReader;
import com.nereusstream.core.read.ReadTargetReaderRegistry;
import com.nereusstream.core.recovery.AnchorAwareCommitWalker;
import com.nereusstream.core.recovery.AppendRecoverySearcher;
import com.nereusstream.core.recovery.CheckpointAppendReplayReader;
import com.nereusstream.core.recovery.CheckpointDerivedIndexRepairer;
import com.nereusstream.core.recovery.GenerationZeroRepairScanner;
import com.nereusstream.materialization.CompactedMaterializationFormatVerifier;
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
import com.nereusstream.materialization.MaterializationStreamTrigger;
import com.nereusstream.materialization.MaterializationSourceRepairer;
import com.nereusstream.materialization.ObjectMaterializationSourceProtectionAdapter;
import com.nereusstream.materialization.MaterializationTaskRecovery;
import com.nereusstream.materialization.MaterializationTaskStore;
import com.nereusstream.materialization.RegisteredMaterializationStreamScanner;
import com.nereusstream.materialization.TaskRecoveryScanner;
import com.nereusstream.materialization.recovery.MetadataRecoveryCheckpointVerifier;
import com.nereusstream.materialization.recovery.RecoveryCheckpointBuilder;
import com.nereusstream.materialization.recovery.RecoveryCheckpointCoordinator;
import com.nereusstream.materialization.recovery.RecoveryCheckpointProtectionManager;
import com.nereusstream.metadata.oxia.GenerationIndexValidator;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.PhysicalObjectMetadataStore;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.checkpoint.DefaultRecoveryCheckpointCodecV1;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointCodecV1;
import com.nereusstream.objectstore.compacted.CompactedObjectVerifier;
import com.nereusstream.objectstore.compacted.ParquetCompactedObjectReader;
import com.nereusstream.objectstore.compacted.ParquetCompactedObjectWriter;
import com.nereusstream.objectstore.staging.StagingFileManager;
import com.nereusstream.objectstore.wal.WalObjectReader;
import com.nereusstream.core.wal.object.ObjectWalReaderAdapter;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Product-owned Phase 4 Object-WAL read/replay/materialization composition.
 *
 * <p>Injected stores remain provider-owned. The dedicated worker executor, staging files and background
 * materialization lifecycle are owned and closed here.
 */
public final class Phase4ObjectWalRuntime implements AutoCloseable {
    private static final String MATERIALIZATION_WRITER_BUILD =
            "nereus-pulsar-f4";

    private final StorageProfileResolver profileResolver =
            new Phase4StorageProfileResolver();
    private final Phase4ReadComponents readComponents;
    private final AppendRecoverySearcher appendRecoverySearcher;
    private final GenerationZeroRepairScanner generationZeroRepairScanner;
    private final MaterializationLagSnapshotReader lagSnapshotReader;
    private final RecoveryCheckpointCodecV1 checkpointCodec;
    private final MaterializationService materializationService;
    private final StagingFileManager stagingFiles;
    private final ExecutorService workerExecutor;
    private final Duration closeTimeout;
    private final AtomicBoolean closed = new AtomicBoolean();

    public Phase4ObjectWalRuntime(
            String cluster,
            String processRunId,
            StreamStorageConfig streamConfig,
            MaterializationConfig config,
            Duration recoveryCheckpointPendingProtectionDuration,
            OxiaMetadataStore l0Metadata,
            GenerationMetadataStore generations,
            PhysicalObjectMetadataStore physicalMetadata,
            ObjectStore objectStore,
            WalObjectReader walObjectReader,
            GenerationZeroPhysicalReferencePublisher physicalReferences,
            ObjectProtectionManager protections,
            ObjectReadPinManager readPins,
            GenerationProtocolActivationGuard activationGuard,
            ScheduledExecutorService scheduler,
            ExecutorService workerExecutor,
            Executor callbackExecutor,
            Clock clock) {
        this(
                cluster,
                processRunId,
                streamConfig,
                config,
                recoveryCheckpointPendingProtectionDuration,
                l0Metadata,
                generations,
                physicalMetadata,
                objectStore,
                walObjectReader,
                physicalReferences,
                protections,
                readPins,
                activationGuard,
                List.of(),
                scheduler,
                workerExecutor,
                callbackExecutor,
                clock);
    }

    public Phase4ObjectWalRuntime(
            String cluster,
            String processRunId,
            StreamStorageConfig streamConfig,
            MaterializationConfig config,
            Duration recoveryCheckpointPendingProtectionDuration,
            OxiaMetadataStore l0Metadata,
            GenerationMetadataStore generations,
            PhysicalObjectMetadataStore physicalMetadata,
            ObjectStore objectStore,
            WalObjectReader walObjectReader,
            GenerationZeroPhysicalReferencePublisher physicalReferences,
            ObjectProtectionManager protections,
            ObjectReadPinManager readPins,
            GenerationProtocolActivationGuard activationGuard,
            List<MaterializationSourceProvider> additionalPrimarySources,
            ScheduledExecutorService scheduler,
            ExecutorService workerExecutor,
            Executor callbackExecutor,
            Clock clock) {
        String exactCluster = requireText(cluster, "cluster");
        String exactProcessRunId = requireText(
                processRunId, "processRunId");
        StreamStorageConfig exactStreamConfig =
                Objects.requireNonNull(streamConfig, "streamConfig");
        MaterializationConfig exactConfig =
                Objects.requireNonNull(config, "config");
        Duration exactRecoveryCheckpointPendingProtectionDuration =
                Objects.requireNonNull(
                        recoveryCheckpointPendingProtectionDuration,
                        "recoveryCheckpointPendingProtectionDuration");
        OxiaMetadataStore exactL0 =
                Objects.requireNonNull(l0Metadata, "l0Metadata");
        GenerationMetadataStore exactGenerations =
                Objects.requireNonNull(generations, "generations");
        PhysicalObjectMetadataStore exactPhysical =
                Objects.requireNonNull(
                        physicalMetadata, "physicalMetadata");
        ObjectStore exactObjectStore =
                Objects.requireNonNull(objectStore, "objectStore");
        WalObjectReader exactWalReader =
                Objects.requireNonNull(
                        walObjectReader, "walObjectReader");
        GenerationZeroPhysicalReferencePublisher exactReferences =
                Objects.requireNonNull(
                        physicalReferences, "physicalReferences");
        ObjectProtectionManager exactProtections =
                Objects.requireNonNull(protections, "protections");
        ObjectReadPinManager exactReadPins =
                Objects.requireNonNull(readPins, "readPins");
        GenerationProtocolActivationGuard exactActivation =
                Objects.requireNonNull(
                        activationGuard, "activationGuard");
        List<MaterializationSourceProvider> exactAdditionalPrimarySources =
                List.copyOf(Objects.requireNonNull(
                        additionalPrimarySources, "additionalPrimarySources"));
        ScheduledExecutorService exactScheduler =
                Objects.requireNonNull(scheduler, "scheduler");
        ExecutorService exactWorkerExecutor =
                Objects.requireNonNull(
                        workerExecutor, "workerExecutor");
        Executor exactCallbackExecutor =
                Objects.requireNonNull(
                        callbackExecutor, "callbackExecutor");
        Clock exactClock = Objects.requireNonNull(clock, "clock");
        if (!exactStreamConfig.cluster().equals(exactCluster)) {
            throw new IllegalArgumentException(
                    "stream config cluster differs from the Phase 4 runtime cluster");
        }
        if (!exactStreamConfig.processRunId()
                .equals(exactProcessRunId)) {
            throw new IllegalArgumentException(
                    "stream config processRunId differs from the Phase 4 runtime identity");
        }

        this.stagingFiles = new StagingFileManager(
                exactConfig.stagingDirectory(),
                exactConfig.maxStagingBytes(),
                exactConfig.uploadChunkBytes(),
                exactConfig.metadataAuditGrace(),
                exactWorkerExecutor);
        this.workerExecutor = exactWorkerExecutor;
        this.closeTimeout = exactConfig.closeTimeout();
        ParquetCompactedObjectReader compactedReader =
                new ParquetCompactedObjectReader(
                        exactObjectStore, exactWorkerExecutor);
        List<ReadTargetReader> targetReaders = new ArrayList<>();
        targetReaders.add(new ObjectWalReaderAdapter(exactWalReader));
        targetReaders.add(new ParquetCompactedTargetReader(compactedReader));
        exactAdditionalPrimarySources.stream()
                .map(MaterializationSourceProvider::reader)
                .forEach(targetReaders::add);
        ReadTargetReaderRegistry readers =
                new ReadTargetReaderRegistry(targetReaders);
        ReadTargetDispatcher targetDispatcher =
                new ReadTargetDispatcher(readers);
        MetadataPhysicalObjectIdentityResolver identities =
                new MetadataPhysicalObjectIdentityResolver(
                        exactCluster, exactL0, exactPhysical);
        List<MaterializationSourceProtectionAdapter<?>> sourceProtectionAdapters =
                new ArrayList<>();
        sourceProtectionAdapters.add(
                new ObjectMaterializationSourceProtectionAdapter(
                        identities, exactProtections));
        exactAdditionalPrimarySources.stream()
                .map(MaterializationSourceProvider::protectionAdapter)
                .forEach(sourceProtectionAdapters::add);
        MaterializationSourceProtectionRegistry sourceProtections =
                new MaterializationSourceProtectionRegistry(
                        sourceProtectionAdapters);
        AnchorAwareCommitWalker walker =
                new AnchorAwareCommitWalker(
                        exactCluster, exactL0, exactGenerations);
        this.checkpointCodec =
                new DefaultRecoveryCheckpointCodecV1(
                        exactObjectStore,
                        stagingFiles,
                        exactWorkerExecutor,
                        new MetadataRecoveryCheckpointVerifier());
        RecoveryCheckpointCoordinator recoveryCheckpoints =
                new RecoveryCheckpointCoordinator(
                        exactCluster,
                        exactGenerations,
                        exactPhysical,
                        exactObjectStore,
                        this.checkpointCodec,
                        new RecoveryCheckpointBuilder(
                                exactCluster,
                                exactL0,
                                exactGenerations,
                                walker,
                                exactConfig,
                                exactClock),
                        new RecoveryCheckpointProtectionManager(
                                exactCluster,
                                exactGenerations,
                                exactPhysical,
                                exactProtections),
                        exactReadPins,
                        exactActivation,
                        exactConfig,
                        exactRecoveryCheckpointPendingProtectionDuration,
                        exactScheduler,
                        exactClock);
        this.appendRecoverySearcher =
                new CheckpointAppendReplayReader(
                        exactCluster,
                        exactGenerations,
                        walker,
                        this.checkpointCodec,
                        exactReadPins,
                        exactClock);
        this.generationZeroRepairScanner =
                new GenerationZeroRepairScanner(
                        exactCluster,
                        exactL0,
                        walker,
                        exactReferences,
                        exactStreamConfig.maxCommitChainScan(),
                        Math.min(
                                exactConfig.taskScanPageSize(),
                                exactStreamConfig.maxCommitChainScan()));
        CheckpointDerivedIndexRepairer indexRepairer =
                new CheckpointDerivedIndexRepairer(
                        exactCluster,
                        exactL0,
                        exactGenerations,
                        exactPhysical,
                        walker,
                        this.checkpointCodec,
                        exactReadPins,
                        exactProtections,
                        exactActivation,
                        new ReadAfterStableCommitRepair(
                                generationZeroRepairScanner),
                        exactStreamConfig
                                .maxDerivedIndexRepairCommitsPerCall(),
                        Math.min(
                                exactConfig.taskScanPageSize(),
                                exactStreamConfig
                                        .maxDerivedIndexRepairCommitsPerCall()),
                        exactClock);
        GenerationReadResolver generationResolver =
                new GenerationReadResolver(
                        exactCluster,
                        exactL0,
                        exactGenerations,
                        GenerationIndexValidator.phase15Targets(),
                        readers,
                        identities,
                        exactReadPins,
                        indexRepairer,
                        exactClock,
                        exactCallbackExecutor);
        this.readComponents = new Phase4ReadComponents(
                generationResolver,
                readers,
                new MetadataGenerationReadFailureHandler(
                        exactCluster,
                        exactGenerations,
                        exactPhysical,
                        exactClock));
        this.lagSnapshotReader =
                new DefaultMaterializationLagSnapshotReader(
                        exactCluster,
                        exactL0,
                        exactGenerations,
                        exactConfig.committedPolicy(),
                        exactConfig.plannerPageSize(),
                        exactStreamConfig.maxCommitChainScan(),
                        exactScheduler,
                        exactClock);

        MaterializationTaskStore tasks =
                new MaterializationTaskStore(
                        exactCluster,
                        exactGenerations,
                        exactClock);
        DefaultMaterializationOutputVerifier outputVerifier =
                new DefaultMaterializationOutputVerifier(
                        exactObjectStore,
                        new CompactedMaterializationFormatVerifier(
                                new CompactedObjectVerifier(
                                        exactObjectStore,
                                        compactedReader)));
        ExactSourceRangeReaderFactory sourceReaders =
                streamId -> new DefaultExactSourceRangeReader(
                        exactCluster,
                        streamId,
                        exactGenerations,
                        identities,
                        exactReadPins,
                        targetDispatcher,
                        exactConfig.sourceReadPageRecords(),
                        Math.toIntExact(
                                exactConfig.sourceReadPageBytes()),
                        exactClock,
                        exactCallbackExecutor);
        DefaultMaterializationWorker worker =
                new DefaultMaterializationWorker(
                        exactCluster,
                        exactProcessRunId,
                        tasks,
                        exactGenerations,
                        identities,
                        exactProtections,
                        sourceProtections,
                        sourceReaders,
                        new ParquetCompactedObjectWriter(
                                stagingFiles,
                                exactWorkerExecutor),
                        exactObjectStore,
                        outputVerifier,
                        exactConfig.sourceReadPageRecords(),
                        Math.toIntExact(
                                exactConfig.sourceReadPageBytes()),
                        exactConfig.workerClaimDuration(),
                        exactConfig.workerClaimRenewInterval(),
                        exactConfig.retryMinBackoff(),
                        exactConfig.maxTaskAttempts(),
                        exactConfig.operationTimeout(),
                        MATERIALIZATION_WRITER_BUILD,
                        exactScheduler,
                        exactCallbackExecutor,
                        exactClock);
        DefaultGenerationCommitter committer =
                new DefaultGenerationCommitter(
                        exactCluster,
                        exactL0,
                        exactGenerations,
                        exactPhysical,
                        exactProtections,
                        sourceProtections,
                        exactActivation,
                        outputVerifier,
                        exactConfig.operationTimeout(),
                        exactScheduler,
                        exactClock);
        DefaultMaterializationTaskDispatcher dispatcher =
                new DefaultMaterializationTaskDispatcher(
                        worker,
                        committer,
                        exactWorkerExecutor,
                        exactConfig.maxConcurrentWorkers(),
                        exactConfig
                                .maxConcurrentWorkersPerStream());
        DefaultMaterializationTaskProtectionReconciler
                taskProtections =
                        new DefaultMaterializationTaskProtectionReconciler(
                                exactCluster,
                                tasks,
                                exactGenerations,
                                identities,
                                exactProtections,
                                sourceProtections,
                                exactConfig.operationTimeout(),
                                exactScheduler);
        MaterializationTaskRecovery taskRecovery =
                new MaterializationTaskRecovery(
                        tasks,
                        taskProtections,
                        new GenerationPublicationReconciler(
                                committer),
                        dispatcher,
                        exactClock,
                        exactConfig.maximumClockSkew(),
                        exactConfig.retryMinBackoff());
        MaterializationSourceRepairer sourceRepairer =
                streamId -> generationZeroRepairScanner
                        .repairAll(
                                streamId,
                                exactConfig.operationTimeout())
                        .thenApply(ignored -> null);
        RegisteredMaterializationStreamScanner scanner =
                new RegisteredMaterializationStreamScanner(
                        exactCluster,
                        exactL0,
                        exactGenerations,
                        exactActivation,
                        sourceRepairer,
                        new DefaultMaterializationPlanner(
                                exactCluster,
                                exactL0,
                                exactGenerations,
                                exactConfig.plannerPageSize()),
                        tasks,
                        taskRecovery,
                        new TaskRecoveryScanner(
                                tasks,
                                taskRecovery,
                                exactConfig.taskScanPageSize()),
                        recoveryCheckpoints,
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
                        exactConfig.maxTasksPerPlan());
        this.materializationService =
                new DefaultMaterializationService(
                        scanner,
                        dispatcher,
                        exactConfig,
                        exactScheduler,
                        exactCallbackExecutor,
                        MaterializationMetricsObserver.noop());
    }

    public StorageProfileResolver profileResolver() {
        return profileResolver;
    }

    public Phase4ReadComponents readComponents() {
        return readComponents;
    }

    public AppendRecoverySearcher appendRecoverySearcher() {
        return appendRecoverySearcher;
    }

    public GenerationZeroRepairScanner
            generationZeroRepairScanner() {
        return generationZeroRepairScanner;
    }

    public MaterializationLagSnapshotReader
            lagSnapshotReader() {
        return lagSnapshotReader;
    }

    /** Borrowed by the physical-GC registration-retirement runtime and closed with this runtime's staging owner. */
    public RecoveryCheckpointCodecV1 checkpointCodec() {
        if (closed.get()) {
            throw new IllegalStateException("Phase 4 Object-WAL runtime is closed");
        }
        return checkpointCodec;
    }

    public MaterializationService materializationService() {
        return materializationService;
    }

    /** Sealed-primary-WAL hint routed through the one registered-stream scanner, never a second planner. */
    public MaterializationStreamTrigger materializationStreamTrigger() {
        if (closed.get()) {
            throw new IllegalStateException("Phase 4 materialization runtime is closed");
        }
        return streamId -> {
            Objects.requireNonNull(streamId, "streamId");
            return materializationService.scanNow().thenApply(ignored -> null);
        };
    }

    public void start() {
        materializationService.start().join();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        Throwable failure = null;
        try {
            materializationService.close();
        } catch (Throwable error) {
            failure = error;
        }
        try {
            stagingFiles.close();
        } catch (Throwable error) {
            if (failure == null) {
                failure = error;
            } else {
                failure.addSuppressed(error);
            }
        }
        workerExecutor.shutdown();
        try {
            if (!workerExecutor.awaitTermination(
                    closeTimeout.toNanos(),
                    TimeUnit.NANOSECONDS)) {
                workerExecutor.shutdownNow();
                if (failure == null) {
                    failure = new IllegalStateException(
                            "Phase 4 worker executor close deadline expired");
                } else {
                    failure.addSuppressed(new IllegalStateException(
                            "Phase 4 worker executor close deadline expired"));
                }
            }
        } catch (ArithmeticException error) {
            workerExecutor.shutdownNow();
            if (failure == null) {
                failure = error;
            } else {
                failure.addSuppressed(error);
            }
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            workerExecutor.shutdownNow();
            if (failure == null) {
                failure = error;
            } else {
                failure.addSuppressed(error);
            }
        }
        if (failure instanceof RuntimeException runtime) {
            throw runtime;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        if (failure != null) {
            throw new IllegalStateException(
                    "failed to close Phase 4 Object-WAL runtime",
                    failure);
        }
    }

    private static String requireText(
            String value,
            String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(
                    field + " cannot be blank");
        }
        return value;
    }
}
