/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import com.nereusstream.core.capability.GenerationProtocolActivationGuard;
import com.nereusstream.core.physical.GcGlobalReferenceScope;
import com.nereusstream.core.physical.GcReferenceDomain;
import com.nereusstream.managedledger.cursor.CursorStorageConfig;
import com.nereusstream.managedledger.generation.ManagedLedgerGenerationProjectionAuthorityReader;
import com.nereusstream.managedledger.retention.CursorSnapshotGcScanner;
import com.nereusstream.managedledger.retention.CursorSnapshotReferenceDomain;
import com.nereusstream.managedledger.retention.ManagedLedgerStreamRetirementAuthorityReader;
import com.nereusstream.managedledger.retention.ProjectionGenerationReferenceDomain;
import com.nereusstream.materialization.MaterializationConfig;
import com.nereusstream.materialization.gc.AppendRecoveryReferenceDomain;
import com.nereusstream.materialization.gc.DefaultPhysicalGcLifecycleService;
import com.nereusstream.materialization.gc.DefaultPhysicalRootTombstoneRetirementCoordinator;
import com.nereusstream.materialization.gc.DefaultGcRetirementJournal;
import com.nereusstream.materialization.gc.FutureCatalogSentinelDomain;
import com.nereusstream.materialization.gc.GcMetadataRetirementRegistry;
import com.nereusstream.materialization.gc.GcPlanMetadataRevalidator;
import com.nereusstream.materialization.gc.GcReferenceDomainRegistry;
import com.nereusstream.materialization.gc.GcReferenceDomainVersion;
import com.nereusstream.materialization.gc.GenerationReferenceDomain;
import com.nereusstream.materialization.gc.GenerationZeroCommitRetirementHandler;
import com.nereusstream.materialization.gc.GenerationZeroIndexRetirementHandler;
import com.nereusstream.materialization.gc.GenerationZeroMarkerRetirementHandler;
import com.nereusstream.materialization.gc.HigherGenerationIndexRetirementHandler;
import com.nereusstream.materialization.gc.MaterializationReferenceDomain;
import com.nereusstream.materialization.gc.ObjectInventoryScanner;
import com.nereusstream.materialization.gc.OwnerlessObjectGcExecutor;
import com.nereusstream.materialization.gc.PhysicalGcLifecyclePass;
import com.nereusstream.materialization.gc.PhysicalGcLifecycleService;
import com.nereusstream.materialization.gc.PhysicalGcConfig;
import com.nereusstream.materialization.gc.PhysicalObjectGarbageCollector;
import com.nereusstream.materialization.gc.PhysicalObjectRootScanner;
import com.nereusstream.materialization.gc.RegisteredStreamGcGlobalReferenceScope;
import com.nereusstream.materialization.gc.SecureGcIdGenerator;
import com.nereusstream.materialization.gc.SourceRetirementCoordinator;
import com.nereusstream.materialization.gc.StreamRegistrationRetirementCoordinator;
import com.nereusstream.materialization.gc.StreamRegistrationRetirementScanner;
import com.nereusstream.metadata.oxia.CursorMetadataStore;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationProtocolActivationStore;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionMetadataStore;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.PhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.retirement.ObjectAuditRetirementStore;
import com.nereusstream.metadata.oxia.retirement.SourceRetirementMetadataStore;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointCodecV1;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Product-owned F4 physical-GC composition and metadata-first lifecycle.
 *
 * <p>Ordinary stores, guards, codecs and executors are borrowed. The two read/delete retirement adapters are
 * transferred to this runtime and closed after its complete-pass loop drains. Enabled configuration starts one
 * non-overlapping root -> registration -> inventory fixed-delay loop; safe defaults remain disabled and dry-run.
 */
public final class Phase4PhysicalGcRuntime implements AutoCloseable {
    private final PhysicalGcConfig config;
    private final CursorSnapshotGcScanner cursorScanner;
    private final CursorSnapshotGcExecutor cursorExecutor;
    private final PhysicalObjectRootScanner rootScanner;
    private final StreamRegistrationRetirementScanner registrationScanner;
    private final ObjectInventoryScanner objectInventoryScanner;
    private final PhysicalGcLifecycleService lifecycleService;
    private final SourceRetirementMetadataStore sourceRetirementMetadata;
    private final ObjectAuditRetirementStore objectAuditRetirement;
    private final AtomicBoolean closed = new AtomicBoolean();

    public Phase4PhysicalGcRuntime(
            String cluster,
            PhysicalGcConfig config,
            MaterializationConfig materializationConfig,
            CursorStorageConfig cursorConfig,
            OxiaMetadataStore l0Metadata,
            GenerationMetadataStore generations,
            ManagedLedgerProjectionMetadataStore projections,
            CursorMetadataStore cursors,
            PhysicalObjectMetadataStore physicalMetadata,
            GenerationProtocolActivationStore activationStore,
            GenerationProtocolActivationGuard activationGuard,
            SourceRetirementMetadataStore sourceRetirementMetadata,
            ObjectAuditRetirementStore objectAuditRetirement,
            ObjectStore objectStore,
            RecoveryCheckpointCodecV1 checkpointCodec,
            ScheduledExecutorService scheduler,
            Executor callbackExecutor,
            Clock clock) {
        String exactCluster = requireText(cluster, "cluster");
        PhysicalGcConfig exactConfig = Objects.requireNonNull(config, "config");
        MaterializationConfig exactMaterializationConfig = Objects.requireNonNull(
                materializationConfig, "materializationConfig");
        CursorStorageConfig exactCursorConfig = Objects.requireNonNull(
                cursorConfig, "cursorConfig");
        OxiaMetadataStore exactL0 = Objects.requireNonNull(
                l0Metadata, "l0Metadata");
        GenerationMetadataStore exactGenerations = Objects.requireNonNull(
                generations, "generations");
        ManagedLedgerProjectionMetadataStore exactProjections = Objects.requireNonNull(
                projections, "projections");
        CursorMetadataStore exactCursors = Objects.requireNonNull(cursors, "cursors");
        PhysicalObjectMetadataStore exactPhysical = Objects.requireNonNull(
                physicalMetadata, "physicalMetadata");
        GenerationProtocolActivationStore exactActivationStore = Objects.requireNonNull(
                activationStore, "activationStore");
        GenerationProtocolActivationGuard exactActivationGuard = Objects.requireNonNull(
                activationGuard, "activationGuard");
        SourceRetirementMetadataStore exactSourceRetirement = Objects.requireNonNull(
                sourceRetirementMetadata, "sourceRetirementMetadata");
        ObjectAuditRetirementStore exactObjectAudit = Objects.requireNonNull(
                objectAuditRetirement, "objectAuditRetirement");
        ObjectStore exactObjectStore = Objects.requireNonNull(objectStore, "objectStore");
        RecoveryCheckpointCodecV1 exactCheckpointCodec = Objects.requireNonNull(
                checkpointCodec, "checkpointCodec");
        ScheduledExecutorService exactScheduler = Objects.requireNonNull(
                scheduler, "scheduler");
        Executor exactCallbackExecutor = Objects.requireNonNull(
                callbackExecutor, "callbackExecutor");
        Clock exactClock = Objects.requireNonNull(clock, "clock");
        if (exactCursorConfig.cursorRecordsPerStreamMax()
                > CursorSnapshotGcScanner.MAX_INVENTORY_VALUES) {
            throw new IllegalArgumentException(
                    "cursorRecordsPerStreamMax exceeds the complete cursor-GC inventory bound");
        }

        this.config = exactConfig;
        this.sourceRetirementMetadata = exactSourceRetirement;
        this.objectAuditRetirement = exactObjectAudit;

        List<GcReferenceDomainVersion> installedDomains =
                NereusGenerationProtocolReferenceDomains.currentV1().stream()
                        .map(value -> new GcReferenceDomainVersion(
                                value.domainId(), value.protocolVersion()))
                        .toList();
        GcGlobalReferenceScope globalScope = new RegisteredStreamGcGlobalReferenceScope(
                exactCluster,
                exactActivationStore,
                exactGenerations,
                installedDomains,
                exactConfig.referenceDomainConfig());
        List<GcReferenceDomain> domains = List.of(
                new AppendRecoveryReferenceDomain(
                        exactCluster,
                        exactL0,
                        exactGenerations,
                        exactConfig,
                        globalScope),
                new CursorSnapshotReferenceDomain(
                        exactCluster,
                        exactCursors,
                        exactConfig.referenceDomainConfig(),
                        globalScope),
                new FutureCatalogSentinelDomain(
                        exactCluster,
                        exactActivationStore,
                        exactConfig.referenceDomainConfig(),
                        installedDomains),
                new GenerationReferenceDomain(
                        exactCluster,
                        exactGenerations,
                        exactConfig,
                        globalScope),
                new MaterializationReferenceDomain(
                        exactCluster,
                        exactGenerations,
                        exactConfig,
                        globalScope),
                new ProjectionGenerationReferenceDomain(
                        exactCluster,
                        exactProjections,
                        exactConfig.referenceDomainConfig(),
                        globalScope));
        GcReferenceDomainRegistry registry = new GcReferenceDomainRegistry(
                exactConfig, exactScheduler, domains);
        if (!registry.requiredDomains().equals(installedDomains)) {
            throw new IllegalArgumentException(
                    "runtime GC domain set differs from the generation activation contract");
        }

        this.cursorScanner = new CursorSnapshotGcScanner(
                exactCluster,
                exactCursors,
                exactPhysical,
                exactObjectStore,
                new CursorSnapshotGcScanner.Configuration(
                        exactConfig.metadataScanPageSize(),
                        exactConfig.objectListPageSize(),
                        exactCursorConfig.cursorScanPageSize(),
                        exactCursorConfig.cursorRecordsPerStreamMax(),
                        Math.min(
                                CursorSnapshotGcScanner.MAX_INVENTORY_VALUES,
                                exactConfig.maxReferencesPerDomainSnapshot()),
                        Math.min(
                                CursorSnapshotGcScanner.MAX_INVENTORY_VALUES,
                                exactConfig.maxReferencesPerDomainSnapshot()),
                        exactConfig.orphanGrace(),
                        exactConfig.maximumClockSkew(),
                        exactConfig.operationTimeout()),
                exactClock,
                exactScheduler);
        DefaultGcRetirementJournal journal = new DefaultGcRetirementJournal(
                exactCluster, exactPhysical, exactConfig);
        GcPlanMetadataRevalidator metadataRevalidator = (candidate, expected) -> {
            var kind = candidate.referenceQuery().kind();
            if ((kind
                                    != com.nereusstream.core.physical.GcReferenceQueryKind
                                            .CURSOR_SNAPSHOT_CANDIDATE
                            && kind
                                    != com.nereusstream.core.physical.GcReferenceQueryKind
                                            .OWNERLESS_ORPHAN_CANDIDATE)
                    || !expected.isEmpty()) {
                return CompletableFuture.failedFuture(new IllegalArgumentException(
                        "runtime-managed cursor/ownerless GC accepts only empty metadata-removal plans"));
            }
            return CompletableFuture.completedFuture(List.of());
        };
        PhysicalObjectGarbageCollector collector = new PhysicalObjectGarbageCollector(
                exactCluster,
                exactConfig,
                exactPhysical,
                registry,
                exactActivationGuard,
                metadataRevalidator,
                journal,
                new SecureGcIdGenerator(),
                exactClock,
                exactScheduler);
        GcMetadataRetirementRegistry metadataRetirements =
                new GcMetadataRetirementRegistry(List.of(
                        new GenerationZeroIndexRetirementHandler(
                                exactCluster,
                                exactGenerations,
                                exactSourceRetirement),
                        new GenerationZeroCommitRetirementHandler(
                                exactCluster,
                                exactSourceRetirement),
                        new GenerationZeroMarkerRetirementHandler(
                                exactCluster,
                                exactSourceRetirement),
                        new HigherGenerationIndexRetirementHandler(
                                exactCluster,
                                exactGenerations)));
        SourceRetirementCoordinator retirement = new SourceRetirementCoordinator(
                exactCluster,
                exactConfig,
                exactPhysical,
                journal,
                metadataRetirements,
                exactObjectStore,
                exactClock,
                exactScheduler);
        this.objectInventoryScanner = new ObjectInventoryScanner(
                exactCluster,
                exactConfig,
                exactPhysical,
                exactObjectStore,
                Phase4ObjectInventoryFamilies.currentV1(exactCluster),
                exactClock,
                exactScheduler);
        this.cursorExecutor = new CursorSnapshotGcExecutor(
                exactConfig,
                cursorScanner,
                registry,
                collector,
                retirement,
                new SecureGcIdGenerator());
        OwnerlessObjectGcExecutor ownerlessExecutor = new OwnerlessObjectGcExecutor(
                exactCluster,
                exactConfig,
                exactPhysical,
                registry,
                collector,
                retirement,
                new SecureGcIdGenerator(),
                exactClock,
                exactScheduler);
        var tombstones = new DefaultPhysicalRootTombstoneRetirementCoordinator(
                exactCluster,
                exactConfig,
                exactPhysical,
                exactObjectAudit,
                registry,
                exactObjectStore,
                exactClock,
                exactScheduler);
        StreamRegistrationRetirementCoordinator registrationRetirement =
                new StreamRegistrationRetirementCoordinator(
                        exactCluster,
                        exactL0,
                        exactGenerations,
                        exactPhysical,
                        new ManagedLedgerGenerationProjectionAuthorityReader(
                                exactCluster, exactProjections),
                        new ManagedLedgerStreamRetirementAuthorityReader(
                                exactCluster,
                                exactCursors,
                                exactConfig.referenceDomainConfig()),
                        exactCheckpointCodec,
                        exactConfig,
                        exactMaterializationConfig.metadataAuditGrace(),
                        exactClock,
                        exactScheduler);
        this.rootScanner = new PhysicalObjectRootScanner(
                exactCluster, exactConfig, exactPhysical, exactScheduler);
        this.registrationScanner = new StreamRegistrationRetirementScanner(
                exactCluster,
                exactGenerations,
                registrationRetirement,
                exactConfig,
                exactScheduler);
        PhysicalGcLifecyclePass lifecyclePass = new PhysicalGcLifecyclePass(
                rootScanner,
                () -> new Phase4PhysicalRootLifecycleRouter(
                        exactCluster,
                        exactProjections,
                        cursorExecutor,
                        ownerlessExecutor,
                        retirement,
                        tombstones),
                registrationScanner,
                objectInventoryScanner);
        this.lifecycleService = new DefaultPhysicalGcLifecycleService(
                lifecyclePass,
                exactConfig,
                exactScheduler,
                exactCallbackExecutor);
    }

    /** Starts periodic work only when the typed physical-GC feature is enabled. */
    public void start() {
        if (closed.get()) {
            throw new IllegalStateException("Phase 4 physical GC runtime is closed");
        }
        if (config.enabled()) {
            lifecycleService.start().join();
        }
    }

    public CursorSnapshotGcExecutor cursorExecutor() {
        if (closed.get()) {
            throw new IllegalStateException("Phase 4 physical GC runtime is closed");
        }
        return cursorExecutor;
    }

    public ObjectInventoryScanner objectInventoryScanner() {
        if (closed.get()) {
            throw new IllegalStateException("Phase 4 physical GC runtime is closed");
        }
        return objectInventoryScanner;
    }

    public PhysicalGcLifecycleService lifecycleService() {
        if (closed.get()) {
            throw new IllegalStateException("Phase 4 physical GC runtime is closed");
        }
        return lifecycleService;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            ArrayList<Throwable> failures = new ArrayList<>();
            closeOne(lifecycleService, failures);
            closeOne(registrationScanner, failures);
            closeOne(rootScanner, failures);
            closeOne(objectInventoryScanner, failures);
            closeOne(cursorScanner, failures);
            closeOne(sourceRetirementMetadata, failures);
            closeOne(objectAuditRetirement, failures);
            if (!failures.isEmpty()) {
                RuntimeException aggregate = new RuntimeException(
                        "failed to close one or more Phase 4 physical-GC resources");
                failures.forEach(aggregate::addSuppressed);
                throw aggregate;
            }
        }
    }

    private static void closeOne(AutoCloseable resource, List<Throwable> failures) {
        try {
            resource.close();
        } catch (Throwable failure) {
            failures.add(failure);
        }
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }
}
