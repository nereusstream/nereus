/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import com.nereusstream.core.capability.GenerationProtocolActivationGuard;
import com.nereusstream.core.physical.GcGlobalReferenceScope;
import com.nereusstream.core.physical.GcReferenceDomain;
import com.nereusstream.managedledger.cursor.CursorStorageConfig;
import com.nereusstream.managedledger.retention.CursorSnapshotGcScanner;
import com.nereusstream.managedledger.retention.CursorSnapshotReferenceDomain;
import com.nereusstream.managedledger.retention.ProjectionGenerationReferenceDomain;
import com.nereusstream.materialization.gc.AppendRecoveryReferenceDomain;
import com.nereusstream.materialization.gc.DefaultGcRetirementJournal;
import com.nereusstream.materialization.gc.FutureCatalogSentinelDomain;
import com.nereusstream.materialization.gc.GcMetadataRetirementRegistry;
import com.nereusstream.materialization.gc.GcPlanMetadataRevalidator;
import com.nereusstream.materialization.gc.GcReferenceDomainRegistry;
import com.nereusstream.materialization.gc.GcReferenceDomainVersion;
import com.nereusstream.materialization.gc.GenerationReferenceDomain;
import com.nereusstream.materialization.gc.MaterializationReferenceDomain;
import com.nereusstream.materialization.gc.ObjectInventoryScanner;
import com.nereusstream.materialization.gc.PhysicalGcConfig;
import com.nereusstream.materialization.gc.PhysicalObjectGarbageCollector;
import com.nereusstream.materialization.gc.RegisteredStreamGcGlobalReferenceScope;
import com.nereusstream.materialization.gc.SecureGcIdGenerator;
import com.nereusstream.materialization.gc.SourceRetirementCoordinator;
import com.nereusstream.metadata.oxia.CursorMetadataStore;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationProtocolActivationStore;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionMetadataStore;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.PhysicalObjectMetadataStore;
import com.nereusstream.objectstore.ObjectStore;
import java.time.Clock;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Product-owned F4 physical-GC composition for cursor snapshot candidates.
 *
 * <p>All stores, guards, and the scheduler are borrowed. This checkpoint installs the exact six-domain collector,
 * journal, destructive recovery coordinator, and cursor execution bridge, but intentionally does not start a
 * periodic root/registration scan. The typed physical-GC config remains disabled and dry-run by default.
 */
public final class Phase4PhysicalGcRuntime implements AutoCloseable {
    private final CursorSnapshotGcScanner cursorScanner;
    private final CursorSnapshotGcExecutor cursorExecutor;
    private final ObjectInventoryScanner objectInventoryScanner;
    private final AtomicBoolean closed = new AtomicBoolean();

    public Phase4PhysicalGcRuntime(
            String cluster,
            PhysicalGcConfig config,
            CursorStorageConfig cursorConfig,
            OxiaMetadataStore l0Metadata,
            GenerationMetadataStore generations,
            ManagedLedgerProjectionMetadataStore projections,
            CursorMetadataStore cursors,
            PhysicalObjectMetadataStore physicalMetadata,
            GenerationProtocolActivationStore activationStore,
            GenerationProtocolActivationGuard activationGuard,
            ObjectStore objectStore,
            ScheduledExecutorService scheduler,
            Clock clock) {
        String exactCluster = requireText(cluster, "cluster");
        PhysicalGcConfig exactConfig = Objects.requireNonNull(config, "config");
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
        ObjectStore exactObjectStore = Objects.requireNonNull(objectStore, "objectStore");
        ScheduledExecutorService exactScheduler = Objects.requireNonNull(
                scheduler, "scheduler");
        Clock exactClock = Objects.requireNonNull(clock, "clock");
        if (exactCursorConfig.cursorRecordsPerStreamMax()
                > CursorSnapshotGcScanner.MAX_INVENTORY_VALUES) {
            throw new IllegalArgumentException(
                    "cursorRecordsPerStreamMax exceeds the complete cursor-GC inventory bound");
        }

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
        GcPlanMetadataRevalidator cursorMetadataRevalidator = (candidate, expected) -> {
            if (candidate.referenceQuery().kind()
                            != com.nereusstream.core.physical.GcReferenceQueryKind.CURSOR_SNAPSHOT_CANDIDATE
                    || !expected.isEmpty()) {
                return CompletableFuture.failedFuture(new IllegalArgumentException(
                        "cursor physical-GC runtime accepts only empty cursor metadata-removal plans"));
            }
            return CompletableFuture.completedFuture(List.of());
        };
        PhysicalObjectGarbageCollector collector = new PhysicalObjectGarbageCollector(
                exactCluster,
                exactConfig,
                exactPhysical,
                registry,
                exactActivationGuard,
                cursorMetadataRevalidator,
                journal,
                new SecureGcIdGenerator(),
                exactClock,
                exactScheduler);
        SourceRetirementCoordinator retirement = new SourceRetirementCoordinator(
                exactCluster,
                exactConfig,
                exactPhysical,
                journal,
                new GcMetadataRetirementRegistry(List.of()),
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

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            objectInventoryScanner.close();
            cursorScanner.close();
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
