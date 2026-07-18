/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger;

import com.nereusstream.api.StreamStorage;
import com.nereusstream.core.capability.GenerationProtocolActivationGuard;
import com.nereusstream.core.physical.ObjectReadPinManager;
import com.nereusstream.managedledger.cursor.CursorProtocolActivationGuard;
import com.nereusstream.managedledger.cursor.CursorRetentionCoordinator;
import com.nereusstream.managedledger.cursor.CursorSnapshotStore;
import com.nereusstream.managedledger.cursor.CursorStorage;
import com.nereusstream.managedledger.cursor.CursorStorageConfig;
import com.nereusstream.managedledger.generation.ManagedLedgerGenerationProtocolActivationCoordinator;
import com.nereusstream.managedledger.generation.ManagedLedgerGenerationRegistrationBackfillProofCoordinator;
import com.nereusstream.managedledger.generation.ManagedLedgerMaterializationRegistrationCoordinator;
import com.nereusstream.managedledger.generation.ManagedLedgerPhysicalDeletionActivationCoordinator;
import com.nereusstream.managedledger.retention.NereusRetentionRuntime;
import com.nereusstream.metadata.oxia.CursorMetadataStore;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationProtocolActivationStore;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionMetadataStore;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.SharedOxiaClientRuntime;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.ObjectStoreProvider;
import java.time.Duration;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

/** Shared F2/F3 owner closed only by the hybrid factory/provider lifecycle. */
public final class NereusManagedLedgerRuntime implements AutoCloseable {
    private static final Pattern PROCESS_RUN_ID = Pattern.compile("[A-Za-z0-9_-]{22,256}");

    private final StreamStorage streamStorage;
    private final ManagedLedgerProjectionMetadataStore projectionStore;
    private final GenerationMetadataStore generationMetadataStore;
    private final ManagedLedgerMaterializationRegistrationCoordinator
            materializationRegistrationCoordinator;
    private final CursorMetadataStore cursorMetadataStore;
    private final CursorSnapshotStore cursorSnapshotStore;
    private final CursorRetentionCoordinator cursorRetentionCoordinator;
    private final CursorStorage cursorStorage;
    private final CursorStorageConfig cursorStorageConfig;
    private final CursorProtocolActivationGuard cursorProtocolActivationGuard;
    private GenerationProtocolActivationStore generationProtocolActivationStore;
    private ManagedLedgerGenerationRegistrationBackfillProofCoordinator
            generationRegistrationBackfillProofCoordinator;
    private ManagedLedgerGenerationProtocolActivationCoordinator
            generationProtocolActivationCoordinator;
    private GenerationProtocolActivationGuard generationProtocolActivationGuard;
    private ManagedLedgerPhysicalDeletionActivationCoordinator
            physicalDeletionActivationCoordinator;
    private AutoCloseable materializationRuntime;
    private NereusRetentionRuntime retentionRuntime;
    private AutoCloseable physicalGcRuntime;
    private final ObjectReadPinManager objectReadPinManager;
    private final AutoCloseable objectProtectionManager;
    private final AutoCloseable physicalMetadataStore;
    private final OxiaMetadataStore l0MetadataStore;
    private final SharedOxiaClientRuntime sharedOxiaRuntime;
    private final ObjectStore objectStore;
    private final ObjectStoreProvider objectStoreProvider;
    private final ScheduledExecutorService scheduler;
    private final ExecutorService callbackExecutor;
    private final NereusManagedLedgerFactoryConfig config;
    private final String cluster;
    private final String processRunId;
    private final String writerId;
    private final Semaphore callbackPermits;
    private final AtomicBoolean closed = new AtomicBoolean();

    public NereusManagedLedgerRuntime(
            StreamStorage streamStorage,
            ManagedLedgerProjectionMetadataStore projectionStore,
            GenerationMetadataStore generationMetadataStore,
            ManagedLedgerMaterializationRegistrationCoordinator
                    materializationRegistrationCoordinator,
            CursorMetadataStore cursorMetadataStore,
            CursorSnapshotStore cursorSnapshotStore,
            CursorRetentionCoordinator cursorRetentionCoordinator,
            CursorStorage cursorStorage,
            CursorStorageConfig cursorStorageConfig,
            CursorProtocolActivationGuard cursorProtocolActivationGuard,
            GenerationProtocolActivationStore generationProtocolActivationStore,
            ManagedLedgerGenerationRegistrationBackfillProofCoordinator
                    generationRegistrationBackfillProofCoordinator,
            ManagedLedgerGenerationProtocolActivationCoordinator
                    generationProtocolActivationCoordinator,
            GenerationProtocolActivationGuard generationProtocolActivationGuard,
            AutoCloseable materializationRuntime,
            NereusRetentionRuntime retentionRuntime,
            AutoCloseable physicalGcRuntime,
            ObjectReadPinManager objectReadPinManager,
            AutoCloseable objectProtectionManager,
            AutoCloseable physicalMetadataStore,
            OxiaMetadataStore l0MetadataStore,
            SharedOxiaClientRuntime sharedOxiaRuntime,
            ObjectStore objectStore,
            ObjectStoreProvider objectStoreProvider,
            ScheduledExecutorService scheduler,
            ExecutorService callbackExecutor,
            NereusManagedLedgerFactoryConfig config,
            String cluster,
            String processRunId,
            String writerId) {
        this(
                streamStorage,
                projectionStore,
                generationMetadataStore,
                materializationRegistrationCoordinator,
                cursorMetadataStore,
                cursorSnapshotStore,
                cursorRetentionCoordinator,
                cursorStorage,
                cursorStorageConfig,
                cursorProtocolActivationGuard,
                generationProtocolActivationStore,
                generationRegistrationBackfillProofCoordinator,
                generationProtocolActivationCoordinator,
                generationProtocolActivationGuard,
                materializationRuntime,
                retentionRuntime,
                physicalGcRuntime,
                null,
                objectReadPinManager,
                objectProtectionManager,
                physicalMetadataStore,
                l0MetadataStore,
                sharedOxiaRuntime,
                objectStore,
                objectStoreProvider,
                scheduler,
                callbackExecutor,
                config,
                cluster,
                processRunId,
                writerId);
    }

    public NereusManagedLedgerRuntime(
            StreamStorage streamStorage,
            ManagedLedgerProjectionMetadataStore projectionStore,
            GenerationMetadataStore generationMetadataStore,
            ManagedLedgerMaterializationRegistrationCoordinator
                    materializationRegistrationCoordinator,
            CursorMetadataStore cursorMetadataStore,
            CursorSnapshotStore cursorSnapshotStore,
            CursorRetentionCoordinator cursorRetentionCoordinator,
            CursorStorage cursorStorage,
            CursorStorageConfig cursorStorageConfig,
            CursorProtocolActivationGuard cursorProtocolActivationGuard,
            OxiaMetadataStore l0MetadataStore,
            SharedOxiaClientRuntime sharedOxiaRuntime,
            ObjectStore objectStore,
            ObjectStoreProvider objectStoreProvider,
            ScheduledExecutorService scheduler,
            ExecutorService callbackExecutor,
            NereusManagedLedgerFactoryConfig config,
            String cluster,
            String processRunId,
            String writerId) {
        this(
                streamStorage,
                projectionStore,
                generationMetadataStore,
                materializationRegistrationCoordinator,
                cursorMetadataStore,
                cursorSnapshotStore,
                cursorRetentionCoordinator,
                cursorStorage,
                cursorStorageConfig,
                cursorProtocolActivationGuard,
                null,
                null,
                null,
                l0MetadataStore,
                sharedOxiaRuntime,
                objectStore,
                objectStoreProvider,
                scheduler,
                callbackExecutor,
                config,
                cluster,
                processRunId,
                writerId);
    }

    public NereusManagedLedgerRuntime(
            StreamStorage streamStorage,
            ManagedLedgerProjectionMetadataStore projectionStore,
            GenerationMetadataStore generationMetadataStore,
            ManagedLedgerMaterializationRegistrationCoordinator
                    materializationRegistrationCoordinator,
            CursorMetadataStore cursorMetadataStore,
            CursorSnapshotStore cursorSnapshotStore,
            CursorRetentionCoordinator cursorRetentionCoordinator,
            CursorStorage cursorStorage,
            CursorStorageConfig cursorStorageConfig,
            CursorProtocolActivationGuard cursorProtocolActivationGuard,
            ObjectReadPinManager objectReadPinManager,
            AutoCloseable objectProtectionManager,
            AutoCloseable physicalMetadataStore,
            OxiaMetadataStore l0MetadataStore,
            SharedOxiaClientRuntime sharedOxiaRuntime,
            ObjectStore objectStore,
            ObjectStoreProvider objectStoreProvider,
            ScheduledExecutorService scheduler,
            ExecutorService callbackExecutor,
            NereusManagedLedgerFactoryConfig config,
            String cluster,
            String processRunId,
            String writerId) {
        this.streamStorage = Objects.requireNonNull(streamStorage, "streamStorage");
        this.projectionStore = Objects.requireNonNull(projectionStore, "projectionStore");
        this.generationMetadataStore = Objects.requireNonNull(
                generationMetadataStore, "generationMetadataStore");
        this.materializationRegistrationCoordinator =
                Objects.requireNonNull(
                        materializationRegistrationCoordinator,
                        "materializationRegistrationCoordinator");
        this.cursorMetadataStore = Objects.requireNonNull(cursorMetadataStore, "cursorMetadataStore");
        this.cursorSnapshotStore = Objects.requireNonNull(cursorSnapshotStore, "cursorSnapshotStore");
        this.cursorRetentionCoordinator = Objects.requireNonNull(
                cursorRetentionCoordinator, "cursorRetentionCoordinator");
        this.cursorStorage = Objects.requireNonNull(cursorStorage, "cursorStorage");
        this.cursorStorageConfig = Objects.requireNonNull(cursorStorageConfig, "cursorStorageConfig");
        this.cursorProtocolActivationGuard = Objects.requireNonNull(
                cursorProtocolActivationGuard, "cursorProtocolActivationGuard");
        boolean hasReadPins = objectReadPinManager != null;
        boolean hasProtections = objectProtectionManager != null;
        boolean hasPhysicalStore = physicalMetadataStore != null;
        if (hasReadPins != hasProtections || hasReadPins != hasPhysicalStore) {
            throw new IllegalArgumentException(
                    "objectReadPinManager, objectProtectionManager, and physicalMetadataStore must be supplied together");
        }
        this.objectReadPinManager = objectReadPinManager;
        this.objectProtectionManager = objectProtectionManager;
        this.physicalMetadataStore = physicalMetadataStore;
        this.l0MetadataStore = Objects.requireNonNull(l0MetadataStore, "l0MetadataStore");
        this.sharedOxiaRuntime = Objects.requireNonNull(sharedOxiaRuntime, "sharedOxiaRuntime");
        this.objectStore = Objects.requireNonNull(objectStore, "objectStore");
        this.objectStoreProvider = Objects.requireNonNull(objectStoreProvider, "objectStoreProvider");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.callbackExecutor = Objects.requireNonNull(callbackExecutor, "callbackExecutor");
        this.config = Objects.requireNonNull(config, "config");
        this.cluster = requireCluster(cluster);
        this.processRunId = requireProcessRunId(processRunId);
        this.writerId = Objects.requireNonNull(writerId, "writerId");
        if (!writerId.equals("pulsar-f2/" + processRunId)) {
            throw new IllegalArgumentException("writerId must equal pulsar-f2/{processRunId}");
        }
        List<Object> ownedResources = new ArrayList<>(List.of(
                streamStorage,
                projectionStore,
                generationMetadataStore,
                cursorMetadataStore,
                cursorSnapshotStore,
                cursorRetentionCoordinator,
                cursorStorage,
                l0MetadataStore,
                sharedOxiaRuntime,
                objectStore,
                objectStoreProvider,
                scheduler,
                callbackExecutor));
        if (objectProtectionManager != null) {
            ownedResources.add(objectProtectionManager);
        }
        if (objectReadPinManager != null) {
            ownedResources.add(objectReadPinManager);
        }
        if (physicalMetadataStore != null) {
            ownedResources.add(physicalMetadataStore);
        }
        requireIdentityDistinct(ownedResources);
        this.callbackPermits = new Semaphore(config.maxPendingCallbacks());
    }

    public NereusManagedLedgerRuntime(
            StreamStorage streamStorage,
            ManagedLedgerProjectionMetadataStore projectionStore,
            GenerationMetadataStore generationMetadataStore,
            ManagedLedgerMaterializationRegistrationCoordinator
                    materializationRegistrationCoordinator,
            CursorMetadataStore cursorMetadataStore,
            CursorSnapshotStore cursorSnapshotStore,
            CursorRetentionCoordinator cursorRetentionCoordinator,
            CursorStorage cursorStorage,
            CursorStorageConfig cursorStorageConfig,
            CursorProtocolActivationGuard cursorProtocolActivationGuard,
            GenerationProtocolActivationStore generationProtocolActivationStore,
            ManagedLedgerGenerationRegistrationBackfillProofCoordinator
                    generationRegistrationBackfillProofCoordinator,
            ManagedLedgerGenerationProtocolActivationCoordinator
                    generationProtocolActivationCoordinator,
            GenerationProtocolActivationGuard generationProtocolActivationGuard,
            ObjectReadPinManager objectReadPinManager,
            AutoCloseable objectProtectionManager,
            AutoCloseable physicalMetadataStore,
            OxiaMetadataStore l0MetadataStore,
            SharedOxiaClientRuntime sharedOxiaRuntime,
            ObjectStore objectStore,
            ObjectStoreProvider objectStoreProvider,
            ScheduledExecutorService scheduler,
            ExecutorService callbackExecutor,
            NereusManagedLedgerFactoryConfig config,
            String cluster,
            String processRunId,
            String writerId) {
        this(
                streamStorage,
                projectionStore,
                generationMetadataStore,
                materializationRegistrationCoordinator,
                cursorMetadataStore,
                cursorSnapshotStore,
                cursorRetentionCoordinator,
                cursorStorage,
                cursorStorageConfig,
                cursorProtocolActivationGuard,
                generationProtocolActivationStore,
                generationRegistrationBackfillProofCoordinator,
                generationProtocolActivationCoordinator,
                generationProtocolActivationGuard,
                null,
                null,
                objectReadPinManager,
                objectProtectionManager,
                physicalMetadataStore,
                l0MetadataStore,
                sharedOxiaRuntime,
                objectStore,
                objectStoreProvider,
                scheduler,
                callbackExecutor,
                config,
                cluster,
                processRunId,
                writerId);
    }

    public NereusManagedLedgerRuntime(
            StreamStorage streamStorage,
            ManagedLedgerProjectionMetadataStore projectionStore,
            GenerationMetadataStore generationMetadataStore,
            ManagedLedgerMaterializationRegistrationCoordinator
                    materializationRegistrationCoordinator,
            CursorMetadataStore cursorMetadataStore,
            CursorSnapshotStore cursorSnapshotStore,
            CursorRetentionCoordinator cursorRetentionCoordinator,
            CursorStorage cursorStorage,
            CursorStorageConfig cursorStorageConfig,
            CursorProtocolActivationGuard cursorProtocolActivationGuard,
            GenerationProtocolActivationStore generationProtocolActivationStore,
            ManagedLedgerGenerationRegistrationBackfillProofCoordinator
                    generationRegistrationBackfillProofCoordinator,
            ManagedLedgerGenerationProtocolActivationCoordinator
                    generationProtocolActivationCoordinator,
            GenerationProtocolActivationGuard generationProtocolActivationGuard,
            AutoCloseable materializationRuntime,
            NereusRetentionRuntime retentionRuntime,
            ObjectReadPinManager objectReadPinManager,
            AutoCloseable objectProtectionManager,
            AutoCloseable physicalMetadataStore,
            OxiaMetadataStore l0MetadataStore,
            SharedOxiaClientRuntime sharedOxiaRuntime,
            ObjectStore objectStore,
            ObjectStoreProvider objectStoreProvider,
            ScheduledExecutorService scheduler,
            ExecutorService callbackExecutor,
            NereusManagedLedgerFactoryConfig config,
            String cluster,
            String processRunId,
            String writerId) {
        this(
                streamStorage,
                projectionStore,
                generationMetadataStore,
                materializationRegistrationCoordinator,
                cursorMetadataStore,
                cursorSnapshotStore,
                cursorRetentionCoordinator,
                cursorStorage,
                cursorStorageConfig,
                cursorProtocolActivationGuard,
                generationProtocolActivationStore,
                generationRegistrationBackfillProofCoordinator,
                generationProtocolActivationCoordinator,
                generationProtocolActivationGuard,
                materializationRuntime,
                retentionRuntime,
                null,
                objectReadPinManager,
                objectProtectionManager,
                physicalMetadataStore,
                l0MetadataStore,
                sharedOxiaRuntime,
                objectStore,
                objectStoreProvider,
                scheduler,
                callbackExecutor,
                config,
                cluster,
                processRunId,
                writerId);
    }

    public NereusManagedLedgerRuntime(
            StreamStorage streamStorage,
            ManagedLedgerProjectionMetadataStore projectionStore,
            GenerationMetadataStore generationMetadataStore,
            ManagedLedgerMaterializationRegistrationCoordinator
                    materializationRegistrationCoordinator,
            CursorMetadataStore cursorMetadataStore,
            CursorSnapshotStore cursorSnapshotStore,
            CursorRetentionCoordinator cursorRetentionCoordinator,
            CursorStorage cursorStorage,
            CursorStorageConfig cursorStorageConfig,
            CursorProtocolActivationGuard cursorProtocolActivationGuard,
            GenerationProtocolActivationStore generationProtocolActivationStore,
            ManagedLedgerGenerationRegistrationBackfillProofCoordinator
                    generationRegistrationBackfillProofCoordinator,
            ManagedLedgerGenerationProtocolActivationCoordinator
                    generationProtocolActivationCoordinator,
            GenerationProtocolActivationGuard generationProtocolActivationGuard,
            AutoCloseable materializationRuntime,
            NereusRetentionRuntime retentionRuntime,
            AutoCloseable physicalGcRuntime,
            ManagedLedgerPhysicalDeletionActivationCoordinator
                    physicalDeletionActivationCoordinator,
            ObjectReadPinManager objectReadPinManager,
            AutoCloseable objectProtectionManager,
            AutoCloseable physicalMetadataStore,
            OxiaMetadataStore l0MetadataStore,
            SharedOxiaClientRuntime sharedOxiaRuntime,
            ObjectStore objectStore,
            ObjectStoreProvider objectStoreProvider,
            ScheduledExecutorService scheduler,
            ExecutorService callbackExecutor,
            NereusManagedLedgerFactoryConfig config,
            String cluster,
            String processRunId,
            String writerId) {
        this(
                streamStorage,
                projectionStore,
                generationMetadataStore,
                materializationRegistrationCoordinator,
                cursorMetadataStore,
                cursorSnapshotStore,
                cursorRetentionCoordinator,
                cursorStorage,
                cursorStorageConfig,
                cursorProtocolActivationGuard,
                objectReadPinManager,
                objectProtectionManager,
                physicalMetadataStore,
                l0MetadataStore,
                sharedOxiaRuntime,
                objectStore,
                objectStoreProvider,
                scheduler,
                callbackExecutor,
                config,
                cluster,
                processRunId,
                writerId);
        this.generationProtocolActivationStore = Objects.requireNonNull(
                generationProtocolActivationStore,
                "generationProtocolActivationStore");
        this.generationRegistrationBackfillProofCoordinator =
                Objects.requireNonNull(
                        generationRegistrationBackfillProofCoordinator,
                        "generationRegistrationBackfillProofCoordinator");
        this.generationProtocolActivationCoordinator =
                Objects.requireNonNull(
                        generationProtocolActivationCoordinator,
                        "generationProtocolActivationCoordinator");
        this.generationProtocolActivationGuard =
                Objects.requireNonNull(
                        generationProtocolActivationGuard,
                        "generationProtocolActivationGuard");
        this.materializationRuntime = materializationRuntime;
        this.retentionRuntime = retentionRuntime;
        this.physicalGcRuntime = physicalGcRuntime;
        this.physicalDeletionActivationCoordinator =
                physicalDeletionActivationCoordinator;
        requireDistinctOptionalResource(
                "materialization runtime",
                materializationRuntime,
                streamStorage,
                projectionStore,
                generationMetadataStore,
                cursorMetadataStore,
                cursorSnapshotStore,
                cursorRetentionCoordinator,
                cursorStorage,
                objectReadPinManager,
                objectProtectionManager,
                physicalMetadataStore,
                l0MetadataStore,
                sharedOxiaRuntime,
                objectStore,
                objectStoreProvider,
                scheduler,
                callbackExecutor);
        requireDistinctOptionalResource(
                "physical GC runtime",
                physicalGcRuntime,
                streamStorage,
                projectionStore,
                generationMetadataStore,
                cursorMetadataStore,
                cursorSnapshotStore,
                cursorRetentionCoordinator,
                cursorStorage,
                materializationRuntime,
                retentionRuntime,
                objectReadPinManager,
                objectProtectionManager,
                physicalMetadataStore,
                l0MetadataStore,
                sharedOxiaRuntime,
                objectStore,
                objectStoreProvider,
                scheduler,
                callbackExecutor);
        requireDistinctOptionalResource(
                "retention runtime",
                retentionRuntime,
                streamStorage,
                projectionStore,
                generationMetadataStore,
                cursorMetadataStore,
                cursorSnapshotStore,
                cursorRetentionCoordinator,
                cursorStorage,
                materializationRuntime,
                objectReadPinManager,
                objectProtectionManager,
                physicalMetadataStore,
                l0MetadataStore,
                sharedOxiaRuntime,
                objectStore,
                objectStoreProvider,
                scheduler,
                callbackExecutor);
    }

    public StreamStorage streamStorage() {
        return streamStorage;
    }

    public ManagedLedgerProjectionMetadataStore projectionStore() {
        return projectionStore;
    }

    public ManagedLedgerMaterializationRegistrationCoordinator
            materializationRegistrationCoordinator() {
        return materializationRegistrationCoordinator;
    }

    public CursorStorage cursorStorage() {
        return cursorStorage;
    }

    public CursorRetentionCoordinator cursorRetentionCoordinator() {
        return cursorRetentionCoordinator;
    }

    public CursorStorageConfig cursorStorageConfig() {
        return cursorStorageConfig;
    }

    public CursorProtocolActivationGuard cursorProtocolActivationGuard() {
        return cursorProtocolActivationGuard;
    }

    public ManagedLedgerGenerationRegistrationBackfillProofCoordinator
            generationRegistrationBackfillProofCoordinator() {
        if (generationRegistrationBackfillProofCoordinator == null) {
            throw new IllegalStateException(
                    "this runtime was assembled without the F4 registration backfill proof coordinator");
        }
        return generationRegistrationBackfillProofCoordinator;
    }

    public GenerationProtocolActivationGuard
            generationProtocolActivationGuard() {
        if (generationProtocolActivationGuard == null) {
            throw new IllegalStateException(
                    "this runtime was assembled without the F4 generation activation guard");
        }
        return generationProtocolActivationGuard;
    }

    public ManagedLedgerGenerationProtocolActivationCoordinator
            generationProtocolActivationCoordinator() {
        if (generationProtocolActivationCoordinator == null) {
            throw new IllegalStateException(
                    "this runtime was assembled without the F4 generation activation coordinator");
        }
        return generationProtocolActivationCoordinator;
    }

    public ManagedLedgerPhysicalDeletionActivationCoordinator
            physicalDeletionActivationCoordinator() {
        if (physicalDeletionActivationCoordinator == null) {
            throw new IllegalStateException(
                    "this runtime was assembled without the F4 physical deletion activation coordinator");
        }
        return physicalDeletionActivationCoordinator;
    }

    public AutoCloseable materializationRuntime() {
        if (materializationRuntime == null) {
            throw new IllegalStateException(
                    "this runtime was assembled without the Phase 4 materialization runtime");
        }
        return materializationRuntime;
    }

    public NereusRetentionRuntime retentionRuntime() {
        if (retentionRuntime == null) {
            throw new IllegalStateException(
                    "this runtime was assembled without the Phase 4 retention runtime");
        }
        return retentionRuntime;
    }

    public boolean hasRetentionRuntime() {
        return retentionRuntime != null;
    }

    public AutoCloseable physicalGcRuntime() {
        if (physicalGcRuntime == null) {
            throw new IllegalStateException(
                    "this runtime was assembled without the Phase 4 physical GC runtime");
        }
        return physicalGcRuntime;
    }

    public boolean hasPhysicalGcRuntime() {
        return physicalGcRuntime != null;
    }

    public ObjectReadPinManager objectReadPinManager() {
        if (objectReadPinManager == null) {
            throw new IllegalStateException(
                    "this runtime was assembled without F4 object read pinning");
        }
        return objectReadPinManager;
    }

    public ScheduledExecutorService scheduler() {
        return scheduler;
    }

    public Executor callbackExecutor() {
        return callbackExecutor;
    }

    public NereusManagedLedgerFactoryConfig config() {
        return config;
    }

    public String cluster() {
        return cluster;
    }

    public String processRunId() {
        return processRunId;
    }

    public String writerId() {
        return writerId;
    }

    boolean tryAcquireCallbackPermit() {
        return !closed.get() && callbackPermits.tryAcquire();
    }

    void releaseCallbackPermit() {
        callbackPermits.release();
    }

    boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        long executorDeadlineNanos = deadlineNanos(config.closeTimeout());
        List<Throwable> failures = new ArrayList<>();
        closeOneIfPresent(physicalGcRuntime, failures);
        closeOneIfPresent(retentionRuntime, failures);
        closeOne(cursorStorage, failures);
        closeOne(cursorRetentionCoordinator, failures);
        closeOne(cursorSnapshotStore, failures);
        closeOne(cursorMetadataStore, failures);
        closeOneIfPresent(materializationRuntime, failures);
        closeOneIfPresent(generationProtocolActivationStore, failures);
        closeOne(generationMetadataStore, failures);
        closeOne(projectionStore, failures);
        closeOne(streamStorage, failures);
        closeOneIfPresent(objectReadPinManager, failures);
        closeOneIfPresent(objectProtectionManager, failures);
        closeOneIfPresent(physicalMetadataStore, failures);
        closeOne(l0MetadataStore, failures);
        closeOne(objectStore, failures);
        closeOne(objectStoreProvider, failures);
        closeOne(sharedOxiaRuntime, failures);
        shutdown(callbackExecutor, executorDeadlineNanos, failures);
        shutdown(scheduler, executorDeadlineNanos, failures);
        if (!failures.isEmpty()) {
            RuntimeException aggregate = new RuntimeException("failed to close one or more Nereus runtime resources");
            failures.forEach(aggregate::addSuppressed);
            throw aggregate;
        }
    }

    private static String requireProcessRunId(String value) {
        Objects.requireNonNull(value, "processRunId");
        if (!PROCESS_RUN_ID.matcher(value).matches()) {
            throw new IllegalArgumentException("processRunId must be a URL-safe 128-bit-or-stronger identifier");
        }
        return value;
    }

    private static String requireCluster(String value) {
        Objects.requireNonNull(value, "cluster");
        if (value.isBlank() || value.indexOf('\0') >= 0) {
            throw new IllegalArgumentException("cluster cannot be blank or contain NUL");
        }
        return value;
    }

    private static void requireIdentityDistinct(List<Object> resources) {
        Map<Object, Boolean> identities = new IdentityHashMap<>();
        for (Object resource : resources) {
            if (identities.put(resource, Boolean.TRUE) != null) {
                throw new IllegalArgumentException("runtime-owned resource identities must be distinct");
            }
        }
    }

    private static void requireDistinctOptionalResource(
            String field,
            Object candidate,
            Object... existing) {
        if (candidate == null) {
            return;
        }
        for (Object resource : existing) {
            if (candidate == resource) {
                throw new IllegalArgumentException(
                        field + " must have a distinct owned identity");
            }
        }
    }

    private static void closeOne(AutoCloseable resource, List<Throwable> failures) {
        try {
            resource.close();
        } catch (Throwable error) {
            failures.add(error);
        }
    }

    private static void closeOneIfPresent(
            AutoCloseable resource,
            List<Throwable> failures) {
        if (resource != null) {
            closeOne(resource, failures);
        }
    }

    private static void shutdown(
            ExecutorService executor,
            long deadlineNanos,
            List<Throwable> failures) {
        executor.shutdown();
        try {
            long remaining = deadlineNanos == Long.MAX_VALUE
                    ? Long.MAX_VALUE
                    : Math.max(0, deadlineNanos - System.nanoTime());
            if (remaining == 0 || !executor.awaitTermination(remaining, TimeUnit.NANOSECONDS)) {
                executor.shutdownNow();
                failures.add(new IllegalStateException("Nereus runtime executor close deadline expired"));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
            failures.add(e);
        }
    }

    private static long deadlineNanos(Duration timeout) {
        long timeoutNanos;
        try {
            timeoutNanos = timeout.toNanos();
        } catch (ArithmeticException ignored) {
            return Long.MAX_VALUE;
        }
        try {
            return Math.addExact(System.nanoTime(), timeoutNanos);
        } catch (ArithmeticException ignored) {
            return Long.MAX_VALUE;
        }
    }
}
