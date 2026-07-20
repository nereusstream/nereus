/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamStorage;
import com.nereusstream.api.keys.DeterministicIds;
import com.nereusstream.bookkeeper.BookKeeperPrimaryPhysicalReferenceAdapter;
import com.nereusstream.core.DefaultStreamStorage;
import com.nereusstream.core.StreamStorageConfig;
import com.nereusstream.core.append.AppendCoordinator;
import com.nereusstream.core.append.DefaultGenerationZeroPhysicalReferencePublisher;
import com.nereusstream.core.append.GenerationZeroPhysicalReferencePublisher;
import com.nereusstream.core.backpressure.MaterializationLagGate;
import com.nereusstream.core.backpressure.MaterializationLagThresholds;
import com.nereusstream.core.capability.GenerationProtocolActivationGuard;
import com.nereusstream.core.physical.DefaultObjectProtectionManager;
import com.nereusstream.core.physical.DefaultObjectReadPinManager;
import com.nereusstream.core.physical.ObjectProtectionManager;
import com.nereusstream.core.physical.ObjectReadPinManager;
import com.nereusstream.core.read.ReadMetricsObserver;
import com.nereusstream.core.profile.StorageProfileResolver;
import com.nereusstream.core.profile.StorageProfileResolverRegistry;
import com.nereusstream.core.trim.TrimMetricsObserver;
import com.nereusstream.core.wal.PrimaryWalRegistry;
import com.nereusstream.managedledger.NereusManagedLedgerRuntime;
import com.nereusstream.managedledger.cursor.CursorProtocolActivationGuard;
import com.nereusstream.managedledger.cursor.CursorRetentionCoordinator;
import com.nereusstream.managedledger.cursor.CursorSnapshotStore;
import com.nereusstream.managedledger.cursor.CursorStateMachine;
import com.nereusstream.managedledger.cursor.CursorStatePersistencePlanner;
import com.nereusstream.managedledger.cursor.CursorStorage;
import com.nereusstream.managedledger.cursor.CursorStorageConfig;
import com.nereusstream.managedledger.cursor.DefaultCursorRetentionCoordinator;
import com.nereusstream.managedledger.cursor.DefaultCursorSnapshotStore;
import com.nereusstream.managedledger.cursor.DefaultCursorStorage;
import com.nereusstream.managedledger.generation.DefaultManagedLedgerGenerationProtocolActivationCoordinator;
import com.nereusstream.managedledger.generation.DefaultManagedLedgerGenerationRegistrationBackfillProofCoordinator;
import com.nereusstream.managedledger.generation.DefaultManagedLedgerMaterializationRegistrationCoordinator;
import com.nereusstream.managedledger.generation.ManagedLedgerGenerationProtocolActivationCoordinator;
import com.nereusstream.managedledger.generation.ManagedLedgerGenerationProtocolActivationGuard;
import com.nereusstream.managedledger.generation.ManagedLedgerGenerationRegistrationBackfillProofCoordinator;
import com.nereusstream.managedledger.generation.ManagedLedgerAsyncAppendAdmissionGuard;
import com.nereusstream.managedledger.generation.ManagedLedgerMaterializationRegistrationCoordinator;
import com.nereusstream.managedledger.retention.NereusRetentionRuntime;
import com.nereusstream.materialization.MaterializationSchedulers;
import com.nereusstream.metadata.oxia.CursorMetadataStore;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationProtocolActivationStore;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionMetadataStore;
import com.nereusstream.metadata.oxia.OxiaJavaClientMetadataStore;
import com.nereusstream.metadata.oxia.OxiaJavaGenerationMetadataStore;
import com.nereusstream.metadata.oxia.OxiaJavaPhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.PhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.SharedOxiaClientRuntime;
import com.nereusstream.metadata.oxia.retirement.ObjectAuditRetirementStore;
import com.nereusstream.metadata.oxia.retirement.OxiaJavaObjectAuditRetirementStore;
import com.nereusstream.metadata.oxia.retirement.OxiaJavaSourceRetirementMetadataStore;
import com.nereusstream.metadata.oxia.retirement.SourceRetirementMetadataStore;
import com.nereusstream.objectstore.DefaultObjectStoreDeleteCapabilityProbe;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.ObjectStoreDeleteCapabilityProbe;
import com.nereusstream.objectstore.ObjectStoreProvider;
import com.nereusstream.objectstore.wal.DefaultWalObjectReader;
import com.nereusstream.objectstore.wal.DefaultWalObjectWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/** Production Object-WAL/Oxia runtime assembly used by the hybrid broker storage provider. */
public final class DefaultNereusRuntimeProvider implements NereusRuntimeProvider {
    private static final String WRITER_VERSION = "nereus-pulsar-f2";

    @Override
    public NereusManagedLedgerRuntime create(
            NereusRuntimeConfiguration configuration,
            NereusRuntimeContext context) throws Exception {
        Objects.requireNonNull(configuration, "configuration");
        Objects.requireNonNull(context, "context");
        StreamStorageConfig streamConfig = configuration.streamStorage();
        var physicalGcConfig = configuration.physicalGc();
        requireIdentity(streamConfig);
        Optional<NereusBookKeeperRuntimeConfiguration> bookKeeperConfiguration =
                configuration.bookKeeper();
        if (bookKeeperConfiguration.isPresent()) {
            if (context.borrowedBookKeeperClient().isEmpty()) {
                throw new IllegalArgumentException(
                        "BookKeeper primary-WAL configuration requires the borrowed broker BookKeeper client");
            }
        }

        ObjectStoreProvider objectStoreProvider = null;
        ObjectStore objectStore = null;
        ObjectStoreDeleteCapabilityProbe objectStoreDeleteCapabilityProbe = null;
        SharedOxiaClientRuntime sharedOxiaRuntime = null;
        OxiaMetadataStore l0MetadataStore = null;
        PhysicalObjectMetadataStore physicalMetadataStore = null;
        SourceRetirementMetadataStore sourceRetirementMetadataStore = null;
        ObjectAuditRetirementStore objectAuditRetirementStore = null;
        ObjectProtectionManager objectProtectionManager = null;
        ObjectReadPinManager objectReadPinManager = null;
        ManagedLedgerProjectionMetadataStore projectionStore = null;
        GenerationMetadataStore generationMetadataStore = null;
        GenerationProtocolActivationStore generationProtocolActivationStore =
                null;
        ManagedLedgerGenerationRegistrationBackfillProofCoordinator
                generationRegistrationBackfillProofCoordinator = null;
        ManagedLedgerGenerationProtocolActivationCoordinator
                generationProtocolActivationCoordinator = null;
        GenerationProtocolActivationGuard generationProtocolActivationGuard =
                null;
        ManagedLedgerMaterializationRegistrationCoordinator
                materializationRegistrationCoordinator = null;
        CursorMetadataStore cursorMetadataStore = null;
        ScheduledExecutorService scheduler = null;
        ExecutorService callbackExecutor = null;
        ExecutorService workerExecutor = null;
        Phase4ObjectWalRuntime phase4Runtime = null;
        ProductionBookKeeperPrimaryWalRuntime bookKeeperRuntime = null;
        CompositeOwnedRuntime ownedWalAndMaterializationRuntime = null;
        StreamStorage streamStorage = null;
        CursorSnapshotStore cursorSnapshotStore = null;
        CursorRetentionCoordinator cursorRetentionCoordinator = null;
        CursorStorage cursorStorage = null;
        NereusRetentionRuntime retentionRuntime = null;
        Phase4PhysicalGcRuntime physicalGcRuntime = null;
        try {
            objectStoreProvider = instantiateObjectStoreProvider(
                    configuration.objectStore().providerClassName(), context.pluginClassLoader());
            objectStore = objectStoreProvider.create(configuration.objectStore(), context.secretResolver());
            Clock clock = Clock.systemUTC();
            objectStoreDeleteCapabilityProbe =
                    new DefaultObjectStoreDeleteCapabilityProbe(
                            objectStore, configuration.objectStore(), clock);
            sharedOxiaRuntime = SharedOxiaClientRuntime.connect(configuration.oxia(), clock);
            l0MetadataStore = bookKeeperConfiguration.isPresent()
                    ? OxiaJavaClientMetadataStore.usingSharedRuntime(
                            configuration.oxia(),
                            sharedOxiaRuntime,
                            clock,
                            bookKeeperConfiguration.orElseThrow().metadataStore())
                    : OxiaJavaClientMetadataStore.usingSharedRuntime(
                            configuration.oxia(), sharedOxiaRuntime, clock);
            physicalMetadataStore = OxiaJavaPhysicalObjectMetadataStore.usingSharedRuntime(
                    configuration.oxia(), sharedOxiaRuntime, clock);
            sourceRetirementMetadataStore =
                    OxiaJavaSourceRetirementMetadataStore.usingSharedRuntime(
                            configuration.oxia(), sharedOxiaRuntime);
            objectAuditRetirementStore =
                    OxiaJavaObjectAuditRetirementStore.usingSharedRuntime(
                            configuration.oxia(), sharedOxiaRuntime);
            objectProtectionManager = new DefaultObjectProtectionManager(
                    streamConfig.cluster(),
                    physicalMetadataStore,
                    physicalGcConfig.pendingProtectionDuration(),
                    physicalGcConfig.maximumClockSkew(),
                    physicalGcConfig.orphanGrace(),
                    clock);
            objectReadPinManager = new DefaultObjectReadPinManager(
                    streamConfig.cluster(),
                    DeterministicIds.stableHashComponent(
                            "f4-reader/" + streamConfig.processRunId()),
                    physicalMetadataStore,
                    physicalGcConfig.readerLeaseDuration(),
                    physicalGcConfig.maximumClockSkew(),
                    physicalGcConfig.orphanGrace(),
                    clock);
            projectionStore = ManagedLedgerProjectionMetadataStore.usingSharedRuntime(
                    configuration.oxia(), sharedOxiaRuntime, configuration.projectionMetadata(), clock);
            generationMetadataStore =
                    OxiaJavaGenerationMetadataStore.usingSharedRuntime(
                            configuration.oxia(),
                            sharedOxiaRuntime,
                            clock);
            generationProtocolActivationStore =
                    GenerationProtocolActivationStore.usingSharedRuntime(
                            configuration.oxia(),
                            sharedOxiaRuntime,
                            clock,
                            DeterministicIds.stableHashComponent(
                                    "generation-activation/"
                                            + streamConfig.processRunId()),
                            NereusGenerationProtocolReferenceDomains
                                    .currentV1());
            Phase4GcReferenceDomainAssembly gcReferenceDomains =
                    Phase4GcReferenceDomainAssembly.create(
                            streamConfig.cluster(),
                            physicalGcConfig,
                            generationProtocolActivationStore,
                            generationMetadataStore,
                            projectionStore);
            generationProtocolActivationCoordinator =
                    new DefaultManagedLedgerGenerationProtocolActivationCoordinator(
                            streamConfig.cluster(),
                            context.generationProtocolActivationEnabled(),
                            context.generationCapabilityReadinessProvider(),
                            generationProtocolActivationStore,
                            NereusGenerationProtocolReferenceDomains
                                    .currentV1(),
                            clock);
            generationProtocolActivationGuard =
                    new ManagedLedgerGenerationProtocolActivationGuard(
                            streamConfig.cluster(),
                            context.generationProtocolActivationEnabled(),
                            context.generationCapabilityReadinessProvider(),
                            generationProtocolActivationStore,
                            NereusGenerationProtocolReferenceDomains
                                    .currentV1(),
                            objectStoreDeleteCapabilityProbe
                                    .expectedCapabilitySha256(),
                            projectionStore,
                            l0MetadataStore,
                            generationMetadataStore,
                            gcReferenceDomains.projectionDomain(),
                            clock);
            materializationRegistrationCoordinator =
                    new DefaultManagedLedgerMaterializationRegistrationCoordinator(
                            streamConfig.cluster(),
                            projectionStore,
                            l0MetadataStore,
                            generationMetadataStore,
                            clock);
            cursorMetadataStore = CursorMetadataStore.usingSharedRuntime(
                    configuration.oxia(), sharedOxiaRuntime, configuration.cursorMetadata());
            scheduler = MaterializationSchedulers.newSingleThreadScheduler(
                    daemonFactory("nereus-f2-scheduler"));
            callbackExecutor = Executors.newFixedThreadPool(
                    Math.min(Runtime.getRuntime().availableProcessors(), 8),
                    daemonFactory("nereus-f2-callback"));
            workerExecutor = Executors.newFixedThreadPool(
                    Math.addExact(
                            configuration.materialization()
                                    .maxConcurrentWorkers(),
                            2),
                    daemonFactory("nereus-f4-worker"));
            DefaultWalObjectReader walObjectReader =
                    new DefaultWalObjectReader(objectStore);
            if (bookKeeperConfiguration.isPresent()) {
                bookKeeperRuntime = ProductionBookKeeperPrimaryWalRuntime.create(
                        bookKeeperConfiguration.orElseThrow(),
                        streamConfig.cluster(),
                        streamConfig.processRunId(),
                        configuration.oxia(),
                        sharedOxiaRuntime,
                        context.borrowedBookKeeperClient().orElseThrow(),
                        new OxiaBookKeeperLedgerIdNamespaceReservationStore(
                                configuration.oxia(),
                                sharedOxiaRuntime),
                        context.secretResolver(),
                        clock);
            }
            List<BookKeeperPrimaryPhysicalReferenceAdapter> additionalPhysicalReferences =
                    bookKeeperRuntime == null
                            ? List.of()
                            : List.of(bookKeeperRuntime.physicalReferences());
            GenerationZeroPhysicalReferencePublisher physicalReferences =
                    new DefaultGenerationZeroPhysicalReferencePublisher(
                            streamConfig.cluster(),
                            l0MetadataStore,
                            physicalMetadataStore,
                            objectProtectionManager,
                            additionalPhysicalReferences);
            var additionalMaterializationSources = bookKeeperRuntime == null
                    ? List.<com.nereusstream.materialization.MaterializationSourceProvider>of()
                    : List.of(bookKeeperRuntime.materializationSourceProvider());
            phase4Runtime = new Phase4ObjectWalRuntime(
                    streamConfig.cluster(),
                    streamConfig.processRunId(),
                    streamConfig,
                    configuration.materialization(),
                    configuration.physicalGc().pendingProtectionDuration(),
                    l0MetadataStore,
                    generationMetadataStore,
                    physicalMetadataStore,
                    objectStore,
                    walObjectReader,
                    physicalReferences,
                    objectProtectionManager,
                    objectReadPinManager,
                    generationProtocolActivationGuard,
                    additionalMaterializationSources,
                    scheduler,
                    workerExecutor,
                    callbackExecutor,
                    clock);
            var lagConfig = configuration.materialization();
            MaterializationLagGate lagGate =
                    new MaterializationLagGate(
                            phase4Runtime.lagSnapshotReader(),
                            new MaterializationLagThresholds(
                                    lagConfig.lagThrottleRecords(),
                                    lagConfig.lagRejectRecords(),
                                    lagConfig.lagThrottleBytes(),
                                    lagConfig.lagRejectBytes(),
                                    lagConfig.lagRejectAge(),
                                    lagConfig.lagThrottleDelay()),
                            scheduler);
            ManagedLedgerAsyncAppendAdmissionGuard appendAdmissionGuard =
                    new ManagedLedgerAsyncAppendAdmissionGuard(
                            streamConfig.cluster(),
                            projectionStore,
                            generationProtocolActivationGuard,
                            lagGate);
            PrimaryWalRegistry objectWalRegistry = AppendCoordinator.productionObjectWalRegistry(
                    streamConfig,
                    l0MetadataStore,
                    new DefaultWalObjectWriter(objectStore, WRITER_VERSION, clock),
                    walObjectReader,
                    physicalReferences,
                    clock);
            List<PrimaryWalRegistry> primaryWalRegistries = new ArrayList<>();
            primaryWalRegistries.add(objectWalRegistry);
            if (bookKeeperRuntime != null) {
                primaryWalRegistries.add(bookKeeperRuntime.walRuntime().primaryWalRegistry());
            }
            PrimaryWalRegistry primaryWalRegistry = PrimaryWalRegistry.combine(primaryWalRegistries);
            EnumMap<StorageProfile, StorageProfileResolver> profileResolvers =
                    new EnumMap<>(StorageProfile.class);
            profileResolvers.put(StorageProfile.OBJECT_WAL_SYNC_OBJECT, phase4Runtime.profileResolver());
            profileResolvers.put(StorageProfile.OBJECT_WAL_ASYNC_OBJECT, phase4Runtime.profileResolver());
            if (bookKeeperRuntime != null) {
                StorageProfileResolver bookKeeperProfiles = bookKeeperRuntime.walRuntime().profileResolver();
                profileResolvers.put(StorageProfile.BOOKKEEPER_WAL_ONLY, bookKeeperProfiles);
                profileResolvers.put(StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT, bookKeeperProfiles);
                profileResolvers.put(StorageProfile.BOOKKEEPER_WAL_SYNC_OBJECT, bookKeeperProfiles);
            }
            streamStorage = new DefaultStreamStorage(
                    streamConfig,
                    l0MetadataStore,
                    primaryWalRegistry,
                    physicalReferences,
                    phase4Runtime.appendRecoverySearcher(),
                    new StorageProfileResolverRegistry(profileResolvers),
                    appendAdmissionGuard,
                    phase4Runtime.requiredObjectGenerationCompletion(),
                    phase4Runtime.readComponents(),
                    clock,
                    callbackExecutor,
                    ReadMetricsObserver.noop(),
                    TrimMetricsObserver.noop());
            CursorStorageConfig cursorConfig = configuration.cursorStorage();
            CursorProtocolActivationGuard activationGuard = cursorProtocolActivationGuard(context);
            cursorSnapshotStore = new DefaultCursorSnapshotStore(
                    streamConfig.cluster(),
                    objectStore,
                    cursorMetadataStore,
                    physicalMetadataStore,
                    objectProtectionManager,
                    objectReadPinManager,
                    cursorConfig,
                    configuration.objectStore().requestTimeout(),
                    physicalGcConfig.pendingProtectionDuration(),
                    clock);
            CursorStateMachine stateMachine = new CursorStateMachine(cursorConfig);
            CursorStatePersistencePlanner persistencePlanner = new CursorStatePersistencePlanner(
                    streamConfig.cluster(), cursorConfig);
            cursorRetentionCoordinator = new DefaultCursorRetentionCoordinator(
                    streamConfig.cluster(),
                    streamStorage,
                    projectionStore,
                    cursorMetadataStore,
                    cursorSnapshotStore,
                    activationGuard,
                    stateMachine,
                    cursorConfig,
                    clock,
                    scheduler);
            cursorStorage = new DefaultCursorStorage(
                    streamConfig.cluster(),
                    streamStorage,
                    projectionStore,
                    cursorMetadataStore,
                    cursorSnapshotStore,
                    cursorRetentionCoordinator,
                    activationGuard,
                    stateMachine,
                    persistencePlanner,
                    cursorConfig,
                    clock,
                    scheduler);
            retentionRuntime = new NereusRetentionRuntime(
                    streamConfig.cluster(),
                    l0MetadataStore,
                    generationMetadataStore,
                    cursorStorage,
                    cursorRetentionCoordinator,
                    generationProtocolActivationGuard,
                    configuration.retention(),
                    clock,
                    daemonFactory("nereus-f4-retention"));
            physicalGcRuntime = new Phase4PhysicalGcRuntime(
                    streamConfig.cluster(),
                    configuration.physicalGc(),
                    configuration.materialization(),
                    cursorConfig,
                    l0MetadataStore,
                    generationMetadataStore,
                    projectionStore,
                    cursorMetadataStore,
                    physicalMetadataStore,
                    generationProtocolActivationStore,
                    gcReferenceDomains,
                    generationProtocolActivationGuard,
                    context.generationCapabilityReadinessProvider(),
                    objectProtectionManager,
                    sourceRetirementMetadataStore,
                    objectAuditRetirementStore,
                    objectStore,
                    objectStoreDeleteCapabilityProbe,
                    configuration.objectStore().requestTimeout(),
                    phase4Runtime.checkpointCodec(),
                    scheduler,
                    callbackExecutor,
                    clock);
            generationRegistrationBackfillProofCoordinator =
                    new DefaultManagedLedgerGenerationRegistrationBackfillProofCoordinator(
                            streamConfig.cluster(),
                            generationProtocolActivationStore,
                            context.generationCapabilityReadinessProvider(),
                            NereusGenerationProtocolReferenceDomains
                                    .currentV1(),
                            physicalGcRuntime,
                            clock);
            if (bookKeeperRuntime != null) {
                context.bookKeeperPrimaryWalCapabilitySink()
                        .install(bookKeeperRuntime.capabilityBinding());
            }
            phase4Runtime.start();
            physicalGcRuntime.start();
            ownedWalAndMaterializationRuntime = new CompositeOwnedRuntime(
                    phase4Runtime,
                    bookKeeperRuntime);
            return new NereusManagedLedgerRuntime(
                    streamStorage,
                    projectionStore,
                    generationMetadataStore,
                    materializationRegistrationCoordinator,
                    cursorMetadataStore,
                    cursorSnapshotStore,
                    cursorRetentionCoordinator,
                    cursorStorage,
                    cursorConfig,
                    activationGuard,
                    generationProtocolActivationStore,
                    generationRegistrationBackfillProofCoordinator,
                    generationProtocolActivationCoordinator,
                    generationProtocolActivationGuard,
                    ownedWalAndMaterializationRuntime,
                    retentionRuntime,
                    physicalGcRuntime,
                    physicalGcRuntime,
                    objectReadPinManager,
                    objectProtectionManager,
                    physicalMetadataStore,
                    l0MetadataStore,
                    sharedOxiaRuntime,
                    objectStore,
                    objectStoreProvider,
                    scheduler,
                    callbackExecutor,
                    configuration.managedLedger(),
                    streamConfig.cluster(),
                    streamConfig.processRunId(),
                    streamConfig.writerId());
        } catch (Throwable failure) {
            closeAfterFailure(
                    failure,
                    physicalGcRuntime,
                    retentionRuntime,
                    cursorStorage,
                    cursorRetentionCoordinator,
                    cursorSnapshotStore,
                    cursorMetadataStore,
                    ownedWalAndMaterializationRuntime,
                    phase4Runtime,
                    bookKeeperRuntime,
                    generationProtocolActivationStore,
                    generationMetadataStore,
                    projectionStore,
                    streamStorage,
                    objectReadPinManager,
                    objectProtectionManager,
                    sourceRetirementMetadataStore,
                    objectAuditRetirementStore,
                    physicalMetadataStore,
                    l0MetadataStore,
                    objectStore,
                    objectStoreProvider,
                    sharedOxiaRuntime);
            shutdown(callbackExecutor);
            shutdown(scheduler);
            shutdown(workerExecutor);
            if (failure instanceof Exception exception) {
                throw exception;
            }
            if (failure instanceof Error error) {
                throw error;
            }
            throw new IllegalStateException("unexpected runtime bootstrap failure", failure);
        }
    }

    static ObjectStoreProvider instantiateObjectStoreProvider(String className, ClassLoader classLoader)
            throws ReflectiveOperationException {
        Class<?> providerClass = Class.forName(
                Objects.requireNonNull(className, "className"), true,
                Objects.requireNonNull(classLoader, "classLoader"));
        if (!ObjectStoreProvider.class.isAssignableFrom(providerClass)) {
            throw new IllegalArgumentException(
                    "configured object-store provider does not implement ObjectStoreProvider");
        }
        Constructor<?> constructor = providerClass.getConstructor();
        try {
            return (ObjectStoreProvider) constructor.newInstance();
        } catch (InvocationTargetException exception) {
            Throwable cause = exception.getCause();
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw exception;
        }
    }

    static CursorProtocolActivationGuard cursorProtocolActivationGuard(
            NereusRuntimeContext context) {
        return Objects.requireNonNull(context, "context").cursorProtocolActivationGuard();
    }

    private static void requireIdentity(StreamStorageConfig configuration) {
        String expectedWriterId = "pulsar-f2/" + configuration.processRunId();
        if (!expectedWriterId.equals(configuration.writerId())) {
            throw new IllegalArgumentException("StreamStorage writerId must equal pulsar-f2/{processRunId}");
        }
    }

    private static void closeAfterFailure(Throwable root, AutoCloseable... resources) {
        List<AutoCloseable> unique = new ArrayList<>();
        for (AutoCloseable resource : resources) {
            if (resource != null && unique.stream().noneMatch(existing -> existing == resource)) {
                unique.add(resource);
            }
        }
        for (AutoCloseable resource : unique) {
            try {
                resource.close();
            } catch (Throwable closeFailure) {
                root.addSuppressed(closeFailure);
            }
        }
    }

    private static void shutdown(ExecutorService executor) {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    private static ThreadFactory daemonFactory(String prefix) {
        AtomicLong ids = new AtomicLong();
        return runnable -> {
            Thread thread = new Thread(runnable, prefix + "-" + ids.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }
}
