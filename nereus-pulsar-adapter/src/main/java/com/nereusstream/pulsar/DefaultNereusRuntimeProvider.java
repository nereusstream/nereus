/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import com.nereusstream.api.StreamStorage;
import com.nereusstream.api.keys.DeterministicIds;
import com.nereusstream.core.DefaultStreamStorage;
import com.nereusstream.core.StreamStorageConfig;
import com.nereusstream.core.append.DefaultGenerationZeroPhysicalReferencePublisher;
import com.nereusstream.core.physical.DefaultObjectProtectionManager;
import com.nereusstream.core.physical.DefaultObjectReadPinManager;
import com.nereusstream.core.physical.ObjectProtectionManager;
import com.nereusstream.core.physical.ObjectReadPinManager;
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
import com.nereusstream.managedledger.generation.DefaultManagedLedgerGenerationRegistrationBackfillProofCoordinator;
import com.nereusstream.managedledger.generation.DefaultManagedLedgerMaterializationRegistrationCoordinator;
import com.nereusstream.managedledger.generation.ManagedLedgerGenerationRegistrationBackfillProofCoordinator;
import com.nereusstream.managedledger.generation.ManagedLedgerMaterializationRegistrationCoordinator;
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
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.ObjectStoreProvider;
import com.nereusstream.objectstore.wal.DefaultWalObjectReader;
import com.nereusstream.objectstore.wal.DefaultWalObjectWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/** Production Object-WAL/Oxia runtime assembly used by the hybrid broker storage provider. */
public final class DefaultNereusRuntimeProvider implements NereusRuntimeProvider {
    private static final String WRITER_VERSION = "nereus-pulsar-f2";
    private static final Duration PENDING_PROTECTION_DURATION = Duration.ofMinutes(5);
    private static final Duration READER_LEASE_DURATION = Duration.ofMinutes(2);
    private static final Duration MAXIMUM_CLOCK_SKEW = Duration.ofSeconds(5);
    private static final Duration ORPHAN_GRACE = Duration.ofDays(1);

    @Override
    public NereusManagedLedgerRuntime create(
            NereusRuntimeConfiguration configuration,
            NereusRuntimeContext context) throws Exception {
        Objects.requireNonNull(configuration, "configuration");
        Objects.requireNonNull(context, "context");
        StreamStorageConfig streamConfig = configuration.streamStorage();
        requireIdentity(streamConfig);

        ObjectStoreProvider objectStoreProvider = null;
        ObjectStore objectStore = null;
        SharedOxiaClientRuntime sharedOxiaRuntime = null;
        OxiaMetadataStore l0MetadataStore = null;
        PhysicalObjectMetadataStore physicalMetadataStore = null;
        ObjectProtectionManager objectProtectionManager = null;
        ObjectReadPinManager objectReadPinManager = null;
        ManagedLedgerProjectionMetadataStore projectionStore = null;
        GenerationMetadataStore generationMetadataStore = null;
        GenerationProtocolActivationStore generationProtocolActivationStore =
                null;
        ManagedLedgerGenerationRegistrationBackfillProofCoordinator
                generationRegistrationBackfillProofCoordinator = null;
        ManagedLedgerMaterializationRegistrationCoordinator
                materializationRegistrationCoordinator = null;
        CursorMetadataStore cursorMetadataStore = null;
        ScheduledExecutorService scheduler = null;
        ExecutorService callbackExecutor = null;
        StreamStorage streamStorage = null;
        CursorSnapshotStore cursorSnapshotStore = null;
        CursorRetentionCoordinator cursorRetentionCoordinator = null;
        CursorStorage cursorStorage = null;
        try {
            objectStoreProvider = instantiateObjectStoreProvider(
                    configuration.objectStore().providerClassName(), context.pluginClassLoader());
            objectStore = objectStoreProvider.create(configuration.objectStore(), context.secretResolver());
            Clock clock = Clock.systemUTC();
            sharedOxiaRuntime = SharedOxiaClientRuntime.connect(configuration.oxia(), clock);
            l0MetadataStore = OxiaJavaClientMetadataStore.usingSharedRuntime(
                    configuration.oxia(), sharedOxiaRuntime, clock);
            physicalMetadataStore = OxiaJavaPhysicalObjectMetadataStore.usingSharedRuntime(
                    configuration.oxia(), sharedOxiaRuntime, clock);
            objectProtectionManager = new DefaultObjectProtectionManager(
                    streamConfig.cluster(),
                    physicalMetadataStore,
                    PENDING_PROTECTION_DURATION,
                    MAXIMUM_CLOCK_SKEW,
                    ORPHAN_GRACE,
                    clock);
            objectReadPinManager = new DefaultObjectReadPinManager(
                    streamConfig.cluster(),
                    DeterministicIds.stableHashComponent(
                            "f4-reader/" + streamConfig.processRunId()),
                    physicalMetadataStore,
                    READER_LEASE_DURATION,
                    MAXIMUM_CLOCK_SKEW,
                    ORPHAN_GRACE,
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
            generationRegistrationBackfillProofCoordinator =
                    new DefaultManagedLedgerGenerationRegistrationBackfillProofCoordinator(
                            streamConfig.cluster(),
                            generationProtocolActivationStore,
                            context.generationCapabilityReadinessProvider(),
                            NereusGenerationProtocolReferenceDomains
                                    .currentV1(),
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
            scheduler = Executors.newSingleThreadScheduledExecutor(daemonFactory("nereus-f2-scheduler"));
            callbackExecutor = Executors.newFixedThreadPool(
                    Math.min(Runtime.getRuntime().availableProcessors(), 8),
                    daemonFactory("nereus-f2-callback"));
            streamStorage = new DefaultStreamStorage(
                    streamConfig,
                    l0MetadataStore,
                    new DefaultWalObjectWriter(objectStore, WRITER_VERSION, clock),
                    new DefaultWalObjectReader(objectStore),
                    new DefaultGenerationZeroPhysicalReferencePublisher(
                            streamConfig.cluster(),
                            l0MetadataStore,
                            physicalMetadataStore,
                            objectProtectionManager),
                    clock,
                    callbackExecutor);
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
                    PENDING_PROTECTION_DURATION,
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
                    cursorStorage,
                    cursorRetentionCoordinator,
                    cursorSnapshotStore,
                    cursorMetadataStore,
                    generationProtocolActivationStore,
                    generationMetadataStore,
                    projectionStore,
                    streamStorage,
                    objectReadPinManager,
                    objectProtectionManager,
                    physicalMetadataStore,
                    l0MetadataStore,
                    objectStore,
                    objectStoreProvider,
                    sharedOxiaRuntime);
            shutdown(callbackExecutor);
            shutdown(scheduler);
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
