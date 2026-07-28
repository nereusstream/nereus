/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.runtime;

import com.nereusstream.api.StreamStorage;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.keys.DeterministicIds;
import com.nereusstream.bookkeeper.BookKeeperLedgerRetentionService;
import com.nereusstream.bookkeeper.BookKeeperPrimaryPhysicalReferenceAdapter;
import com.nereusstream.bookkeeper.BookKeeperPrimaryWalRuntime;
import com.nereusstream.bookkeeper.OxiaBookKeeperLedgerIdNamespaceReservationStore;
import com.nereusstream.bookkeeper.OxiaBookKeeperProtocolActivationStore;
import com.nereusstream.core.DefaultStreamStorage;
import com.nereusstream.core.append.AppendAdmissionGuard;
import com.nereusstream.core.append.AppendCoordinator;
import com.nereusstream.core.append.DefaultGenerationZeroPhysicalReferencePublisher;
import com.nereusstream.core.append.GenerationZeroPhysicalReferencePublisher;
import com.nereusstream.core.append.RequiredObjectGenerationCompletion;
import com.nereusstream.core.backpressure.MaterializationLagGate;
import com.nereusstream.core.backpressure.MaterializationLagThresholds;
import com.nereusstream.core.physical.DefaultObjectReadPinManager;
import com.nereusstream.core.physical.DefaultObjectProtectionManager;
import com.nereusstream.core.physical.ObjectReadPinManager;
import com.nereusstream.core.physical.ObjectProtectionManager;
import com.nereusstream.core.read.GenerationReadResolver;
import com.nereusstream.core.read.MetadataGenerationReadFailureHandler;
import com.nereusstream.core.read.MetadataPhysicalObjectIdentityResolver;
import com.nereusstream.core.read.ParquetV2CompactedTargetReader;
import com.nereusstream.core.read.Phase4ReadComponents;
import com.nereusstream.core.read.ReadTargetReader;
import com.nereusstream.core.read.ReadTargetReaderRegistry;
import com.nereusstream.core.profile.Phase4StorageProfileResolver;
import com.nereusstream.core.profile.StorageProfileResolver;
import com.nereusstream.core.profile.StorageProfileResolverRegistry;
import com.nereusstream.core.recovery.MetadataAppendRecoverySearcher;
import com.nereusstream.core.wal.PrimaryWalRegistry;
import com.nereusstream.core.wal.object.ObjectWalReaderAdapter;
import com.nereusstream.kafka.activation.KafkaStorageActivationRuntime;
import com.nereusstream.kafka.activation.KafkaStorageActivationVerifier;
import com.nereusstream.kafka.activation.KafkaStorageBindingAwareClusterSnapshotProvider;
import com.nereusstream.kafka.activation.KafkaAsyncAppendAdmissionGuard;
import com.nereusstream.kafka.activation.KafkaBookKeeperStreamCoverageProofProducer;
import com.nereusstream.kafka.activation.KafkaGenerationProtocolActivationGuard;
import com.nereusstream.kafka.checkpoint.DurableKafkaCheckpointFailureQuarantine;
import com.nereusstream.kafka.checkpoint.KafkaCanonicalCheckpointPublicationFactory;
import com.nereusstream.kafka.checkpoint.KafkaCheckpointFailureQuarantine;
import com.nereusstream.kafka.checkpoint.KafkaCheckpointPublicationCoordinator;
import com.nereusstream.kafka.compaction.KafkaActivatedGenerationAuthority;
import com.nereusstream.kafka.compaction.KafkaActivatedGenerationSetResolver;
import com.nereusstream.kafka.compaction.KafkaCompactionProductionRuntimeFactory;
import com.nereusstream.kafka.metadata.KafkaMaterializationStreamRegistration;
import com.nereusstream.kafka.recovery.DefaultKafkaPartitionRecoveryLauncher;
import com.nereusstream.kafka.recovery.KafkaCheckpointRecoveryCoordinator;
import com.nereusstream.kafka.recovery.KafkaPartitionRecoveryLauncher;
import com.nereusstream.kafka.retention.DefaultKafkaPartitionMaintenance;
import com.nereusstream.kafka.retention.KafkaPartitionMaintenanceFactory;
import com.nereusstream.kafka.retention.KafkaPartitionMaintenanceRuntime;
import com.nereusstream.materialization.MaterializationSourceProvider;
import com.nereusstream.metadata.oxia.GenerationIndexValidator;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.KafkaCheckpointFailureMetadataStore;
import com.nereusstream.metadata.oxia.KafkaStorageActivationMetadataStore;
import com.nereusstream.metadata.oxia.OxiaJavaClientMetadataStore;
import com.nereusstream.metadata.oxia.OxiaJavaGenerationMetadataStore;
import com.nereusstream.metadata.oxia.OxiaJavaKafkaPartitionMetadataStore;
import com.nereusstream.metadata.oxia.OxiaJavaPhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.PhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.SharedOxiaClientRuntime;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.ObjectStoreProvider;
import com.nereusstream.objectstore.compacted.ParquetKafkaTopicCompactedReader;
import com.nereusstream.objectstore.compacted.ParquetRangedCompactedObjectReader;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointCodecV1;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointReader;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointVerifier;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointWriter;
import com.nereusstream.objectstore.staging.StagingFileManager;
import com.nereusstream.objectstore.wal.DefaultWalObjectReader;
import com.nereusstream.objectstore.wal.DefaultWalObjectWriter;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/** Real Oxia/Object provider bootstrap for the strict synchronous Object-WAL Kafka profile. */
public final class NereusKafkaObjectWalRuntimeFactory {
    private static final String WRITER_VERSION = "nereus-kafka-f9";

    private NereusKafkaObjectWalRuntimeFactory() { }

    /** Provider-bootstrap-only path retained package-private for failure-cut tests. */
    static NereusKafkaRuntime createUnactivatedForTesting(
            NereusKafkaObjectWalRuntimeConfiguration configuration,
            NereusKafkaObjectWalRuntimeContext context) {
        return create(configuration, context, null);
    }

    /**
     * Creates provider clients immediately and transfers their ownership to a production runtime fenced by
     * capability publication and ACTIVE/readiness. The Kafka scheduler, clock and recovery launcher remain borrowed.
     */
    public static NereusKafkaRuntime createActivated(
            NereusKafkaObjectWalRuntimeConfiguration configuration,
            NereusKafkaObjectWalRuntimeContext context,
            NereusKafkaObjectWalActivationContext activationContext) {
        return create(
                configuration,
                context,
                Objects.requireNonNull(activationContext, "activationContext"));
    }

    private static NereusKafkaRuntime create(
            NereusKafkaObjectWalRuntimeConfiguration configuration,
            NereusKafkaObjectWalRuntimeContext context,
            NereusKafkaObjectWalActivationContext activationContext) {
        NereusKafkaObjectWalRuntimeConfiguration exactConfiguration = Objects.requireNonNull(
                configuration, "configuration");
        NereusKafkaObjectWalRuntimeContext exactContext = Objects.requireNonNull(context, "context");
        ObjectStoreProvider provider = exactContext.objectStoreProvider();
        if (!provider.getClass().getName().equals(
                exactConfiguration.objectStore().providerClassName())) {
            throw new IllegalArgumentException(
                    "ObjectStore provider instance does not match configured providerClassName");
        }
        validateActivationContext(exactConfiguration, activationContext);
        validateProviderContexts(exactConfiguration, exactContext);

        List<KafkaRuntimeResources.Resource> constructedResources = new ArrayList<>();
        List<KafkaRuntimeResources.Resource> providerResources = new ArrayList<>();
        OxiaJavaKafkaPartitionMetadataStore partitionMetadataStore;
        StreamStorage streamStorage;
        KafkaPartitionRecoveryLauncher recoveryLauncher;
        KafkaActivatedGenerationAuthority activatedGenerations;
        KafkaMaterializationStreamRegistration materializations = null;
        KafkaStorageActivationVerifier activationVerifier = null;
        List<KafkaRuntimeBackgroundServiceFactory> backgroundServiceFactories =
                new ArrayList<>();
        Optional<KafkaPartitionMaintenanceFactory> maintenanceFactory = Optional.empty();
        KafkaRuntimeStartup startup = KafkaRuntimeStartup.from(exactContext.startupAction());
        try {
            providerResources.add(registerOwned(
                    constructedResources, "object-store-provider", provider));
            ObjectStore objectStore = provider.create(
                    exactConfiguration.objectStore(), exactContext.secretResolver());
            providerResources.add(registerOwned(
                    constructedResources, "object-store", objectStore));
            SharedOxiaClientRuntime oxiaRuntime = SharedOxiaClientRuntime.connect(
                    exactConfiguration.oxia(), exactContext.clock());
            providerResources.add(registerOwned(
                    constructedResources, "shared-oxia-runtime", oxiaRuntime));
            OxiaMetadataStore l0MetadataStore =
                    exactConfiguration.bookKeeper().isPresent()
                            ? OxiaJavaClientMetadataStore.usingSharedRuntime(
                                    exactConfiguration.oxia(),
                                    oxiaRuntime,
                                    exactContext.clock(),
                                    exactConfiguration.bookKeeper()
                                            .orElseThrow()
                                            .metadataStore())
                            : OxiaJavaClientMetadataStore.usingSharedRuntime(
                                    exactConfiguration.oxia(),
                                    oxiaRuntime,
                                    exactContext.clock());
            providerResources.add(registerOwned(
                    constructedResources, "l0-metadata-store", l0MetadataStore));
            GenerationMetadataStore generationMetadataStore =
                    OxiaJavaGenerationMetadataStore.usingSharedRuntime(
                            exactConfiguration.oxia(), oxiaRuntime, exactContext.clock());
            providerResources.add(registerOwned(
                    constructedResources,
                    "generation-metadata-store",
                    generationMetadataStore));
            PhysicalObjectMetadataStore physicalMetadataStore =
                    OxiaJavaPhysicalObjectMetadataStore.usingSharedRuntime(
                            exactConfiguration.oxia(), oxiaRuntime, exactContext.clock());
            providerResources.add(registerOwned(
                    constructedResources, "physical-object-metadata-store", physicalMetadataStore));
            ObjectProtectionManager protections = new DefaultObjectProtectionManager(
                    exactConfiguration.runtime().nereusCluster(),
                    physicalMetadataStore,
                    exactConfiguration.pendingProtectionDuration(),
                    exactConfiguration.maximumClockSkew(),
                    exactConfiguration.orphanGrace(),
                    exactContext.clock());
            providerResources.add(registerOwned(
                    constructedResources, "object-protection-manager", protections));
            ExecutorService callbackExecutor = Executors.newFixedThreadPool(
                    exactConfiguration.callbackThreads(), daemonFactory("nereus-kafka-callback"));
            providerResources.add(registerOwned(
                    constructedResources,
                    "stream-callback-executor",
                    new ExecutorResource(callbackExecutor)));
            partitionMetadataStore = OxiaJavaKafkaPartitionMetadataStore.usingSharedRuntime(
                    exactConfiguration.oxia(),
                    oxiaRuntime,
                    exactConfiguration.runtime().nereusCluster(),
                    exactConfiguration.runtime().kafkaClusterId());
            registerOwned(
                    constructedResources, "kafka-partition-metadata-store", partitionMetadataStore);
            KafkaCheckpointFailureMetadataStore checkpointFailures =
                    KafkaCheckpointFailureMetadataStore.usingSharedRuntime(
                            exactConfiguration.oxia(),
                            oxiaRuntime,
                            exactConfiguration.runtime().nereusCluster(),
                            exactConfiguration.runtime().kafkaClusterId());
            providerResources.add(
                    registerOwned(
                            constructedResources,
                            "kafka-checkpoint-failure-metadata-store",
                            checkpointFailures));
            KafkaCheckpointFailureQuarantine checkpointQuarantine =
                    new DurableKafkaCheckpointFailureQuarantine(
                            checkpointFailures, exactContext.clock());
            if (activationContext != null) {
                KafkaStorageActivationMetadataStore activationStore =
                        KafkaStorageActivationMetadataStore.usingSharedRuntime(
                                exactConfiguration.oxia(),
                                oxiaRuntime,
                                exactConfiguration.runtime().nereusCluster(),
                                exactConfiguration.runtime().kafkaClusterId());
                providerResources.add(registerOwned(
                        constructedResources,
                        "kafka-storage-activation-metadata-store",
                        activationStore));
                KafkaStorageBindingAwareClusterSnapshotProvider clusterSnapshots =
                        new KafkaStorageBindingAwareClusterSnapshotProvider(
                                activationContext.clusterSnapshots(), partitionMetadataStore);
                KafkaStorageActivationRuntime activationRuntime = new KafkaStorageActivationRuntime(
                        activationStore,
                        activationContext.capability(),
                        clusterSnapshots,
                        exactContext.renewalScheduler(),
                        exactContext.clock(),
                        activationContext.activationWaitTimeout(),
                        activationContext.activationPollInterval(),
                        exactContext.startupAction());
                providerResources.add(registerOwned(
                        constructedResources,
                        "kafka-storage-activation-runtime",
                        activationRuntime));
                startup = activationRuntime;
                activationVerifier = new KafkaStorageActivationVerifier(
                                activationStore,
                                activationContext.capability(),
                                clusterSnapshots,
                                exactContext.clock());
                materializations = new KafkaMaterializationStreamRegistration(
                        exactConfiguration.runtime().nereusCluster(),
                        generationMetadataStore,
                        exactContext.clock());
            }
            BookKeeperPrimaryWalRuntime bookKeeperRuntime = null;
            KafkaObjectMaterializationRuntime objectMaterializationRuntime = null;
            List<BookKeeperPrimaryPhysicalReferenceAdapter> additionalPhysicalReferences =
                    List.of();
            if (exactConfiguration.bookKeeper().isPresent()) {
                NereusKafkaBookKeeperWalRuntimeConfiguration bookKeeperConfiguration =
                        exactConfiguration.bookKeeper().orElseThrow();
                NereusKafkaBookKeeperWalRuntimeContext bookKeeperContext =
                        exactContext.bookKeeper().orElseThrow();
                providerResources.add(registerBorrowed(
                        constructedResources,
                        "bookkeeper-client",
                        bookKeeperContext.client()));
                bookKeeperRuntime = BookKeeperPrimaryWalRuntime
                        .createWithOperations(
                                bookKeeperConfiguration.deploymentId(),
                                exactConfiguration.runtime().nereusCluster(),
                                exactConfiguration.streamStorage().processRunId(),
                                bookKeeperConfiguration.wal(),
                                exactConfiguration.oxia(),
                                oxiaRuntime,
                                bookKeeperContext.operations(),
                                new OxiaBookKeeperLedgerIdNamespaceReservationStore(
                                        exactConfiguration.oxia(),
                                        oxiaRuntime),
                                new OxiaBookKeeperProtocolActivationStore(
                                        exactConfiguration.oxia(),
                                        oxiaRuntime),
                                bookKeeperContext.brokerReadiness(),
                                bookKeeperContext.passwords(),
                                exactContext.clock());
                providerResources.add(registerOwned(
                        constructedResources,
                        "bookkeeper-primary-wal-runtime",
                        bookKeeperRuntime));
                if (bookKeeperRuntime.capabilityBinding().isEmpty()) {
                    throw new IllegalStateException(
                            "BookKeeper WAL-only runtime requires an ACTIVE exact publication activation");
                }
                additionalPhysicalReferences =
                        List.of(bookKeeperRuntime.physicalReferences());
            }
            GenerationZeroPhysicalReferencePublisher physicalReferences =
                    new DefaultGenerationZeroPhysicalReferencePublisher(
                            exactConfiguration.runtime().nereusCluster(),
                            l0MetadataStore,
                            physicalMetadataStore,
                            protections,
                            additionalPhysicalReferences);
            ObjectReadPinManager readPins = new DefaultObjectReadPinManager(
                    exactConfiguration.runtime().nereusCluster(),
                    durableProcessRunId(exactConfiguration.streamStorage().processRunId()),
                    physicalMetadataStore,
                    exactConfiguration.pendingProtectionDuration(),
                    exactConfiguration.maximumClockSkew(),
                    exactConfiguration.orphanGrace(),
                    exactContext.clock());
            providerResources.add(registerOwned(
                    constructedResources, "kafka-checkpoint-read-pins", readPins));
            DefaultWalObjectReader walObjectReader = new DefaultWalObjectReader(objectStore);
            List<ReadTargetReader> installedReaders = new ArrayList<>();
            installedReaders.add(new ObjectWalReaderAdapter(walObjectReader));
            if (bookKeeperRuntime != null) {
                installedReaders.add(bookKeeperRuntime.walRuntime().primaryWalReader());
            }
            installedReaders.add(new ParquetV2CompactedTargetReader(
                    new ParquetRangedCompactedObjectReader(
                            objectStore, callbackExecutor),
                    new ParquetKafkaTopicCompactedReader(
                            objectStore, callbackExecutor)));
            ReadTargetReaderRegistry readers = new ReadTargetReaderRegistry(installedReaders);
            MetadataPhysicalObjectIdentityResolver identities =
                    new MetadataPhysicalObjectIdentityResolver(
                            exactConfiguration.runtime().nereusCluster(),
                            l0MetadataStore,
                            physicalMetadataStore);
            GenerationReadResolver generationResolver = new GenerationReadResolver(
                    exactConfiguration.runtime().nereusCluster(),
                    l0MetadataStore,
                    generationMetadataStore,
                    GenerationIndexValidator.phase15Targets(),
                    readers,
                    identities,
                    readPins,
                    exactConfiguration
                            .streamStorage()
                            .maxDerivedIndexRepairCommitsPerCall(),
                    exactContext.clock(),
                    callbackExecutor);
            Phase4ReadComponents readComponents = new Phase4ReadComponents(
                    generationResolver,
                    readers,
                    new MetadataGenerationReadFailureHandler(
                            exactConfiguration.runtime().nereusCluster(),
                            generationMetadataStore,
                            physicalMetadataStore,
                            exactContext.clock()));
            AppendAdmissionGuard appendAdmissionGuard =
                    AppendAdmissionGuard.noOp();
            RequiredObjectGenerationCompletion
                    requiredObjectGeneration = null;
            if (activationContext != null) {
                KafkaStorageActivationVerifier
                        exactActivationVerifier =
                                Objects.requireNonNull(
                                        activationVerifier,
                                        "Kafka materialization requires an activation verifier");
                KafkaGenerationProtocolActivationGuard
                        generationActivation =
                                new KafkaGenerationProtocolActivationGuard(
                                        exactConfiguration
                                                .runtime()
                                                .nereusCluster(),
                                        generationMetadataStore,
                                        exactActivationVerifier,
                                        exactContext.clock());
                List<MaterializationSourceProvider>
                        additionalMaterializationSources =
                                bookKeeperRuntime == null
                                        ? List.of()
                                        : List.of(
                                                bookKeeperRuntime
                                                        .materializationSourceProvider());
                ExecutorService materializationWorkers =
                        Executors.newFixedThreadPool(
                                exactConfiguration
                                        .materialization()
                                        .maxConcurrentWorkers(),
                                daemonFactory(
                                        "nereus-kafka-materialization"));
                KafkaObjectMaterializationRuntime
                        exactMaterialization =
                                new KafkaObjectMaterializationRuntime(
                                        exactConfiguration
                                                .runtime()
                                                .nereusCluster(),
                                        durableProcessRunId(
                                                exactConfiguration
                                                        .streamStorage()
                                                        .processRunId()),
                                        exactConfiguration
                                                .streamStorage()
                                                .maxCommitChainScan(),
                                        exactConfiguration
                                                .materialization(),
                                        l0MetadataStore,
                                        generationMetadataStore,
                                        physicalMetadataStore,
                                        objectStore,
                                        identities,
                                        protections,
                                        readPins,
                                        readComponents,
                                        generationActivation,
                                        additionalMaterializationSources,
                                        exactContext
                                                .renewalScheduler(),
                                        materializationWorkers,
                                        callbackExecutor,
                                        exactContext.clock());
                objectMaterializationRuntime = exactMaterialization;
                providerResources.add(
                        registerOwned(
                                constructedResources,
                                "kafka-object-materialization-runtime",
                                exactMaterialization));
                backgroundServiceFactories.add(
                        ignored -> exactMaterialization);
                MaterializationLagThresholds thresholds =
                        new MaterializationLagThresholds(
                                exactConfiguration
                                        .materialization()
                                        .lagThrottleRecords(),
                                exactConfiguration
                                        .materialization()
                                        .lagRejectRecords(),
                                exactConfiguration
                                        .materialization()
                                        .lagThrottleBytes(),
                                exactConfiguration
                                        .materialization()
                                        .lagRejectBytes(),
                                exactConfiguration
                                        .materialization()
                                        .lagRejectAge(),
                                exactConfiguration
                                        .materialization()
                                        .lagThrottleDelay());
                appendAdmissionGuard =
                        new KafkaAsyncAppendAdmissionGuard(
                                generationActivation,
                                new MaterializationLagGate(
                                        exactMaterialization
                                                .lagSnapshotReader(),
                                        thresholds,
                                        exactContext
                                                .renewalScheduler()));
                requiredObjectGeneration =
                        exactMaterialization
                                .requiredObjectGenerationCompletion();
            }
            if (bookKeeperRuntime != null) {
                NereusKafkaBookKeeperWalRuntimeConfiguration
                        bookKeeperConfiguration =
                                exactConfiguration
                                        .bookKeeper()
                                        .orElseThrow();
                if (bookKeeperConfiguration.ledgerGc().enabled()
                        && !bookKeeperConfiguration.ledgerGc().dryRun()
                        && objectMaterializationRuntime == null) {
                    throw new IllegalArgumentException(
                            "enabled BookKeeper ledger GC requires the activated Kafka materialization runtime");
                }
                if (objectMaterializationRuntime != null) {
                    KafkaObjectMaterializationRuntime exactMaterialization =
                            objectMaterializationRuntime;
                    if (bookKeeperConfiguration.ledgerGc().enabled()
                            && !bookKeeperConfiguration.ledgerGc().dryRun()) {
                        KafkaBookKeeperDeletionActivationService deletionActivation =
                                new KafkaBookKeeperDeletionActivationService(
                                        bookKeeperRuntime,
                                        bookKeeperConfiguration.ledgerGc(),
                                        new KafkaBookKeeperStreamCoverageProofProducer(
                                                exactConfiguration
                                                        .runtime()
                                                        .nereusCluster(),
                                                exactConfiguration
                                                        .runtime()
                                                        .kafkaClusterId(),
                                                bookKeeperConfiguration.wal(),
                                                bookKeeperRuntime
                                                        .namespace()
                                                        .ledgerIdNamespaceSha256()
                                                        .value(),
                                                generationMetadataStore,
                                                l0MetadataStore,
                                                partitionMetadataStore),
                                        "kafka-gc-"
                                                + durableProcessRunId(
                                                        exactConfiguration
                                                                .streamStorage()
                                                                .processRunId()),
                                        bookKeeperConfiguration
                                                .wal()
                                                .operationTimeout());
                        backgroundServiceFactories.add(
                                ignored -> deletionActivation);
                    }
                    bookKeeperRuntime
                            .createRetentionService(
                                    bookKeeperConfiguration.ledgerGc(),
                                    l0MetadataStore,
                                    exactMaterialization
                                            .committedGenerationRetirementAuthority(),
                                    exactMaterialization.streamTrigger(),
                                    exactContext.renewalScheduler(),
                                    callbackExecutor)
                            .ifPresent(
                                    retention -> {
                                        providerResources.add(
                                                registerOwned(
                                                        constructedResources,
                                                        "bookkeeper-ledger-retention-service",
                                                        retention));
                                        backgroundServiceFactories.add(
                                                ignored ->
                                                        retentionBackgroundService(
                                                                retention));
                                    });
                }
            }
            PrimaryWalRegistry objectWalRegistry =
                    AppendCoordinator.productionObjectWalRegistry(
                            exactConfiguration.streamStorage(),
                            l0MetadataStore,
                            new DefaultWalObjectWriter(
                                    objectStore,
                                    WRITER_VERSION,
                                    exactContext.clock()),
                            walObjectReader,
                            physicalReferences,
                            exactContext.clock());
            List<PrimaryWalRegistry> primaryWalRegistries =
                    new ArrayList<>(List.of(objectWalRegistry));
            EnumMap<StorageProfile, StorageProfileResolver> profileResolvers =
                    new EnumMap<>(StorageProfile.class);
            Phase4StorageProfileResolver objectProfiles =
                    new Phase4StorageProfileResolver();
            profileResolvers.put(
                    StorageProfile.OBJECT_WAL_SYNC_OBJECT,
                    objectProfiles);
            profileResolvers.put(
                    StorageProfile.OBJECT_WAL_ASYNC_OBJECT,
                    objectProfiles);
            if (bookKeeperRuntime != null) {
                primaryWalRegistries.add(
                        bookKeeperRuntime.walRuntime().primaryWalRegistry());
                profileResolvers.put(
                        StorageProfile.BOOKKEEPER_WAL_ONLY,
                        bookKeeperRuntime.walRuntime().profileResolver());
                profileResolvers.put(
                        StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT,
                        bookKeeperRuntime.walRuntime().profileResolver());
                profileResolvers.put(
                        StorageProfile.BOOKKEEPER_WAL_SYNC_OBJECT,
                        bookKeeperRuntime.walRuntime().profileResolver());
            }
            streamStorage = new DefaultStreamStorage(
                    exactConfiguration.streamStorage(),
                    l0MetadataStore,
                    PrimaryWalRegistry.combine(primaryWalRegistries),
                    physicalReferences,
                    new MetadataAppendRecoverySearcher(
                            exactConfiguration.runtime().nereusCluster(),
                            l0MetadataStore),
                    new StorageProfileResolverRegistry(profileResolvers),
                    appendAdmissionGuard,
                    requiredObjectGeneration,
                    readComponents,
                    exactContext.clock(),
                    callbackExecutor,
                    com.nereusstream.core.read.ReadMetricsObserver.noop(),
                    com.nereusstream.core.trim.TrimMetricsObserver.noop());
            registerOwned(constructedResources, "stream-storage", streamStorage);
            activatedGenerations = new KafkaActivatedGenerationSetResolver(
                    exactConfiguration.runtime().nereusCluster(),
                    generationMetadataStore);
            KafkaCheckpointCodecV1 checkpointCodec = new KafkaCheckpointCodecV1();
            KafkaCheckpointReader checkpointReader =
                    new KafkaCheckpointReader(objectStore, checkpointCodec);
            KafkaCheckpointVerifier checkpointVerifier = new KafkaCheckpointVerifier();
            KafkaCheckpointRecoveryCoordinator checkpoints =
                    new KafkaCheckpointRecoveryCoordinator(
                            exactConfiguration.runtime().nereusCluster(),
                            partitionMetadataStore,
                            physicalMetadataStore,
                            readPins,
                            checkpointReader,
                            checkpointVerifier,
                            exactContext.clock(),
                            checkpointQuarantine);
            recoveryLauncher =
                    new DefaultKafkaPartitionRecoveryLauncher(
                            checkpoints,
                            streamStorage,
                            exactContext.recoveryStateFactory(),
                            exactConfiguration.runtime().recoveryChunkRecords(),
                            exactConfiguration.runtime().recoveryChunkBytes(),
                            callbackExecutor,
                            exactContext.clock());
            if (activationContext != null && activationContext.maintenance().isPresent()) {
                NereusKafkaMaintenanceContext maintenanceContext =
                        activationContext.maintenance().orElseThrow();
                NereusKafkaMaintenanceConfiguration maintenance =
                        maintenanceContext.configuration();
                StagingFileManager checkpointStaging = new StagingFileManager(
                        maintenance.stagingDirectory(),
                        maintenance.maxStagingBytes(),
                        maintenance.uploadChunkBytes(),
                        maintenance.stagingOrphanGrace(),
                        callbackExecutor);
                providerResources.add(registerOwned(
                        constructedResources,
                        "kafka-checkpoint-staging-files",
                        checkpointStaging));
                KafkaCheckpointWriter checkpointWriter = new KafkaCheckpointWriter(
                        objectStore,
                        checkpointStaging,
                        callbackExecutor,
                        checkpointCodec,
                        checkpointReader,
                        checkpointVerifier);
                KafkaCheckpointPublicationCoordinator publication =
                        new KafkaCheckpointPublicationCoordinator(
                                partitionMetadataStore,
                                checkpointWriter,
                                checkpointVerifier,
                                protections,
                                exactContext.clock());
                KafkaCanonicalCheckpointPublicationFactory publicationFactory =
                        new KafkaCanonicalCheckpointPublicationFactory(
                                exactConfiguration.runtime().nereusCluster(),
                                maintenance.contentPolicySha256(),
                                maintenance.checkpointObjectTimeout(),
                                maintenance.pendingProtectionTtl(),
                                maintenance.writerBuild());
                maintenanceFactory =
                        Optional.of(
                                (identity, leaderEpoch, streamId, sourceValidator) ->
                                        new DefaultKafkaPartitionMaintenance(
                                                identity,
                                                leaderEpoch,
                                                streamId,
                                                sourceValidator,
                                                partitionMetadataStore,
                                                streamStorage,
                                                checkpoints,
                                                publicationFactory,
                                                publication,
                                                checkpointQuarantine,
                                                maintenance.checkpointVerificationTimeout(),
                                                maintenance.trimTimeout(),
                                                exactContext.clock()));
                backgroundServiceFactories.add(
                        partitions ->
                                new KafkaPartitionMaintenanceRuntime(
                                        partitions,
                                        maintenanceContext.ownedPartitions(),
                                        maintenance.retentionInterval(),
                                        maintenance.maxConcurrentPartitions(),
                                        maintenance.maxPartitionsPerPass(),
                                        exactContext.renewalScheduler(),
                                        callbackExecutor));
            }
            if (activationContext != null && activationContext.compaction().isPresent()) {
                NereusKafkaCompactionContext compaction =
                        activationContext.compaction().orElseThrow();
                KafkaStorageActivationVerifier exactActivationVerifier =
                        Objects.requireNonNull(
                                activationVerifier,
                                "Kafka compaction requires an activation verifier");
                StagingFileManager stagingFiles = new StagingFileManager(
                        compaction.configuration().stagingDirectory(),
                        compaction.configuration().maxStagingBytes(),
                        compaction.configuration().uploadChunkBytes(),
                        compaction.configuration().stagingOrphanGrace(),
                        callbackExecutor);
                providerResources.add(registerOwned(
                        constructedResources,
                        "kafka-compaction-staging-files",
                        stagingFiles));
                backgroundServiceFactories.add(
                        partitions -> KafkaCompactionProductionRuntimeFactory.create(
                                compaction,
                                exactConfiguration.runtime().nereusCluster(),
                                exactConfiguration.streamStorage().processRunId(),
                                partitions,
                                l0MetadataStore,
                                generationMetadataStore,
                                physicalMetadataStore,
                                partitionMetadataStore,
                                partitionMetadataStore,
                                protections,
                                readPins,
                                readers,
                                objectStore,
                                stagingFiles,
                                exactActivationVerifier,
                                exactContext.renewalScheduler(),
                                callbackExecutor,
                                exactContext.clock()));
            }
        } catch (Throwable failure) {
            closeAfterFailure(constructedResources, failure);
            throw propagate(failure);
        }

        NereusKafkaRuntimeDependencies dependencies = new NereusKafkaRuntimeDependencies(
                streamStorage,
                ResourceOwnership.OWNED,
                partitionMetadataStore,
                ResourceOwnership.OWNED,
                activatedGenerations,
                exactContext.renewalScheduler(),
                recoveryLauncher,
                exactContext.clock(),
                exactContext.startupAction(),
                providerResources);
        List<KafkaRuntimeBackgroundServiceFactory> exactBackgroundFactories =
                List.copyOf(backgroundServiceFactories);
        KafkaRuntimeBackgroundServiceFactory backgroundServices =
                partitions ->
                        KafkaRuntimeBackgroundService.composite(
                                exactBackgroundFactories.stream()
                                        .map(factory -> factory.create(partitions))
                                        .toList());
        return NereusKafkaRuntimeFactory.create(
                exactConfiguration.runtime(),
                dependencies,
                startup,
                materializations,
                backgroundServices,
                maintenanceFactory);
    }

    static String durableProcessRunId(String runtimeProcessId) {
        String exact = Objects.requireNonNull(runtimeProcessId, "runtimeProcessId");
        if (exact.isBlank()) {
            throw new IllegalArgumentException("runtimeProcessId cannot be blank");
        }
        return DeterministicIds.stableHashComponent(exact);
    }

    private static KafkaRuntimeBackgroundService retentionBackgroundService(
            BookKeeperLedgerRetentionService retention) {
        BookKeeperLedgerRetentionService exact =
                Objects.requireNonNull(retention, "retention");
        return new KafkaRuntimeBackgroundService() {
            @Override
            public CompletionStage<Void> start() {
                return exact.start();
            }

            @Override
            public CompletionStage<Void> closeAsync() {
                return exact.closeAsync();
            }
        };
    }

    private static void validateActivationContext(
            NereusKafkaObjectWalRuntimeConfiguration configuration,
            NereusKafkaObjectWalActivationContext activationContext) {
        if (activationContext == null) return;
        if (!activationContext.capability().kafkaClusterId().equals(
                configuration.runtime().kafkaClusterId())) {
            throw new IllegalArgumentException(
                    "activation capability Kafka cluster must match the runtime");
        }
        List<String> executableProfiles = configuration.runtime().executableProfiles()
                .stream().map(Enum::name).sorted().toList();
        if (!activationContext.capability().supportedStorageProfiles().equals(executableProfiles)) {
            throw new IllegalArgumentException(
                    "activation capability profiles must match executable runtime profiles");
        }
    }

    private static void validateProviderContexts(
            NereusKafkaObjectWalRuntimeConfiguration configuration,
            NereusKafkaObjectWalRuntimeContext context) {
        if (configuration.bookKeeper().isPresent()
                != context.bookKeeper().isPresent()) {
            throw new IllegalArgumentException(
                    "BookKeeper runtime configuration and context must be installed together");
        }
    }

    private static KafkaRuntimeResources.Resource registerOwned(
            List<KafkaRuntimeResources.Resource> resources,
            String name,
            AutoCloseable value) {
        KafkaRuntimeResources.Resource resource = KafkaRuntimeResources.Resource.owned(name, value);
        for (KafkaRuntimeResources.Resource registered : resources) {
            if (registered.value() == value) {
                throw new IllegalArgumentException(
                        "Kafka runtime resource " + name + " duplicates " + registered.name());
            }
        }
        resources.add(resource);
        return resource;
    }

    private static KafkaRuntimeResources.Resource registerBorrowed(
            List<KafkaRuntimeResources.Resource> resources,
            String name,
            AutoCloseable value) {
        KafkaRuntimeResources.Resource resource =
                KafkaRuntimeResources.Resource.borrowed(name, value);
        for (KafkaRuntimeResources.Resource registered : resources) {
            if (registered.value() == value) {
                throw new IllegalArgumentException(
                        "Kafka runtime resource " + name
                                + " duplicates " + registered.name());
            }
        }
        resources.add(resource);
        return resource;
    }

    private static void closeAfterFailure(
            List<KafkaRuntimeResources.Resource> resources, Throwable failure) {
        try {
            new KafkaRuntimeResources(resources).close();
        } catch (Throwable closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }

    private static RuntimeException propagate(Throwable failure) {
        if (failure instanceof Error error) {
            throw error;
        }
        if (failure instanceof RuntimeException runtimeFailure) {
            return runtimeFailure;
        }
        return new IllegalStateException("Failed to bootstrap Nereus Kafka Object-WAL runtime", failure);
    }

    private static ThreadFactory daemonFactory(String prefix) {
        AtomicLong ids = new AtomicLong();
        return runnable -> {
            Thread thread = new Thread(runnable, prefix + "-" + ids.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }

    private static final class ExecutorResource implements AutoCloseable {
        private final ExecutorService executor;
        private final AtomicBoolean closed = new AtomicBoolean();

        private ExecutorResource(ExecutorService executor) {
            this.executor = Objects.requireNonNull(executor, "executor");
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                executor.shutdownNow();
            }
        }
    }
}
