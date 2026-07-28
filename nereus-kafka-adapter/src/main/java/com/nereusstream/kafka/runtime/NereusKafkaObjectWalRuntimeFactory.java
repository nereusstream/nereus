/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.runtime;

import com.nereusstream.api.StreamStorage;
import com.nereusstream.core.DefaultStreamStorage;
import com.nereusstream.core.append.DefaultGenerationZeroPhysicalReferencePublisher;
import com.nereusstream.core.append.GenerationZeroPhysicalReferencePublisher;
import com.nereusstream.core.physical.DefaultObjectReadPinManager;
import com.nereusstream.core.physical.DefaultObjectProtectionManager;
import com.nereusstream.core.physical.ObjectReadPinManager;
import com.nereusstream.core.physical.ObjectProtectionManager;
import com.nereusstream.core.read.GenerationReadResolver;
import com.nereusstream.core.read.MetadataGenerationReadFailureHandler;
import com.nereusstream.core.read.MetadataPhysicalObjectIdentityResolver;
import com.nereusstream.core.read.ParquetV2CompactedTargetReader;
import com.nereusstream.core.read.Phase4ReadComponents;
import com.nereusstream.core.read.ReadTargetReaderRegistry;
import com.nereusstream.core.wal.object.ObjectWalReaderAdapter;
import com.nereusstream.kafka.activation.KafkaStorageActivationRuntime;
import com.nereusstream.kafka.activation.KafkaStorageActivationVerifier;
import com.nereusstream.kafka.activation.KafkaStorageBindingAwareClusterSnapshotProvider;
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
import java.util.List;
import java.util.Objects;
import java.util.Optional;
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
            OxiaMetadataStore l0MetadataStore = OxiaJavaClientMetadataStore.usingSharedRuntime(
                    exactConfiguration.oxia(), oxiaRuntime, exactContext.clock());
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
            GenerationZeroPhysicalReferencePublisher physicalReferences =
                    new DefaultGenerationZeroPhysicalReferencePublisher(
                            exactConfiguration.runtime().nereusCluster(),
                            l0MetadataStore,
                            physicalMetadataStore,
                            protections);
            ObjectReadPinManager readPins = new DefaultObjectReadPinManager(
                    exactConfiguration.runtime().nereusCluster(),
                    exactConfiguration.streamStorage().processRunId(),
                    physicalMetadataStore,
                    exactConfiguration.pendingProtectionDuration(),
                    exactConfiguration.maximumClockSkew(),
                    exactConfiguration.orphanGrace(),
                    exactContext.clock());
            providerResources.add(registerOwned(
                    constructedResources, "kafka-checkpoint-read-pins", readPins));
            DefaultWalObjectReader walObjectReader = new DefaultWalObjectReader(objectStore);
            ReadTargetReaderRegistry readers = new ReadTargetReaderRegistry(List.of(
                    new ObjectWalReaderAdapter(walObjectReader),
                    new ParquetV2CompactedTargetReader(
                            new ParquetRangedCompactedObjectReader(
                                    objectStore, callbackExecutor),
                            new ParquetKafkaTopicCompactedReader(
                                    objectStore, callbackExecutor))));
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
            streamStorage = new DefaultStreamStorage(
                    exactConfiguration.streamStorage(),
                    l0MetadataStore,
                    new DefaultWalObjectWriter(objectStore, WRITER_VERSION, exactContext.clock()),
                    walObjectReader,
                    physicalReferences,
                    readComponents,
                    exactContext.clock(),
                    callbackExecutor);
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
