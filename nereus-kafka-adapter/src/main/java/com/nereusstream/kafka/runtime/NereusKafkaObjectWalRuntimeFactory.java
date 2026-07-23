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
import com.nereusstream.kafka.activation.KafkaStorageActivationRuntime;
import com.nereusstream.kafka.activation.KafkaStorageBindingAwareClusterSnapshotProvider;
import com.nereusstream.kafka.recovery.DefaultKafkaPartitionRecoveryLauncher;
import com.nereusstream.kafka.recovery.KafkaCheckpointRecoveryCoordinator;
import com.nereusstream.kafka.recovery.KafkaPartitionRecoveryLauncher;
import com.nereusstream.metadata.oxia.KafkaPartitionMetadataStore;
import com.nereusstream.metadata.oxia.KafkaStorageActivationMetadataStore;
import com.nereusstream.metadata.oxia.OxiaJavaClientMetadataStore;
import com.nereusstream.metadata.oxia.OxiaJavaKafkaPartitionMetadataStore;
import com.nereusstream.metadata.oxia.OxiaJavaPhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.PhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.SharedOxiaClientRuntime;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.ObjectStoreProvider;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointCodecV1;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointReader;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointVerifier;
import com.nereusstream.objectstore.wal.DefaultWalObjectReader;
import com.nereusstream.objectstore.wal.DefaultWalObjectWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
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
        KafkaPartitionMetadataStore partitionMetadataStore;
        StreamStorage streamStorage;
        KafkaPartitionRecoveryLauncher recoveryLauncher;
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
                    constructedResources,
                    "kafka-partition-metadata-store",
                    partitionMetadataStore);
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
                KafkaStorageActivationRuntime activationRuntime = new KafkaStorageActivationRuntime(
                        activationStore,
                        activationContext.capability(),
                        new KafkaStorageBindingAwareClusterSnapshotProvider(
                                activationContext.clusterSnapshots(), partitionMetadataStore),
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
            }
            GenerationZeroPhysicalReferencePublisher physicalReferences =
                    new DefaultGenerationZeroPhysicalReferencePublisher(
                            exactConfiguration.runtime().nereusCluster(),
                            l0MetadataStore,
                            physicalMetadataStore,
                            protections);
            streamStorage = new DefaultStreamStorage(
                    exactConfiguration.streamStorage(),
                    l0MetadataStore,
                    new DefaultWalObjectWriter(objectStore, WRITER_VERSION, exactContext.clock()),
                    new DefaultWalObjectReader(objectStore),
                    physicalReferences,
                    exactContext.clock(),
                    callbackExecutor);
            registerOwned(constructedResources, "stream-storage", streamStorage);
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
            KafkaCheckpointRecoveryCoordinator checkpoints =
                    new KafkaCheckpointRecoveryCoordinator(
                            exactConfiguration.runtime().nereusCluster(),
                            partitionMetadataStore,
                            physicalMetadataStore,
                            readPins,
                            new KafkaCheckpointReader(
                                    objectStore, new KafkaCheckpointCodecV1()),
                            new KafkaCheckpointVerifier(),
                            exactContext.clock(),
                            (reference, failure) -> { });
            recoveryLauncher = new DefaultKafkaPartitionRecoveryLauncher(
                    checkpoints,
                    streamStorage,
                    exactContext.recoveryStateFactory(),
                    exactConfiguration.runtime().recoveryChunkRecords(),
                    exactConfiguration.runtime().recoveryChunkBytes(),
                    callbackExecutor,
                    exactContext.clock());
        } catch (Throwable failure) {
            closeAfterFailure(constructedResources, failure);
            throw propagate(failure);
        }

        NereusKafkaRuntimeDependencies dependencies = new NereusKafkaRuntimeDependencies(
                streamStorage,
                ResourceOwnership.OWNED,
                partitionMetadataStore,
                ResourceOwnership.OWNED,
                exactContext.renewalScheduler(),
                recoveryLauncher,
                exactContext.clock(),
                exactContext.startupAction(),
                providerResources);
        return NereusKafkaRuntimeFactory.create(
                exactConfiguration.runtime(), dependencies, startup);
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
