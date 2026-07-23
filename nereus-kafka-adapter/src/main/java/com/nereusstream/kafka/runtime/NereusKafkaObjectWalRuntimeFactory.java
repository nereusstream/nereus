/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.runtime;

import com.nereusstream.api.StreamStorage;
import com.nereusstream.core.DefaultStreamStorage;
import com.nereusstream.core.append.DefaultGenerationZeroPhysicalReferencePublisher;
import com.nereusstream.core.append.GenerationZeroPhysicalReferencePublisher;
import com.nereusstream.core.physical.DefaultObjectProtectionManager;
import com.nereusstream.core.physical.ObjectProtectionManager;
import com.nereusstream.metadata.oxia.KafkaPartitionMetadataStore;
import com.nereusstream.metadata.oxia.OxiaJavaClientMetadataStore;
import com.nereusstream.metadata.oxia.OxiaJavaKafkaPartitionMetadataStore;
import com.nereusstream.metadata.oxia.OxiaJavaPhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.PhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.SharedOxiaClientRuntime;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.ObjectStoreProvider;
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

    /**
     * Creates provider clients immediately and transfers their ownership to the returned runtime. The Kafka scheduler,
     * clock, recovery launcher and startup action remain borrowed.
     */
    public static NereusKafkaRuntime create(
            NereusKafkaObjectWalRuntimeConfiguration configuration,
            NereusKafkaObjectWalRuntimeContext context) {
        NereusKafkaObjectWalRuntimeConfiguration exactConfiguration = Objects.requireNonNull(
                configuration, "configuration");
        NereusKafkaObjectWalRuntimeContext exactContext = Objects.requireNonNull(context, "context");
        ObjectStoreProvider provider = exactContext.objectStoreProvider();
        if (!provider.getClass().getName().equals(
                exactConfiguration.objectStore().providerClassName())) {
            throw new IllegalArgumentException(
                    "ObjectStore provider instance does not match configured providerClassName");
        }

        List<KafkaRuntimeResources.Resource> constructedResources = new ArrayList<>();
        List<KafkaRuntimeResources.Resource> providerResources = new ArrayList<>();
        KafkaPartitionMetadataStore partitionMetadataStore;
        StreamStorage streamStorage;
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
        } catch (Throwable failure) {
            closeAfterFailure(constructedResources, failure);
            throw propagate(failure);
        }

        return NereusKafkaRuntimeFactory.create(
                exactConfiguration.runtime(),
                new NereusKafkaRuntimeDependencies(
                        streamStorage,
                        ResourceOwnership.OWNED,
                        partitionMetadataStore,
                        ResourceOwnership.OWNED,
                        exactContext.renewalScheduler(),
                        exactContext.recoveryLauncher(),
                        exactContext.clock(),
                        exactContext.startupAction(),
                        providerResources));
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
