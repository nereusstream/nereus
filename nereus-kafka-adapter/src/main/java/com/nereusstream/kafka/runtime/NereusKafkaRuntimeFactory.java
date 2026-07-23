/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.runtime;

import com.nereusstream.kafka.codec.KafkaAppendBatchEncoder;
import com.nereusstream.kafka.codec.KafkaFetchAssembler;
import com.nereusstream.kafka.codec.KafkaRecordBatchCodec;
import com.nereusstream.kafka.metadata.KafkaPartitionLifecycleCoordinator;
import com.nereusstream.kafka.partition.DefaultKafkaPartitionOpener;
import com.nereusstream.kafka.partition.DefaultKafkaPartitionStorageManager;
import com.nereusstream.metadata.oxia.KafkaPartitionKeyspace;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Product-owned, Kafka-type-free assembly of the native Kafka partition runtime. */
public final class NereusKafkaRuntimeFactory {
    private static final String PARTITION_METADATA_STORE_RESOURCE = "kafka-partition-metadata-store";
    private static final String STREAM_STORAGE_RESOURCE = "stream-storage";

    private NereusKafkaRuntimeFactory() { }

    /**
     * Assembles one runtime after provider construction. Ownership transfers only after the resource ledger is valid.
     * Provider resources must be supplied in construction order and are closed in reverse order after partition drain.
     */
    public static NereusKafkaRuntime create(
            NereusKafkaRuntimeConfiguration configuration,
            NereusKafkaRuntimeDependencies dependencies) {
        NereusKafkaRuntimeConfiguration exactConfiguration = Objects.requireNonNull(
                configuration, "configuration");
        NereusKafkaRuntimeDependencies exactDependencies = Objects.requireNonNull(
                dependencies, "dependencies");
        KafkaRuntimeResources resources = resources(exactDependencies);
        try {
            KafkaPartitionKeyspace keyspace = new KafkaPartitionKeyspace(
                    exactConfiguration.nereusCluster(), exactConfiguration.kafkaClusterId());
            KafkaPartitionLifecycleCoordinator lifecycle = new KafkaPartitionLifecycleCoordinator(
                    exactDependencies.partitionMetadataStore(),
                    exactDependencies.streamStorage(),
                    keyspace,
                    exactDependencies.clock());
            KafkaRecordBatchCodec codec = new KafkaRecordBatchCodec();
            DefaultKafkaPartitionOpener opener = new DefaultKafkaPartitionOpener(
                    exactDependencies.streamStorage(),
                    exactConfiguration.writerId(),
                    exactConfiguration.appendSessionTtl(),
                    exactConfiguration.appendSessionRenewalInterval(),
                    exactDependencies.renewalScheduler(),
                    exactDependencies.recoveryLauncher(),
                    new KafkaAppendBatchEncoder(codec),
                    new KafkaFetchAssembler(codec),
                    exactDependencies.clock());
            DefaultKafkaPartitionStorageManager manager = new DefaultKafkaPartitionStorageManager(
                    lifecycle,
                    opener,
                    exactDependencies.clock(),
                    exactConfiguration.operationOwnerId(),
                    exactConfiguration.operationOwnerEpoch(),
                    exactConfiguration.operationTtl());
            return new DefaultNereusKafkaRuntime(
                    new KafkaStorageAdmission(),
                    manager,
                    exactDependencies.startupAction(),
                    resources);
        } catch (RuntimeException | Error failure) {
            closeAfterFailure(resources, failure);
            throw failure;
        }
    }

    private static KafkaRuntimeResources resources(NereusKafkaRuntimeDependencies dependencies) {
        List<KafkaRuntimeResources.Resource> resources = new ArrayList<>(dependencies.providerResources());
        resources.add(new KafkaRuntimeResources.Resource(
                PARTITION_METADATA_STORE_RESOURCE,
                dependencies.partitionMetadataStore(),
                dependencies.partitionMetadataStoreOwnership()));
        resources.add(new KafkaRuntimeResources.Resource(
                STREAM_STORAGE_RESOURCE,
                dependencies.streamStorage(),
                dependencies.streamStorageOwnership()));
        return new KafkaRuntimeResources(resources);
    }

    private static void closeAfterFailure(KafkaRuntimeResources resources, Throwable failure) {
        try {
            resources.close();
        } catch (Throwable closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }
}
