/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.runtime;

import com.nereusstream.api.StreamStorage;
import com.nereusstream.kafka.recovery.KafkaPartitionRecoveryLauncher;
import com.nereusstream.metadata.oxia.KafkaPartitionMetadataStore;
import java.time.Clock;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

/** Explicit provider and Kafka-fork dependencies for one product runtime assembly. */
public record NereusKafkaRuntimeDependencies(
        StreamStorage streamStorage,
        ResourceOwnership streamStorageOwnership,
        KafkaPartitionMetadataStore partitionMetadataStore,
        ResourceOwnership partitionMetadataStoreOwnership,
        ScheduledExecutorService renewalScheduler,
        KafkaPartitionRecoveryLauncher recoveryLauncher,
        Clock clock,
        Supplier<? extends CompletionStage<Void>> startupAction,
        List<KafkaRuntimeResources.Resource> providerResources) {
    public NereusKafkaRuntimeDependencies {
        Objects.requireNonNull(streamStorage, "streamStorage");
        Objects.requireNonNull(streamStorageOwnership, "streamStorageOwnership");
        Objects.requireNonNull(partitionMetadataStore, "partitionMetadataStore");
        Objects.requireNonNull(partitionMetadataStoreOwnership, "partitionMetadataStoreOwnership");
        Objects.requireNonNull(renewalScheduler, "renewalScheduler");
        Objects.requireNonNull(recoveryLauncher, "recoveryLauncher");
        Objects.requireNonNull(clock, "clock");
        Objects.requireNonNull(startupAction, "startupAction");
        providerResources = List.copyOf(Objects.requireNonNull(providerResources, "providerResources"));
    }
}
