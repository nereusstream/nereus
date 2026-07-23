/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.runtime;

import com.nereusstream.api.StreamStorage;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.kafka.partition.DefaultKafkaPartitionStorageManager;
import com.nereusstream.metadata.oxia.KafkaPartitionMetadataStore;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NereusKafkaRuntimeFactoryTest {
    @Test
    void assemblesOneManagerAndClosesOwnedProviderGraphInReverseOrder() {
        List<String> closes = new ArrayList<>();
        StreamStorage streams = proxy(StreamStorage.class, "streams", closes);
        KafkaPartitionMetadataStore metadata = proxy(
                KafkaPartitionMetadataStore.class, "metadata", closes);
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        AtomicInteger starts = new AtomicInteger();
        try {
            NereusKafkaRuntime runtime = NereusKafkaRuntimeFactory.create(
                    configuration(),
                    dependencies(
                            streams,
                            ResourceOwnership.OWNED,
                            metadata,
                            ResourceOwnership.OWNED,
                            scheduler,
                            () -> {
                                starts.incrementAndGet();
                                return CompletableFuture.completedFuture(null);
                            },
                            List.of(KafkaRuntimeResources.Resource.owned(
                                    "provider", () -> closes.add("provider")))));

            assertThat(runtime.partitionStorageManager())
                    .isInstanceOf(DefaultKafkaPartitionStorageManager.class);
            runtime.start().toCompletableFuture().join();
            runtime.start().toCompletableFuture().join();
            assertThat(starts).hasValue(1);
            assertThat(runtime.health().ready()).isTrue();

            runtime.close();
            runtime.close();

            assertThat(closes).containsExactly("streams", "metadata", "provider");
            assertThat(scheduler.isShutdown()).isFalse();
        } finally {
            scheduler.shutdownNow();
        }
    }

    @Test
    void borrowedProviderGraphIsNeverClosed() {
        List<String> closes = new ArrayList<>();
        StreamStorage streams = proxy(StreamStorage.class, "streams", closes);
        KafkaPartitionMetadataStore metadata = proxy(
                KafkaPartitionMetadataStore.class, "metadata", closes);
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            NereusKafkaRuntime runtime = NereusKafkaRuntimeFactory.create(
                    configuration(),
                    dependencies(
                            streams,
                            ResourceOwnership.BORROWED,
                            metadata,
                            ResourceOwnership.BORROWED,
                            scheduler,
                            () -> CompletableFuture.completedFuture(null),
                            List.of(KafkaRuntimeResources.Resource.borrowed(
                                    "provider", () -> closes.add("provider")))));

            runtime.close();

            assertThat(closes).isEmpty();
        } finally {
            scheduler.shutdownNow();
        }
    }

    @Test
    void rejectsDuplicateResourceIdentityBeforeOwnershipTransfer() {
        List<String> closes = new ArrayList<>();
        StreamStorage streams = proxy(StreamStorage.class, "streams", closes);
        KafkaPartitionMetadataStore metadata = proxy(
                KafkaPartitionMetadataStore.class, "metadata", closes);
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            NereusKafkaRuntimeDependencies dependencies = dependencies(
                    streams,
                    ResourceOwnership.OWNED,
                    metadata,
                    ResourceOwnership.OWNED,
                    scheduler,
                    () -> CompletableFuture.completedFuture(null),
                    List.of(KafkaRuntimeResources.Resource.owned("duplicate-stream", streams)));

            assertThatThrownBy(() -> NereusKafkaRuntimeFactory.create(configuration(), dependencies))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("duplicates");
            assertThat(closes).isEmpty();
        } finally {
            scheduler.shutdownNow();
        }
    }

    @Test
    void validatesDurationsAndOwnerEpochBeforeAssembly() {
        assertThatThrownBy(() -> new NereusKafkaRuntimeConfiguration(
                "nereus",
                "kraft",
                "broker-run",
                Duration.ofSeconds(30),
                Duration.ofSeconds(30),
                "broker-run",
                1,
                Duration.ofSeconds(30),
                Set.of(StorageProfile.OBJECT_WAL_SYNC_OBJECT)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("shorter");
        assertThatThrownBy(() -> new NereusKafkaRuntimeConfiguration(
                "nereus",
                "kraft",
                "broker-run",
                Duration.ofSeconds(30),
                Duration.ofSeconds(10),
                "broker-run",
                0,
                Duration.ofSeconds(30),
                Set.of(StorageProfile.OBJECT_WAL_SYNC_OBJECT)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("operationOwnerEpoch");
    }

    private static NereusKafkaRuntimeConfiguration configuration() {
        return new NereusKafkaRuntimeConfiguration(
                "nereus",
                "kraft",
                "broker-run",
                Duration.ofSeconds(30),
                Duration.ofSeconds(10),
                "broker-run",
                7,
                Duration.ofSeconds(30),
                Set.of(StorageProfile.OBJECT_WAL_SYNC_OBJECT));
    }

    private static NereusKafkaRuntimeDependencies dependencies(
            StreamStorage streams,
            ResourceOwnership streamOwnership,
            KafkaPartitionMetadataStore metadata,
            ResourceOwnership metadataOwnership,
            ScheduledExecutorService scheduler,
            java.util.function.Supplier<CompletableFuture<Void>> startup,
            List<KafkaRuntimeResources.Resource> providerResources) {
        return new NereusKafkaRuntimeDependencies(
                streams,
                streamOwnership,
                metadata,
                metadataOwnership,
                scheduler,
                request -> CompletableFuture.failedFuture(new AssertionError("recovery not expected")),
                Clock.systemUTC(),
                startup,
                providerResources);
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, String name, List<String> closes) {
        return (T) Proxy.newProxyInstance(
                type.getClassLoader(),
                new Class<?>[] {type},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "close" -> {
                        closes.add(name);
                        yield null;
                    }
                    case "toString" -> name;
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == arguments[0];
                    default -> throw new AssertionError("unexpected call: " + method.getName());
                });
    }
}
