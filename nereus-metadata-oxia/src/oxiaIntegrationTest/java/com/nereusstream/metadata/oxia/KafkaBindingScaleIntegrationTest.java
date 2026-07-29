/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.metadata.oxia.records.KafkaPartitionBindingRecord;
import com.nereusstream.metadata.oxia.records.KafkaPartitionOperationType;
import com.nereusstream.metadata.oxia.records.KafkaPartitionPendingOperationRecord;
import com.nereusstream.metadata.oxia.records.KafkaPartitionRegistryRecord;
import io.oxia.testcontainers.OxiaContainer;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * F9-M7 real-Oxia scale boundary for the authoritative partition registry.
 *
 * <p>The test deliberately seeds every binding through the production codec/CAS store, closes the
 * complete client runtime, reconnects, and scans with a page size that forces continuation on every
 * shard. Registry hints are never counted as authority: every returned hint is reloaded through the
 * binding root and its exact durable hash is checked.
 */
@Testcontainers
class KafkaBindingScaleIntegrationTest {
    private static final String IMAGE = "oxia/oxia:0.16.3";
    private static final int BINDING_COUNT = 16_384;
    private static final int BINDINGS_PER_SHARD =
            BINDING_COUNT / KafkaPartitionKeyspace.REGISTRY_SHARDS;
    private static final int WRITE_WINDOW = 64;
    private static final int PAGE_SIZE = 17;

    @Container
    private static final OxiaContainer OXIA =
            new OxiaContainer(DockerImageName.parse(IMAGE)).withShards(4);

    @Test
    @Timeout(value = 10, unit = TimeUnit.MINUTES)
    void scenarioKfScl001() {
        String nereusCluster = "f9/scale/" + UUID.randomUUID();
        String kafkaCluster = "kraft-scale";
        KafkaPartitionKeyspace keys =
                new KafkaPartitionKeyspace(nereusCluster, kafkaCluster);
        List<KafkaPartitionId> identities = identitiesWithExactShardCardinality(keys);
        OxiaClientConfiguration configuration = configuration();

        try (SharedOxiaClientRuntime runtime =
                        SharedOxiaClientRuntime.connect(configuration, Clock.systemUTC());
                OxiaJavaKafkaPartitionMetadataStore store =
                        OxiaJavaKafkaPartitionMetadataStore.usingSharedRuntime(
                                configuration, runtime, nereusCluster, kafkaCluster)) {
            for (int start = 0; start < identities.size(); start += WRITE_WINDOW) {
                int end = Math.min(identities.size(), start + WRITE_WINDOW);
                CompletableFuture<?>[] writes = new CompletableFuture<?>[end - start];
                for (int index = start; index < end; index++) {
                    writes[index - start] = createActiveBinding(
                            store, keys, identities.get(index), index);
                }
                CompletableFuture.allOf(writes).join();
            }
        }

        Set<KafkaPartitionId> reloaded = new HashSet<>(BINDING_COUNT);
        int[] shardCounts = new int[KafkaPartitionKeyspace.REGISTRY_SHARDS];
        int[] shardPages = new int[KafkaPartitionKeyspace.REGISTRY_SHARDS];
        try (SharedOxiaClientRuntime runtime =
                        SharedOxiaClientRuntime.connect(configuration, Clock.systemUTC());
                OxiaJavaKafkaPartitionMetadataStore store =
                        OxiaJavaKafkaPartitionMetadataStore.usingSharedRuntime(
                                configuration, runtime, nereusCluster, kafkaCluster)) {
            for (int shard = 0; shard < KafkaPartitionKeyspace.REGISTRY_SHARDS; shard++) {
                Optional<String> continuation = Optional.empty();
                do {
                    KafkaPartitionScanPage page =
                            store.scanRegistry(shard, continuation, PAGE_SIZE).join();
                    shardPages[shard] = Math.addExact(shardPages[shard], 1);
                    assertThat(page.values()).hasSizeLessThanOrEqualTo(PAGE_SIZE);
                    for (VersionedKafkaPartitionRegistry hint : page.values()) {
                        VersionedKafkaPartitionBinding root =
                                store.get(hint.value().identity()).join().orElseThrow();
                        assertThat(root.key()).isEqualTo(hint.value().bindingRootKey());
                        assertThat(HexFormat.of().parseHex(root.durableValueSha256().value()))
                                .isEqualTo(hint.value().bindingRootSha256());
                        assertThat(root.value().bindingEpoch())
                                .isGreaterThanOrEqualTo(hint.value().bindingEpoch());
                        assertThat(reloaded.add(root.value().identity())).isTrue();
                        shardCounts[shard] = Math.addExact(shardCounts[shard], 1);
                    }
                    continuation = page.continuation();
                } while (continuation.isPresent());
            }
        }

        assertThat(reloaded).containsExactlyInAnyOrderElementsOf(identities);
        assertThat(shardCounts).containsOnly(BINDINGS_PER_SHARD);
        assertThat(shardPages).containsOnly(16);
    }

    private static CompletableFuture<VersionedKafkaPartitionBinding> createActiveBinding(
            OxiaJavaKafkaPartitionMetadataStore store,
            KafkaPartitionKeyspace keys,
            KafkaPartitionId id,
            int index) {
        long metadataOffset = index + 1L;
        String attempt =
                KafkaPartitionMetadataTransitions.deterministicCreateAttemptId(
                        id, metadataOffset);
        KafkaPartitionBindingRecord creating =
                KafkaPartitionMetadataTransitions.creating(
                        id,
                        "scale-topic-" + index,
                        "OBJECT_WAL_SYNC_OBJECT",
                        metadataOffset,
                        1_000,
                        new KafkaPartitionPendingOperationRecord(
                                KafkaPartitionOperationType.CREATE.wireId(),
                                attempt,
                                "scale-seeder",
                                1,
                                60_000,
                                metadataOffset,
                                1_000,
                                ""));
        return store.putCreatingIfAbsent(creating)
                .thenCompose(created -> store.compareAndSet(
                        created,
                        KafkaPartitionMetadataTransitions.activate(
                                created.value(),
                                KafkaPartitionMetadataTransitions
                                        .deterministicStreamName(id, 1),
                                "stream-scale-" + index,
                                metadataOffset,
                                1_001)))
                .thenCompose(active -> store.putRegistryHint(
                                new KafkaPartitionRegistryRecord(
                                        1,
                                        id.kafkaClusterId(),
                                        id.topicId(),
                                        id.partitionId(),
                                        keys.bindingRootKey(id),
                                        HexFormat.of().parseHex(
                                                active.durableValueSha256().value()),
                                        active.value().lifecycleId(),
                                        active.value().bindingEpoch(),
                                        1_002,
                                        0))
                        .thenApply(ignored -> active));
    }

    private static List<KafkaPartitionId> identitiesWithExactShardCardinality(
            KafkaPartitionKeyspace keys) {
        int[] remaining = new int[KafkaPartitionKeyspace.REGISTRY_SHARDS];
        java.util.Arrays.fill(remaining, BINDINGS_PER_SHARD);
        ArrayList<KafkaPartitionId> identities = new ArrayList<>(BINDING_COUNT);
        for (long candidate = 1; identities.size() < BINDING_COUNT; candidate++) {
            KafkaPartitionId id =
                    new KafkaPartitionId("kraft-scale", topicId(candidate), 0);
            int shard = keys.registryShard(id);
            if (remaining[shard] == 0) {
                continue;
            }
            remaining[shard]--;
            identities.add(id);
        }
        assertThat(remaining).containsOnly(0);
        return List.copyOf(identities);
    }

    private static String topicId(long value) {
        byte[] bytes =
                ByteBuffer.allocate(16)
                        .putLong(0x4e45524555534639L)
                        .putLong(value)
                        .array();
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    private static OxiaClientConfiguration configuration() {
        return new OxiaClientConfiguration(
                OXIA.getServiceAddress(),
                "default",
                Duration.ofSeconds(30),
                Duration.ofSeconds(60),
                512,
                1_024);
    }
}
