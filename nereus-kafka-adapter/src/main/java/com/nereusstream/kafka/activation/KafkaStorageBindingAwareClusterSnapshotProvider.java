/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.activation;

import com.nereusstream.metadata.oxia.KafkaPartitionKeyspace;
import com.nereusstream.metadata.oxia.KafkaPartitionMetadataStore;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Enriches a fork-owned KRaft/local-log snapshot with an exact durable Kafka-binding existence proof.
 *
 * <p>The registry is an acceleration hint, but a non-empty hint is sufficient to reject first activation. Every
 * registry shard is read from its first key; an empty result is accepted only after all 64 shards return empty.
 */
public final class KafkaStorageBindingAwareClusterSnapshotProvider
        implements KafkaStorageClusterSnapshotProvider {
    private static final int EXISTENCE_PAGE_SIZE = 1;

    private final KafkaStorageClusterSnapshotProvider delegate;
    private final KafkaPartitionMetadataStore bindings;

    public KafkaStorageBindingAwareClusterSnapshotProvider(
            KafkaStorageClusterSnapshotProvider delegate,
            KafkaPartitionMetadataStore bindings) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.bindings = Objects.requireNonNull(bindings, "bindings");
    }

    @Override
    public CompletionStage<KafkaStorageClusterSnapshot> currentSnapshot() {
        CompletionStage<KafkaStorageClusterSnapshot> supplied = Objects.requireNonNull(
                delegate.currentSnapshot(), "KRaft snapshot future");
        return supplied.thenCompose(snapshot -> {
            KafkaStorageClusterSnapshot exact = Objects.requireNonNull(snapshot, "KRaft snapshot");
            if (exact.bindingsPresent()) {
                return CompletableFuture.completedFuture(exact);
            }
            return bindingsPresent().thenApply(present -> present ? withBindings(exact) : exact);
        });
    }

    private CompletableFuture<Boolean> bindingsPresent() {
        List<CompletableFuture<Boolean>> reads =
                new ArrayList<>(KafkaPartitionKeyspace.REGISTRY_SHARDS);
        for (int shard = 0; shard < KafkaPartitionKeyspace.REGISTRY_SHARDS; shard++) {
            reads.add(bindings.scanRegistry(
                            shard, Optional.empty(), EXISTENCE_PAGE_SIZE)
                    .thenApply(page -> !page.values().isEmpty()));
        }
        return CompletableFuture.allOf(reads.toArray(CompletableFuture[]::new))
                .thenApply(ignored -> reads.stream().anyMatch(CompletableFuture::join));
    }

    private static KafkaStorageClusterSnapshot withBindings(
            KafkaStorageClusterSnapshot snapshot) {
        return new KafkaStorageClusterSnapshot(
                snapshot.kafkaClusterId(),
                snapshot.metadataOffset(),
                snapshot.kafkaFeatureLevel(),
                snapshot.brokers(),
                snapshot.topicsPresent(),
                snapshot.authoritativeLocalLogsPresent(),
                true);
    }
}
