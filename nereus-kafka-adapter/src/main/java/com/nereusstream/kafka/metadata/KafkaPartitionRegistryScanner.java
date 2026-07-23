/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.metadata;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.metadata.oxia.KafkaPartitionKeyspace;
import com.nereusstream.metadata.oxia.KafkaPartitionMetadataStore;
import com.nereusstream.metadata.oxia.VersionedKafkaPartitionBinding;
import com.nereusstream.metadata.oxia.VersionedKafkaPartitionRegistry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Bounded 64-shard hint scanner that always reloads the authoritative root. */
public final class KafkaPartitionRegistryScanner {
    private final KafkaPartitionMetadataStore store;

    public KafkaPartitionRegistryScanner(KafkaPartitionMetadataStore store) {
        this.store = Objects.requireNonNull(store, "store");
    }

    public CompletableFuture<List<VersionedKafkaPartitionBinding>> scanAll(int pageSize) {
        if (pageSize <= 0 || pageSize > 1_024) {
            throw new IllegalArgumentException("pageSize must be in [1,1024]");
        }
        ArrayList<VersionedKafkaPartitionBinding> roots = new ArrayList<>();
        return scanShard(0, Optional.empty(), pageSize, roots).thenApply(ignored -> List.copyOf(roots));
    }

    private CompletableFuture<Void> scanShard(
            int shard,
            Optional<String> continuation,
            int pageSize,
            ArrayList<VersionedKafkaPartitionBinding> roots) {
        if (shard == KafkaPartitionKeyspace.REGISTRY_SHARDS) {
            return CompletableFuture.completedFuture(null);
        }
        return store.scanRegistry(shard, continuation, pageSize).thenCompose(page ->
                loadRoots(page.values(), 0, roots).thenCompose(ignored -> page.continuation().isPresent()
                        ? scanShard(shard, page.continuation(), pageSize, roots)
                        : scanShard(shard + 1, Optional.empty(), pageSize, roots)));
    }

    private CompletableFuture<Void> loadRoots(
            List<VersionedKafkaPartitionRegistry> hints,
            int index,
            ArrayList<VersionedKafkaPartitionBinding> roots) {
        if (index == hints.size()) return CompletableFuture.completedFuture(null);
        VersionedKafkaPartitionRegistry hint = hints.get(index);
        return store.get(hint.value().identity()).thenCompose(optional -> {
            optional.ifPresent(root -> {
                if (!root.key().equals(hint.value().bindingRootKey())) {
                    throw new NereusException(
                            ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                            "Kafka registry hint points at another authoritative root");
                }
                if (root.value().bindingEpoch() < hint.value().bindingEpoch()) {
                    throw new NereusException(
                            ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                            "Kafka registry hint is newer than its authoritative root");
                }
                if (root.value().bindingEpoch() == hint.value().bindingEpoch()
                        && !Arrays.equals(
                                HexFormat.of().parseHex(root.durableValueSha256().value()),
                                hint.value().bindingRootSha256())) {
                    throw new NereusException(
                            ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                            "Kafka registry hint hash disagrees with the authoritative root");
                }
                roots.add(root);
            });
            return loadRoots(hints, index + 1, roots);
        });
    }
}
