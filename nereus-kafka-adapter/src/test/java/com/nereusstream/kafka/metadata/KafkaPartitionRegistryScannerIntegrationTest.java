/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.kafka.partition.KafkaPartitionIdentity;
import com.nereusstream.kafka.testing.TestStreamStorage;
import com.nereusstream.metadata.oxia.KafkaPartitionKeyspace;
import com.nereusstream.metadata.oxia.KafkaPartitionMetadataTransitions;
import com.nereusstream.metadata.oxia.VersionedKafkaPartitionBinding;
import com.nereusstream.metadata.oxia.records.KafkaPartitionRegistryRecord;
import com.nereusstream.metadata.oxia.testing.FakeKafkaPartitionMetadataStore;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class KafkaPartitionRegistryScannerIntegrationTest {
    @Test
    void scansEveryShardWithPaginationAndReloadsAuthoritativeRoots() {
        FakeKafkaPartitionMetadataStore metadata = new FakeKafkaPartitionMetadataStore("nereus", "kraft");
        TestStreamStorage streams = new TestStreamStorage();
        KafkaPartitionLifecycleCoordinator coordinator =
                KafkaPartitionLifecycleCoordinatorTest.coordinator(metadata, streams);
        List<KafkaPartitionIdentity> identities = identitiesCoveringEveryShard(metadata.keyspace());
        for (int index = 0; index < identities.size(); index++) {
            coordinator.ensureBinding(KafkaPartitionLifecycleCoordinatorTest.request(
                    identities.get(index), 100L + index)).join();
        }

        KafkaPartitionIdentity staleHintIdentity = identities.getFirst();
        VersionedKafkaPartitionBinding active = metadata.get(staleHintIdentity.durableId()).join().orElseThrow();
        metadata.compareAndSet(active, KafkaPartitionMetadataTransitions.observe(
                active.value(), staleHintIdentity.observedTopicName(),
                active.value().lastAppliedMetadataOffset() + 1, 1, 1, 1,
                0, 0, 20_000)).join();

        List<VersionedKafkaPartitionBinding> roots = new KafkaPartitionRegistryScanner(metadata).scanAll(1).join();

        assertThat(roots).hasSize(KafkaPartitionKeyspace.REGISTRY_SHARDS);
        assertThat(roots).extracting(root -> metadata.keyspace().registryShard(root.value().identity()))
                .containsExactlyInAnyOrderElementsOf(java.util.stream.IntStream.range(0, 64).boxed().toList());
        VersionedKafkaPartitionBinding reloaded = roots.stream()
                .filter(root -> root.value().identity().equals(staleHintIdentity.durableId()))
                .findFirst().orElseThrow();
        assertThat(reloaded.value().bindingEpoch()).isEqualTo(active.value().bindingEpoch() + 1);
    }

    @Test
    void rejectsSameEpochHintWhoseRootDigestWasCorrupted() {
        FakeKafkaPartitionMetadataStore metadata = new FakeKafkaPartitionMetadataStore("nereus", "kraft");
        KafkaPartitionIdentity identity = KafkaPartitionLifecycleCoordinatorTest.identity(100, 2, "audit");
        VersionedKafkaPartitionBinding root = KafkaPartitionLifecycleCoordinatorTest.coordinator(
                metadata, new TestStreamStorage()).ensureBinding(
                        KafkaPartitionLifecycleCoordinatorTest.request(identity, 500)).join().durableRoot();
        metadata.putRegistryHint(new KafkaPartitionRegistryRecord(
                1, identity.kafkaClusterId(), identity.topicId(), identity.partition(), root.key(), new byte[32],
                root.value().lifecycleId(), root.value().bindingEpoch(), 30_000, 0)).join();

        assertThatThrownBy(() -> new KafkaPartitionRegistryScanner(metadata).scanAll(1).join())
                .isInstanceOf(CompletionException.class)
                .cause().isInstanceOf(NereusException.class)
                .extracting(failure -> ((NereusException) failure).code())
                .isEqualTo(ErrorCode.METADATA_INVARIANT_VIOLATION);
    }

    private static List<KafkaPartitionIdentity> identitiesCoveringEveryShard(KafkaPartitionKeyspace keys) {
        ArrayList<KafkaPartitionIdentity> identities = new ArrayList<>();
        Set<Integer> shards = new HashSet<>();
        for (long candidate = 1; candidate < 1_000_000 && shards.size() < 64; candidate++) {
            KafkaPartitionIdentity identity = KafkaPartitionLifecycleCoordinatorTest.identity(candidate, 0, "topic");
            if (shards.add(keys.registryShard(identity.durableId()))) identities.add(identity);
        }
        assertThat(shards).hasSize(64);
        return identities;
    }
}
