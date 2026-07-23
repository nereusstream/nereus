/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.testing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.metadata.oxia.KafkaMetadataConditionFailedException;
import com.nereusstream.metadata.oxia.KafkaPartitionId;
import com.nereusstream.metadata.oxia.KafkaPartitionKeyspaceTest;
import com.nereusstream.metadata.oxia.KafkaPartitionMetadataTransitions;
import com.nereusstream.metadata.oxia.VersionedKafkaPartitionBinding;
import com.nereusstream.metadata.oxia.codec.KafkaMetadataCodecTest;
import com.nereusstream.metadata.oxia.records.KafkaPartitionBindingRecord;
import com.nereusstream.metadata.oxia.records.KafkaPartitionLifecycle;
import com.nereusstream.metadata.oxia.records.KafkaPartitionOperationType;
import com.nereusstream.metadata.oxia.records.KafkaPartitionPendingOperationRecord;
import com.nereusstream.metadata.oxia.records.KafkaPartitionRegistryRecord;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class KafkaBindingRaceIntegrationTest {
    @Test
    void racingCreatorsConvergeAndStaleCasIsRejected() {
        FakeKafkaPartitionMetadataStore store = new FakeKafkaPartitionMetadataStore("nereus", "kraft");
        KafkaPartitionId id = new KafkaPartitionId("kraft", KafkaPartitionKeyspaceTest.topicId(21), 7);
        String attemptId = KafkaPartitionMetadataTransitions.deterministicCreateAttemptId(id, 10);
        KafkaPartitionBindingRecord creating = KafkaPartitionMetadataTransitions.creating(
                id, "orders", "BOOKKEEPER_WAL_ASYNC_OBJECT", 10, 1_000,
                operation(KafkaPartitionOperationType.CREATE, attemptId, 10));

        VersionedKafkaPartitionBinding first = store.putCreatingIfAbsent(creating).join();
        VersionedKafkaPartitionBinding raced = store.putCreatingIfAbsent(creating).join();
        assertThat(raced).isEqualTo(first);
        KafkaPartitionBindingRecord active = KafkaPartitionMetadataTransitions.activate(
                first.value(), KafkaPartitionMetadataTransitions.deterministicStreamName(id, 1),
                "stream-id", 11, 1_100);
        VersionedKafkaPartitionBinding winner = store.compareAndSet(first, active).join();
        assertThat(winner.value().lifecycle()).isEqualTo(KafkaPartitionLifecycle.ACTIVE);
        assertThatThrownBy(() -> store.compareAndSet(first, active).join())
                .isInstanceOf(CompletionException.class)
                .cause().isInstanceOf(KafkaMetadataConditionFailedException.class);
    }

    @Test
    void registryPagesStrictlyWithinOneOfSixtyFourShards() {
        FakeKafkaPartitionMetadataStore store = new FakeKafkaPartitionMetadataStore("nereus", "kraft");
        int targetShard = -1;
        int inserted = 0;
        for (int candidate = 1; candidate < 100_000 && inserted < 5; candidate++) {
            KafkaPartitionId id = new KafkaPartitionId("kraft", KafkaPartitionKeyspaceTest.topicId(candidate), 0);
            int shard = store.keyspace().registryShard(id);
            if (targetShard < 0) targetShard = shard;
            if (shard != targetShard) continue;
            store.putRegistryHint(new KafkaPartitionRegistryRecord(
                    1, id.kafkaClusterId(), id.topicId(), id.partitionId(),
                    store.keyspace().bindingRootKey(id), KafkaMetadataCodecTest.bytes(candidate),
                    KafkaPartitionLifecycle.ACTIVE.wireId(), candidate, 1_000 + candidate, 0)).join();
            inserted++;
        }
        assertThat(inserted).isEqualTo(5);

        var first = store.scanRegistry(targetShard, Optional.empty(), 2).join();
        var second = store.scanRegistry(targetShard, first.continuation(), 2).join();
        var third = store.scanRegistry(targetShard, second.continuation(), 2).join();
        assertThat(first.values()).hasSize(2);
        assertThat(second.values()).hasSize(2);
        assertThat(third.values()).hasSize(1);
        assertThat(third.continuation()).isEmpty();
        assertThat(first.values().getLast().key()).isLessThan(second.values().getFirst().key());
    }

    private static KafkaPartitionPendingOperationRecord operation(
            KafkaPartitionOperationType type, String attemptId, long offset) {
        return new KafkaPartitionPendingOperationRecord(
                type.wireId(), attemptId, "owner", 1, 2_000, offset, 1_000, "");
    }
}
