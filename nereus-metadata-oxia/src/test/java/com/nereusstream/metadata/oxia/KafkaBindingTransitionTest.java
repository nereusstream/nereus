/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.metadata.oxia.codec.KafkaMetadataCodecTest;
import com.nereusstream.metadata.oxia.records.KafkaPartitionBindingRecord;
import com.nereusstream.metadata.oxia.records.KafkaPartitionLifecycle;
import com.nereusstream.metadata.oxia.records.KafkaPartitionOperationType;
import com.nereusstream.metadata.oxia.records.KafkaPartitionPendingOperationRecord;
import org.junit.jupiter.api.Test;

class KafkaBindingTransitionTest {
    @Test
    void deterministicCreateActivateDeleteTransitionsAreMonotonic() {
        KafkaPartitionId id = new KafkaPartitionId("kraft", KafkaPartitionKeyspaceTest.topicId(10), 2);
        String createAttempt = KafkaPartitionMetadataTransitions.deterministicCreateAttemptId(id, 7);
        KafkaPartitionPendingOperationRecord create = operation(
                KafkaPartitionOperationType.CREATE, createAttempt, 7, 2_000);
        KafkaPartitionBindingRecord creating = KafkaPartitionMetadataTransitions.creating(
                id, "orders", "BOOKKEEPER_WAL_ASYNC_OBJECT", 7, 1_000, create);

        KafkaPartitionBindingRecord active = KafkaPartitionMetadataTransitions.activate(
                creating, KafkaPartitionMetadataTransitions.deterministicStreamName(id, 1),
                "stream-id", 8, 1_100);

        assertThat(active.lifecycle()).isEqualTo(KafkaPartitionLifecycle.ACTIVE);
        assertThat(active.bindingEpoch()).isEqualTo(2);
        assertThat(active.pendingOperation().isEmpty()).isTrue();
        String deleteAttempt = KafkaPartitionMetadataTransitions.deterministicDeleteAttemptId(id, 9);
        KafkaPartitionBindingRecord deleting = KafkaPartitionMetadataTransitions.beginDelete(
                active, operation(KafkaPartitionOperationType.DELETE, deleteAttempt, 9, 3_000), 1_200);
        KafkaPartitionBindingRecord deleted = KafkaPartitionMetadataTransitions.markDeleted(
                deleting, deleteAttempt, 1_300);
        assertThat(deleted.lifecycle()).isEqualTo(KafkaPartitionLifecycle.DELETED);
        assertThat(deleted.bindingEpoch()).isEqualTo(4);
        assertThatThrownBy(() -> KafkaPartitionMetadataTransitions.observe(
                deleted, "orders", 8, -1, -1, -1, 0, 0, 1_400))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void checkpointPublicationRetainsThreeDescendingReferencesAndRequiresTrimCoverage() {
        KafkaPartitionBindingRecord current = KafkaMetadataCodecTest.fullBinding();

        KafkaPartitionBindingRecord updated = KafkaPartitionMetadataTransitions.prependCheckpoint(
                current, KafkaMetadataCodecTest.checkpoint(13, "latest"), 5, 20, 2_100);

        assertThat(updated.checkpointReferences()).extracting(reference -> reference.checkpointOffset())
                .containsExactly(13L, 12L, 8L);
        assertThatThrownBy(() -> KafkaPartitionMetadataTransitions.prependCheckpoint(
                current, KafkaMetadataCodecTest.checkpoint(4, "too-old"), 5, 20, 2_100))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void immutableIdentityAndMappingCannotChange() {
        KafkaPartitionBindingRecord current = KafkaMetadataCodecTest.fullBinding();
        KafkaPartitionBindingRecord changed = new KafkaPartitionBindingRecord(
                current.formatVersion(), current.kafkaClusterId(), current.topicId(), current.partitionId(),
                current.observedTopicName(), current.incarnation(), current.streamName(), current.streamId(),
                current.payloadMappingId(), "OBJECT_WAL_SYNC_OBJECT", current.lifecycleId(),
                current.bindingEpoch() + 1, current.createdMetadataOffset(), current.lastAppliedMetadataOffset(),
                current.observedLeaderId(), current.observedLeaderEpoch(), current.observedBrokerEpoch(),
                current.observedLogStartOffset(), current.observedStableEndOffset(), current.compactionCoverage(),
                current.checkpointReferences(), current.pendingOperation(), current.createdAtMillis(),
                current.updatedAtMillis() + 1, 0);
        assertThatThrownBy(() -> KafkaPartitionMetadataTransitions.validate(current, changed))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("immutable");
    }

    @Test
    void liveOperationLeaseCannotBeTakenByAnotherWorkerUsingTheSameAttempt() {
        KafkaPartitionId id = new KafkaPartitionId("kraft", KafkaPartitionKeyspaceTest.topicId(11), 2);
        String attempt = KafkaPartitionMetadataTransitions.deterministicCreateAttemptId(id, 7);
        KafkaPartitionBindingRecord current = KafkaPartitionMetadataTransitions.creating(
                id, "orders", "BOOKKEEPER_WAL_ASYNC_OBJECT", 7, 1_000,
                operation(KafkaPartitionOperationType.CREATE, attempt, 7, 2_000));
        KafkaPartitionPendingOperationRecord contender = new KafkaPartitionPendingOperationRecord(
                KafkaPartitionOperationType.CREATE.wireId(), attempt, "another-worker", 2,
                3_000, 7, 1_100, "");

        assertThatThrownBy(() -> KafkaPartitionMetadataTransitions.claimOperation(current, contender, 1_500))
                .isInstanceOf(KafkaMetadataConditionFailedException.class)
                .hasMessageContaining("another worker");
    }

    private static KafkaPartitionPendingOperationRecord operation(
            KafkaPartitionOperationType type, String attempt, long metadataOffset, long expiry) {
        return new KafkaPartitionPendingOperationRecord(
                type.wireId(), attempt, "broker-run", 1, expiry, metadataOffset, 1_000, "");
    }
}
