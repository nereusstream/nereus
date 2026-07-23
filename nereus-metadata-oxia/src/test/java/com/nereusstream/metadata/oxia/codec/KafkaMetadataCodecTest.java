/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.metadata.oxia.KafkaPartitionKeyspaceTest;
import com.nereusstream.metadata.oxia.records.KafkaCheckpointReferenceRecord;
import com.nereusstream.metadata.oxia.records.KafkaCompactionCoverageRecord;
import com.nereusstream.metadata.oxia.records.KafkaPartitionBindingRecord;
import com.nereusstream.metadata.oxia.records.KafkaPartitionLifecycle;
import com.nereusstream.metadata.oxia.records.KafkaPartitionOperationType;
import com.nereusstream.metadata.oxia.records.KafkaPartitionPendingOperationRecord;
import com.nereusstream.metadata.oxia.records.KafkaPartitionRegistryRecord;
import com.nereusstream.metadata.oxia.records.KafkaPayloadMapping;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import org.junit.jupiter.api.Test;

public class KafkaMetadataCodecTest {
    @Test
    void fullBindingAndRegistryRoundTripDeterministically() {
        KafkaPartitionBindingRecord binding = fullBinding();
        byte[] first = KafkaMetadataCodecs.encodeEnvelope(binding, KafkaPartitionBindingRecord.class);
        byte[] second = KafkaMetadataCodecs.encodeEnvelope(binding, KafkaPartitionBindingRecord.class);

        assertThat(second).isEqualTo(first);
        assertThat(KafkaMetadataCodecs.decodeEnvelope(first, KafkaPartitionBindingRecord.class))
                .isEqualTo(binding);
        assertThat(sha256(first)).isEqualTo("c196685df742d8ff9528bfa5eb4fa7e3c7a9ec8b7077818a19d100a4050ba578");

        KafkaPartitionRegistryRecord registry = new KafkaPartitionRegistryRecord(
                1, "kraft", KafkaPartitionKeyspaceTest.topicId(1), 3,
                "/nereus/root", bytes(3), KafkaPartitionLifecycle.ACTIVE.wireId(), 7, 2_000, 0);
        byte[] registryBytes = KafkaMetadataCodecs.encodeEnvelope(
                registry, KafkaPartitionRegistryRecord.class);
        assertThat(KafkaMetadataCodecs.decodeEnvelope(registryBytes, KafkaPartitionRegistryRecord.class))
                .isEqualTo(registry);
        assertThat(sha256(registryBytes)).isEqualTo("8919c79ce1e19e4128ef905b78d18e45ec49d1df4a2f2a582e2e183f249a3b55");
    }

    @Test
    void corruptionUnknownIdsAndCheckpointBoundsFailClosed() {
        byte[] encoded = KafkaMetadataCodecs.encodeEnvelope(fullBinding(), KafkaPartitionBindingRecord.class);
        encoded[encoded.length - 1] ^= 1;
        assertThatThrownBy(() -> KafkaMetadataCodecs.decodeEnvelope(
                encoded, KafkaPartitionBindingRecord.class)).isInstanceOf(MetadataCodecException.class);
        assertThatThrownBy(() -> KafkaPartitionLifecycle.fromWireId(99))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> KafkaPayloadMapping.fromWireId(99))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> KafkaPartitionOperationType.fromWireId(99))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> bindingWithCheckpoints(List.of(
                checkpoint(10, "a"), checkpoint(11, "b"))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> bindingWithCheckpoints(List.of(
                checkpoint(12, "a"), checkpoint(11, "b"), checkpoint(10, "c"), checkpoint(9, "d"))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    public static KafkaPartitionBindingRecord fullBinding() {
        return new KafkaPartitionBindingRecord(
                1, "kraft", KafkaPartitionKeyspaceTest.topicId(1), 3, "orders", 1,
                "kafka/kraft/topic/3/incarnation-1", "stream-id",
                KafkaPayloadMapping.KAFKA_RECORD_BATCH_V1.wireId(), "BOOKKEEPER_WAL_ASYNC_OBJECT",
                KafkaPartitionLifecycle.ACTIVE.wireId(), 7, 20, 25, 4, 8, 11, 5, 12,
                new KafkaCompactionCoverageRecord(1, 5, 10, 2, bytes(1), bytes(2), 1_900),
                List.of(checkpoint(12, "new"), checkpoint(8, "old")),
                KafkaPartitionPendingOperationRecord.EMPTY, 1_000, 2_000, 0);
    }

    public static KafkaPartitionBindingRecord bindingWithCheckpoints(List<KafkaCheckpointReferenceRecord> refs) {
        KafkaPartitionBindingRecord current = fullBinding();
        return new KafkaPartitionBindingRecord(
                current.formatVersion(), current.kafkaClusterId(), current.topicId(), current.partitionId(),
                current.observedTopicName(), current.incarnation(), current.streamName(), current.streamId(),
                current.payloadMappingId(), current.storageProfile(), current.lifecycleId(), current.bindingEpoch(),
                current.createdMetadataOffset(), current.lastAppliedMetadataOffset(), current.observedLeaderId(),
                current.observedLeaderEpoch(), current.observedBrokerEpoch(), current.observedLogStartOffset(),
                current.observedStableEndOffset(), current.compactionCoverage(), refs,
                current.pendingOperation(), current.createdAtMillis(), current.updatedAtMillis(), 0);
    }

    public static KafkaCheckpointReferenceRecord checkpoint(long offset, String id) {
        return new KafkaCheckpointReferenceRecord(
                1, id, "checkpoints/" + id, 100, bytes(id.hashCode()), offset, 0,
                offset, bytes(id.hashCode() + 1), "build", 1_500 + offset);
    }

    public static byte[] bytes(int seed) {
        byte[] value = new byte[32];
        for (int index = 0; index < value.length; index++) value[index] = (byte) (seed + index);
        return value;
    }

    private static String sha256(byte[] value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value));
        } catch (Exception failure) {
            throw new AssertionError(failure);
        }
    }
}
