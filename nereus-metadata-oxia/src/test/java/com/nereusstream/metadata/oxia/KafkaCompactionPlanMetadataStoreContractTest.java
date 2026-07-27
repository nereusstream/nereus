/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.metadata.oxia.records.KafkaCompactionPlanRecord;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import org.junit.jupiter.api.Test;

class KafkaCompactionPlanMetadataStoreContractTest {
    @Test
    void immutableCreateIsIdempotentAndExactVersionDeleteCannotRemoveAReplacement() {
        InMemoryPartitionedOxiaBackend backend = new InMemoryPartitionedOxiaBackend();
        OxiaJavaKafkaPartitionMetadataStore store =
                new OxiaJavaKafkaPartitionMetadataStore(
                        new PartitionedOxiaClient(backend),
                        new KafkaPartitionKeyspace("nereus", "kraft"));
        KafkaCompactionPlanRecord requested = plan();

        VersionedKafkaCompactionPlan first =
                store.putCompactionPlanIfAbsent(requested).join();
        VersionedKafkaCompactionPlan raced =
                store.putCompactionPlanIfAbsent(requested).join();

        assertThat(raced).isEqualTo(first);
        assertThat(store.getCompactionPlan(
                        requested.identity(), requested.materializationTaskId()).join())
                .contains(first);
        store.deleteCompactionPlan(first).join();
        assertThat(store.getCompactionPlan(
                        requested.identity(), requested.materializationTaskId()).join())
                .isEmpty();
        assertThatThrownBy(() -> store.deleteCompactionPlan(first).join())
                .hasRootCauseInstanceOf(F4MetadataConditionFailedException.class);
    }

    @Test
    void samePlanIdWithDifferentCanonicalBytesFailsClosed() {
        InMemoryPartitionedOxiaBackend backend = new InMemoryPartitionedOxiaBackend();
        OxiaJavaKafkaPartitionMetadataStore store =
                new OxiaJavaKafkaPartitionMetadataStore(
                        new PartitionedOxiaClient(backend),
                        new KafkaPartitionKeyspace("nereus", "kraft"));
        KafkaCompactionPlanRecord first = plan();
        store.putCompactionPlanIfAbsent(first).join();
        byte[] changed = "different-kcp1".getBytes(StandardCharsets.UTF_8);
        KafkaCompactionPlanRecord conflict =
                new KafkaCompactionPlanRecord(
                        first.formatVersion(),
                        first.kafkaClusterId(),
                        first.topicId(),
                        first.partitionId(),
                        first.streamId(),
                        first.planId(),
                        first.materializationTaskId(),
                        first.outputStartOffset(),
                        first.outputEndOffset(),
                        first.decisionEndOffset(),
                        sha256(changed),
                        changed,
                        first.createdAtMillis(),
                        0);

        assertThatThrownBy(() -> store.putCompactionPlanIfAbsent(conflict).join())
                .hasRootCauseMessage(
                        "existing Kafka compaction plan conflicts with canonical bytes");
    }

    private static KafkaCompactionPlanRecord plan() {
        byte[] bytes = "canonical-kcp1".getBytes(StandardCharsets.UTF_8);
        return new KafkaCompactionPlanRecord(
                1,
                "kraft",
                KafkaPartitionKeyspaceTest.topicId(19),
                4,
                "stream-19",
                "kcp1-" + "a".repeat(52),
                "mat1-" + "b".repeat(52),
                0,
                10,
                12,
                sha256(bytes),
                bytes,
                1_000,
                0);
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (Exception failure) {
            throw new AssertionError(failure);
        }
    }
}
