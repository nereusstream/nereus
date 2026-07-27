/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.metadata.oxia.records.KafkaCompactionPlanRecord;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Optional;
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

    @Test
    void scanIsBoundedOrderedAndContinuationIsPartitionScoped() {
        InMemoryPartitionedOxiaBackend backend = new InMemoryPartitionedOxiaBackend();
        OxiaJavaKafkaPartitionMetadataStore store =
                new OxiaJavaKafkaPartitionMetadataStore(
                        new PartitionedOxiaClient(backend),
                        new KafkaPartitionKeyspace("nereus", "kraft"));
        KafkaCompactionPlanRecord first = plan('a', 'b', 1_000);
        KafkaCompactionPlanRecord second = plan('c', 'd', 2_000);
        store.putCompactionPlanIfAbsent(second).join();
        store.putCompactionPlanIfAbsent(first).join();

        KafkaCompactionPlanScanPage firstPage =
                store.scanCompactionPlans(first.identity(), Optional.empty(), 1).join();
        KafkaCompactionPlanScanPage secondPage =
                store.scanCompactionPlans(
                                first.identity(), firstPage.continuation(), 1)
                        .join();
        KafkaCompactionPlanScanPage terminal =
                store.scanCompactionPlans(
                                first.identity(), secondPage.continuation(), 1)
                        .join();

        assertThat(firstPage.plans())
                .extracting(value -> value.value().materializationTaskId())
                .containsExactly(first.materializationTaskId());
        assertThat(secondPage.plans())
                .extracting(value -> value.value().materializationTaskId())
                .containsExactly(second.materializationTaskId());
        assertThat(terminal.plans()).isEmpty();
        assertThat(terminal.continuation()).isEmpty();
        KafkaPartitionId another =
                new KafkaPartitionId(
                        first.kafkaClusterId(), KafkaPartitionKeyspaceTest.topicId(20), 4);
        assertThatThrownBy(
                        () ->
                                store.scanCompactionPlans(
                                        another, firstPage.continuation(), 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("another partition");
    }

    private static KafkaCompactionPlanRecord plan() {
        return plan('a', 'b', 1_000);
    }

    private static KafkaCompactionPlanRecord plan(
            char planCharacter, char taskCharacter, long createdAtMillis) {
        byte[] bytes = "canonical-kcp1".getBytes(StandardCharsets.UTF_8);
        return new KafkaCompactionPlanRecord(
                1,
                "kraft",
                KafkaPartitionKeyspaceTest.topicId(19),
                4,
                "stream-19",
                "kcp1-" + Character.toString(planCharacter).repeat(52),
                "mat1-" + Character.toString(taskCharacter).repeat(52),
                0,
                10,
                12,
                sha256(bytes),
                bytes,
                createdAtMillis,
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
