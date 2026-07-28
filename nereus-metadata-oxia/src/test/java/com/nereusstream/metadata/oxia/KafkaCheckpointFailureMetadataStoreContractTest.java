/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.metadata.oxia.records.KafkaCheckpointFailureRecord;
import com.nereusstream.metadata.oxia.records.KafkaCheckpointFailureSource;

import org.junit.jupiter.api.Test;

class KafkaCheckpointFailureMetadataStoreContractTest {
    @Test
    void immutableCreateIsIdempotentAcrossConcurrentFailureClassifications() {
        OxiaJavaKafkaCheckpointFailureMetadataStore store =
                store(new InMemoryPartitionedOxiaBackend());
        KafkaCheckpointFailureRecord first = failure(1, 2);
        KafkaCheckpointFailureRecord raced =
                new KafkaCheckpointFailureRecord(
                        1,
                        first.kafkaClusterId(),
                        first.topicId(),
                        first.partitionId(),
                        first.partitionIncarnation(),
                        first.objectId(),
                        first.referenceSha256(),
                        KafkaCheckpointFailureSource.RETENTION.wireId(),
                        "UNSUPPORTED_FORMAT",
                        bytes(3),
                        first.quarantinedAtMillis() + 1,
                        0);

        VersionedKafkaCheckpointFailure created = store.putIfAbsent(first).join();
        VersionedKafkaCheckpointFailure recovered = store.putIfAbsent(raced).join();

        assertThat(recovered).isEqualTo(created);
        assertThat(
                        store.get(first.identity(), first.partitionIncarnation(), first.objectId())
                                .join())
                .contains(created);
        assertThat(created.value().source()).isEqualTo(KafkaCheckpointFailureSource.RECOVERY);
    }

    @Test
    void sameKeyCannotAliasAnotherExactReference() {
        OxiaJavaKafkaCheckpointFailureMetadataStore store =
                store(new InMemoryPartitionedOxiaBackend());
        KafkaCheckpointFailureRecord first = failure(1, 2);
        store.putIfAbsent(first).join();
        KafkaCheckpointFailureRecord conflict =
                new KafkaCheckpointFailureRecord(
                        first.formatVersion(),
                        first.kafkaClusterId(),
                        first.topicId(),
                        first.partitionId(),
                        first.partitionIncarnation(),
                        first.objectId(),
                        bytes(9),
                        first.sourceId(),
                        first.failureCode(),
                        first.failureSha256(),
                        first.quarantinedAtMillis(),
                        0);

        assertThatThrownBy(() -> store.putIfAbsent(conflict).join())
                .hasRootCauseMessage(
                        "existing Kafka checkpoint quarantine conflicts with exact reference");
    }

    @Test
    void appliedButResponseLostCreateReloadsTheDurableWinner() {
        ResponseLossPartitionedOxiaBackend backend = new ResponseLossPartitionedOxiaBackend();
        OxiaJavaKafkaCheckpointFailureMetadataStore store = store(backend);
        KafkaCheckpointFailureRecord requested = failure(4, 5);
        backend.loseNextResponse(ResponseLossPartitionedOxiaBackend.Operation.PUT_IF_ABSENT);

        VersionedKafkaCheckpointFailure recovered = store.putIfAbsent(requested).join();

        assertThat(backend.responseWasLost()).isTrue();
        assertThat(recovered.value().withMetadataVersion(0)).isEqualTo(requested);
    }

    private static OxiaJavaKafkaCheckpointFailureMetadataStore store(
            PartitionedOxiaClient.Backend backend) {
        return new OxiaJavaKafkaCheckpointFailureMetadataStore(
                new PartitionedOxiaClient(backend), new KafkaPartitionKeyspace("nereus", "kraft"));
    }

    private static KafkaCheckpointFailureRecord failure(int referenceSeed, int failureSeed) {
        return new KafkaCheckpointFailureRecord(
                1,
                "kraft",
                KafkaPartitionKeyspaceTest.topicId(27),
                3,
                5,
                "checkpoint-object",
                bytes(referenceSeed),
                KafkaCheckpointFailureSource.RECOVERY.wireId(),
                "OBJECT_CHECKSUM_MISMATCH",
                bytes(failureSeed),
                2_000,
                0);
    }

    private static byte[] bytes(int seed) {
        byte[] value = new byte[32];
        for (int index = 0; index < value.length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
