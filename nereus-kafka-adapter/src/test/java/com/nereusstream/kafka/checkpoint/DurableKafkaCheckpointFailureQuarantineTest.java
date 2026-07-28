/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.metadata.oxia.KafkaCheckpointFailureMetadataStore;
import com.nereusstream.metadata.oxia.KafkaPartitionId;
import com.nereusstream.metadata.oxia.VersionedKafkaCheckpointFailure;
import com.nereusstream.metadata.oxia.codec.KafkaMetadataCodecs;
import com.nereusstream.metadata.oxia.records.KafkaCheckpointFailureRecord;
import com.nereusstream.metadata.oxia.records.KafkaCheckpointFailureSource;
import com.nereusstream.metadata.oxia.records.KafkaCheckpointReferenceRecord;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

class DurableKafkaCheckpointFailureQuarantineTest {
    private static final KafkaPartitionId IDENTITY =
            new KafkaPartitionId("kraft", "AAAAAAAAAAAAAAAAAAAAAQ", 3);
    private static final KafkaCheckpointReferenceRecord REFERENCE =
            reference("checkpoint-object", "objects/checkpoint");
    private static final Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(5_000), ZoneOffset.UTC);

    @Test
    void persistsRedactedFirstFailureAndSkipsTheExactReferenceAfterRestart() {
        FakeStore store = new FakeStore();
        DurableKafkaCheckpointFailureQuarantine first =
                new DurableKafkaCheckpointFailureQuarantine(store, CLOCK);

        first.quarantine(
                        IDENTITY,
                        7,
                        REFERENCE,
                        KafkaCheckpointFailureSource.RECOVERY,
                        new NereusException(
                                ErrorCode.OBJECT_CHECKSUM_MISMATCH,
                                false,
                                "secret object path must not be persisted"))
                .join();

        KafkaCheckpointFailureRecord durable = store.current.orElseThrow().value();
        assertThat(durable.failureCode()).isEqualTo("OBJECT_CHECKSUM_MISMATCH");
        assertThat(durable.source()).isEqualTo(KafkaCheckpointFailureSource.RECOVERY);
        assertThat(durable.referenceSha256())
                .isEqualTo(DurableKafkaCheckpointFailureQuarantine.referenceSha256(REFERENCE));
        assertThat(
                        new String(
                                KafkaMetadataCodecs.encodeEnvelope(
                                        durable.withMetadataVersion(0),
                                        KafkaCheckpointFailureRecord.class),
                                StandardCharsets.UTF_8))
                .doesNotContain("secret object path");

        DurableKafkaCheckpointFailureQuarantine restarted =
                new DurableKafkaCheckpointFailureQuarantine(store, CLOCK);
        assertThat(restarted.isQuarantined(IDENTITY, 7, REFERENCE).join()).isTrue();
    }

    @Test
    void existingObjectIdCannotAliasChangedReferenceBytes() {
        FakeStore store = new FakeStore();
        DurableKafkaCheckpointFailureQuarantine quarantine =
                new DurableKafkaCheckpointFailureQuarantine(store, CLOCK);
        quarantine
                .quarantine(
                        IDENTITY,
                        7,
                        REFERENCE,
                        KafkaCheckpointFailureSource.RECOVERY,
                        new NereusException(ErrorCode.OBJECT_NOT_FOUND, true, "missing"))
                .join();
        KafkaCheckpointReferenceRecord changed =
                reference(REFERENCE.objectId(), "objects/different-checkpoint");

        assertThatThrownBy(() -> quarantine.isQuarantined(IDENTITY, 7, changed).join())
                .hasRootCauseMessage(
                        "durable Kafka checkpoint quarantine conflicts with exact reference");
    }

    @Test
    void transientFailureCannotBeMisclassifiedAsQuarantined() {
        DurableKafkaCheckpointFailureQuarantine quarantine =
                new DurableKafkaCheckpointFailureQuarantine(new FakeStore(), CLOCK);

        assertThatThrownBy(
                        () ->
                                quarantine
                                        .quarantine(
                                                IDENTITY,
                                                7,
                                                REFERENCE,
                                                KafkaCheckpointFailureSource.RETENTION,
                                                new NereusException(
                                                        ErrorCode.TIMEOUT,
                                                        true,
                                                        "temporary timeout"))
                                        .join())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not quarantine-eligible");
    }

    private static KafkaCheckpointReferenceRecord reference(String objectId, String objectKey) {
        return new KafkaCheckpointReferenceRecord(
                1, objectId, objectKey, 100, bytes(1), 40, 0, 8, bytes(2), "test-build", 4_000);
    }

    private static byte[] bytes(int seed) {
        byte[] value = new byte[32];
        for (int index = 0; index < value.length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }

    private static final class FakeStore implements KafkaCheckpointFailureMetadataStore {
        private Optional<VersionedKafkaCheckpointFailure> current = Optional.empty();

        @Override
        public CompletableFuture<Optional<VersionedKafkaCheckpointFailure>> get(
                KafkaPartitionId identity, long partitionIncarnation, String objectId) {
            return CompletableFuture.completedFuture(current);
        }

        @Override
        public CompletableFuture<VersionedKafkaCheckpointFailure> putIfAbsent(
                KafkaCheckpointFailureRecord value) {
            if (current.isEmpty()) {
                KafkaCheckpointFailureRecord stored = value.withMetadataVersion(1);
                current =
                        Optional.of(
                                new VersionedKafkaCheckpointFailure(
                                        "checkpoint-failure",
                                        stored,
                                        1,
                                        new Checksum(ChecksumType.SHA256, "00".repeat(32))));
            }
            return CompletableFuture.completedFuture(current.orElseThrow());
        }

        @Override
        public void close() {}
    }
}
