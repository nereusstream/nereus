/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.F4Keyspace;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationMetadataStoreTestFactory;
import com.nereusstream.metadata.oxia.records.MaterializationStreamRegistrationRecord;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class StreamRegistrationRetirementScannerTest {
    private static final String CLUSTER = "cluster-a";

    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor();

    @AfterEach
    void closeScheduler() {
        scheduler.shutdownNow();
    }

    @Test
    void scansEveryShardAndPageAndCountsEveryCoordinatorOutcome() {
        GenerationMetadataStore generations = GenerationMetadataStoreTestFactory.inMemory(
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC));
        F4Keyspace keys = new F4Keyspace(CLUSTER);
        List<StreamId> streams = streamsInShard(keys, 0, 2);
        streams.forEach(stream -> generations.createOrVerifyStreamRegistration(
                CLUSTER, registration(stream)).join());
        ArrayList<StreamId> visited = new ArrayList<>();
        StreamRegistrationRetirementScanner scanner =
                new StreamRegistrationRetirementScanner(
                        CLUSTER,
                        generations,
                        stream -> {
                            visited.add(stream);
                            StreamRegistrationRetirementStatus status =
                                    visited.size() == 1
                                            ? StreamRegistrationRetirementStatus.RETIRED
                                            : StreamRegistrationRetirementStatus.STREAM_NOT_DELETED;
                            return CompletableFuture.completedFuture(
                                    status == StreamRegistrationRetirementStatus.RETIRED
                                            ? new StreamRegistrationRetirementResult(
                                                    stream,
                                                    1,
                                                    status,
                                                    0,
                                                    0,
                                                    0,
                                                    0,
                                                    0,
                                                    0,
                                                    false,
                                                    true)
                                            : StreamRegistrationRetirementResult.simple(
                                                    stream, 1, status));
                        },
                        config(Duration.ofSeconds(2)),
                        scheduler);

        StreamRegistrationRetirementScanResult result = scanner.scan().join();

        assertThat(result.shardsScanned()).isEqualTo(64);
        assertThat(result.registrationsScanned()).isEqualTo(2);
        assertThat(result.count(StreamRegistrationRetirementStatus.RETIRED)).isEqualTo(1);
        assertThat(result.count(StreamRegistrationRetirementStatus.STREAM_NOT_DELETED))
                .isEqualTo(1);
        assertThat(visited).containsExactlyElementsOf(streams);
    }

    @Test
    void overlappingScanFailsAndAdmissionRecoversAfterTheFirstPass() {
        GenerationMetadataStore generations = GenerationMetadataStoreTestFactory.inMemory(
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC));
        F4Keyspace keys = new F4Keyspace(CLUSTER);
        StreamId stream = streamsInShard(keys, 0, 1).get(0);
        generations.createOrVerifyStreamRegistration(CLUSTER, registration(stream)).join();
        CompletableFuture<StreamRegistrationRetirementResult> held = new CompletableFuture<>();
        StreamRegistrationRetirementScanner scanner =
                new StreamRegistrationRetirementScanner(
                        CLUSTER,
                        generations,
                        ignored -> held,
                        config(Duration.ofSeconds(2)),
                        scheduler);

        CompletableFuture<StreamRegistrationRetirementScanResult> first = scanner.scan();
        assertThatThrownBy(() -> scanner.scan().join())
                .hasRootCauseMessage("a registration-retirement scan is already running");

        held.complete(StreamRegistrationRetirementResult.simple(
                stream,
                1,
                StreamRegistrationRetirementStatus.STREAM_NOT_DELETED));
        assertThat(first.join().registrationsScanned()).isEqualTo(1);
        assertThat(scanner.scan().join().registrationsScanned()).isEqualTo(1);
    }

    @Test
    void closeRejectsNewPassWithoutClosingBorrowedGenerationStore() {
        GenerationMetadataStore generations = GenerationMetadataStoreTestFactory.inMemory(
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC));
        StreamRegistrationRetirementScanner scanner =
                new StreamRegistrationRetirementScanner(
                        CLUSTER,
                        generations,
                        stream -> CompletableFuture.completedFuture(
                                StreamRegistrationRetirementResult.simple(
                                        stream,
                                        0,
                                        StreamRegistrationRetirementStatus.ALREADY_ABSENT)),
                        config(Duration.ofSeconds(2)),
                        scheduler);

        scanner.close();

        assertThatThrownBy(() -> scanner.scan().join())
                .satisfies(failure -> assertThat(unwrap(failure))
                        .isInstanceOfSatisfying(NereusException.class, error ->
                                assertThat(error.code()).isEqualTo(ErrorCode.STORAGE_CLOSED)));
        assertThat(generations.scanStreamRegistrations(
                        CLUSTER, 0, java.util.Optional.empty(), 1).join())
                .isNotNull();
    }

    private static MaterializationStreamRegistrationRecord registration(StreamId stream) {
        return new MaterializationStreamRegistrationRecord(
                1,
                stream.value(),
                "projection-" + stream.value(),
                "a".repeat(64),
                StorageProfile.OBJECT_WAL_ASYNC_OBJECT.name(),
                100,
                0,
                100,
                0);
    }

    private static List<StreamId> streamsInShard(
            F4Keyspace keys, int shard, int count) {
        ArrayList<StreamId> streams = new ArrayList<>();
        for (int index = 0; streams.size() < count; index++) {
            StreamId candidate = new StreamId("stream-" + index);
            if (keys.materializationRegistryShard(candidate) == shard) {
                streams.add(candidate);
            }
        }
        return List.copyOf(streams);
    }

    private static PhysicalGcConfig config(Duration closeTimeout) {
        return new PhysicalGcConfig(
                true,
                true,
                1,
                1,
                1,
                4_096,
                100,
                100,
                Duration.ofDays(1),
                Duration.ofSeconds(10),
                Duration.ofSeconds(3),
                Duration.ZERO,
                Duration.ofSeconds(10),
                Duration.ofMinutes(1),
                Duration.ofHours(25),
                Duration.ofDays(7),
                Duration.ofSeconds(1),
                closeTimeout);
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
