/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ProjectionRef;
import com.nereusstream.api.ProjectionType;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamCreateOptions;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamName;
import com.nereusstream.api.StreamState;
import com.nereusstream.core.capability.GenerationProjectionAuthoritySnapshot;
import com.nereusstream.core.capability.StreamRetirementReferenceAuthoritySnapshot;
import com.nereusstream.core.physical.GcAuthorityToken;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationMetadataStoreTestFactory;
import com.nereusstream.metadata.oxia.ProjectionIdentity;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.StreamStateTransitionRequest;
import com.nereusstream.metadata.oxia.records.MaterializationStreamRegistrationRecord;
import com.nereusstream.metadata.oxia.testing.FakeOxiaMetadataStore;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointCodecV1;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class StreamRegistrationRetirementCoordinatorTest {
    private static final String CLUSTER = "cluster-registration-retirement";
    private static final Clock CLOCK = Clock.fixed(
            Instant.parse("2026-07-18T08:00:00Z"), ZoneOffset.UTC);
    private static final ProjectionRef PROJECTION =
            new ProjectionRef(ProjectionType.VIRTUAL_LEDGER, "retirement-projection");

    @Test
    void registrationIsRemovedOnlyAfterExactDeletedAuthoritiesAreRecaptured() {
        try (Fixture fixture = fixture(true, stableExternal())) {
            StreamRegistrationRetirementResult result =
                    fixture.coordinator().retire(fixture.stream()).join();

            assertThat(result.status()).isEqualTo(
                    StreamRegistrationRetirementStatus.RETIRED);
            assertThat(result.registrationRetired()).isTrue();
            assertThat(fixture.generations()
                            .getStreamRegistration(CLUSTER, fixture.stream())
                            .join())
                    .isEmpty();
        }
    }

    @Test
    void lostRegistrationDeleteResponseConvergesFromExactAbsence() {
        AtomicBoolean loseResponse = new AtomicBoolean(true);
        try (Fixture fixture = fixture(true, stableExternal(), loseResponse)) {
            StreamRegistrationRetirementResult result =
                    fixture.coordinator().retire(fixture.stream()).join();

            assertThat(result.status()).isEqualTo(
                    StreamRegistrationRetirementStatus.RETIRED);
            assertThat(loseResponse).isFalse();
            assertThat(fixture.generations()
                            .getStreamRegistration(CLUSTER, fixture.stream())
                            .join())
                    .isEmpty();
        }
    }

    @Test
    void emptyRecoveryRootDeleteResponseLossConvergesBeforeRegistrationRetirement() {
        AtomicBoolean loseRootResponse = new AtomicBoolean(true);
        try (Fixture fixture = fixture(
                true,
                stableExternal(),
                null,
                loseRootResponse,
                true,
                false)) {
            fixture.generations()
                    .getOrCreateRecoveryRoot(CLUSTER, fixture.stream())
                    .join();

            StreamRegistrationRetirementResult result =
                    fixture.coordinator().retire(fixture.stream()).join();

            assertThat(result.status()).isEqualTo(
                    StreamRegistrationRetirementStatus.RETIRED);
            assertThat(result.recoveryRootRetired()).isTrue();
            assertThat(loseRootResponse).isFalse();
            assertThat(fixture.generations()
                            .getRecoveryRoot(CLUSTER, fixture.stream())
                            .join())
                    .isEmpty();
            assertThat(fixture.generations()
                            .getStreamRegistration(CLUSTER, fixture.stream())
                            .join())
                    .isEmpty();
        }
    }

    @Test
    void finalExternalAuthorityDriftRetainsRegistration() {
        AtomicInteger captures = new AtomicInteger();
        try (Fixture fixture = fixture(
                true,
                subject -> {
                    int capture = captures.incrementAndGet();
                    return CompletableFuture.completedFuture(
                            StreamRetirementReferenceAuthoritySnapshot.complete(
                                    subject,
                                    0,
                                    List.of(new GcAuthorityToken(
                                            "/cursor/authority",
                                            capture,
                                            sha(capture == 1 ? '1' : '2')))));
                })) {
            StreamRegistrationRetirementResult result =
                    fixture.coordinator().retire(fixture.stream()).join();

            assertThat(result.status()).isEqualTo(
                    StreamRegistrationRetirementStatus.VERSION_CHANGED);
            assertThat(fixture.generations()
                            .getStreamRegistration(CLUSTER, fixture.stream())
                            .join())
                    .isPresent();
        }
    }

    @Test
    void nonDeletedL0RemainsNonDestructive() {
        try (Fixture fixture = fixture(true, stableExternal(), null, null, false, false)) {
            StreamRegistrationRetirementResult result =
                    fixture.coordinator().retire(fixture.stream()).join();

            assertThat(result.status()).isEqualTo(
                    StreamRegistrationRetirementStatus.STREAM_NOT_DELETED);
            assertThat(fixture.generations()
                            .getStreamRegistration(CLUSTER, fixture.stream())
                            .join())
                    .isPresent();
        }
    }

    private static com.nereusstream.core.capability.StreamRetirementReferenceAuthorityReader
            stableExternal() {
        return subject -> CompletableFuture.completedFuture(
                StreamRetirementReferenceAuthoritySnapshot.complete(
                        subject, 0, List.of()));
    }

    private static Fixture fixture(
            boolean enabled,
            com.nereusstream.core.capability.StreamRetirementReferenceAuthorityReader external) {
        return fixture(enabled, external, null);
    }

    private static Fixture fixture(
            boolean enabled,
            com.nereusstream.core.capability.StreamRetirementReferenceAuthorityReader external,
            AtomicBoolean loseResponse) {
        return fixture(enabled, external, loseResponse, null, true, false);
    }

    private static Fixture fixture(
            boolean enabled,
            com.nereusstream.core.capability.StreamRetirementReferenceAuthorityReader external,
            AtomicBoolean loseRegistrationResponse,
            AtomicBoolean loseRecoveryRootResponse,
            boolean deleted,
            boolean projectionLive) {
        FakeOxiaMetadataStore l0 = new FakeOxiaMetadataStore(CLOCK::millis);
        StreamId stream = new StreamId(l0.createOrGetStream(
                        CLUSTER,
                        new StreamName("registration-retirement"),
                        new StreamCreateOptions(
                                StorageProfile.OBJECT_WAL_SYNC_OBJECT,
                                Map.of()))
                .join()
                .streamId());
        if (deleted) {
            StreamMetadataSnapshot active = l0.getStreamSnapshot(CLUSTER, stream).join();
            StreamMetadataSnapshot deleting = l0.transitionStreamState(
                            CLUSTER,
                            new StreamStateTransitionRequest(
                                    stream,
                                    StreamState.ACTIVE,
                                    StreamState.DELETING,
                                    active.metadataVersion()))
                    .join();
            l0.transitionStreamState(
                            CLUSTER,
                            new StreamStateTransitionRequest(
                                    stream,
                                    StreamState.DELETING,
                                    StreamState.DELETED,
                                    deleting.metadataVersion()))
                    .join();
        }

        GenerationMetadataStore durable = GenerationMetadataStoreTestFactory.inMemory(CLOCK);
        durable.createOrVerifyStreamRegistration(
                        CLUSTER,
                        new MaterializationStreamRegistrationRecord(
                                1,
                                stream.value(),
                                ProjectionIdentity.encode(Optional.of(PROJECTION)),
                                sha('a').value(),
                                StorageProfile.OBJECT_WAL_SYNC_OBJECT.name(),
                                1,
                                0,
                                1,
                                0))
                .join();
        GenerationMetadataStore exposed = loseRegistrationResponse == null
                        && loseRecoveryRootResponse == null
                ? durable
                : loseDeleteResponses(
                        durable,
                        loseRegistrationResponse,
                        loseRecoveryRootResponse);
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        var projectionReader = (com.nereusstream.core.capability.GenerationProjectionAuthorityReader)
                subject -> CompletableFuture.completedFuture(
                        new GenerationProjectionAuthoritySnapshot(
                                subject,
                                projectionLive,
                                projectionLive
                                        ? Optional.of(new com.nereusstream.metadata.oxia.records
                                                .ManagedLedgerProjectionIdentity(
                                                        1,
                                                        1,
                                                        stream.value(),
                                                        1_000_000))
                                        : Optional.empty(),
                                List.of(new GcAuthorityToken(
                                        "/projection/authority",
                                        1,
                                        sha('b')))));
        StreamRegistrationRetirementCoordinator coordinator =
                new StreamRegistrationRetirementCoordinator(
                        CLUSTER,
                        l0,
                        exposed,
                        l0,
                        projectionReader,
                        external,
                        unavailableCheckpointCodec(),
                        config(enabled),
                        Duration.ofSeconds(1),
                        CLOCK,
                        scheduler);
        return new Fixture(stream, l0, durable, scheduler, coordinator);
    }

    private static GenerationMetadataStore loseDeleteResponses(
            GenerationMetadataStore delegate,
            AtomicBoolean loseRegistrationResponse,
            AtomicBoolean loseRecoveryRootResponse) {
        return (GenerationMetadataStore) Proxy.newProxyInstance(
                GenerationMetadataStore.class.getClassLoader(),
                new Class<?>[] {GenerationMetadataStore.class},
                (proxy, method, arguments) -> {
                    try {
                        Object result = method.invoke(delegate, arguments);
                        if (method.getName().equals("deleteStreamRegistration")
                                && loseRegistrationResponse != null
                                && loseRegistrationResponse.compareAndSet(true, false)) {
                            @SuppressWarnings("unchecked")
                            CompletableFuture<Void> deleted = (CompletableFuture<Void>) result;
                            return deleted.thenCompose(ignored -> CompletableFuture.failedFuture(
                                    new IllegalStateException("lost registration delete response")));
                        }
                        if (method.getName().equals("deleteRecoveryRoot")
                                && loseRecoveryRootResponse != null
                                && loseRecoveryRootResponse.compareAndSet(true, false)) {
                            @SuppressWarnings("unchecked")
                            CompletableFuture<Void> deleted = (CompletableFuture<Void>) result;
                            return deleted.thenCompose(ignored -> CompletableFuture.failedFuture(
                                    new IllegalStateException("lost recovery-root delete response")));
                        }
                        return result;
                    } catch (InvocationTargetException failure) {
                        throw failure.getCause();
                    }
                });
    }

    private static RecoveryCheckpointCodecV1 unavailableCheckpointCodec() {
        return (RecoveryCheckpointCodecV1) Proxy.newProxyInstance(
                RecoveryCheckpointCodecV1.class.getClassLoader(),
                new Class<?>[] {RecoveryCheckpointCodecV1.class},
                (proxy, method, arguments) -> {
                    throw new AssertionError(
                            "an absent recovery root must not call " + method.getName());
                });
    }

    private static PhysicalGcConfig config(boolean enabled) {
        return new PhysicalGcConfig(
                enabled,
                false,
                1,
                1,
                1,
                10,
                100,
                100,
                Duration.ofSeconds(1),
                Duration.ofSeconds(10),
                Duration.ofSeconds(3),
                Duration.ofMillis(5),
                Duration.ofSeconds(11),
                Duration.ofSeconds(30),
                Duration.ofHours(1),
                Duration.ofHours(2),
                Duration.ofSeconds(2),
                Duration.ofSeconds(2));
    }

    private static Checksum sha(char value) {
        return new Checksum(
                ChecksumType.SHA256, String.valueOf(value).repeat(64));
    }

    private record Fixture(
            StreamId stream,
            FakeOxiaMetadataStore l0,
            GenerationMetadataStore generations,
            ScheduledExecutorService scheduler,
            StreamRegistrationRetirementCoordinator coordinator)
            implements AutoCloseable {
        @Override
        public void close() {
            scheduler.shutdownNow();
            generations.close();
            l0.close();
        }
    }
}
