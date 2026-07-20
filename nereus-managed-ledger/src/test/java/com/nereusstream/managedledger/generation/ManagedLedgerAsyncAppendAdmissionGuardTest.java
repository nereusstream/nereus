/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.generation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.DurabilityLevel;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamMetadata;
import com.nereusstream.api.StreamState;
import com.nereusstream.core.append.AppendAdmissionRequest;
import com.nereusstream.core.backpressure.MaterializationLagGate;
import com.nereusstream.core.backpressure.MaterializationLagSnapshot;
import com.nereusstream.core.backpressure.MaterializationLagThresholds;
import com.nereusstream.core.capability.GenerationActivationProof;
import com.nereusstream.core.capability.GenerationActivationSubject;
import com.nereusstream.core.capability.GenerationOperation;
import com.nereusstream.core.capability.GenerationProtocolActivationGuard;
import com.nereusstream.metadata.oxia.FakeManagedLedgerProjectionMetadataStore;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionNames;
import com.nereusstream.metadata.oxia.ProjectionCreateRequest;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class ManagedLedgerAsyncAppendAdmissionGuardTest {
    private static final String CLUSTER = "async-admission-cluster";
    private static final String NAME =
            "tenant/ns/persistent/async-admission";

    @Test
    void asyncAdmissionActivatesThenMeasuresAndRevalidates() {
        assertAsyncAdmissionActivatesThenMeasuresAndRevalidates(
                StorageProfile.OBJECT_WAL_ASYNC_OBJECT);
        assertAsyncAdmissionActivatesThenMeasuresAndRevalidates(
                StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT);
    }

    private static void assertAsyncAdmissionActivatesThenMeasuresAndRevalidates(
            StorageProfile profile) {
        ScheduledExecutorService scheduler =
                Executors.newSingleThreadScheduledExecutor();
        try (FakeManagedLedgerProjectionMetadataStore projections =
                new FakeManagedLedgerProjectionMetadataStore()) {
            var topic = createProjection(
                    projections,
                    profile);
            AtomicInteger activationCalls = new AtomicInteger();
            AtomicInteger revalidationCalls = new AtomicInteger();
            AtomicReference<GenerationActivationSubject> subject =
                    new AtomicReference<>();
            GenerationProtocolActivationGuard activation =
                    new GenerationProtocolActivationGuard() {
                        @Override
                        public CompletableFuture<GenerationActivationProof>
                                requireReady(
                                        GenerationOperation operation,
                                        GenerationActivationSubject exactSubject,
                                        boolean activateIfAbsent) {
                            activationCalls.incrementAndGet();
                            subject.set(exactSubject);
                            assertThat(operation).isEqualTo(
                                    GenerationOperation
                                            .GENERATION_PUBLISH);
                            assertThat(activateIfAbsent).isTrue();
                            return CompletableFuture.completedFuture(
                                    GenerationActivationProof.create(
                                            operation,
                                            exactSubject,
                                            topic.metadataVersion(),
                                            3,
                                            4,
                                            sha(),
                                            true,
                                            false,
                                            10));
                        }

                        @Override
                        public CompletableFuture<Void> revalidate(
                                GenerationActivationProof proof) {
                            revalidationCalls.incrementAndGet();
                            assertThat(proof.subject())
                                    .isEqualTo(subject.get());
                            return CompletableFuture.completedFuture(null);
                        }
                    };
            AtomicInteger lagReads = new AtomicInteger();
            MaterializationLagGate lagGate =
                    new MaterializationLagGate(
                            (stream, timeout) -> {
                                lagReads.incrementAndGet();
                                return CompletableFuture.completedFuture(
                                        new MaterializationLagSnapshot(
                                                stream,
                                                5,
                                                5,
                                                0,
                                                0,
                                                0,
                                                7,
                                                10));
                            },
                            new MaterializationLagThresholds(
                                    10,
                                    20,
                                    100,
                                    200,
                                    Duration.ofMinutes(1),
                                    Duration.ofMillis(1)),
                            scheduler);
            ManagedLedgerAsyncAppendAdmissionGuard guard =
                    new ManagedLedgerAsyncAppendAdmissionGuard(
                            CLUSTER,
                            projections,
                            activation,
                            lagGate);

            guard.admit(new AppendAdmissionRequest(
                            new com.nereusstream.api.StreamId(
                                    topic.streamId()),
                            profile,
                            DurabilityLevel.WAL_DURABLE,
                            Duration.ofSeconds(2)))
                    .join();

            assertThat(activationCalls).hasValue(1);
            assertThat(lagReads).hasValue(1);
            assertThat(revalidationCalls).hasValue(1);
            assertThat(subject.get())
                    .isInstanceOf(
                            com.nereusstream.core.capability
                                    .LiveProjectionSubject.class);
        } finally {
            scheduler.shutdownNow();
        }
    }

    @Test
    void syncProfileBypassesGenerationAdmissionAndMissingAsyncProjectionFailsClosed() {
        ScheduledExecutorService scheduler =
                Executors.newSingleThreadScheduledExecutor();
        try (FakeManagedLedgerProjectionMetadataStore projections =
                new FakeManagedLedgerProjectionMetadataStore()) {
            AtomicInteger activationCalls = new AtomicInteger();
            AtomicInteger lagReads = new AtomicInteger();
            GenerationProtocolActivationGuard activation =
                    new GenerationProtocolActivationGuard() {
                        @Override
                        public CompletableFuture<GenerationActivationProof>
                                requireReady(
                                        GenerationOperation operation,
                                        GenerationActivationSubject subject,
                                        boolean activateIfAbsent) {
                            activationCalls.incrementAndGet();
                            return CompletableFuture.failedFuture(
                                    new AssertionError(
                                            "sync append must bypass activation"));
                        }

                        @Override
                        public CompletableFuture<Void> revalidate(
                                GenerationActivationProof proof) {
                            return CompletableFuture.failedFuture(
                                    new AssertionError(
                                            "sync append must bypass revalidation"));
                        }
                    };
            MaterializationLagGate lagGate =
                    new MaterializationLagGate(
                            (stream, timeout) -> {
                                lagReads.incrementAndGet();
                                return CompletableFuture.failedFuture(
                                        new AssertionError(
                                                "sync append must bypass lag"));
                            },
                            new MaterializationLagThresholds(
                                    1,
                                    2,
                                    1,
                                    2,
                                    Duration.ofSeconds(1),
                                    Duration.ofMillis(1)),
                            scheduler);
            ManagedLedgerAsyncAppendAdmissionGuard guard =
                    new ManagedLedgerAsyncAppendAdmissionGuard(
                            CLUSTER,
                            projections,
                            activation,
                            lagGate);

            guard.admit(new AppendAdmissionRequest(
                            ManagedLedgerProjectionNames.streamId(
                                    NAME, 1),
                            StorageProfile
                                    .OBJECT_WAL_SYNC_OBJECT,
                            DurabilityLevel
                                    .WAL_DURABLE_AND_INDEX_COMMITTED,
                            Duration.ofSeconds(1)))
                    .join();

            assertThat(activationCalls).hasValue(0);
            assertThat(lagReads).hasValue(0);

            assertThatThrownBy(() -> guard.admit(
                            new AppendAdmissionRequest(
                                    ManagedLedgerProjectionNames
                                            .streamId(NAME, 1),
                                    StorageProfile
                                            .OBJECT_WAL_ASYNC_OBJECT,
                                    DurabilityLevel.WAL_DURABLE,
                                    Duration.ofSeconds(1)))
                    .join())
                    .hasRootCauseInstanceOf(NereusException.class)
                    .hasRootCauseMessage(
                            "async stream has no managed-ledger projection binding");
        } finally {
            scheduler.shutdownNow();
        }
    }

    private static com.nereusstream.metadata.oxia.records
                    .TopicProjectionRecord
            createProjection(
                    FakeManagedLedgerProjectionMetadataStore projections,
                    StorageProfile profile) {
        StreamMetadata empty = new StreamMetadata(
                ManagedLedgerProjectionNames.streamId(NAME, 1),
                ManagedLedgerProjectionNames.streamName(NAME, 1),
                StreamState.ACTIVE,
                profile,
                Map.of(
                        ManagedLedgerProjectionNames
                                .PAYLOAD_MAPPING_ATTRIBUTE,
                        ManagedLedgerProjectionNames
                                .PAYLOAD_MAPPING_V1),
                10,
                0,
                0,
                0,
                0);
        return projections.createFirstProjection(
                        CLUSTER,
                        new ProjectionCreateRequest(
                                NAME,
                                1,
                                1,
                                empty,
                                Map.of()),
                        () -> CompletableFuture.completedFuture(null))
                .join();
    }

    private static Checksum sha() {
        return new Checksum(
                ChecksumType.SHA256,
                "a".repeat(64));
    }
}
