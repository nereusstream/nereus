/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.target.BookKeeperEntryRangeReadTarget;
import com.nereusstream.api.target.ReadTargetType;
import com.nereusstream.core.StreamStorageConfig;
import com.nereusstream.core.append.DefaultGenerationZeroPhysicalReferencePublisher;
import com.nereusstream.core.capability.GenerationProtocolActivationGuard;
import com.nereusstream.core.physical.DefaultObjectProtectionManager;
import com.nereusstream.core.physical.DefaultObjectReadPinManager;
import com.nereusstream.core.read.ReadTargetReader;
import com.nereusstream.core.read.ReadTargetReaderKey;
import com.nereusstream.materialization.MaterializationConfig;
import com.nereusstream.materialization.MaterializationSourceProtection;
import com.nereusstream.materialization.MaterializationSourceProtectionAdapter;
import com.nereusstream.materialization.MaterializationSourceProvider;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationMetadataStoreTestFactory;
import com.nereusstream.metadata.oxia.testing.FakeOxiaMetadataStore;
import com.nereusstream.objectstore.testing.LocalFileObjectStore;
import com.nereusstream.objectstore.wal.DefaultWalObjectReader;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class Phase4ObjectWalRuntimeTest {
    private static final String CLUSTER = "cluster-runtime";
    private static final String PROCESS_RUN_ID =
            "a".repeat(26);
    private static final Clock CLOCK = Clock.fixed(
            Instant.parse("2026-07-16T08:00:00Z"),
            ZoneOffset.UTC);

    @TempDir
    Path root;

    @Test
    void composesReadReplayLagAndMaterializationAsOneOwnedRuntime() {
        FakeOxiaMetadataStore l0 =
                new FakeOxiaMetadataStore(CLOCK::millis);
        GenerationMetadataStore generations =
                GenerationMetadataStoreTestFactory.inMemory(CLOCK);
        LocalFileObjectStore objects =
                new LocalFileObjectStore(root.resolve("objects"));
        var protections = new DefaultObjectProtectionManager(
                CLUSTER,
                l0,
                Duration.ofMinutes(5),
                Duration.ofSeconds(5),
                Duration.ofDays(1),
                CLOCK);
        var readPins = new DefaultObjectReadPinManager(
                CLUSTER,
                "a".repeat(26),
                l0,
                Duration.ofMinutes(2),
                Duration.ofSeconds(5),
                Duration.ofDays(1),
                CLOCK);
        var physicalReferences =
                new DefaultGenerationZeroPhysicalReferencePublisher(
                        CLUSTER,
                        l0,
                        l0,
                        protections);
        ScheduledExecutorService scheduler =
                Executors.newSingleThreadScheduledExecutor();
        ExecutorService workers =
                Executors.newFixedThreadPool(4);
        ExecutorService callbacks =
                Executors.newSingleThreadExecutor();
        Phase4ObjectWalRuntime runtime = new Phase4ObjectWalRuntime(
                CLUSTER,
                PROCESS_RUN_ID,
                streamConfig(),
                MaterializationConfig.defaults(
                        root.resolve("staging").toAbsolutePath()),
                Duration.ofMinutes(1),
                l0,
                generations,
                l0,
                objects,
                new DefaultWalObjectReader(objects),
                physicalReferences,
                protections,
                readPins,
                unavailableActivationGuard(),
                List.of(bookKeeperSourceProvider()),
                scheduler,
                workers,
                callbacks,
                CLOCK);
        try {
            assertThat(runtime.profileResolver()).isNotNull();
            assertThat(runtime.readComponents()).isNotNull();
            assertThat(runtime.appendRecoverySearcher()).isNotNull();
            assertThat(runtime.generationZeroRepairScanner()).isNotNull();
            assertThat(runtime.lagSnapshotReader()).isNotNull();
            assertThat(runtime.committedGenerationRetirementAuthority()).isNotNull();
            assertThat(runtime.materializationService().isRunning())
                    .isFalse();

            runtime.start();

            assertThat(runtime.materializationService().isRunning())
                    .isTrue();
        } finally {
            runtime.close();
            callbacks.shutdownNow();
            scheduler.shutdownNow();
            readPins.close();
            protections.close();
            generations.close();
            objects.close();
            l0.close();
        }

        assertThat(workers.isShutdown()).isTrue();
        assertThat(runtime.materializationService().isRunning())
                .isFalse();
    }

    private static MaterializationSourceProvider bookKeeperSourceProvider() {
        ReadTargetReader reader = new ReadTargetReader() {
            @Override
            public ReadTargetReaderKey key() {
                return new ReadTargetReaderKey(
                        ReadTargetType.BOOKKEEPER_ENTRY_RANGE,
                        1,
                        Optional.empty(),
                        Optional.empty());
            }

            @Override
            public long reservationBytes(com.nereusstream.api.ResolvedRange range) {
                return 1;
            }
        };
        MaterializationSourceProtectionAdapter<BookKeeperEntryRangeReadTarget> protections =
                new MaterializationSourceProtectionAdapter<>() {
                    @Override
                    public ReadTargetType targetType() {
                        return ReadTargetType.BOOKKEEPER_ENTRY_RANGE;
                    }

                    @Override
                    public Class<BookKeeperEntryRangeReadTarget> targetClass() {
                        return BookKeeperEntryRangeReadTarget.class;
                    }

                    @Override
                    public CompletableFuture<MaterializationSourceProtection> acquireOrTransfer(
                            com.nereusstream.api.StreamId streamId,
                            com.nereusstream.materialization.SourceGeneration source,
                            String referenceId,
                            com.nereusstream.core.physical.ObjectProtectionOwner owner,
                            OwnerRevalidator ownerRevalidator) {
                        return unavailable();
                    }

                    @Override
                    public CompletableFuture<MaterializationSourceProtection> revalidate(
                            MaterializationSourceProtection protection,
                            OwnerRevalidator ownerRevalidator) {
                        return unavailable();
                    }

                    @Override
                    public CompletableFuture<MaterializationSourceProtection> transfer(
                            MaterializationSourceProtection protection,
                            com.nereusstream.core.physical.ObjectProtectionOwner newOwner,
                            OwnerRevalidator newOwnerRevalidator) {
                        return unavailable();
                    }

                    @Override
                    public CompletableFuture<Void> release(
                            MaterializationSourceProtection protection,
                            RemovalAuthorizer removalAuthorizer) {
                        return unavailable();
                    }

                    private <T> CompletableFuture<T> unavailable() {
                        return CompletableFuture.failedFuture(new NereusException(
                                ErrorCode.METADATA_INVARIANT_VIOLATION,
                                false,
                                "empty runtime registry must not use the BK source provider"));
                    }
                };
        return new MaterializationSourceProvider(reader, protections);
    }

    private static StreamStorageConfig streamConfig() {
        return new StreamStorageConfig(
                CLUSTER,
                "pulsar-f2/" + PROCESS_RUN_ID,
                Duration.ofSeconds(30),
                Duration.ofSeconds(10),
                Duration.ofSeconds(5),
                Duration.ofSeconds(30),
                Duration.ofSeconds(30),
                Duration.ofSeconds(75),
                64,
                10_000,
                256,
                10_000,
                1_024,
                64L << 20,
                64,
                128L << 20,
                16 << 20,
                1,
                Duration.ofSeconds(5),
                true,
                false,
                true,
                PROCESS_RUN_ID,
                Duration.ofSeconds(5),
                Duration.ofMillis(100),
                Duration.ofSeconds(5),
                Duration.ofMinutes(10),
                1_024,
                2_048);
    }

    private static GenerationProtocolActivationGuard
            unavailableActivationGuard() {
        return new GenerationProtocolActivationGuard() {
            @Override
            public CompletableFuture<com.nereusstream.core.capability.GenerationActivationProof>
                    requireReady(
                            com.nereusstream.core.capability.GenerationOperation
                                    operation,
                            com.nereusstream.core.capability.GenerationActivationSubject
                                    subject,
                            boolean activateLiveProjectionIfAbsent) {
                return CompletableFuture.failedFuture(
                        new AssertionError(
                                "an empty registry scan must not request activation"));
            }

            @Override
            public CompletableFuture<Void> revalidate(
                    com.nereusstream.core.capability.GenerationActivationProof
                            proof) {
                return CompletableFuture.failedFuture(
                        new AssertionError(
                                "an empty registry scan must not revalidate activation"));
            }
        };
    }
}
