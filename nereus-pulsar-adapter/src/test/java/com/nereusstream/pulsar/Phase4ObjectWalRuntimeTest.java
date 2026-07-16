/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.core.StreamStorageConfig;
import com.nereusstream.core.append.DefaultGenerationZeroPhysicalReferencePublisher;
import com.nereusstream.core.capability.GenerationProtocolActivationGuard;
import com.nereusstream.core.physical.DefaultObjectProtectionManager;
import com.nereusstream.core.physical.DefaultObjectReadPinManager;
import com.nereusstream.materialization.MaterializationConfig;
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
                l0,
                generations,
                l0,
                objects,
                new DefaultWalObjectReader(objects),
                physicalReferences,
                protections,
                readPins,
                unavailableActivationGuard(),
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
