/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.runtime;

import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import com.nereusstream.bookkeeper.BookKeeperBrokerReadiness;
import com.nereusstream.bookkeeper.BookKeeperBrokerReadinessProvider;
import com.nereusstream.bookkeeper.BookKeeperClientOperations;
import com.nereusstream.bookkeeper.BookKeeperDigestType;
import com.nereusstream.bookkeeper.BookKeeperLedgerGcConfiguration;
import com.nereusstream.bookkeeper.BookKeeperLedgerIdNamespaceProvisioningCoordinator;
import com.nereusstream.bookkeeper.BookKeeperOperationDeadline;
import com.nereusstream.bookkeeper.BookKeeperProtocolActivationCoordinator;
import com.nereusstream.bookkeeper.BookKeeperProtocolActivationUpdate;
import com.nereusstream.bookkeeper.BookKeeperSecretRef;
import com.nereusstream.bookkeeper.BookKeeperWalConfiguration;
import com.nereusstream.bookkeeper.DefaultBookKeeperClientOperations;
import com.nereusstream.bookkeeper.OxiaBookKeeperLedgerIdNamespaceReservationStore;
import com.nereusstream.bookkeeper.OxiaBookKeeperProtocolActivationStore;
import com.nereusstream.core.StreamStorageConfig;
import com.nereusstream.materialization.MaterializationConfig;
import com.nereusstream.kafka.activation.KafkaBrokerCapabilitySpecification;
import com.nereusstream.kafka.activation.KafkaStorageCapabilityDigests;
import com.nereusstream.kafka.activation.KafkaStorageClusterSnapshot;
import com.nereusstream.kafka.partition.KafkaAppendContext;
import com.nereusstream.kafka.partition.KafkaPartitionIdentity;
import com.nereusstream.kafka.partition.KafkaPartitionLeaderOpenRequest;
import com.nereusstream.kafka.partition.KafkaPartitionState;
import com.nereusstream.kafka.partition.KafkaPartitionStorage;
import com.nereusstream.kafka.partition.KafkaStableAppendResult;
import com.nereusstream.kafka.partition.KafkaStorageReadRequest;
import com.nereusstream.kafka.recovery.KafkaRecoveryState;
import com.nereusstream.kafka.recovery.KafkaRecoveryStateCodec;
import com.nereusstream.kafka.recovery.KafkaReplayBatch;
import com.nereusstream.metadata.oxia.OxiaClientConfiguration;
import com.nereusstream.metadata.oxia.BookKeeperMetadataStoreConfig;
import com.nereusstream.metadata.oxia.BookKeeperScanToken;
import com.nereusstream.metadata.oxia.BookKeeperVersionedValue;
import com.nereusstream.metadata.oxia.BookKeeperKeyspace;
import com.nereusstream.metadata.oxia.GenerationIndexIdentity;
import com.nereusstream.metadata.oxia.OxiaJavaBookKeeperMetadataStore;
import com.nereusstream.metadata.oxia.OxiaJavaGenerationMetadataStore;
import com.nereusstream.metadata.oxia.OxiaJavaKafkaPartitionMetadataStore;
import com.nereusstream.metadata.oxia.KafkaBrokerIdentity;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.metadata.oxia.KafkaStorageActivationMetadataStore;
import com.nereusstream.metadata.oxia.SharedOxiaClientRuntime;
import com.nereusstream.metadata.oxia.records.KafkaBrokerCapabilityRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerRootRecord;
import com.nereusstream.metadata.oxia.records.KafkaPayloadMapping;
import com.nereusstream.metadata.oxia.records.KafkaStorageActivationLifecycle;
import com.nereusstream.metadata.oxia.records.KafkaStorageProtocolActivationRecord;
import com.nereusstream.metadata.oxia.records.KafkaStorageReadinessRecord;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import com.nereusstream.objectstore.ObjectPutRetryPolicy;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.ObjectStoreConfiguration;
import com.nereusstream.objectstore.ObjectStoreProvider;
import com.nereusstream.objectstore.ObjectStoreSecretResolver;
import com.nereusstream.objectstore.testing.LocalFileObjectStore;
import io.netty.buffer.ByteBuf;
import io.oxia.testcontainers.OxiaContainer;
import java.net.URI;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.api.WriteAdvHandle;
import org.apache.bookkeeper.common.allocator.PoolingPolicy;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.apache.pulsar.metadata.bookkeeper.BKCluster;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class NereusKafkaObjectWalRuntimeIntegrationTest {
    private static final String OXIA_IMAGE = "oxia/oxia:0.16.3";

    @Container
    private static final OxiaContainer OXIA =
            new OxiaContainer(DockerImageName.parse(OXIA_IMAGE)).withShards(4);

    @TempDir
    Path root;

    @Test
    void activatesThenRoundTripsStableKafkaBatchThroughRealOxiaProviderGraph() {
        String nereusCluster = "f9-provider-" + java.util.UUID.randomUUID();
        String kafkaCluster = "kraft-cluster";
        String writer = "kafka-broker-1-epoch-9";
        Clock clock = Clock.systemUTC();
        LocalObjectStoreProvider provider = new LocalObjectStoreProvider(root.resolve("objects"));
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        NereusKafkaRuntime runtime = null;
        try {
            NereusKafkaRuntimeConfiguration runtimeConfiguration = new NereusKafkaRuntimeConfiguration(
                    nereusCluster,
                    kafkaCluster,
                    writer,
                    Duration.ofSeconds(30),
                    Duration.ofSeconds(5),
                    writer,
                    9,
                    Duration.ofSeconds(30),
                    100_000,
                    256 * 1024 * 1024,
                    Set.of(
                            StorageProfile.OBJECT_WAL_SYNC_OBJECT,
                            StorageProfile.OBJECT_WAL_ASYNC_OBJECT));
            OxiaClientConfiguration oxia = new OxiaClientConfiguration(
                    OXIA.getServiceAddress(),
                    "default",
                    Duration.ofSeconds(10),
                    Duration.ofSeconds(30),
                    10_000,
                    1_024);
            NereusKafkaObjectWalRuntimeConfiguration configuration =
                    new NereusKafkaObjectWalRuntimeConfiguration(
                            runtimeConfiguration,
                            streamConfiguration(nereusCluster, writer),
                            oxia,
                            objectConfiguration(provider),
                            Duration.ofMinutes(10),
                            Duration.ofSeconds(5),
                            Duration.ofHours(24),
                            2,
                            MaterializationConfig.kafkaDefaults(
                                    root.resolve(
                                                    "object-materialization")
                                            .toAbsolutePath()));
            KafkaBrokerIdentity broker = new KafkaBrokerIdentity(1, 9);
            KafkaBrokerCapabilitySpecification capability = new KafkaBrokerCapabilitySpecification(
                    kafkaCluster,
                    broker,
                    writer,
                    "4.3.0",
                    "f9-provider-test",
                    System.getProperty("java.version"),
                    Set.of(
                            StorageProfile.OBJECT_WAL_SYNC_OBJECT,
                            StorageProfile.OBJECT_WAL_ASYNC_OBJECT),
                    StorageProfile.OBJECT_WAL_SYNC_OBJECT,
                    bytes(1),
                    bytes(2),
                    bytes(3),
                    Duration.ofSeconds(1),
                    Duration.ofSeconds(30));
            KafkaStorageClusterSnapshot clusterSnapshot = new KafkaStorageClusterSnapshot(
                    kafkaCluster,
                    101,
                    KafkaStorageProtocolActivationRecord.KAFKA_FEATURE_LEVEL,
                    List.of(broker),
                    false,
                    false,
                    false);
            seedActiveAuthority(oxia, nereusCluster, capability, clusterSnapshot, clock);
            runtime = NereusKafkaObjectWalRuntimeFactory.createActivated(
                    configuration,
                    new NereusKafkaObjectWalRuntimeContext(
                            provider,
                            reference -> Optional.empty(),
                            scheduler,
                            request -> new KafkaRecoveryState<>(
                                    new EmptyRecoveryStateCodec(),
                                    recovered -> CompletableFuture.completedFuture(null)),
                            clock,
                            () -> CompletableFuture.completedFuture(null)),
                    new NereusKafkaObjectWalActivationContext(
                            capability,
                            () -> CompletableFuture.completedFuture(clusterSnapshot),
                            Duration.ofSeconds(10),
                            Duration.ofMillis(100)));
            runtime.start().toCompletableFuture().join();

            KafkaPartitionStorage storage = runtime.partitionStorageManager().openLeader(
                    new KafkaPartitionLeaderOpenRequest(
                            new KafkaPartitionIdentity(
                                    kafkaCluster,
                                    "AAAAAAAAAAAAAAAAAAAAAQ",
                                    0,
                                    "orders"),
                            1,
                            1,
                            9,
                            StorageProfile.OBJECT_WAL_SYNC_OBJECT,
                            1,
                            Duration.ofSeconds(10)))
                    .join();
            assertThat(storage.state()).isEqualTo(KafkaPartitionState.LEADER_WRITABLE);

            ByteBuffer records = MemoryRecords.withRecords(
                            0,
                            Compression.of(CompressionType.NONE).build(),
                            new SimpleRecord(1_000, "value".getBytes()))
                    .buffer()
                    .duplicate();
            KafkaStableAppendResult appendResult = storage.append(
                            records,
                            new KafkaAppendContext(
                                    0,
                                    1,
                                    (short) -1,
                                    Duration.ofSeconds(10),
                                    Map.of()))
                    .join();
            assertThat(appendResult.stableSnapshot().stableEndOffset()).isEqualTo(1);
            storage.publishDerivedOffsets(1, 1, 1);
            assertThat(storage.read(new KafkaStorageReadRequest(
                            0,
                            1,
                            10,
                            1024 * 1024,
                            1024 * 1024,
                            true,
                            0,
                            0,
                            Duration.ofSeconds(10)))
                    .join()
                    .fetchAssembly()
                    .nextLogicalOffset())
                    .isEqualTo(1);

            KafkaPartitionIdentity asyncIdentity =
                    new KafkaPartitionIdentity(
                            kafkaCluster,
                            "AAAAAAAAAAAAAAAAAAAAAw",
                            0,
                            "orders-async");
            KafkaPartitionStorage asyncStorage =
                    runtime.partitionStorageManager().openLeader(
                            new KafkaPartitionLeaderOpenRequest(
                                    asyncIdentity,
                                    1,
                                    1,
                                    9,
                                    StorageProfile.OBJECT_WAL_ASYNC_OBJECT,
                                    1,
                                    Duration.ofSeconds(10)))
                            .join();
            ByteArrayOutputStream expectedAsyncBytes =
                    new ByteArrayOutputStream();
            for (int offset = 0; offset < 4; offset++) {
                ByteBuffer asyncRecords =
                        MemoryRecords.withRecords(
                                        offset,
                                        Compression.of(
                                                        CompressionType
                                                                .NONE)
                                                .build(),
                                        new SimpleRecord(
                                                2_000 + offset,
                                                ("async-value-" + offset)
                                                        .getBytes()))
                                .buffer()
                                .duplicate();
                byte[] expected = new byte[asyncRecords.remaining()];
                asyncRecords.duplicate().get(expected);
                expectedAsyncBytes.writeBytes(expected);
                KafkaStableAppendResult asyncAppend =
                        asyncStorage
                                .append(
                                        asyncRecords,
                                        new KafkaAppendContext(
                                                offset,
                                                1,
                                                (short) -1,
                                                Duration.ofSeconds(10),
                                                Map.of()))
                                .join();
                assertThat(
                                asyncAppend
                                        .stableSnapshot()
                                        .stableEndOffset())
                        .isEqualTo(offset + 1L);
                asyncStorage.publishDerivedOffsets(
                        offset + 1L,
                        offset + 1L,
                        offset + 1L);
            }
            VersionedGenerationIndex ncp2 =
                    awaitCommittedGeneration(
                            oxia,
                            nereusCluster,
                            kafkaCluster,
                            asyncIdentity,
                            clock,
                            4);
            assertThat(ncp2.value().lifecycle())
                    .isEqualTo(GenerationLifecycle.COMMITTED);
            assertThat(ncp2.value().payloadFormat())
                    .isEqualTo("KAFKA_RECORD_BATCH");
            assertThat(ncp2.value().offsetStart()).isZero();
            assertThat(ncp2.value().offsetEnd()).isEqualTo(4);
            assertThat(
                            asyncStorage
                                    .read(
                                            new KafkaStorageReadRequest(
                                                    0,
                                                    4,
                                                    10,
                                                    1024 * 1024,
                                                    1024 * 1024,
                                                    true,
                                                    0,
                                                    0,
                                                    Duration.ofSeconds(10)))
                                    .join()
                                    .fetchAssembly()
                                    .encodedRecords())
                    .containsExactly(expectedAsyncBytes.toByteArray());
        } finally {
            if (runtime != null) {
                runtime.close();
            }
            scheduler.shutdownNow();
        }
        assertThat(provider.closed()).isTrue();
    }

    @Test
    void higherLeaderEpochTakesOverLiveBrokerAndRecoversCommittedKafkaBatch() {
        String nereusCluster =
                "f9-provider-takeover-" + java.util.UUID.randomUUID();
        String kafkaCluster = "kraft-takeover-cluster";
        String writerA = "kafka-broker-1-epoch-31";
        String writerB = "kafka-broker-2-epoch-41";
        Clock clock = Clock.systemUTC();
        OxiaClientConfiguration oxia =
                new OxiaClientConfiguration(
                        OXIA.getServiceAddress(),
                        "default",
                        Duration.ofSeconds(10),
                        Duration.ofSeconds(30),
                        10_000,
                        1_024);
        Path objectRoot = root.resolve("takeover-objects");
        LocalObjectStoreProvider providerA =
                new LocalObjectStoreProvider(objectRoot);
        LocalObjectStoreProvider providerB =
                new LocalObjectStoreProvider(objectRoot);
        ScheduledExecutorService schedulerA =
                Executors.newSingleThreadScheduledExecutor();
        ScheduledExecutorService schedulerB =
                Executors.newSingleThreadScheduledExecutor();
        KafkaBrokerIdentity brokerA = new KafkaBrokerIdentity(1, 31);
        KafkaBrokerIdentity brokerB = new KafkaBrokerIdentity(2, 41);
        Set<StorageProfile> profiles =
                Set.of(
                        StorageProfile.OBJECT_WAL_SYNC_OBJECT,
                        StorageProfile.OBJECT_WAL_ASYNC_OBJECT);
        KafkaBrokerCapabilitySpecification capabilityA =
                objectCapability(
                        kafkaCluster,
                        brokerA,
                        writerA,
                        profiles);
        KafkaBrokerCapabilitySpecification capabilityB =
                objectCapability(
                        kafkaCluster,
                        brokerB,
                        writerB,
                        profiles);
        KafkaStorageClusterSnapshot clusterSnapshot =
                new KafkaStorageClusterSnapshot(
                        kafkaCluster,
                        301,
                        KafkaStorageProtocolActivationRecord
                                .KAFKA_FEATURE_LEVEL,
                        List.of(brokerA, brokerB),
                        false,
                        false,
                        false);
        seedActiveAuthority(
                oxia,
                nereusCluster,
                List.of(capabilityA, capabilityB),
                clusterSnapshot,
                clock);

        AtomicReference<List<KafkaReplayBatch>> recoveredByA =
                new AtomicReference<>();
        AtomicReference<List<KafkaReplayBatch>> recoveredByB =
                new AtomicReference<>();
        NereusKafkaRuntime runtimeA = null;
        NereusKafkaRuntime runtimeB = null;
        try {
            runtimeA =
                    createObjectRuntime(
                            nereusCluster,
                            kafkaCluster,
                            writerA,
                            31,
                            oxia,
                            providerA,
                            schedulerA,
                            capabilityA,
                            clusterSnapshot,
                            root.resolve("takeover-materialization-a"),
                            clock,
                            recoveredByA);
            runtimeA.start().toCompletableFuture().join();

            KafkaPartitionIdentity identity =
                    new KafkaPartitionIdentity(
                            kafkaCluster,
                            "AAAAAAAAAAAAAAAAAAAAHw",
                            0,
                            "takeover-orders");
            KafkaPartitionStorage storageA =
                    runtimeA
                            .partitionStorageManager()
                            .openLeader(
                                    new KafkaPartitionLeaderOpenRequest(
                                            identity,
                                            1,
                                            7,
                                            31,
                                            StorageProfile
                                                    .OBJECT_WAL_SYNC_OBJECT,
                                            302,
                                            Duration.ofSeconds(20)))
                            .join();
            byte[] firstBatch =
                    appendKafkaBatch(
                            storageA,
                            0,
                            7,
                            7_000,
                            "broker-a-value");
            assertThat(recoveredByA.get()).isEmpty();

            runtimeB =
                    createObjectRuntime(
                            nereusCluster,
                            kafkaCluster,
                            writerB,
                            41,
                            oxia,
                            providerB,
                            schedulerB,
                            capabilityB,
                            clusterSnapshot,
                            root.resolve("takeover-materialization-b"),
                            clock,
                            recoveredByB);
            runtimeB.start().toCompletableFuture().join();
            KafkaPartitionStorage storageB =
                    runtimeB
                            .partitionStorageManager()
                            .openLeader(
                                    new KafkaPartitionLeaderOpenRequest(
                                            identity,
                                            2,
                                            8,
                                            41,
                                            StorageProfile
                                                    .OBJECT_WAL_SYNC_OBJECT,
                                            303,
                                            Duration.ofSeconds(20)))
                            .join();

            assertThat(storageB.stableSnapshot().stableEndOffset())
                    .isEqualTo(1);
            assertThat(recoveredByB.get())
                    .singleElement()
                    .satisfies(
                            replay -> {
                                assertThat(replay.baseOffset()).isZero();
                                assertThat(replay.lastOffset()).isZero();
                                assertThat(replay.encodedBatch())
                                        .containsExactly(firstBatch);
                            });

            ByteBuffer staleRecords =
                    kafkaBatch(1, 8_000, "stale-broker-a");
            assertFailureCode(
                    storageA.append(
                            staleRecords,
                            new KafkaAppendContext(
                                    1,
                                    7,
                                    (short) -1,
                                    Duration.ofSeconds(10),
                                    Map.of())),
                    ErrorCode.FENCED_APPEND);
            assertThat(storageA.state())
                    .isEqualTo(
                            KafkaPartitionState
                                    .WRITE_FENCED_RECOVERY_REQUIRED);

            ByteBuffer currentRecords =
                    kafkaBatch(1, 8_001, "current-broker-b");
            byte[] secondBatch =
                    new byte[currentRecords.remaining()];
            currentRecords.duplicate().get(secondBatch);
            KafkaStableAppendResult currentAppend =
                    storageB
                            .append(
                                    currentRecords,
                                    new KafkaAppendContext(
                                            1,
                                            8,
                                            (short) -1,
                                            Duration.ofSeconds(10),
                                            Map.of()))
                            .join();
            assertThat(
                            currentAppend
                                    .stableSnapshot()
                                    .stableEndOffset())
                    .isEqualTo(2);
            storageB.publishDerivedOffsets(2, 2, 2);

            ByteArrayOutputStream expected =
                    new ByteArrayOutputStream();
            expected.writeBytes(firstBatch);
            expected.writeBytes(secondBatch);
            assertThat(
                            storageB
                                    .read(
                                            new KafkaStorageReadRequest(
                                                    0,
                                                    2,
                                                    10,
                                                    1024 * 1024,
                                                    1024 * 1024,
                                                    true,
                                                    0,
                                                    0,
                                                    Duration.ofSeconds(20)))
                                    .join()
                                    .fetchAssembly()
                                    .encodedRecords())
                    .containsExactly(expected.toByteArray());
            assertThat(
                            runtimeA
                                    .partitionStorageManager()
                                    .current(identity))
                    .contains(storageA);
            assertThat(
                            runtimeB
                                    .partitionStorageManager()
                                    .current(identity))
                    .contains(storageB);
        } finally {
            if (runtimeB != null) runtimeB.close();
            if (runtimeA != null) runtimeA.close();
            schedulerB.shutdownNow();
            schedulerA.shutdownNow();
        }
        assertThat(providerA.closed()).isTrue();
        assertThat(providerB.closed()).isTrue();
    }

    @Test
    void activatesThenRoundTripsKafkaBatchThroughRealBookKeeperWalOnlyGraph()
            throws Exception {
        String nereusCluster =
                "f9-bookkeeper-provider-" + java.util.UUID.randomUUID();
        String kafkaCluster = "kraft-bookkeeper-cluster";
        String writer = "kafka-broker-1-epoch-11";
        String deployment = "kafka-deployment-a";
        Clock clock = Clock.systemUTC();
        OxiaClientConfiguration oxia = new OxiaClientConfiguration(
                OXIA.getServiceAddress(),
                "default",
                Duration.ofSeconds(10),
                Duration.ofSeconds(30),
                10_000,
                1_024);
        BookKeeperWalConfiguration bookKeeperConfiguration =
                bookKeeperConfiguration();
        String readinessSha256 = "55".repeat(32);
        seedBookKeeperAuthority(
                oxia,
                bookKeeperConfiguration,
                deployment,
                readinessSha256,
                clock);

        LocalObjectStoreProvider provider =
                new LocalObjectStoreProvider(root.resolve("bookkeeper-objects"));
        ScheduledExecutorService scheduler =
                Executors.newSingleThreadScheduledExecutor();
        NereusKafkaRuntime runtime = null;
        String metadataServiceUri = "oxia://" + OXIA.getServiceAddress();
        try (BKCluster bookKeeperCluster =
                startBookKeeper(metadataServiceUri)) {
            BookKeeper client = bookKeeperCluster.newClient();
            try {
            Set<StorageProfile> profiles = Set.of(
                    StorageProfile.OBJECT_WAL_SYNC_OBJECT,
                    StorageProfile.OBJECT_WAL_ASYNC_OBJECT,
                    StorageProfile.BOOKKEEPER_WAL_ONLY,
                    StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT,
                    StorageProfile.BOOKKEEPER_WAL_SYNC_OBJECT);
            NereusKafkaRuntimeConfiguration runtimeConfiguration =
                    new NereusKafkaRuntimeConfiguration(
                            nereusCluster,
                            kafkaCluster,
                            writer,
                            Duration.ofSeconds(30),
                            Duration.ofSeconds(5),
                            writer,
                            11,
                            Duration.ofSeconds(30),
                            100_000,
                            256 * 1024 * 1024,
                            profiles);
            NereusKafkaObjectWalRuntimeConfiguration configuration =
                    new NereusKafkaObjectWalRuntimeConfiguration(
                            runtimeConfiguration,
                            streamConfiguration(nereusCluster, writer),
                            oxia,
                            objectConfiguration(provider),
                            Duration.ofMinutes(10),
                            Duration.ofSeconds(5),
                            Duration.ofHours(24),
                            2,
                            MaterializationConfig.kafkaDefaults(
                                    root.resolve(
                                                    "bookkeeper-materialization")
                                            .toAbsolutePath()),
                            Optional.of(
                                    new NereusKafkaBookKeeperWalRuntimeConfiguration(
                                            deployment,
                                            bookKeeperConfiguration)));
            KafkaBrokerIdentity broker = new KafkaBrokerIdentity(1, 11);
            KafkaBrokerCapabilitySpecification capability =
                    new KafkaBrokerCapabilitySpecification(
                            kafkaCluster,
                            broker,
                            writer,
                            "4.3.0",
                            "f9-bookkeeper-provider-test",
                            System.getProperty("java.version"),
                            profiles,
                            StorageProfile.BOOKKEEPER_WAL_ONLY,
                            bytes(11),
                            bytes(12),
                            bytes(13),
                            Duration.ofSeconds(1),
                            Duration.ofSeconds(30));
            KafkaStorageClusterSnapshot clusterSnapshot =
                    new KafkaStorageClusterSnapshot(
                            kafkaCluster,
                            111,
                            KafkaStorageProtocolActivationRecord
                                    .KAFKA_FEATURE_LEVEL,
                            List.of(broker),
                            false,
                            false,
                            false);
            seedActiveAuthority(
                    oxia,
                    nereusCluster,
                    capability,
                    clusterSnapshot,
                    clock);
            runtime = NereusKafkaObjectWalRuntimeFactory.createActivated(
                    configuration,
                    new NereusKafkaObjectWalRuntimeContext(
                            provider,
                            reference -> Optional.empty(),
                            scheduler,
                            request -> new KafkaRecoveryState<>(
                                    new EmptyRecoveryStateCodec(),
                                    recovered -> CompletableFuture.completedFuture(null)),
                            clock,
                            () -> CompletableFuture.completedFuture(null),
                            Optional.of(
                                    new NereusKafkaBookKeeperWalRuntimeContext(
                                            client,
                                            readinessProvider(
                                                    readinessSha256),
                                            ignored ->
                                                    "f9-bookkeeper-password"
                                                            .getBytes(
                                                                    java.nio.charset
                                                                            .StandardCharsets
                                                                            .UTF_8)))),
                    new NereusKafkaObjectWalActivationContext(
                            capability,
                            () -> CompletableFuture.completedFuture(
                                    clusterSnapshot),
                            Duration.ofSeconds(10),
                            Duration.ofMillis(100)));
            runtime.start().toCompletableFuture().join();

            KafkaPartitionStorage storage =
                    runtime.partitionStorageManager().openLeader(
                            new KafkaPartitionLeaderOpenRequest(
                                    new KafkaPartitionIdentity(
                                            kafkaCluster,
                                            "AAAAAAAAAAAAAAAAAAAAAg",
                                            0,
                                            "bookkeeper-orders"),
                                    1,
                                    1,
                                    11,
                                    StorageProfile.BOOKKEEPER_WAL_ONLY,
                                    1,
                                    Duration.ofSeconds(10)))
                            .join();
            assertThat(storage.state())
                    .isEqualTo(KafkaPartitionState.LEADER_WRITABLE);

            ByteBuffer records = MemoryRecords.withRecords(
                            0,
                            Compression.of(CompressionType.NONE).build(),
                            new SimpleRecord(
                                    2_000,
                                    "bookkeeper-value".getBytes()))
                    .buffer()
                    .duplicate();
            KafkaStableAppendResult appendResult = storage.append(
                            records,
                            new KafkaAppendContext(
                                    0,
                                    1,
                                    (short) -1,
                                    Duration.ofSeconds(10),
                                    Map.of()))
                    .join();
            assertThat(
                            appendResult
                                    .stableSnapshot()
                                    .stableEndOffset())
                    .isEqualTo(1);
            storage.publishDerivedOffsets(1, 1, 1);
            assertThat(storage.read(new KafkaStorageReadRequest(
                                    0,
                                    1,
                                    10,
                                    1024 * 1024,
                                    1024 * 1024,
                                    true,
                                    0,
                                    0,
                                    Duration.ofSeconds(10)))
                            .join()
                            .fetchAssembly()
                            .nextLogicalOffset())
                    .isEqualTo(1);

            KafkaPartitionIdentity asyncIdentity =
                    new KafkaPartitionIdentity(
                            kafkaCluster,
                            "AAAAAAAAAAAAAAAAAAAABA",
                            0,
                            "bookkeeper-orders-async");
            KafkaPartitionStorage asyncStorage =
                    runtime.partitionStorageManager().openLeader(
                            new KafkaPartitionLeaderOpenRequest(
                                    asyncIdentity,
                                    1,
                                    1,
                                    11,
                                    StorageProfile
                                            .BOOKKEEPER_WAL_ASYNC_OBJECT,
                                    1,
                                    Duration.ofSeconds(10)))
                            .join();
            byte[] expectedAsyncBytes =
                    appendKafkaBatches(
                            asyncStorage,
                            4,
                            3_000,
                            "bookkeeper-async-value-");
            VersionedGenerationIndex asyncNcp2 =
                    awaitCommittedGeneration(
                            oxia,
                            nereusCluster,
                            kafkaCluster,
                            asyncIdentity,
                            clock,
                            4);
            assertThat(asyncNcp2.value().lifecycle())
                    .isEqualTo(GenerationLifecycle.COMMITTED);
            assertThat(asyncNcp2.value().payloadFormat())
                    .isEqualTo("KAFKA_RECORD_BATCH");
            assertThat(
                            asyncStorage
                                    .read(
                                            new KafkaStorageReadRequest(
                                                    0,
                                                    4,
                                                    10,
                                                    1024 * 1024,
                                                    1024 * 1024,
                                                    true,
                                                    0,
                                                    0,
                                                    Duration.ofSeconds(10)))
                                    .join()
                                    .fetchAssembly()
                                    .encodedRecords())
                    .containsExactly(expectedAsyncBytes);

            KafkaPartitionIdentity syncIdentity =
                    new KafkaPartitionIdentity(
                            kafkaCluster,
                            "AAAAAAAAAAAAAAAAAAAABQ",
                            0,
                            "bookkeeper-orders-sync");
            KafkaPartitionStorage syncStorage =
                    runtime.partitionStorageManager().openLeader(
                            new KafkaPartitionLeaderOpenRequest(
                                    syncIdentity,
                                    1,
                                    1,
                                    11,
                                    StorageProfile
                                            .BOOKKEEPER_WAL_SYNC_OBJECT,
                                    1,
                                    Duration.ofSeconds(10)))
                            .join();
            byte[] expectedSyncBytes =
                    appendKafkaBatches(
                            syncStorage,
                            1,
                            4_000,
                            "bookkeeper-sync-value-");
            VersionedGenerationIndex syncNcp2 =
                    awaitCommittedGeneration(
                            oxia,
                            nereusCluster,
                            kafkaCluster,
                            syncIdentity,
                            clock,
                            1);
            assertThat(syncNcp2.value().lifecycle())
                    .isEqualTo(GenerationLifecycle.COMMITTED);
            assertThat(syncNcp2.value().payloadFormat())
                    .isEqualTo("KAFKA_RECORD_BATCH");
            assertThat(
                            syncStorage
                                    .read(
                                            new KafkaStorageReadRequest(
                                                    0,
                                                    1,
                                                    10,
                                                    1024 * 1024,
                                                    1024 * 1024,
                                                    true,
                                                    0,
                                                    0,
                                                    Duration.ofSeconds(10)))
                                    .join()
                                    .fetchAssembly()
                                    .encodedRecords())
                    .containsExactly(expectedSyncBytes);
            } finally {
                if (runtime != null) {
                    runtime.close();
                    runtime = null;
                }
                client.close();
            }
        } finally {
            scheduler.shutdownNow();
        }
        assertThat(provider.closed()).isTrue();
    }

    @Test
    void activatesKafkaProofThenPhysicallyDeletesSealedBookKeeperLedger()
            throws Exception {
        String nereusCluster =
                "f9-bookkeeper-gc-" + java.util.UUID.randomUUID();
        String kafkaCluster = "kraft-bookkeeper-gc";
        String writer = "kafka-broker-1-epoch-21";
        String deployment = "kafka-deployment-gc";
        Clock clock = Clock.systemUTC();
        OxiaClientConfiguration oxia =
                new OxiaClientConfiguration(
                        OXIA.getServiceAddress(),
                        "default",
                        Duration.ofSeconds(10),
                        Duration.ofSeconds(30),
                        10_000,
                        1_024);
        BookKeeperWalConfiguration bookKeeperConfiguration =
                bookKeeperDeletionConfiguration();
        BookKeeperLedgerGcConfiguration ledgerGc =
                new BookKeeperLedgerGcConfiguration(
                        1,
                        Duration.ZERO,
                        bookKeeperConfiguration.readerLeaseTtl(),
                        Duration.ofSeconds(1),
                        true,
                        false);
        String readinessSha256 = "66".repeat(32);
        seedBookKeeperAuthority(
                oxia,
                bookKeeperConfiguration,
                deployment,
                readinessSha256,
                clock);

        LocalObjectStoreProvider provider =
                new LocalObjectStoreProvider(
                        root.resolve("bookkeeper-gc-objects"));
        ScheduledExecutorService scheduler =
                Executors.newSingleThreadScheduledExecutor();
        NereusKafkaRuntime runtime = null;
        String metadataServiceUri =
                "oxia://" + OXIA.getServiceAddress();
        try (BKCluster bookKeeperCluster =
                startBookKeeper(metadataServiceUri)) {
            BookKeeper client = bookKeeperCluster.newClient();
            try {
                AppliedDeleteResponseLossOperations deletionOperations =
                        new AppliedDeleteResponseLossOperations(
                                new DefaultBookKeeperClientOperations(
                                        client));
                Set<StorageProfile> profiles =
                        Set.of(
                                StorageProfile.OBJECT_WAL_SYNC_OBJECT,
                                StorageProfile.OBJECT_WAL_ASYNC_OBJECT,
                                StorageProfile.BOOKKEEPER_WAL_ONLY,
                                StorageProfile
                                        .BOOKKEEPER_WAL_ASYNC_OBJECT,
                                StorageProfile
                                        .BOOKKEEPER_WAL_SYNC_OBJECT);
                NereusKafkaRuntimeConfiguration runtimeConfiguration =
                        new NereusKafkaRuntimeConfiguration(
                                nereusCluster,
                                kafkaCluster,
                                writer,
                                Duration.ofSeconds(30),
                                Duration.ofSeconds(5),
                                writer,
                                21,
                                Duration.ofSeconds(30),
                                100_000,
                                256 * 1024 * 1024,
                                profiles);
                NereusKafkaObjectWalRuntimeConfiguration configuration =
                        new NereusKafkaObjectWalRuntimeConfiguration(
                                runtimeConfiguration,
                                streamConfiguration(
                                        nereusCluster,
                                        writer),
                                oxia,
                                objectConfiguration(provider),
                                Duration.ofMinutes(10),
                                Duration.ofSeconds(5),
                                Duration.ofHours(24),
                                2,
                                bookKeeperDeletionMaterializationConfig(
                                        root.resolve(
                                                        "bookkeeper-gc-materialization")
                                                .toAbsolutePath()),
                                Optional.of(
                                        new NereusKafkaBookKeeperWalRuntimeConfiguration(
                                                deployment,
                                                bookKeeperConfiguration,
                                                ledgerGc)));
                KafkaBrokerIdentity broker =
                        new KafkaBrokerIdentity(1, 21);
                KafkaBrokerCapabilitySpecification capability =
                        new KafkaBrokerCapabilitySpecification(
                                kafkaCluster,
                                broker,
                                writer,
                                "4.3.0",
                                "f9-bookkeeper-gc-test",
                                System.getProperty("java.version"),
                                profiles,
                                StorageProfile
                                        .BOOKKEEPER_WAL_ASYNC_OBJECT,
                                bytes(21),
                                bytes(22),
                                bytes(23),
                                Duration.ofSeconds(1),
                                Duration.ofMinutes(5));
                KafkaStorageClusterSnapshot clusterSnapshot =
                        new KafkaStorageClusterSnapshot(
                                kafkaCluster,
                                211,
                                KafkaStorageProtocolActivationRecord
                                        .KAFKA_FEATURE_LEVEL,
                                List.of(broker),
                                false,
                                false,
                                false);
                seedActiveAuthority(
                        oxia,
                        nereusCluster,
                        capability,
                        clusterSnapshot,
                        clock);
                runtime =
                        NereusKafkaObjectWalRuntimeFactory
                                .createActivated(
                                        configuration,
                                        new NereusKafkaObjectWalRuntimeContext(
                                                provider,
                                                reference -> Optional.empty(),
                                                scheduler,
                                                request ->
                                                        new KafkaRecoveryState<>(
                                                                new EmptyRecoveryStateCodec(),
                                                                recovered ->
                                                                        CompletableFuture
                                                                                .completedFuture(
                                                                                        null)),
                                                clock,
                                                () ->
                                                        CompletableFuture
                                                                .completedFuture(
                                                                        null),
                                                Optional.of(
                                                        new NereusKafkaBookKeeperWalRuntimeContext(
                                                                client,
                                                                readinessProvider(
                                                                        readinessSha256),
                                                                ignored ->
                                                                        "f9-bookkeeper-password"
                                                                                .getBytes(
                                                                                        java.nio.charset
                                                                                                .StandardCharsets
                                                                                                .UTF_8),
                                                                deletionOperations))),
                                        new NereusKafkaObjectWalActivationContext(
                                                capability,
                                                () ->
                                                        CompletableFuture
                                                                .completedFuture(
                                                                        clusterSnapshot),
                                                Duration.ofSeconds(10),
                                                Duration.ofMillis(100)));
                runtime.start().toCompletableFuture().join();
                deletionOperations.arm();

                KafkaPartitionIdentity identity =
                        new KafkaPartitionIdentity(
                                kafkaCluster,
                                "AAAAAAAAAAAAAAAAAAAAGA",
                                0,
                                "bookkeeper-gc-orders");
                KafkaPartitionStorage storage =
                        runtime.partitionStorageManager()
                                .openLeader(
                                        new KafkaPartitionLeaderOpenRequest(
                                                identity,
                                                1,
                                                1,
                                                21,
                                                StorageProfile
                                                        .BOOKKEEPER_WAL_ASYNC_OBJECT,
                                                1,
                                                Duration.ofSeconds(20)))
                                .join();
                byte[] expected =
                        appendKafkaBatches(
                                storage,
                                2,
                                5_000,
                                "bookkeeper-gc-value-");
                VersionedGenerationIndex ncp2 =
                        awaitCommittedGeneration(
                                oxia,
                                nereusCluster,
                                kafkaCluster,
                                identity,
                                clock,
                                2);
                assertThat(ncp2.value().lifecycle())
                        .isEqualTo(GenerationLifecycle.COMMITTED);

                StreamId streamId =
                        partitionStreamId(
                                oxia,
                                nereusCluster,
                                kafkaCluster,
                                identity,
                                clock);
                long deletedLedgerId;
                BookKeeperMetadataStoreConfig metadataConfiguration =
                        bookKeeperMetadataConfiguration(
                                bookKeeperConfiguration);
                try (SharedOxiaClientRuntime inspector =
                                SharedOxiaClientRuntime.connect(
                                        oxia,
                                        clock);
                        OxiaJavaBookKeeperMetadataStore metadata =
                                OxiaJavaBookKeeperMetadataStore
                                        .usingSharedRuntime(
                                                oxia,
                                                inspector,
                                                clock,
                                                metadataConfiguration);
                        OxiaJavaGenerationMetadataStore generations =
                                OxiaJavaGenerationMetadataStore
                                        .usingSharedRuntime(
                                                oxia,
                                                inspector,
                                                clock)) {
                    BookKeeperVersionedValue<BookKeeperLedgerRootRecord>
                            retired =
                                    awaitRetiredLedgerRoot(
                                            metadata,
                                            nereusCluster,
                                            streamId,
                                            Duration.ofSeconds(30));
                    deletedLedgerId = retired.value().ledgerId();
                    BookKeeperVersionedValue<BookKeeperLedgerRootRecord>
                            deleted =
                                    awaitDeletedLedgerRoot(
                                            metadata,
                                            nereusCluster,
                                            bookKeeperConfiguration
                                                    .providerScopeSha256(),
                                            deletedLedgerId,
                                            generations,
                                            streamId,
                                            Duration.ofSeconds(35));
                    assertThat(deleted.value().lifecycle())
                            .isEqualTo(
                                    BookKeeperLedgerLifecycle.DELETED);
                }
                assertPhysicalLedgerAbsent(
                        client,
                        deletedLedgerId);
                assertThat(deletionOperations.responseLossInjected())
                        .isTrue();
                assertThat(deletionOperations.injectedLedgerId())
                        .isEqualTo(deletedLedgerId);
                assertThat(
                                storage.read(
                                                new KafkaStorageReadRequest(
                                                        0,
                                                        2,
                                                        10,
                                                        1024 * 1024,
                                                        1024 * 1024,
                                                        true,
                                                        0,
                                                        0,
                                                        Duration.ofSeconds(
                                                                20)))
                                        .join()
                                        .fetchAssembly()
                                        .encodedRecords())
                        .containsExactly(expected);
            } finally {
                if (runtime != null) {
                    runtime.close();
                    runtime = null;
                }
                client.close();
            }
        } finally {
            scheduler.shutdownNow();
        }
        assertThat(provider.closed()).isTrue();
    }

    private static byte[] appendKafkaBatches(
            KafkaPartitionStorage storage,
            int batchCount,
            long timestampBase,
            String valuePrefix) {
        ByteArrayOutputStream expectedBytes = new ByteArrayOutputStream();
        for (int offset = 0; offset < batchCount; offset++) {
            ByteBuffer records =
                    MemoryRecords.withRecords(
                                    offset,
                                    Compression.of(
                                                    CompressionType
                                                            .NONE)
                                            .build(),
                                    new SimpleRecord(
                                            timestampBase + offset,
                                            (valuePrefix + offset)
                                                    .getBytes()))
                            .buffer()
                            .duplicate();
            byte[] batchBytes = new byte[records.remaining()];
            records.duplicate().get(batchBytes);
            expectedBytes.writeBytes(batchBytes);
            KafkaStableAppendResult appendResult =
                    storage
                            .append(
                                    records,
                                    new KafkaAppendContext(
                                            offset,
                                            1,
                                            (short) -1,
                                            Duration.ofSeconds(10),
                                            Map.of()))
                            .join();
            assertThat(
                            appendResult
                                    .stableSnapshot()
                                    .stableEndOffset())
                    .isEqualTo(offset + 1L);
            storage.publishDerivedOffsets(
                    offset + 1L,
                    offset + 1L,
                    offset + 1L);
        }
        return expectedBytes.toByteArray();
    }

    private static byte[] appendKafkaBatch(
            KafkaPartitionStorage storage,
            long offset,
            int leaderEpoch,
            long timestamp,
            String value) {
        ByteBuffer records =
                kafkaBatch(offset, timestamp, value);
        byte[] expected = new byte[records.remaining()];
        records.duplicate().get(expected);
        KafkaStableAppendResult result =
                storage
                        .append(
                                records,
                                new KafkaAppendContext(
                                        offset,
                                        leaderEpoch,
                                        (short) -1,
                                        Duration.ofSeconds(10),
                                        Map.of()))
                        .join();
        assertThat(result.stableSnapshot().stableEndOffset())
                .isEqualTo(offset + 1);
        storage.publishDerivedOffsets(
                offset + 1,
                offset + 1,
                offset + 1);
        return expected;
    }

    private static ByteBuffer kafkaBatch(
            long offset, long timestamp, String value) {
        return MemoryRecords.withRecords(
                        offset,
                        Compression.of(CompressionType.NONE).build(),
                        new SimpleRecord(timestamp, value.getBytes()))
                .buffer()
                .duplicate();
    }

    private static NereusKafkaRuntime createObjectRuntime(
            String nereusCluster,
            String kafkaCluster,
            String writer,
            long brokerEpoch,
            OxiaClientConfiguration oxia,
            LocalObjectStoreProvider provider,
            ScheduledExecutorService scheduler,
            KafkaBrokerCapabilitySpecification capability,
            KafkaStorageClusterSnapshot clusterSnapshot,
            Path materializationRoot,
            Clock clock,
            AtomicReference<List<KafkaReplayBatch>> recoveredBatches) {
        Set<StorageProfile> profiles =
                Set.of(
                        StorageProfile.OBJECT_WAL_SYNC_OBJECT,
                        StorageProfile.OBJECT_WAL_ASYNC_OBJECT);
        NereusKafkaRuntimeConfiguration runtimeConfiguration =
                new NereusKafkaRuntimeConfiguration(
                        nereusCluster,
                        kafkaCluster,
                        writer,
                        Duration.ofSeconds(30),
                        Duration.ofSeconds(5),
                        writer,
                        brokerEpoch,
                        Duration.ofSeconds(30),
                        100_000,
                        256 * 1024 * 1024,
                        profiles);
        NereusKafkaObjectWalRuntimeConfiguration configuration =
                new NereusKafkaObjectWalRuntimeConfiguration(
                        runtimeConfiguration,
                        streamConfiguration(nereusCluster, writer),
                        oxia,
                        objectConfiguration(provider),
                        Duration.ofMinutes(10),
                        Duration.ofSeconds(5),
                        Duration.ofHours(24),
                        2,
                        MaterializationConfig.kafkaDefaults(
                                materializationRoot.toAbsolutePath()));
        return NereusKafkaObjectWalRuntimeFactory.createActivated(
                configuration,
                new NereusKafkaObjectWalRuntimeContext(
                        provider,
                        reference -> Optional.empty(),
                        scheduler,
                        request ->
                                new KafkaRecoveryState<>(
                                        new AcceptingRecoveryStateCodec(),
                                        recovered -> {
                                            recoveredBatches.set(
                                                    List.copyOf(
                                                            recovered
                                                                    .state()));
                                            return CompletableFuture
                                                    .completedFuture(null);
                                        }),
                        clock,
                        () -> CompletableFuture.completedFuture(null)),
                new NereusKafkaObjectWalActivationContext(
                        capability,
                        () -> CompletableFuture.completedFuture(
                                clusterSnapshot),
                        Duration.ofSeconds(10),
                        Duration.ofMillis(100)));
    }

    private static KafkaBrokerCapabilitySpecification objectCapability(
            String kafkaCluster,
            KafkaBrokerIdentity broker,
            String writer,
            Set<StorageProfile> profiles) {
        return new KafkaBrokerCapabilitySpecification(
                kafkaCluster,
                broker,
                writer,
                "4.3.0",
                "f9-provider-takeover-test",
                System.getProperty("java.version"),
                profiles,
                StorageProfile.OBJECT_WAL_SYNC_OBJECT,
                bytes(31),
                bytes(32),
                bytes(33),
                Duration.ofSeconds(1),
                Duration.ofMinutes(5));
    }

    private static void seedActiveAuthority(
            OxiaClientConfiguration oxia,
            String nereusCluster,
            KafkaBrokerCapabilitySpecification specification,
            KafkaStorageClusterSnapshot snapshot,
            Clock clock) {
        seedActiveAuthority(
                oxia,
                nereusCluster,
                List.of(specification),
                snapshot,
                clock);
    }

    private static void seedActiveAuthority(
            OxiaClientConfiguration oxia,
            String nereusCluster,
            List<KafkaBrokerCapabilitySpecification> specifications,
            KafkaStorageClusterSnapshot snapshot,
            Clock clock) {
        if (specifications.isEmpty()) {
            throw new IllegalArgumentException(
                    "Kafka activation requires at least one capability");
        }
        try (SharedOxiaClientRuntime shared = SharedOxiaClientRuntime.connect(oxia, clock);
                KafkaStorageActivationMetadataStore store =
                        KafkaStorageActivationMetadataStore.usingSharedRuntime(
                                oxia, shared, nereusCluster, snapshot.kafkaClusterId())) {
            long now = clock.millis();
            KafkaBrokerCapabilitySpecification specification =
                    specifications.get(0);
            KafkaBrokerCapabilityRecord capability =
                    specification.initialRecord(now);
            byte[] capabilitySha256 =
                    KafkaStorageCapabilityDigests.compatibilitySha256(
                            capability);
            for (KafkaBrokerCapabilitySpecification exact :
                    specifications) {
                KafkaBrokerCapabilityRecord candidate =
                        exact.initialRecord(now);
                if (!java.util.Arrays.equals(
                                capabilitySha256,
                                KafkaStorageCapabilityDigests
                                        .compatibilitySha256(candidate))
                        || !java.util.Arrays.equals(
                                specification.providerScopeSha256(),
                                exact.providerScopeSha256())) {
                    throw new IllegalArgumentException(
                            "Kafka activation capabilities are incompatible");
                }
                store.createCapability(candidate).join();
            }
            byte[] brokerSetSha256 = KafkaStorageReadinessRecord.brokerSetSha256(snapshot.brokers());
            store.createReadiness(new KafkaStorageReadinessRecord(
                    KafkaStorageReadinessRecord.RECORD_VERSION,
                    snapshot.kafkaClusterId(),
                    1,
                    snapshot.metadataOffset(),
                    snapshot.brokers(),
                    brokerSetSha256,
                    capabilitySha256,
                    specification.providerScopeSha256(),
                    now,
                    now + Duration.ofMinutes(5).toMillis(),
                    0)).join();
            store.createActivation(new KafkaStorageProtocolActivationRecord(
                    KafkaStorageProtocolActivationRecord.RECORD_VERSION,
                    KafkaStorageActivationLifecycle.ACTIVE.wireId(),
                    snapshot.kafkaClusterId(),
                    KafkaStorageProtocolActivationRecord.PROTOCOL_VERSION,
                    KafkaStorageProtocolActivationRecord.API_VERSION,
                    KafkaStorageProtocolActivationRecord.STREAM_HEAD_SESSION_VERSION,
                    KafkaStorageProtocolActivationRecord.BINDING_VERSION,
                    KafkaPayloadMapping.KAFKA_RECORD_BATCH_V1.wireId(),
                    KafkaStorageProtocolActivationRecord.OBJECT_WAL_ENTRY_INDEX_VERSION,
                    KafkaStorageProtocolActivationRecord.NCP_VERSION,
                    KafkaStorageProtocolActivationRecord.NTC_VERSION,
                    KafkaStorageProtocolActivationRecord.CHECKPOINT_VERSION,
                    KafkaStorageProtocolActivationRecord.COMPACTION_STRATEGY_VERSION,
                    specification.supportedStorageProfiles(),
                    specification.defaultStorageProfile(),
                    capabilitySha256,
                    brokerSetSha256,
                    KafkaStorageProtocolActivationRecord.KAFKA_FEATURE_LEVEL,
                    snapshot.metadataOffset(),
                    1,
                    now,
                    now,
                    0)).join();
        }
    }

    private static VersionedGenerationIndex awaitCommittedGeneration(
            OxiaClientConfiguration oxia,
            String nereusCluster,
            String kafkaCluster,
            KafkaPartitionIdentity identity,
            Clock clock,
            long expectedEndOffset) {
        long deadline = System.nanoTime() + Duration.ofSeconds(45).toNanos();
        try (SharedOxiaClientRuntime inspector =
                        SharedOxiaClientRuntime.connect(oxia, clock);
                OxiaJavaKafkaPartitionMetadataStore partitions =
                        OxiaJavaKafkaPartitionMetadataStore
                                .usingSharedRuntime(
                                        oxia,
                                        inspector,
                                        nereusCluster,
                                        kafkaCluster);
                OxiaJavaGenerationMetadataStore generations =
                        OxiaJavaGenerationMetadataStore
                                .usingSharedRuntime(
                                        oxia, inspector, clock)) {
            StreamId streamId =
                    new StreamId(
                            partitions
                                    .get(identity.durableId())
                                    .join()
                                    .orElseThrow()
                                    .value()
                                    .streamId());
            while (System.nanoTime() < deadline) {
                for (var candidate :
                        generations
                                .scanIndex(
                                        nereusCluster,
                                        streamId,
                                        ReadView.COMMITTED,
                                        1,
                                        expectedEndOffset,
                                        Optional.empty(),
                                        100)
                                .join()
                                .values()) {
                    if (candidate
                                    instanceof VersionedGenerationIndex
                                            index
                            && index.value().lifecycle()
                                    == GenerationLifecycle.COMMITTED
                            && index.value().offsetStart() == 0
                            && index.value().offsetEnd()
                                    == expectedEndOffset) {
                        return index;
                    }
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(
                            "interrupted while awaiting Kafka NCP2 generation",
                            failure);
                }
            }
            throw new AssertionError(
                    "Kafka NCP2 generation did not commit before the integration deadline; "
                            + "materialization tasks="
                            + materializationInventory(
                                    generations,
                                    nereusCluster,
                                    streamId));
        }
    }

    private static StreamId partitionStreamId(
            OxiaClientConfiguration oxia,
            String nereusCluster,
            String kafkaCluster,
            KafkaPartitionIdentity identity,
            Clock clock) {
        try (SharedOxiaClientRuntime inspector =
                        SharedOxiaClientRuntime.connect(oxia, clock);
                OxiaJavaKafkaPartitionMetadataStore partitions =
                        OxiaJavaKafkaPartitionMetadataStore
                                .usingSharedRuntime(
                                        oxia,
                                        inspector,
                                        nereusCluster,
                                        kafkaCluster)) {
            return new StreamId(
                    partitions
                            .get(identity.durableId())
                            .join()
                            .orElseThrow()
                            .value()
                            .streamId());
        }
    }

    private static BookKeeperVersionedValue<BookKeeperLedgerRootRecord>
            awaitRetiredLedgerRoot(
                    OxiaJavaBookKeeperMetadataStore metadata,
                    String cluster,
                    StreamId streamId,
                    Duration timeout) {
        long deadline =
                System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            ArrayList<
                            BookKeeperVersionedValue<
                                    BookKeeperLedgerRootRecord>>
                    candidates = new ArrayList<>();
            for (int shard = 0;
                    shard < BookKeeperKeyspace.LEDGER_SHARDS;
                    shard++) {
                Optional<BookKeeperScanToken> continuation =
                        Optional.empty();
                do {
                    var page =
                            metadata.scanRoots(
                                            cluster,
                                            shard,
                                            continuation,
                                            100)
                                    .join();
                    page.values().stream()
                            .filter(
                                    value ->
                                            value.value()
                                                    .streamId()
                                                    .equals(
                                                            streamId
                                                                    .value()))
                            .filter(
                                    value ->
                                            switch (value
                                                    .value()
                                                    .lifecycle()) {
                                                case SEALED,
                                                        MARKED,
                                                        DELETING,
                                                        DELETED ->
                                                        true;
                                                default -> false;
                                            })
                            .forEach(candidates::add);
                    continuation = page.continuation();
                } while (continuation.isPresent());
            }
            Optional<
                            BookKeeperVersionedValue<
                                    BookKeeperLedgerRootRecord>>
                    earliest =
                            candidates.stream()
                                    .min(
                                            java.util.Comparator.comparingLong(
                                                    value ->
                                                            value.value()
                                                                    .segmentSequence()));
            if (earliest.isPresent()) {
                return earliest.orElseThrow();
            }
            pauseForProviderState(
                    "retired BookKeeper ledger root");
        }
        throw new AssertionError(
                "BookKeeper rollover did not publish a retired ledger root before the deadline");
    }

    private static BookKeeperVersionedValue<BookKeeperLedgerRootRecord>
            awaitDeletedLedgerRoot(
                    OxiaJavaBookKeeperMetadataStore metadata,
            String cluster,
            String providerScopeSha256,
            long ledgerId,
            OxiaJavaGenerationMetadataStore generations,
            StreamId streamId,
            Duration timeout) {
        long deadline =
                System.nanoTime() + timeout.toNanos();
        BookKeeperVersionedValue<BookKeeperLedgerRootRecord> last =
                null;
        while (System.nanoTime() < deadline) {
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord>
                    root =
                            metadata.getRoot(
                                            cluster,
                                            providerScopeSha256,
                                            ledgerId)
                                    .join()
                                    .orElseThrow();
            last = root;
            if (root.value().lifecycle()
                    == BookKeeperLedgerLifecycle.DELETED) {
                return root;
            }
            if (root.value().lifecycle()
                            == BookKeeperLedgerLifecycle.QUARANTINED
                    || root.value().lifecycle()
                            == BookKeeperLedgerLifecycle.ABORTED) {
                throw new AssertionError(
                        "BookKeeper retention entered terminal "
                                + root.value().lifecycle());
            }
            pauseForProviderState(
                    "deleted BookKeeper ledger root");
        }
        throw new AssertionError(
                "BookKeeper ledger did not reach DELETED before the integration deadline; last root="
                        + last
                        + "; retention inventory="
                        + retentionInventory(
                                metadata,
                                cluster,
                                providerScopeSha256,
                                ledgerId,
                                last)
                        + "; materialization tasks="
                        + materializationInventory(
                                generations,
                                cluster,
                                streamId));
    }

    private static String materializationInventory(
            OxiaJavaGenerationMetadataStore generations,
            String cluster,
            StreamId streamId) {
        var tasks =
                generations.scanTasks(
                                cluster,
                                streamId,
                                Optional.empty(),
                                100)
                        .join();
        StringBuilder diagnostic =
                new StringBuilder(tasks.toString());
        for (var task : tasks.values()) {
            diagnostic
                    .append("; checkpoint[")
                    .append(task.value().taskId())
                    .append("]=")
                    .append(
                            generations.getMaterializationCheckpoint(
                                            cluster,
                                            streamId,
                                            task.value().policyId(),
                                            task.value()
                                                    .policyVersion())
                                    .join());
            if (task.value()
                    .allocatedGeneration()
                    .isPresent()) {
                diagnostic
                        .append("; index[")
                        .append(task.value().taskId())
                        .append("]=")
                        .append(
                                generations.getIndex(
                                                cluster,
                                                new GenerationIndexIdentity(
                                                        streamId,
                                                        ReadView.COMMITTED,
                                                        task.value()
                                                                .offsetEnd(),
                                                        task.value()
                                                                .allocatedGeneration()
                                                                .orElseThrow()))
                                        .join());
            }
        }
        return diagnostic.toString();
    }

    private static String retentionInventory(
            OxiaJavaBookKeeperMetadataStore metadata,
            String cluster,
            String providerScopeSha256,
            long ledgerId,
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root) {
        var protections =
                metadata.scanProtections(
                                cluster,
                                providerScopeSha256,
                                ledgerId,
                                Optional.empty(),
                                100)
                        .join();
        var readers =
                metadata.scanReaderLeases(
                                cluster,
                                providerScopeSha256,
                                ledgerId,
                                Optional.empty(),
                                100)
                        .join();
        var writer =
                metadata.getWriter(
                                cluster,
                                new StreamId(
                                        root.value()
                                                .streamId()))
                        .join();
        var allocationSlot =
                metadata.getAllocationSlot(
                                cluster,
                                root.value()
                                        .allocationSlot())
                        .join();
        return "protections="
                + protections.values()
                + ", protectionContinuation="
                + protections.continuation()
                + ", readers="
                + readers.values()
                + ", readerContinuation="
                + readers.continuation()
                + ", writer="
                + writer
                + ", allocationSlot="
                + allocationSlot;
    }

    private static void assertPhysicalLedgerAbsent(
            BookKeeper client,
            long ledgerId)
            throws Exception {
        try {
            var handle =
                    client.openLedgerNoRecovery(
                            ledgerId,
                            BookKeeper.DigestType.CRC32C,
                            "f9-bookkeeper-password"
                                    .getBytes(
                                            java.nio.charset
                                                    .StandardCharsets
                                                    .UTF_8));
            try {
                handle.close();
            } finally {
                throw new AssertionError(
                        "physically deleted BookKeeper ledger reopened: "
                                + ledgerId);
            }
        } catch (BKException failure) {
            assertThat(failure.getCode())
                    .isIn(
                            BKException.Code
                                    .NoSuchLedgerExistsException,
                            BKException.Code
                                    .NoSuchLedgerExistsOnMetadataServerException);
        }
    }

    private static void pauseForProviderState(String state) {
        try {
            Thread.sleep(100);
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "interrupted while awaiting " + state,
                    failure);
        }
    }

    private static BookKeeperMetadataStoreConfig
            bookKeeperMetadataConfiguration(
                    BookKeeperWalConfiguration configuration) {
        return new BookKeeperMetadataStoreConfig(
                configuration.maxAppendRangesPerLedger(),
                configuration.protectionSlotsPerRange(),
                configuration.maxReaderLeasesPerLedger(),
                configuration.maxUncertainAllocations());
    }

    private static byte[] bytes(int seed) {
        byte[] value = new byte[32];
        for (int index = 0; index < value.length; index++) value[index] = (byte) (seed + index);
        return value;
    }

    private static void seedBookKeeperAuthority(
            OxiaClientConfiguration oxia,
            BookKeeperWalConfiguration configuration,
            String deployment,
            String readinessSha256,
            Clock clock) {
        try (SharedOxiaClientRuntime shared =
                SharedOxiaClientRuntime.connect(oxia, clock)) {
            OxiaBookKeeperLedgerIdNamespaceReservationStore namespaces =
                    new OxiaBookKeeperLedgerIdNamespaceReservationStore(
                            oxia,
                            shared);
            var reservation =
                    new BookKeeperLedgerIdNamespaceProvisioningCoordinator(
                                    namespaces,
                                    clock)
                            .provision(
                                    configuration,
                                    deployment,
                                    "44".repeat(32),
                                    Duration.ofSeconds(30))
                            .join();
            OxiaBookKeeperProtocolActivationStore activations =
                    new OxiaBookKeeperProtocolActivationStore(oxia, shared);
            BookKeeperProtocolActivationCoordinator coordinator =
                    new BookKeeperProtocolActivationCoordinator(
                            activations,
                            clock);
            var prepared = coordinator.prepare(
                            configuration,
                            reservation,
                            1,
                            readinessSha256,
                            Duration.ofSeconds(30))
                    .join();
            coordinator.activate(
                            configuration,
                            reservation,
                            BookKeeperProtocolActivationUpdate.publications(
                                    1,
                                    readinessSha256,
                                    true,
                                    true,
                                    prepared.metadataVersion()),
                            Duration.ofSeconds(30))
                    .join();
        }
    }

    private static BookKeeperBrokerReadinessProvider readinessProvider(
            String readinessSha256) {
        BookKeeperBrokerReadiness readiness =
                new BookKeeperBrokerReadiness(
                        1,
                        new Checksum(
                                ChecksumType.SHA256,
                                readinessSha256),
                        1);
        return new BookKeeperBrokerReadinessProvider() {
            @Override
            public CompletableFuture<BookKeeperBrokerReadiness>
                    requireBookKeeperPrimaryWalReadiness() {
                return CompletableFuture.completedFuture(readiness);
            }

            @Override
            public Optional<BookKeeperBrokerReadiness>
                    currentBookKeeperPrimaryWalReadiness() {
                return Optional.of(readiness);
            }
        };
    }

    private static BookKeeperWalConfiguration bookKeeperConfiguration() {
        return new BookKeeperWalConfiguration(
                "primary",
                "11".repeat(32),
                12,
                0x801,
                "reservation-1",
                2,
                2,
                2,
                BookKeeperDigestType.CRC32C,
                new BookKeeperSecretRef(
                        "secret://bookkeeper/password",
                        "v1"),
                100_000,
                256L * 1024 * 1024,
                1_000,
                8,
                64,
                32,
                Duration.ofHours(1),
                8,
                8,
                64L * 1024 * 1024,
                Duration.ofSeconds(30),
                Duration.ofSeconds(20),
                Duration.ofSeconds(30),
                Duration.ofSeconds(30),
                Duration.ofMinutes(2),
                Duration.ofSeconds(30),
                Duration.ofMinutes(1),
                256);
    }

    private static BookKeeperWalConfiguration
            bookKeeperDeletionConfiguration() {
        return new BookKeeperWalConfiguration(
                "primary",
                "22".repeat(32),
                12,
                0x802,
                "reservation-gc",
                2,
                2,
                2,
                BookKeeperDigestType.CRC32C,
                new BookKeeperSecretRef(
                        "secret://bookkeeper/password",
                        "v1"),
                1,
                256L * 1024 * 1024,
                1_000,
                8,
                64,
                32,
                Duration.ofHours(1),
                8,
                8,
                64L * 1024 * 1024,
                Duration.ofSeconds(20),
                Duration.ofSeconds(10),
                Duration.ofSeconds(20),
                Duration.ofSeconds(20),
                Duration.ofSeconds(21),
                Duration.ofSeconds(2),
                Duration.ofMillis(250),
                256);
    }

    private static MaterializationConfig
            bookKeeperDeletionMaterializationConfig(
                    Path stagingDirectory) {
        MaterializationConfig defaults =
                MaterializationConfig.kafkaDefaults(
                        stagingDirectory);
        return new MaterializationConfig(
                defaults.committedPolicy(),
                defaults.registryScanPageSize(),
                Duration.ofMillis(250),
                defaults.plannerPageSize(),
                defaults.taskScanPageSize(),
                defaults.maxTasksPerPlan(),
                defaults.maxConcurrentWorkers(),
                defaults.maxConcurrentWorkersPerStream(),
                defaults.sourceReadPageRecords(),
                defaults.sourceReadPageBytes(),
                defaults.stagingDirectory(),
                defaults.maxStagingBytes(),
                defaults.uploadChunkBytes(),
                defaults.workerClaimDuration(),
                defaults.workerClaimRenewInterval(),
                defaults.maximumClockSkew(),
                defaults.operationTimeout(),
                defaults.closeTimeout(),
                defaults.retryMinBackoff(),
                defaults.retryMaxBackoff(),
                defaults.maxTaskAttempts(),
                defaults.lagThrottleRecords(),
                defaults.lagRejectRecords(),
                defaults.lagThrottleBytes(),
                defaults.lagRejectBytes(),
                defaults.lagRejectAge(),
                defaults.lagThrottleDelay(),
                Duration.ofSeconds(1),
                Duration.ofSeconds(1),
                Duration.ofSeconds(1),
                defaults.recoveryCheckpointMaxEntries(),
                defaults.recoveryCheckpointMaxBytes());
    }

    private static BKCluster startBookKeeper(
            String metadataServiceUri) throws Exception {
        ServerConfiguration configuration = new ServerConfiguration();
        configuration.setProperty(
                "dbStorage_writeCacheMaxSizeMb",
                32);
        configuration.setProperty(
                "dbStorage_readAheadCacheMaxSizeMb",
                4);
        configuration.setProperty(
                "dbStorage_rocksDB_writeBufferSizeMB",
                4);
        configuration.setProperty(
                "dbStorage_rocksDB_blockCacheSize",
                4 * 1024 * 1024);
        configuration.setJournalSyncData(false);
        configuration.setJournalWriteData(false);
        configuration.setProperty("journalMaxGroupWaitMSec", 0L);
        configuration.setProperty("journalPreAllocSizeMB", 1);
        configuration.setFlushInterval(60_000);
        configuration.setGcWaitTime(60_000);
        configuration.setAllowLoopback(true);
        configuration.setAdvertisedAddress("127.0.0.1");
        configuration.setAllowEphemeralPorts(true);
        configuration.setNumAddWorkerThreads(0);
        configuration.setNumReadWorkerThreads(0);
        configuration.setNumHighPriorityWorkerThreads(0);
        configuration.setNumJournalCallbackThreads(0);
        configuration.setServerNumIOThreads(1);
        configuration.setNumLongPollWorkerThreads(1);
        configuration.setAllocatorPoolingPolicy(
                PoolingPolicy.UnpooledHeap);
        configuration.setLedgerStorageClass(
                "org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage");
        configuration.setDiskUsageThreshold(0.999F);
        configuration.setDiskUsageWarnThreshold(0.99F);
        return BKCluster.builder()
                .baseServerConfiguration(configuration)
                .metadataServiceUri(metadataServiceUri)
                .numBookies(2)
                .clearOldData(true)
                .build();
    }

    private static StreamStorageConfig streamConfiguration(String cluster, String writer) {
        return new StreamStorageConfig(
                cluster,
                writer,
                Duration.ofSeconds(30),
                Duration.ofSeconds(10),
                Duration.ofSeconds(5),
                Duration.ofSeconds(30),
                Duration.ofSeconds(30),
                Duration.ofSeconds(30),
                64,
                10_000,
                256,
                10_000,
                1_024,
                64L * 1024 * 1024,
                64,
                128L * 1024 * 1024,
                16 * 1024 * 1024,
                100_000,
                Duration.ofSeconds(5),
                false,
                false,
                true);
    }

    private static ObjectStoreConfiguration objectConfiguration(ObjectStoreProvider provider) {
        return new ObjectStoreConfiguration(
                provider.getClass().getName(),
                URI.create("http://localhost:9000"),
                "us-east-1",
                "bucket",
                "nereus/kafka",
                true,
                Duration.ofSeconds(30),
                new ObjectPutRetryPolicy(3, Duration.ofMillis(25), Duration.ofSeconds(1)),
                32,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    private static void assertFailureCode(
            CompletableFuture<?> completion, ErrorCode expected) {
        Throwable failure =
                completion.handle((ignored, exact) -> exact).join();
        while (failure instanceof java.util.concurrent.CompletionException
                && failure.getCause() != null) {
            failure = failure.getCause();
        }
        assertThat(failure)
                .isInstanceOfSatisfying(
                        NereusException.class,
                        exact ->
                                assertThat(exact.code())
                                        .isEqualTo(expected));
    }

    private static final class AcceptingRecoveryStateCodec
            implements KafkaRecoveryStateCodec<List<KafkaReplayBatch>> {
        @Override
        public List<KafkaReplayBatch> freshState() {
            return new ArrayList<>();
        }

        @Override
        public void hydrateCheckpoint(
                List<KafkaReplayBatch> state,
                com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointHeader header,
                List<com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointSection>
                        sections) {
            throw new AssertionError(
                    "takeover provider test does not publish checkpoints");
        }

        @Override
        public void replayBatch(
                List<KafkaReplayBatch> state,
                KafkaReplayBatch batch) {
            long nextOffset =
                    state.isEmpty()
                            ? 0
                            : state.get(state.size() - 1).lastOffset() + 1;
            assertThat(batch.baseOffset()).isEqualTo(nextOffset);
            state.add(batch);
        }

        @Override
        public void validateRecoveredState(
                List<KafkaReplayBatch> state,
                com.nereusstream.kafka.checkpoint.KafkaCheckpointSourceState
                        frozenSource) {
            long recoveredEnd =
                    state.isEmpty()
                            ? 0
                            : state.get(state.size() - 1).lastOffset() + 1;
            assertThat(recoveredEnd)
                    .isEqualTo(frozenSource.endOffset());
        }
    }

    private static final class EmptyRecoveryStateCodec
            implements KafkaRecoveryStateCodec<Object> {
        @Override
        public Object freshState() {
            return new Object();
        }

        @Override
        public void hydrateCheckpoint(
                Object state,
                com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointHeader header,
                List<com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointSection>
                        sections) {
            throw new AssertionError("checkpoint hydration not expected");
        }

        @Override
        public void replayBatch(Object state, KafkaReplayBatch batch) {
            throw new AssertionError("batch replay not expected");
        }

        @Override
        public void validateRecoveredState(
                Object state,
                com.nereusstream.kafka.checkpoint.KafkaCheckpointSourceState frozenSource) {
            if (frozenSource.endOffset() != 0) {
                throw new AssertionError("empty recovery expected");
            }
        }
    }

    private static final class AppliedDeleteResponseLossOperations
            implements BookKeeperClientOperations {
        private final BookKeeperClientOperations delegate;
        private final AtomicBoolean inject = new AtomicBoolean();
        private final AtomicBoolean injected = new AtomicBoolean();
        private final AtomicLong injectedLedgerId =
                new AtomicLong(Long.MIN_VALUE);

        private AppliedDeleteResponseLossOperations(
                BookKeeperClientOperations delegate) {
            this.delegate = delegate;
        }

        @Override
        public CompletableFuture<WriteAdvHandle> createAdvanced(
                long ledgerId,
                BookKeeperWalConfiguration configuration,
                byte[] password,
                Map<String, byte[]> customMetadata,
                BookKeeperOperationDeadline deadline) {
            return delegate.createAdvanced(
                    ledgerId,
                    configuration,
                    password,
                    customMetadata,
                    deadline);
        }

        @Override
        public CompletableFuture<ReadHandle> open(
                long ledgerId,
                BookKeeperDigestType digestType,
                byte[] password,
                boolean recovery,
                BookKeeperOperationDeadline deadline) {
            return delegate.open(
                    ledgerId,
                    digestType,
                    password,
                    recovery,
                    deadline);
        }

        @Override
        public CompletableFuture<Long> write(
                WriteAdvHandle handle,
                long entryId,
                ByteBuf entry,
                BookKeeperOperationDeadline deadline) {
            return delegate.write(
                    handle,
                    entryId,
                    entry,
                    deadline);
        }

        @Override
        public CompletableFuture<LedgerEntries> readUnconfirmed(
                ReadHandle handle,
                long firstEntryId,
                long lastEntryIdInclusive,
                BookKeeperOperationDeadline deadline) {
            return delegate.readUnconfirmed(
                    handle,
                    firstEntryId,
                    lastEntryIdInclusive,
                    deadline);
        }

        @Override
        public CompletableFuture<LedgerMetadata> metadata(
                long ledgerId,
                BookKeeperOperationDeadline deadline) {
            return delegate.metadata(ledgerId, deadline);
        }

        @Override
        public CompletableFuture<Void> delete(
                long ledgerId,
                BookKeeperOperationDeadline deadline) {
            return delegate.delete(ledgerId, deadline)
                    .thenCompose(ignored -> {
                        if (inject.compareAndSet(true, false)) {
                            injected.set(true);
                            injectedLedgerId.set(ledgerId);
                            return CompletableFuture.failedFuture(
                                    new IllegalStateException(
                                            "injected applied BookKeeper delete response loss"));
                        }
                        return CompletableFuture.completedFuture(null);
                    });
        }

        private boolean responseLossInjected() {
            return injected.get();
        }

        private long injectedLedgerId() {
            return injectedLedgerId.get();
        }

        private void arm() {
            if (!inject.compareAndSet(false, true)) {
                throw new IllegalStateException(
                        "delete response loss already armed");
            }
        }
    }

    private static final class LocalObjectStoreProvider implements ObjectStoreProvider {
        private final Path root;
        private final AtomicBoolean used = new AtomicBoolean();
        private final AtomicBoolean closed = new AtomicBoolean();
        private ObjectStore store;

        private LocalObjectStoreProvider(Path root) {
            this.root = root;
        }

        @Override
        public ObjectStore create(
                ObjectStoreConfiguration configuration,
                ObjectStoreSecretResolver secretResolver) {
            if (!used.compareAndSet(false, true)) {
                throw new IllegalStateException("provider already used");
            }
            store = new LocalFileObjectStore(root);
            return store;
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true) && store != null) {
                store.close();
            }
        }

        private boolean closed() {
            return closed.get();
        }
    }
}
