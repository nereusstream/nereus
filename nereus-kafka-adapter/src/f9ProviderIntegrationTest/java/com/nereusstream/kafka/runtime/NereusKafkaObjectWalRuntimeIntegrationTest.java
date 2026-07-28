/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.runtime;

import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import com.nereusstream.bookkeeper.BookKeeperBrokerReadiness;
import com.nereusstream.bookkeeper.BookKeeperBrokerReadinessProvider;
import com.nereusstream.bookkeeper.BookKeeperDigestType;
import com.nereusstream.bookkeeper.BookKeeperLedgerIdNamespaceProvisioningCoordinator;
import com.nereusstream.bookkeeper.BookKeeperProtocolActivationCoordinator;
import com.nereusstream.bookkeeper.BookKeeperProtocolActivationUpdate;
import com.nereusstream.bookkeeper.BookKeeperSecretRef;
import com.nereusstream.bookkeeper.BookKeeperWalConfiguration;
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
import com.nereusstream.metadata.oxia.OxiaJavaGenerationMetadataStore;
import com.nereusstream.metadata.oxia.OxiaJavaKafkaPartitionMetadataStore;
import com.nereusstream.metadata.oxia.KafkaBrokerIdentity;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.metadata.oxia.KafkaStorageActivationMetadataStore;
import com.nereusstream.metadata.oxia.SharedOxiaClientRuntime;
import com.nereusstream.metadata.oxia.records.KafkaBrokerCapabilityRecord;
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
import io.oxia.testcontainers.OxiaContainer;
import java.net.URI;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.client.BookKeeper;
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

    private static void seedActiveAuthority(
            OxiaClientConfiguration oxia,
            String nereusCluster,
            KafkaBrokerCapabilitySpecification specification,
            KafkaStorageClusterSnapshot snapshot,
            Clock clock) {
        try (SharedOxiaClientRuntime shared = SharedOxiaClientRuntime.connect(oxia, clock);
                KafkaStorageActivationMetadataStore store =
                        KafkaStorageActivationMetadataStore.usingSharedRuntime(
                                oxia, shared, nereusCluster, snapshot.kafkaClusterId())) {
            long now = clock.millis();
            KafkaBrokerCapabilityRecord capability = specification.initialRecord(now);
            byte[] capabilitySha256 = KafkaStorageCapabilityDigests.compatibilitySha256(capability);
            byte[] brokerSetSha256 = KafkaStorageReadinessRecord.brokerSetSha256(snapshot.brokers());
            store.createCapability(capability).join();
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
                    now + 30_000,
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
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
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
        }
        throw new AssertionError(
                "Kafka NCP2 generation did not commit before the integration deadline");
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

    private static final class EmptyRecoveryStateCodec
            implements KafkaRecoveryStateCodec<Object> {
        @Override
        public Object freshState() {
            return new Object();
        }

        @Override
        public void hydrateCheckpoint(
                Object state,
                List<com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointSection> sections,
                long checkpointOffset) {
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
