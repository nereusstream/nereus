/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamMetadata;
import com.nereusstream.api.StreamName;
import com.nereusstream.api.StreamState;
import com.nereusstream.api.keys.DeterministicIds;
import com.nereusstream.core.physical.DefaultObjectProtectionManager;
import com.nereusstream.core.physical.DefaultObjectReadPinManager;
import com.nereusstream.core.physical.ObjectProtectionManager;
import com.nereusstream.core.physical.ObjectReadPinManager;
import com.nereusstream.metadata.oxia.CursorMetadataStore;
import com.nereusstream.metadata.oxia.CursorMetadataStoreConfig;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionMetadataStore;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionNames;
import com.nereusstream.metadata.oxia.OxiaClientConfiguration;
import com.nereusstream.metadata.oxia.OxiaJavaPhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.PhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.ProjectionCreateRequest;
import com.nereusstream.metadata.oxia.ProjectionMetadataStoreConfig;
import com.nereusstream.metadata.oxia.SharedOxiaClientRuntime;
import com.nereusstream.metadata.oxia.records.TopicProjectionRecord;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.ObjectStoreConfiguration;
import com.nereusstream.objectstore.ObjectStoreSecretResolver;
import com.nereusstream.objectstore.S3CompatibleObjectStoreProvider;
import io.oxia.testcontainers.OxiaContainer;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

@Testcontainers
class CursorStorageOxiaS3IntegrationTest {
    private static final String OXIA_IMAGE = "oxia/oxia:0.16.3";
    private static final DockerImageName LOCALSTACK_IMAGE =
            DockerImageName.parse("localstack/localstack:4.14.0");
    private static final String BUCKET = "nereus-cursor-m2-test";

    @Container
    private static final OxiaContainer OXIA =
            new OxiaContainer(DockerImageName.parse(OXIA_IMAGE)).withShards(4);

    @Container
    private static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(LOCALSTACK_IMAGE)
                    .withServices(LocalStackContainer.Service.S3);

    @BeforeAll
    static void createBucket() {
        try (S3AsyncClient admin = s3Client()) {
            admin.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build()).join();
        }
    }

    @Test
    void responseLossOwnerTakeoverAndRestartHydrateOneRealOxiaRootAndRealS3Snapshot()
            throws Exception {
        String cluster = "f3/m2/" + UUID.randomUUID();
        String topic = "persistent://tenant/ns/" + UUID.randomUUID();
        StreamId streamId = ManagedLedgerProjectionNames.streamId(topic, 1);
        StreamName streamName = ManagedLedgerProjectionNames.streamName(topic, 1);
        CursorStorageTestSupport.TestStreamStorage streamStorage =
                new CursorStorageTestSupport.TestStreamStorage(
                        streamMetadata(streamId, streamName, 0, 2_000));
        CursorStorageConfig cursorConfig = CursorStorageConfig.defaults();
        TopicProjectionRecord projection;
        CursorState afterTakeover;

        try (RuntimeBundle first = new RuntimeBundle(
                cluster, streamStorage, cursorConfig, objectStoreConfiguration())) {
            projection = first.projectionStore.createFirstProjection(
                            cluster,
                            new ProjectionCreateRequest(
                                    topic,
                                    1,
                                    1,
                                    streamMetadata(streamId, streamName, 0, 0),
                                    Map.of()),
                            () -> CompletableFuture.completedFuture(null))
                    .join();
            CursorLedgerIdentity ledger = ledger(topic, projection);
            CursorOwnerSession firstOwner = new CursorOwnerSession(
                    ledger, CursorStorageTestSupport.OWNER_1);
            CursorHandle firstHandle = first.storage.open(
                            firstOwner,
                            "subscription-real",
                            new CursorOpenRequest(
                                    new InitialCursorPosition.Earliest(),
                                    Map.of("created-at", 1L),
                                    Map.of("mode", "real-services"),
                                    0,
                                    2_000))
                    .join();
            first.metadataStore.failNext(
                    CursorStorageTestSupport.MetadataOperation.CAS_CURSOR,
                    CursorStorageTestSupport.FaultCut.AFTER,
                    transientFailure("real Oxia cursor CAS response lost"));

            CursorMutationResult recoveredAck = first.storage
                    .individualAck(firstHandle, disjointAcks(1, 2, 300))
                    .join();
            assertThat(recoveredAck.outcome())
                    .isEqualTo(CursorMutationOutcome.ALREADY_APPLIED);
            assertThat(recoveredAck.state().snapshotReference()).isPresent();
            assertThat(first.activationCalls).hasValue(1);

            try (RuntimeBundle second = new RuntimeBundle(
                    cluster, streamStorage, cursorConfig, objectStoreConfiguration())) {
                CursorOwnerSession secondOwner = new CursorOwnerSession(
                        ledger, CursorStorageTestSupport.OWNER_2);
                CursorHandle claimed = second.storage
                        .claimAndLoadActiveCursors(secondOwner)
                        .join()
                        .getFirst();
                assertThat(claimed.state().acknowledgements())
                        .isEqualTo(recoveredAck.state().acknowledgements());
                assertThat(claimed.state().snapshotReference())
                        .isEqualTo(recoveredAck.state().snapshotReference());
                assertThat(claimed.state().cursorProperties())
                        .containsExactly(Map.entry("mode", "real-services"));
                assertThat(second.activationCalls).hasValue(0);

                assertThatThrownBy(() -> first.storage
                                .individualAck(
                                        firstHandle,
                                        List.of(new CursorAckRequest(
                                                1_001, Optional.empty(), Map.of())))
                                .join())
                        .hasCauseInstanceOf(
                                ManagedLedgerException.ManagedLedgerFencedException.class);

                afterTakeover = second.storage
                        .individualAck(
                                claimed,
                                List.of(new CursorAckRequest(
                                        1_001, Optional.empty(), Map.of())))
                        .join()
                        .state();
                assertThat(afterTakeover.acknowledgements()
                                .isWholeEntryAcknowledged(1_001))
                        .isTrue();
            }
        }

        try (RuntimeBundle restarted = new RuntimeBundle(
                cluster, streamStorage, cursorConfig, objectStoreConfiguration())) {
            CursorLedgerIdentity ledger = ledger(topic, projection);
            CursorOwnerSession restartedOwner = new CursorOwnerSession(
                    ledger, CursorStorageTestSupport.OWNER_3);
            CursorHandle recovered = restarted.storage
                    .claimAndLoadActiveCursors(restartedOwner)
                    .join()
                    .getFirst();

            assertThat(recovered.state().acknowledgements())
                    .isEqualTo(afterTakeover.acknowledgements());
            assertThat(recovered.state().snapshotReference())
                    .isEqualTo(afterTakeover.snapshotReference());
            assertThat(recovered.state().ownerSessionId())
                    .isEqualTo(CursorStorageTestSupport.OWNER_3);
            assertThat(restarted.activationCalls).hasValue(0);
        }
    }

    private static CursorLedgerIdentity ledger(
            String topic, TopicProjectionRecord projection) {
        return new CursorLedgerIdentity(
                topic,
                ManagedLedgerProjectionNames.managedLedgerNameHash(topic),
                projection.projectionIdentity());
    }

    private static List<CursorAckRequest> disjointAcks(
            long firstOffset, long step, int count) {
        List<CursorAckRequest> requests = new ArrayList<>();
        for (int index = 0; index < count; index++) {
            requests.add(new CursorAckRequest(
                    firstOffset + index * step, Optional.empty(), Map.of()));
        }
        return List.copyOf(requests);
    }

    private static NereusException transientFailure(String message) {
        return new NereusException(ErrorCode.METADATA_UNAVAILABLE, true, message);
    }

    private static StreamMetadata streamMetadata(
            StreamId streamId,
            StreamName streamName,
            long trimOffset,
            long committedEndOffset) {
        return new StreamMetadata(
                streamId,
                streamName,
                StreamState.ACTIVE,
                StorageProfile.OBJECT_WAL_SYNC_OBJECT,
                Map.of(
                        ManagedLedgerProjectionNames.PAYLOAD_MAPPING_ATTRIBUTE,
                        ManagedLedgerProjectionNames.PAYLOAD_MAPPING_V1),
                100,
                0,
                committedEndOffset,
                committedEndOffset * 100,
                trimOffset);
    }

    private static OxiaClientConfiguration oxiaConfiguration() {
        return new OxiaClientConfiguration(
                OXIA.getServiceAddress(),
                "default",
                Duration.ofSeconds(10),
                Duration.ofSeconds(30),
                100,
                1_024);
    }

    private static ObjectStoreConfiguration objectStoreConfiguration() {
        return new ObjectStoreConfiguration(
                S3CompatibleObjectStoreProvider.class.getName(),
                LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.S3),
                LOCALSTACK.getRegion(),
                BUCKET,
                "cursor-m2-objects",
                true,
                Duration.ofSeconds(10),
                4,
                Optional.of("access"),
                Optional.of("secret"),
                Optional.empty());
    }

    private static ObjectStoreSecretResolver secrets() {
        return reference -> Optional.of(("access".equals(reference)
                ? LOCALSTACK.getAccessKey()
                : LOCALSTACK.getSecretKey()).toCharArray());
    }

    private static S3AsyncClient s3Client() {
        return S3AsyncClient.builder()
                .endpointOverride(LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.S3))
                .region(Region.of(LOCALSTACK.getRegion()))
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(true)
                        .build())
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(
                                LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey())))
                .build();
    }

    private static final class RuntimeBundle implements AutoCloseable {
        private final ScheduledThreadPoolExecutor scheduler =
                new ScheduledThreadPoolExecutor(4);
        private final AtomicInteger activationCalls = new AtomicInteger();
        private final SharedOxiaClientRuntime oxiaRuntime;
        private final PhysicalObjectMetadataStore physicalMetadataStore;
        private final ObjectProtectionManager objectProtectionManager;
        private final ObjectReadPinManager objectReadPinManager;
        private final ManagedLedgerProjectionMetadataStore projectionStore;
        private final CursorStorageTestSupport.ControllableCursorMetadataStore metadataStore;
        private final S3CompatibleObjectStoreProvider objectStoreProvider;
        private final ObjectStore objectStore;
        private final DefaultCursorSnapshotStore snapshotStore;
        private final DefaultCursorRetentionCoordinator retention;
        private final DefaultCursorStorage storage;

        private RuntimeBundle(
                String cluster,
                CursorStorageTestSupport.TestStreamStorage streamStorage,
                CursorStorageConfig cursorConfig,
                ObjectStoreConfiguration objectStoreConfiguration) throws Exception {
            Clock clock = Clock.systemUTC();
            OxiaClientConfiguration oxiaConfiguration = oxiaConfiguration();
            oxiaRuntime = SharedOxiaClientRuntime.connect(oxiaConfiguration, clock);
            physicalMetadataStore =
                    OxiaJavaPhysicalObjectMetadataStore.usingSharedRuntime(
                            oxiaConfiguration, oxiaRuntime, clock);
            objectProtectionManager = new DefaultObjectProtectionManager(
                    cluster,
                    physicalMetadataStore,
                    Duration.ofMinutes(5),
                    Duration.ofSeconds(5),
                    Duration.ofHours(1),
                    clock);
            objectReadPinManager = new DefaultObjectReadPinManager(
                    cluster,
                    DeterministicIds.stableHashComponent(
                            "cursor-m2-reader/" + cluster),
                    physicalMetadataStore,
                    Duration.ofMinutes(2),
                    Duration.ofSeconds(5),
                    Duration.ofHours(1),
                    clock);
            projectionStore = ManagedLedgerProjectionMetadataStore.usingSharedRuntime(
                    oxiaConfiguration,
                    oxiaRuntime,
                    ProjectionMetadataStoreConfig.defaults(),
                    clock);
            CursorMetadataStore realMetadataStore = CursorMetadataStore.usingSharedRuntime(
                    oxiaConfiguration,
                    oxiaRuntime,
                    CursorMetadataStoreConfig.defaults());
            metadataStore = new CursorStorageTestSupport.ControllableCursorMetadataStore(
                    realMetadataStore);
            objectStoreProvider = new S3CompatibleObjectStoreProvider();
            objectStore = objectStoreProvider.create(objectStoreConfiguration, secrets());
            snapshotStore = new DefaultCursorSnapshotStore(
                    cluster,
                    objectStore,
                    metadataStore,
                    physicalMetadataStore,
                    objectProtectionManager,
                    objectReadPinManager,
                    cursorConfig,
                    objectStoreConfiguration.requestTimeout(),
                    Duration.ofMinutes(5),
                    clock);
            CursorProtocolActivationGuard activationGuard = ignored -> {
                activationCalls.incrementAndGet();
                return CompletableFuture.completedFuture(null);
            };
            CursorStateMachine stateMachine = new CursorStateMachine(cursorConfig);
            retention = new DefaultCursorRetentionCoordinator(
                    cluster,
                    streamStorage,
                    projectionStore,
                    metadataStore,
                    snapshotStore,
                    activationGuard,
                    stateMachine,
                    cursorConfig,
                    clock,
                    scheduler);
            storage = new DefaultCursorStorage(
                    cluster,
                    streamStorage,
                    projectionStore,
                    metadataStore,
                    snapshotStore,
                    retention,
                    activationGuard,
                    stateMachine,
                    new CursorStatePersistencePlanner(cluster, cursorConfig),
                    cursorConfig,
                    clock,
                    scheduler);
        }

        @Override
        public void close() throws Exception {
            Exception failure = null;
            try {
                closeAll(
                        storage,
                        retention,
                        snapshotStore,
                        metadataStore,
                        projectionStore,
                        objectReadPinManager,
                        objectProtectionManager,
                        physicalMetadataStore,
                        objectStore,
                        objectStoreProvider,
                        oxiaRuntime);
            } catch (Exception error) {
                failure = error;
            } finally {
                scheduler.shutdownNow();
            }
            if (failure != null) {
                throw failure;
            }
        }

        private static void closeAll(AutoCloseable... resources) throws Exception {
            Exception failure = null;
            for (AutoCloseable resource : resources) {
                try {
                    resource.close();
                } catch (Exception error) {
                    if (failure == null) {
                        failure = error;
                    } else {
                        failure.addSuppressed(error);
                    }
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }
}
