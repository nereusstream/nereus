/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.core.physical.DefaultObjectProtectionManager;
import com.nereusstream.core.physical.DefaultObjectReadPinManager;
import com.nereusstream.core.physical.ObjectProtectionManager;
import com.nereusstream.core.physical.ObjectReadPinManager;
import com.nereusstream.metadata.oxia.CursorNames;
import com.nereusstream.metadata.oxia.FakeCursorMetadataStore;
import com.nereusstream.metadata.oxia.FakePhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionNames;
import com.nereusstream.metadata.oxia.VersionedCursorState;
import com.nereusstream.metadata.oxia.records.ManagedLedgerProjectionIdentity;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.ObjectStoreConfiguration;
import com.nereusstream.objectstore.ObjectStoreSecretResolver;
import com.nereusstream.objectstore.S3CompatibleObjectStoreProvider;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

class DefaultCursorSnapshotStoreLocalStackIntegrationTest {
    private static final DockerImageName IMAGE =
            DockerImageName.parse("localstack/localstack:4.14.0");
    private static final String BUCKET = "nereus-cursor-test";
    private static final String COLLISION_RETRY_ID = "00110011001100110011001100110011";

    @Test
    void immutableSnapshotCollisionAndRestartRoundTripAgainstRealS3Provider() throws Exception {
        try (LocalStackContainer localstack = new LocalStackContainer(IMAGE)
                .withServices(LocalStackContainer.Service.S3)) {
            localstack.start();
            createBucket(localstack);
            ObjectStoreConfiguration configuration = configuration(localstack);
            CursorSnapshotReference reference;
            CursorAckState expected = ackState();
            CursorSnapshotWriteRequest request = new CursorSnapshotWriteRequest(
                    identity(), 7, expected, 100);
            Clock clock = Clock.systemUTC();

            try (ProtocolFixture protocol = new ProtocolFixture(
                    "cluster/s3-integration", identity(), clock)) {
                S3CompatibleObjectStoreProvider firstProvider =
                        new S3CompatibleObjectStoreProvider();
                ObjectStore firstObjectStore =
                        firstProvider.create(configuration, secrets(localstack));
                try (DefaultCursorSnapshotStore first =
                        protocol.newStore(firstObjectStore)) {
                    CursorSnapshotPublication original =
                            first.prepareWrite(request, protocol.authority(request)).join();
                    CursorSnapshotReference originalReference =
                            protocol.publish(first, original, expected);
                    assertThat(first.read(originalReference, identity()).join())
                            .isEqualTo(expected);

                    CursorSnapshotWriteRequest nextRequest =
                            new CursorSnapshotWriteRequest(
                                    identity(), 8, expected, 101);
                    ArrayDeque<String> ids = new ArrayDeque<>(List.of(
                            original.reference().snapshotId(),
                            COLLISION_RETRY_ID));
                    try (DefaultCursorSnapshotStore exactCollisionRetry =
                            protocol.newStore(firstObjectStore, ids)) {
                        CursorSnapshotPublication retried =
                                exactCollisionRetry.prepareWrite(
                                        nextRequest,
                                        protocol.authority(nextRequest)).join();
                        reference = protocol.publish(
                                exactCollisionRetry, retried, expected);
                        assertThat(reference.snapshotId())
                                .isEqualTo(COLLISION_RETRY_ID);
                        assertThat(reference.objectKey())
                                .isNotEqualTo(original.reference().objectKey());
                        assertThat(exactCollisionRetry.read(
                                reference, identity()).join()).isEqualTo(expected);
                    }
                } finally {
                    firstObjectStore.close();
                    firstProvider.close();
                }

                S3CompatibleObjectStoreProvider restartedProvider =
                        new S3CompatibleObjectStoreProvider();
                ObjectStore restartedObjectStore =
                        restartedProvider.create(configuration, secrets(localstack));
                try (DefaultCursorSnapshotStore restarted =
                        protocol.newStore(restartedObjectStore)) {
                    assertThat(restarted.read(reference, identity()).join())
                            .isEqualTo(expected);
                } finally {
                    restartedObjectStore.close();
                    restartedProvider.close();
                }
            }
        }
    }

    private static final class ProtocolFixture implements AutoCloseable {
        private static final String OWNER =
                "0123456789abcdef0123456789abcdef";
        private static final String PROTECTION_ATTEMPT =
                "abcdef0123456789abcdef0123456789";
        private static final Duration PENDING = Duration.ofMinutes(5);

        private final String cluster;
        private final CursorIdentity identity;
        private final Clock clock;
        private final CursorStorageConfig config = CursorStorageConfig.defaults();
        private final FakeCursorMetadataStore cursorStore =
                new FakeCursorMetadataStore();
        private final FakePhysicalObjectMetadataStore physicalStore =
                new FakePhysicalObjectMetadataStore();
        private final ObjectProtectionManager protections;
        private final ObjectReadPinManager readPins;
        private final CursorStatePersistencePlanner planner;
        private final List<DefaultCursorSnapshotStore> stores =
                new ArrayList<>();
        private VersionedCursorState currentRoot;

        private ProtocolFixture(
                String cluster,
                CursorIdentity identity,
                Clock clock) {
            this.cluster = cluster;
            this.identity = identity;
            this.clock = clock;
            protections = new DefaultObjectProtectionManager(
                    cluster,
                    physicalStore,
                    PENDING,
                    Duration.ofSeconds(1),
                    Duration.ofHours(1),
                    clock);
            readPins = new DefaultObjectReadPinManager(
                    cluster,
                    "r".repeat(26),
                    physicalStore,
                    Duration.ofMinutes(2),
                    Duration.ofSeconds(1),
                    Duration.ofHours(1),
                    clock);
            planner = new CursorStatePersistencePlanner(cluster, config);
            currentRoot = cursorStore.createCursor(
                    cluster,
                    planner.recordWithoutSnapshot(state(
                            6,
                            CursorAckState.empty(4),
                            99,
                            0))).join();
        }

        private DefaultCursorSnapshotStore newStore(ObjectStore objectStore) {
            DefaultCursorSnapshotStore created =
                    new DefaultCursorSnapshotStore(
                            cluster,
                            objectStore,
                            cursorStore,
                            physicalStore,
                            protections,
                            readPins,
                            config,
                            Duration.ofSeconds(10),
                            PENDING,
                            clock);
            stores.add(created);
            return created;
        }

        private DefaultCursorSnapshotStore newStore(
                ObjectStore objectStore,
                ArrayDeque<String> ids) {
            DefaultCursorSnapshotStore created =
                    new DefaultCursorSnapshotStore(
                            cluster,
                            objectStore,
                            cursorStore,
                            physicalStore,
                            protections,
                            readPins,
                            config,
                            Duration.ofSeconds(10),
                            PENDING,
                            clock,
                            ids::removeFirst,
                            System::nanoTime);
            stores.add(created);
            return created;
        }

        private CursorSnapshotWriteAuthority authority(
                CursorSnapshotWriteRequest request) {
            return new CursorSnapshotWriteAuthority(
                    currentRoot,
                    OWNER,
                    request.sourceMutationSequence());
        }

        private CursorSnapshotReference publish(
                DefaultCursorSnapshotStore store,
                CursorSnapshotPublication publication,
                CursorAckState acknowledgements) {
            CursorState candidate = state(
                    publication.request().sourceMutationSequence(),
                    acknowledgements,
                    publication.request().createdAtMillis(),
                    currentRoot.metadataVersion());
            currentRoot = cursorStore.compareAndSetCursor(
                    cluster,
                    planner.afterSnapshot(candidate, publication.reference()),
                    currentRoot.metadataVersion()).join();
            store.completeWrite(publication, currentRoot).join();
            return publication.reference();
        }

        private CursorState state(
                long mutationSequence,
                CursorAckState acknowledgements,
                long updatedAtMillis,
                long metadataVersion) {
            return new CursorState(
                    identity,
                    OWNER,
                    CursorLifecycle.ACTIVE,
                    mutationSequence,
                    mutationSequence,
                    PROTECTION_ATTEMPT,
                    acknowledgements,
                    Map.of(),
                    Map.of(),
                    Optional.empty(),
                    90,
                    updatedAtMillis,
                    metadataVersion);
        }

        @Override
        public void close() {
            stores.forEach(DefaultCursorSnapshotStore::close);
            readPins.close();
            protections.close();
            cursorStore.close();
            physicalStore.close();
        }
    }

    private static CursorIdentity identity() {
        String managedLedgerName = "tenant/ns/persistent/cursor-s3";
        ManagedLedgerProjectionIdentity projection = new ManagedLedgerProjectionIdentity(
                3,
                1,
                ManagedLedgerProjectionNames.streamId(managedLedgerName, 1).value(),
                ManagedLedgerProjectionNames.MIN_VIRTUAL_LEDGER_ID + 17);
        CursorLedgerIdentity ledger = new CursorLedgerIdentity(
                managedLedgerName,
                ManagedLedgerProjectionNames.managedLedgerNameHash(managedLedgerName),
                projection);
        String cursorName = "subscription-s3";
        return new CursorIdentity(
                ledger, cursorName, CursorNames.cursorNameHash(cursorName), 1);
    }

    private static CursorAckState ackState() {
        TreeMap<Long, BatchAckState> partials = new TreeMap<>();
        partials.put(8L, new BatchAckState(4, new long[] {0b1011}));
        return new CursorAckState(4, List.of(new OffsetRange(6, 8)), partials);
    }

    private static void createBucket(LocalStackContainer localstack) {
        try (S3AsyncClient admin = client(localstack)) {
            admin.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build()).join();
        }
    }

    private static S3AsyncClient client(LocalStackContainer localstack) {
        return S3AsyncClient.builder()
                .endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.S3))
                .region(Region.of(localstack.getRegion()))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())))
                .build();
    }

    private static ObjectStoreConfiguration configuration(LocalStackContainer localstack) {
        return new ObjectStoreConfiguration(
                S3CompatibleObjectStoreProvider.class.getName(),
                localstack.getEndpointOverride(LocalStackContainer.Service.S3),
                localstack.getRegion(),
                BUCKET,
                "cursor-objects",
                true,
                Duration.ofSeconds(10),
                4,
                Optional.of("access"),
                Optional.of("secret"),
                Optional.empty());
    }

    private static ObjectStoreSecretResolver secrets(LocalStackContainer localstack) {
        return reference -> Optional.of(("access".equals(reference)
                ? localstack.getAccessKey()
                : localstack.getSecretKey()).toCharArray());
    }
}
