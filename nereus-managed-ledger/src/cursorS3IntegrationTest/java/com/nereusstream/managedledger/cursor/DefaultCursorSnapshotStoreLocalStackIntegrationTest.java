/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.metadata.oxia.CursorNames;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionNames;
import com.nereusstream.metadata.oxia.records.ManagedLedgerProjectionIdentity;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.ObjectStoreConfiguration;
import com.nereusstream.objectstore.ObjectStoreSecretResolver;
import com.nereusstream.objectstore.S3CompatibleObjectStoreProvider;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.List;
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

            S3CompatibleObjectStoreProvider firstProvider = new S3CompatibleObjectStoreProvider();
            ObjectStore firstObjectStore = firstProvider.create(configuration, secrets(localstack));
            try (DefaultCursorSnapshotStore first = new DefaultCursorSnapshotStore(
                    "cluster/s3-integration",
                    firstObjectStore,
                    CursorStorageConfig.defaults(),
                    Duration.ofSeconds(10),
                    Clock.systemUTC())) {
                reference = first.write(request).join();
                assertThat(first.read(reference, identity()).join()).isEqualTo(expected);

                ArrayDeque<String> ids = new ArrayDeque<>(List.of(
                        reference.snapshotId(), COLLISION_RETRY_ID));
                try (DefaultCursorSnapshotStore collisionRetry = new DefaultCursorSnapshotStore(
                        "cluster/s3-integration",
                        firstObjectStore,
                        CursorStorageConfig.defaults(),
                        Duration.ofSeconds(10),
                        Clock.systemUTC(),
                        ids::removeFirst,
                        System::nanoTime)) {
                    CursorSnapshotReference retried = collisionRetry.write(request).join();
                    assertThat(retried.snapshotId()).isEqualTo(COLLISION_RETRY_ID);
                    assertThat(retried.objectKey()).isNotEqualTo(reference.objectKey());
                    assertThat(collisionRetry.read(retried, identity()).join()).isEqualTo(expected);
                }
            } finally {
                firstObjectStore.close();
                firstProvider.close();
            }

            S3CompatibleObjectStoreProvider restartedProvider = new S3CompatibleObjectStoreProvider();
            ObjectStore restartedObjectStore = restartedProvider.create(configuration, secrets(localstack));
            try (DefaultCursorSnapshotStore restarted = new DefaultCursorSnapshotStore(
                    "cluster/s3-integration",
                    restartedObjectStore,
                    CursorStorageConfig.defaults(),
                    Duration.ofSeconds(10),
                    Clock.systemUTC())) {
                assertThat(restarted.read(reference, identity()).join()).isEqualTo(expected);
            } finally {
                restartedObjectStore.close();
                restartedProvider.close();
            }
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
