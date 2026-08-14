/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.pulsar.offload;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;
import com.nereusstream.pulsar.offload.PulsarOffloadObjectStoreV1.Body;
import com.nereusstream.pulsar.offload.PulsarOffloadObjectStoreV1.FailureKind;
import com.nereusstream.pulsar.offload.PulsarOffloadObjectStoreV1.ObjectStoreException;
import com.nereusstream.pulsar.offload.PulsarSealedLedgerAttemptV1.DeleteState;
import com.nereusstream.pulsar.offload.PulsarSealedLedgerAttemptV1.RetentionClass;
import com.nereusstream.pulsar.offload.PulsarSealedLedgerPublisherV1.PreparedAttempt;
import com.nereusstream.pulsar.offload.npd1.Npd1CodecV1;
import com.nereusstream.pulsar.offload.npd1.Npd1CodecV1.CompressionFamily;
import com.nereusstream.pulsar.offload.npd1.Npd1CodecV1.EntryPayload;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1.CustomMetadataValue;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1.DigestType;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1.EnsembleSegment;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1.SealedLedgerSection;
import java.io.ByteArrayInputStream;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsRequest;

@Testcontainers
class S3PulsarOffloadObjectStoreV1Test {
    private static final String BUCKET = "nereus-m2-p6";
    private static final PulsarOffloadLimitCandidateV1 LIMITS =
            PulsarOffloadLimitCandidateV1.adr0056EvidenceCandidate();
    private static final UUID ATTEMPT = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");
    private static final SecretKey KEY = new SecretKeySpec(new byte[32], "AES");

    @Container
    private static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:4.14.0")).withServices(S3);

    private static ExecutorService executor;
    private static S3Client client;
    private static S3PulsarOffloadObjectStoreV1 store;

    @TempDir
    Path temporaryDirectory;

    @BeforeAll
    static void setUp() {
        executor = Executors.newFixedThreadPool(4);
        client = S3Client.builder()
                .endpointOverride(LOCALSTACK.getEndpointOverride(S3))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey())))
                .region(Region.of(LOCALSTACK.getRegion()))
                .forcePathStyle(true)
                .build();
        client.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
        store = new S3PulsarOffloadObjectStoreV1(client, BUCKET, executor, true);
    }

    @AfterAll
    static void tearDown() {
        if (store != null) {
            store.close().toCompletableFuture().join();
        }
    }

    @Test
    void conditionallyCreatesHeadsRangesAndProvesDeletion() {
        byte[] bytes = "canonical-s3-body".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        String sha256 = sha256(bytes);
        Body body = new Body(bytes.length, sha256, () -> new ByteArrayInputStream(bytes));

        var proof = store.createImmutable("evidence/direct/data", body)
                .toCompletableFuture()
                .join();

        assertThat(proof)
                .isEqualTo(
                        store.head("evidence/direct/data").toCompletableFuture().join());
        assertThat(store.readRange("evidence/direct/data", 2, 7)
                        .toCompletableFuture()
                        .join())
                .containsExactly(java.util.Arrays.copyOfRange(bytes, 2, 9));
        assertThatThrownBy(() -> store.createImmutable("evidence/direct/data", body)
                        .toCompletableFuture()
                        .join())
                .satisfies(failure ->
                        assertThat(objectStoreFailure(failure).kind()).isEqualTo(FailureKind.CONFLICT));

        store.deleteAndProveAbsent("evidence/direct/data").toCompletableFuture().join();
        assertThatThrownBy(() ->
                        store.head("evidence/direct/data").toCompletableFuture().join())
                .satisfies(failure ->
                        assertThat(objectStoreFailure(failure).kind()).isEqualTo(FailureKind.NOT_FOUND));
    }

    @Test
    void abortsAndRelistsMultipartResidueByAttemptPrefix() {
        String prefix = "evidence/multipart/attempt/";
        client.createMultipartUpload(CreateMultipartUploadRequest.builder()
                .bucket(BUCKET)
                .key(prefix + "data")
                .build());
        assertThat(client.listMultipartUploads(ListMultipartUploadsRequest.builder()
                                .bucket(BUCKET)
                                .prefix(prefix)
                                .build())
                        .uploads())
                .hasSize(1);

        store.cleanupAttemptMultipartResidue(prefix).toCompletableFuture().join();

        assertThat(client.listMultipartUploads(ListMultipartUploadsRequest.builder()
                                .bucket(BUCKET)
                                .prefix(prefix)
                                .build())
                        .uploads())
                .isEmpty();
    }

    @Test
    void publishesAndReadsCanonicalNpd1Npo1ThroughTheProductionAdapter() {
        PreparedAttempt prepared = prepared();
        PulsarPublishedAttemptVerifierV1 verifier = new PulsarPublishedAttemptVerifierV1(store, LIMITS, attempt -> KEY);
        var publication = new PulsarSealedLedgerPublisherV1(store, LIMITS, verifier, Runnable::run)
                .publish(prepared)
                .toCompletableFuture()
                .join();

        PulsarObjectReadHandleV1 handle = PulsarObjectReadHandleV1.open(store, LIMITS, prepared.attempt(), KEY)
                .toCompletableFuture()
                .join();
        assertThat(handle.read(1, 3).toCompletableFuture().join())
                .extracting(EntryPayload::entryId)
                .containsExactly(1L, 2L, 3L);
        assertThat(handle.verifyCompleteLedger().toCompletableFuture().join().dataSha256())
                .isEqualTo(publication.dataObject().sha256());
        handle.close().toCompletableFuture().join();
    }

    private PreparedAttempt prepared() {
        List<EntryPayload> entries = List.of(
                new EntryPayload(0, new byte[] {0, 1, 2}),
                new EntryPayload(1, new byte[] {3, 4, 5}),
                new EntryPayload(2, new byte[] {6, 7, 8}),
                new EntryPayload(3, new byte[] {9, 10, 11}),
                new EntryPayload(4, new byte[] {12, 13, 14}));
        var data = Npd1CodecV1.encode(
                temporaryDirectory.resolve("p6.npd1"),
                entries,
                PulsarOffloadLimitCandidateV1.MIB,
                CompressionFamily.NONE,
                KEY,
                ATTEMPT,
                LIMITS);
        PulsarSealedLedgerAttemptV1 attempt = new PulsarSealedLedgerAttemptV1(
                42,
                ATTEMPT,
                4,
                5,
                15,
                100,
                7,
                "evidence/provider/cells/pulsar-a",
                RetentionClass.DELETE_AFTER_VERIFIED,
                DeleteState.BK_DELETE_NONE,
                false);
        SealedLedgerSection sealed = new SealedLedgerSection(
                4,
                5,
                15,
                100,
                7,
                3,
                3,
                2,
                DigestType.CRC32C,
                Map.of("binary", new CustomMetadataValue(new byte[] {0, (byte) 0xff})),
                List.of(new EnsembleSegment(0, List.of("bookie-1", "bookie-2", "bookie-3"))));
        return new PreparedAttempt(attempt, sealed, data, PulsarOffloadLimitCandidateV1.MIB);
    }

    private static ObjectStoreException objectStoreFailure(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        assertThat(current).isInstanceOf(ObjectStoreException.class);
        return (ObjectStoreException) current;
    }

    private static String sha256(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException(failure);
        }
    }
}
