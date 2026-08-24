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
import com.nereusstream.pulsar.offload.PulsarOffloadObjectStoreV1.Body;
import com.nereusstream.pulsar.offload.PulsarOffloadObjectStoreV1.FailureKind;
import com.nereusstream.pulsar.offload.PulsarOffloadObjectStoreV1.ObjectStoreException;
import com.nereusstream.pulsar.offload.PulsarSealedLedgerAttemptV1.DeleteState;
import com.nereusstream.pulsar.offload.PulsarSealedLedgerAttemptV1.RetentionClass;
import com.nereusstream.pulsar.offload.PulsarSealedLedgerPublisherV1.PreparedAttempt;
import com.nereusstream.pulsar.offload.npd1.Npd1CodecV1;
import com.nereusstream.pulsar.offload.npd1.Npd1CodecV1.CompressionPolicy;
import com.nereusstream.pulsar.offload.npd1.Npd1CodecV1.EntryPayload;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1.AttemptKeyEnvelope;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1.CustomMetadataValue;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1.DigestType;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1.EnsembleSegment;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1.SealedLedgerSection;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsRequest;

class P6MinioProviderEvidenceTest {
    private static final String BUCKET = "nereus-m2-p6-provider";
    private static final PulsarOffloadLimitCandidateV1 LIMITS =
            PulsarOffloadLimitCandidateV1.adr0056EvidenceCandidate();
    private static final SecretKey KEY = new SecretKeySpec(new byte[32], "AES");
    private static final AttemptKeyEnvelope KEY_ENVELOPE = new AttemptKeyEnvelope(
            1, "test-kms", "cells/pulsar-a/kms", "version-7", "aes-kwp", new byte[] {1, 2, 3, 4});
    private static final UUID ATTEMPT = UUID.fromString("3a31bc29-9d41-45f5-b481-ff03228fa538");

    @TempDir
    Path temporaryDirectory;

    @Test
    void provesProductionAdapterAgainstFixedMinioProvider() throws Exception {
        String endpoint = requiredEnvironment("NEREUS_P6_MINIO_ENDPOINT");
        String accessKey = requiredEnvironment("NEREUS_P6_MINIO_ACCESS_KEY");
        String secretKey = requiredEnvironment("NEREUS_P6_MINIO_SECRET_KEY");
        String sourceCommit = requiredProperty("nereus.p6.testedSourceCommit");
        String imageReference = requiredProperty("nereus.p6.minioImageReference");
        String imageDigest = requiredProperty("nereus.p6.minioImageDigest");
        Path output = Path.of(requiredProperty("nereus.p6.realProviderOutput"));
        ExecutorService executor = Executors.newFixedThreadPool(4);
        S3Client client = S3Client.builder()
                .endpointOverride(URI.create(endpoint))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
                .region(Region.US_EAST_1)
                .forcePathStyle(true)
                .build();
        S3PulsarOffloadObjectStoreV1 store = new S3PulsarOffloadObjectStoreV1(client, BUCKET, executor, true);
        try {
            client.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
            directProviderContract(store);
            multipartCleanup(store, client);
            canonicalPublication(store);
            Files.createDirectories(output.toAbsolutePath().getParent());
            Files.writeString(output, json(sourceCommit, imageReference, imageDigest), StandardCharsets.UTF_8);
        } finally {
            store.close().toCompletableFuture().join();
        }
    }

    private static void directProviderContract(S3PulsarOffloadObjectStoreV1 store) {
        byte[] bytes = "minio-production-provider-proof".getBytes(StandardCharsets.UTF_8);
        Body body = new Body(bytes.length, sha256(bytes), () -> new ByteArrayInputStream(bytes));
        var created = store.createImmutable("direct/object", body)
                .toCompletableFuture()
                .join();
        assertThat(store.head("direct/object").toCompletableFuture().join()).isEqualTo(created);
        assertThat(store.readRange("direct/object", 6, 10).toCompletableFuture().join())
                .containsExactly(java.util.Arrays.copyOfRange(bytes, 6, 16));
        assertThatThrownBy(() -> store.createImmutable("direct/object", body)
                        .toCompletableFuture()
                        .join())
                .satisfies(failure ->
                        assertThat(objectStoreFailure(failure).kind()).isEqualTo(FailureKind.CONFLICT));
        store.deleteAndProveAbsent("direct/object").toCompletableFuture().join();
    }

    private static void multipartCleanup(S3PulsarOffloadObjectStoreV1 store, S3Client client) {
        String prefix = "multipart/attempt/";
        client.createMultipartUpload(CreateMultipartUploadRequest.builder()
                .bucket(BUCKET)
                .key(prefix + "data")
                .build());
        store.cleanupAttemptMultipartResidue(prefix).toCompletableFuture().join();
        assertThat(client.listMultipartUploads(ListMultipartUploadsRequest.builder()
                                .bucket(BUCKET)
                                .prefix(prefix)
                                .build())
                        .uploads())
                .isEmpty();
    }

    private void canonicalPublication(S3PulsarOffloadObjectStoreV1 store) {
        List<EntryPayload> entries = new ArrayList<>();
        for (int entryId = 0; entryId < 2_000; entryId++) {
            entries.add(new EntryPayload(entryId, new byte[1_000]));
        }
        Npd1CodecV1.DataObject data;
        try (var encoder = Npd1CodecV1.openStreaming(
                temporaryDirectory.resolve("minio.npd1"),
                PulsarOffloadLimitCandidateV1.MIB,
                CompressionPolicy.ZSTD_IF_SMALLER,
                KEY,
                ATTEMPT,
                LIMITS)) {
            entries.forEach(encoder::append);
            data = encoder.finish();
        }
        PulsarSealedLedgerAttemptV1 attempt = new PulsarSealedLedgerAttemptV1(
                73,
                ATTEMPT,
                1_999,
                2_000,
                2_000_000,
                100,
                7,
                "provider/minio/cells/pulsar-a",
                RetentionClass.DELETE_AFTER_VERIFIED,
                DeleteState.BK_DELETE_NONE,
                false);
        SealedLedgerSection sealed = new SealedLedgerSection(
                1_999,
                2_000,
                2_000_000,
                100,
                7,
                3,
                3,
                2,
                DigestType.CRC32C,
                Map.of("provider", new CustomMetadataValue(new byte[] {1})),
                List.of(new EnsembleSegment(0, List.of("bookie-1", "bookie-2", "bookie-3"))));
        PreparedAttempt prepared =
                new PreparedAttempt(attempt, sealed, data, PulsarOffloadLimitCandidateV1.MIB, KEY_ENVELOPE);
        PulsarPublishedAttemptVerifierV1 verifier =
                new PulsarPublishedAttemptVerifierV1(store, LIMITS, (ignored, envelope) -> KEY);
        new PulsarSealedLedgerPublisherV1(store, LIMITS, verifier, Runnable::run)
                .publish(prepared)
                .toCompletableFuture()
                .join();
        PulsarObjectReadHandleV1 handle = PulsarObjectReadHandleV1.open(store, LIMITS, attempt, KEY)
                .toCompletableFuture()
                .join();
        assertThat(handle.read(999, 1_001).toCompletableFuture().join())
                .extracting(EntryPayload::entryId)
                .containsExactly(999L, 1_000L, 1_001L);
        handle.verifyCompleteLedger().toCompletableFuture().join();
        handle.close().toCompletableFuture().join();
        new PulsarSealedLedgerPublisherV1(
                        store,
                        LIMITS,
                        ignored -> java.util.concurrent.CompletableFuture.completedFuture(null),
                        Runnable::run)
                .deleteAttempt(attempt)
                .toCompletableFuture()
                .join();
    }

    private static String json(String sourceCommit, String imageReference, String imageDigest) {
        return String.format(
                Locale.ROOT,
                "{\n"
                        + "  \"schema\": \"NEREUS_V2_M2_PULSAR_P6_REAL_PROVIDER_V1\",\n"
                        + "  \"generatedAt\": \"%s\",\n"
                        + "  \"testedSourceCommit\": \"%s\",\n"
                        + "  \"provider\": \"MINIO_S3_COMPATIBLE\",\n"
                        + "  \"imageReference\": \"%s\",\n"
                        + "  \"imageDigest\": \"%s\",\n"
                        + "  \"conditionalCreate\": true,\n"
                        + "  \"boundedRangeRead\": true,\n"
                        + "  \"deleteAndProveAbsent\": true,\n"
                        + "  \"multipartCleanupAndRelist\": true,\n"
                        + "  \"canonicalNpd1Npo1RoundTrip\": true,\n"
                        + "  \"result\": \"PASS_MINIO_PROVIDER_ONLY\",\n"
                        + "  \"claimBoundary\": \"Admits this MinIO release only; "
                        + "no Amazon S3 endorsement or performance claim\"\n"
                        + "}\n",
                Instant.now(),
                sourceCommit,
                imageReference,
                imageDigest);
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

    private static String requiredEnvironment(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("missing required MinIO evidence environment " + name);
        }
        return value;
    }

    private static String requiredProperty(String name) {
        String value = System.getProperty(name);
        if (value == null || value.isBlank() || "UNSET".equals(value)) {
            throw new IllegalArgumentException("missing required MinIO evidence property " + name);
        }
        return value;
    }
}
