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

package com.nereusstream.storage.object.s3;

import static org.assertj.core.api.Assertions.assertThat;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.object.provider.M5MultipartCleanupSessionV1;
import com.nereusstream.storage.object.provider.M5MultipartCleanupSessionV1.CleanupKindV1;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import com.nereusstream.storage.object.provider.ObjectProviderCapabilities;
import com.nereusstream.storage.object.provider.ObjectProviderTransport;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsRequest;
import software.amazon.awssdk.services.s3.model.ListPartsRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

class M5MinioMultipartCleanupTest {
    private static final String IMAGE =
            "quay.io/minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e";
    private static final String PRODUCT_VERSION = "RELEASE.2025-09-07T16-13-09Z";
    private static final String ACCESS_KEY = "nereus-m5-multipart";
    private static final String SECRET_KEY = "nereus-m5-multipart-secret";
    private static final String BUCKET = "nereus-m5-multipart-cleanup";

    @Test
    void exactOwnedInventoryReconcilesPaginationResponseLossAndForeignResidue() throws Exception {
        try (DockerCliEvidenceContainer minio = DockerCliEvidenceContainer.start(
                "m5-multipart-cleanup",
                IMAGE,
                9000,
                List.of("MINIO_ROOT_USER=" + ACCESS_KEY, "MINIO_ROOT_PASSWORD=" + SECRET_KEY),
                List.of("server", "/data", "--console-address", ":9001"))) {
            URI endpoint = minio.endpoint();
            awaitHealthy(endpoint.resolve("/minio/health/ready"));
            try (S3Client client = client(endpoint)) {
                client.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
                try (S3C1ObjectProviderTransport transport = S3C1ObjectProviderTransport.admitMultipartCleanupV1(
                        client, BUCKET, "minio/" + PRODUCT_VERSION, false, 1)) {
                    transport.multipartCleanupCapabilities().requireExactUploadIdAbortV1();

                    var sameKeyFirst = create(client, "cell-a/attempt-1/data");
                    var sameKeySecond = create(client, "cell-a/attempt-1/data");
                    assertThat(client.listParts(ListPartsRequest.builder()
                                            .bucket(BUCKET)
                                            .key(sameKeyFirst.key())
                                            .uploadId(new String(
                                                    sameKeyFirst.uploadId().toByteArray(), StandardCharsets.UTF_8))
                                            .build())
                                    .parts())
                            .hasSize(1);
                    assertThat(client.listMultipartUploads(ListMultipartUploadsRequest.builder()
                                            .bucket(BUCKET)
                                            .prefix("cell-a/attempt-1/")
                                            .build())
                                    .uploads())
                            .as("this MinIO release does not admit directory-prefix multipart listing")
                            .isEmpty();
                    assertThat(client.listMultipartUploads(ListMultipartUploadsRequest.builder()
                                            .bucket(BUCKET)
                                            .prefix("cell-a/attempt-1/data")
                                            .build())
                                    .uploads())
                            .hasSize(2);
                    Set<ObjectProviderTransport.MultipartUploadIdentity> paginated =
                            Set.of(sameKeyFirst, sameKeySecond);
                    M5MultipartCleanupSessionV1 first = session(transport, "cell-a/attempt-1");
                    var paginatedResult = first.cleanupOwned(first.inventoryRoot(paginated), paginated);
                    assertThat(paginatedResult.kind()).isEqualTo(CleanupKindV1.AUTHORITATIVELY_ABSENT);
                    assertThat(paginatedResult.totalListPages()).isGreaterThanOrEqualTo(3);

                    var responseLoss = create(client, "cell-a/attempt-2/root");
                    ObjectProviderTransport faultCut = new LostAbortResponseTransport(transport);
                    M5MultipartCleanupSessionV1 second = session(faultCut, "cell-a/attempt-2");
                    Set<ObjectProviderTransport.MultipartUploadIdentity> lostOwned = Set.of(responseLoss);
                    assertThat(second.cleanupOwned(second.inventoryRoot(lostOwned), lostOwned)
                                    .kind())
                            .isEqualTo(CleanupKindV1.AUTHORITATIVELY_ABSENT);

                    var owned = create(client, "cell-a/attempt-3/data");
                    var foreign = create(client, "cell-a/attempt-3/data");
                    M5MultipartCleanupSessionV1 third = session(transport, "cell-a/attempt-3");
                    Set<ObjectProviderTransport.MultipartUploadIdentity> exactOwned = Set.of(owned);
                    var foreignResult = third.cleanupOwned(third.inventoryRoot(exactOwned), exactOwned);
                    assertThat(foreignResult.kind()).isEqualTo(CleanupKindV1.DIFFERENT_OR_FOREIGN_IDENTITY);
                    assertThat(foreignResult.exactAbortAttempts()).isZero();

                    abort(client, owned);
                    abort(client, foreign);
                }
            }
            assertThat(minio.imageConfigDigest())
                    .isEqualTo("sha256:8f08aee614800a237906bd48114d733e5ac5bfac4ccdf731f141b0e880d7a253");
        }
    }

    private static M5MultipartCleanupSessionV1 session(ObjectProviderTransport transport, String namespace) {
        return new M5MultipartCleanupSessionV1(transport, scope(), namespace, 16, 1_024, 512_000, 1_024);
    }

    private static ObjectProviderTransport.MultipartUploadIdentity create(S3Client client, String key) {
        String uploadId = client.createMultipartUpload(CreateMultipartUploadRequest.builder()
                        .bucket(BUCKET)
                        .key(key)
                        .build())
                .uploadId();
        client.uploadPart(
                UploadPartRequest.builder()
                        .bucket(BUCKET)
                        .key(key)
                        .uploadId(uploadId)
                        .partNumber(1)
                        .build(),
                RequestBody.fromBytes(new byte[5 * 1_024 * 1_024]));
        return new ObjectProviderTransport.MultipartUploadIdentity(
                key, CanonicalBytes.copyOf(uploadId.getBytes(StandardCharsets.UTF_8)));
    }

    private static void abort(S3Client client, ObjectProviderTransport.MultipartUploadIdentity identity) {
        client.abortMultipartUpload(AbortMultipartUploadRequest.builder()
                .bucket(BUCKET)
                .key(identity.key())
                .uploadId(new String(identity.uploadId().toByteArray(), StandardCharsets.UTF_8))
                .build());
    }

    private static S3Client client(URI endpoint) {
        return S3Client.builder()
                .endpointOverride(endpoint)
                .credentialsProvider(
                        StaticCredentialsProvider.create(AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                .region(Region.US_EAST_1)
                .forcePathStyle(true)
                .build();
    }

    private static CellProviderScopeId scope() {
        byte[] raw = new byte[Sha256Digest.LENGTH];
        raw[raw.length - 1] = 1;
        return new CellProviderScopeId(Sha256Digest.copyOf(raw));
    }

    private static void awaitHealthy(URI uri) throws Exception {
        var client = java.net.http.HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (System.nanoTime() < deadline) {
            try {
                var response = client.send(
                        java.net.http.HttpRequest.newBuilder(uri)
                                .timeout(Duration.ofSeconds(2))
                                .GET()
                                .build(),
                        java.net.http.HttpResponse.BodyHandlers.discarding());
                if (response.statusCode() == 200) {
                    return;
                }
            } catch (IOException ignored) {
                // The exact-digest product is still starting.
            }
            Thread.sleep(100);
        }
        throw new IllegalStateException("MinIO did not become healthy within the bounded startup deadline");
    }

    private record LostAbortResponseTransport(ObjectProviderTransport delegate) implements ObjectProviderTransport {
        private LostAbortResponseTransport {
            delegate = java.util.Objects.requireNonNull(delegate, "delegate");
        }

        @Override
        public ObjectProviderCapabilities capabilities() {
            return delegate.capabilities();
        }

        @Override
        public MultipartCleanupCapabilities multipartCleanupCapabilities() {
            return delegate.multipartCleanupCapabilities();
        }

        @Override
        public ConditionalCreateResult putIfAbsent(ObjectIdentity identity, InputStream body) throws IOException {
            return delegate.putIfAbsent(identity, body);
        }

        @Override
        public StreamingObject get(String key, Optional<CanonicalBytes> exactVersionToken) throws IOException {
            return delegate.get(key, exactVersionToken);
        }

        @Override
        public StreamingObject getRange(
                String key, long inclusiveStart, long exclusiveEnd, Optional<CanonicalBytes> versionToken)
                throws IOException {
            return delegate.getRange(key, inclusiveStart, exclusiveEnd, versionToken);
        }

        @Override
        public ListPage list(String prefix, Optional<CanonicalBytes> continuationToken, int maximumKeys)
                throws IOException {
            return delegate.list(prefix, continuationToken, maximumKeys);
        }

        @Override
        public MultipartListPage listMultipartUploads(
                String prefix, Optional<CanonicalBytes> continuationToken, int maximumUploads) throws IOException {
            return delegate.listMultipartUploads(prefix, continuationToken, maximumUploads);
        }

        @Override
        public ExactMultipartAbortResult abortMultipartUploadExact(String key, CanonicalBytes exactUploadId)
                throws IOException {
            delegate.abortMultipartUploadExact(key, exactUploadId);
            return ExactMultipartAbortResult.RESPONSE_UNKNOWN;
        }

        @Override
        public FailureKind classifyFailure(IOException failure) {
            return delegate.classifyFailure(failure);
        }
    }
}
