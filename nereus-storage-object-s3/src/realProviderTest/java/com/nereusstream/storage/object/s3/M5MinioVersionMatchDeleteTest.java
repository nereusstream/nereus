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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.object.provider.C1ObjectProviderSession;
import com.nereusstream.storage.object.provider.M5ObjectDeleteSessionV1;
import com.nereusstream.storage.object.provider.M5ObjectDeleteSessionV1.ReconciliationKindV1;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import com.nereusstream.storage.object.provider.ObjectProviderCapabilities;
import com.nereusstream.storage.object.provider.ObjectProviderTransport;
import com.nereusstream.storage.object.provider.RepeatableObjectBody;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutBucketVersioningRequest;
import software.amazon.awssdk.services.s3.model.VersioningConfiguration;

class M5MinioVersionMatchDeleteTest {
    private static final String IMAGE =
            "quay.io/minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e";
    private static final String PRODUCT_VERSION = "RELEASE.2025-09-07T16-13-09Z";
    private static final String ACCESS_KEY = "nereus-m5-delete";
    private static final String SECRET_KEY = "nereus-m5-delete-secret";
    private static final String VERSIONED_BUCKET = "nereus-m5-versioned-delete";
    private static final String UNVERSIONED_BUCKET = "nereus-m5-unversioned-delete";

    @Test
    void exactVersionDeleteReconcilesLossAndNeverDeletesARecreatedVersion() throws Exception {
        try (DockerCliEvidenceContainer minio = DockerCliEvidenceContainer.start(
                "m5-version-delete",
                IMAGE,
                9000,
                List.of("MINIO_ROOT_USER=" + ACCESS_KEY, "MINIO_ROOT_PASSWORD=" + SECRET_KEY),
                List.of("server", "/data", "--console-address", ":9001"))) {
            URI endpoint = minio.endpoint();
            awaitHealthy(endpoint.resolve("/minio/health/ready"));
            try (S3Client client = client(endpoint)) {
                client.createBucket(
                        CreateBucketRequest.builder().bucket(VERSIONED_BUCKET).build());
                client.createBucket(
                        CreateBucketRequest.builder().bucket(UNVERSIONED_BUCKET).build());
                client.putBucketVersioning(PutBucketVersioningRequest.builder()
                        .bucket(VERSIONED_BUCKET)
                        .versioningConfiguration(VersioningConfiguration.builder()
                                .status(BucketVersioningStatus.ENABLED)
                                .build())
                        .build());

                assertThatThrownBy(() -> S3C1ObjectProviderTransport.admitVersionMatchDeleteV1(
                                client, UNVERSIONED_BUCKET, "minio/" + PRODUCT_VERSION, false, 2))
                        .isInstanceOf(S3C1ProviderException.class)
                        .hasMessageContaining("versioned bucket");

                try (S3C1ObjectProviderTransport transport = S3C1ObjectProviderTransport.admitVersionMatchDeleteV1(
                        client, VERSIONED_BUCKET, "minio/" + PRODUCT_VERSION, false, 2)) {
                    transport.deleteCapabilities().requireVersionMatchDeleteV1();
                    CellProviderScopeId scope = scope();
                    M5ObjectDeleteSessionV1 delete = deleteSession(transport, scope);
                    Body first = new Body("m5-delete/objects/first", bytes("first-body"));
                    try (C1ObjectProviderSession create = createSession(transport, scope)) {
                        assertThat(create.conditionalCreate(first).outcome().name())
                                .isEqualTo("APPLIED_EXACT");
                    }
                    CanonicalBytes firstVersion =
                            delete.readExactForDelete(first.identity()).immutableVersionToken();
                    assertThat(delete.deleteExactVersion(first.identity(), firstVersion))
                            .isEqualTo(ObjectProviderTransport.ConditionalDeleteResult.DELETED_EXACT);
                    assertThat(delete.reconcile(first.identity(), "m5-delete/objects/", firstVersion)
                                    .kind())
                            .isEqualTo(ReconciliationKindV1.AUTHORITATIVELY_ABSENT);

                    Body recreated = new Body("m5-delete/objects/first", bytes("recreated-body"));
                    try (C1ObjectProviderSession create = createSession(transport, scope)) {
                        assertThat(create.conditionalCreate(recreated).outcome().name())
                                .isEqualTo("APPLIED_EXACT");
                    }
                    CanonicalBytes recreatedVersion =
                            delete.readExactForDelete(recreated.identity()).immutableVersionToken();
                    assertThat(recreatedVersion).isNotEqualTo(firstVersion);
                    delete.deleteExactVersion(first.identity(), firstVersion);
                    assertThat(delete.readExactForDelete(recreated.identity()).immutableVersionToken())
                            .isEqualTo(recreatedVersion);

                    Body responseLoss = new Body("m5-delete/objects/response-loss", bytes("response-loss-body"));
                    try (C1ObjectProviderSession create = createSession(transport, scope)) {
                        assertThat(create.conditionalCreate(responseLoss)
                                        .outcome()
                                        .name())
                                .isEqualTo("APPLIED_EXACT");
                    }
                    ObjectProviderTransport faultCut = new LostDeleteResponseTransport(transport);
                    M5ObjectDeleteSessionV1 lost = deleteSession(faultCut, scope);
                    CanonicalBytes responseLossVersion =
                            lost.readExactForDelete(responseLoss.identity()).immutableVersionToken();
                    assertThat(lost.deleteExactVersion(responseLoss.identity(), responseLossVersion))
                            .isEqualTo(ObjectProviderTransport.ConditionalDeleteResult.RESPONSE_UNKNOWN);
                    assertThat(lost.reconcile(responseLoss.identity(), "m5-delete/objects/", responseLossVersion)
                                    .kind())
                            .isEqualTo(ReconciliationKindV1.AUTHORITATIVELY_ABSENT);
                }
            }
            assertThat(minio.imageConfigDigest())
                    .isEqualTo("sha256:8f08aee614800a237906bd48114d733e5ac5bfac4ccdf731f141b0e880d7a253");
        }
    }

    private static C1ObjectProviderSession createSession(ObjectProviderTransport transport, CellProviderScopeId scope) {
        return new C1ObjectProviderSession(transport, scope, "m5-delete", 1_024, 1_024);
    }

    private static M5ObjectDeleteSessionV1 deleteSession(ObjectProviderTransport transport, CellProviderScopeId scope) {
        return new M5ObjectDeleteSessionV1(transport, scope, "m5-delete", 1_024, 8, 128, 64_000, 1_024);
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

    private static CanonicalBytes bytes(String value) {
        return CanonicalBytes.copyOf(value.getBytes(StandardCharsets.US_ASCII));
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

    private record Body(ObjectIdentity identity, byte[] body) implements RepeatableObjectBody {
        private Body(String key, CanonicalBytes body) {
            this(new ObjectIdentity(key, body.length(), Sha256Digest.hash(body)), body.toByteArray());
        }

        private Body {
            body = body.clone();
        }

        @Override
        public InputStream openStream() {
            return new ByteArrayInputStream(body);
        }
    }

    private record LostDeleteResponseTransport(ObjectProviderTransport delegate) implements ObjectProviderTransport {
        private LostDeleteResponseTransport {
            delegate = java.util.Objects.requireNonNull(delegate, "delegate");
        }

        @Override
        public ObjectProviderCapabilities capabilities() {
            return delegate.capabilities();
        }

        @Override
        public ObjectDeleteCapabilities deleteCapabilities() {
            return delegate.deleteCapabilities();
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
        public ConditionalDeleteResult deleteExactVersion(String key, CanonicalBytes exactVersionToken)
                throws IOException {
            delegate.deleteExactVersion(key, exactVersionToken);
            return ConditionalDeleteResult.RESPONSE_UNKNOWN;
        }

        @Override
        public FailureKind classifyFailure(IOException failure) {
            return delegate.classifyFailure(failure);
        }
    }
}
