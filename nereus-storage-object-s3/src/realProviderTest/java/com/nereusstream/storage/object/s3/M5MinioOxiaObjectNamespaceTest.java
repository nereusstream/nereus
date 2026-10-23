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
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMetadataNamespaceV2;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.object.gc.DeleteObservationContextV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCoordinatorV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ExternalIdentityObservationV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.PhysicalDeleteTargetV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterClassV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterEnrollmentV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityStateMachineV1;
import com.nereusstream.storage.object.gc.SyntheticDeleteAuthorityFixturesV2;
import com.nereusstream.storage.object.provider.C1ObjectProviderSession;
import com.nereusstream.storage.object.provider.M5ObjectDeleteSessionV1;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import com.nereusstream.storage.object.provider.RepeatableObjectBody;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1;
import io.oxia.client.api.OxiaClientBuilder;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsRequest;
import software.amazon.awssdk.services.s3.model.PutBucketVersioningRequest;
import software.amazon.awssdk.services.s3.model.VersioningConfiguration;

/** Real S3/Oxia binding and exact native identity read; protocol eligibility is intentionally synthetic. */
class M5MinioOxiaObjectNamespaceTest {
    private static final String MINIO_IMAGE =
            "quay.io/minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e";
    private static final String BUCKET = "nereus-m5-oxia-object-namespace";
    private static final String ACCESS_KEY = "nereus-m5-object-oxia";
    private static final String SECRET_KEY = "nereus-m5-object-oxia-secret";

    @Test
    void boundRouteAndNativeVersionReaderRejectChangedPhysicalMarker() throws Exception {
        String oxiaAddress = System.getProperty("nereus.m5.objectOxiaAddress");
        if (oxiaAddress == null || oxiaAddress.isBlank()) {
            throw new IllegalStateException("native S3/Oxia test requires a real Oxia service address");
        }
        try (var minio = DockerCliEvidenceContainer.start(
                        "m5-object-oxia-namespace",
                        MINIO_IMAGE,
                        9000,
                        List.of("MINIO_ROOT_USER=" + ACCESS_KEY, "MINIO_ROOT_PASSWORD=" + SECRET_KEY),
                        List.of("server", "/data", "--console-address", ":9001"));
                var oxia = OxiaClientBuilder.create(oxiaAddress)
                        .namespace("default")
                        .requestTimeout(Duration.ofSeconds(10))
                        .asyncClient()
                        .get(30, TimeUnit.SECONDS);
                var aliasOxia = OxiaClientBuilder.create(oxiaAddress)
                        .namespace("default")
                        .requestTimeout(Duration.ofSeconds(10))
                        .asyncClient()
                        .get(30, TimeUnit.SECONDS)) {
            awaitHealthy(minio.endpoint().resolve("/minio/health/ready"));
            try (var s3 = s3(minio.endpoint())) {
                s3.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
                s3.putBucketVersioning(PutBucketVersioningRequest.builder()
                        .bucket(BUCKET)
                        .versioningConfiguration(VersioningConfiguration.builder()
                                .status(BucketVersioningStatus.ENABLED)
                                .build())
                        .build());
                var metadata = OxiaPhysicalMetadataNamespaceV2.provision(oxia)
                        .toCompletableFuture()
                        .join();
                var backend = S3M5PhysicalNamespaceAuthorityV2.provision(s3, BUCKET, metadata.identity());
                var binding = metadata.bind(backend).toCompletableFuture().join();
                assertThat(binding.physicalNamespace()).isEqualTo(backend.namespace());
                var route = metadata.openAuthorityRoute(backend, new Oxia09ExactMetadataTransactionStoreV1(oxia))
                        .toCompletableFuture()
                        .join();
                route.initialize(2_200_000).toCompletableFuture().join();
                var facts = new Oxia09ExactMetadataTransactionStoreV1(oxia);

                Body body = new Body("m5-object-oxia/objects/target", "native-object-body");
                CellProviderScopeId scope = scope();
                try (var transport = backend.openVersionMatchDeleteTransport(2)) {
                    try (var create =
                            new C1ObjectProviderSession(transport, scope, "m5-object-oxia", 1_024 * 1_024, 1_024)) {
                        assertThat(create.conditionalCreate(body).outcome().name())
                                .isEqualTo("APPLIED_EXACT");
                    }
                    CanonicalBytes version = new M5ObjectDeleteSessionV1(
                                    transport, scope, "m5-object-oxia", 1_024 * 1_024, 8, 128, 64_000, 1_024)
                            .readExactForDelete(body.identity())
                            .immutableVersionToken();
                    AtomicBoolean loseDeleteAck = new AtomicBoolean(true);
                    S3Client lossyS3 = (S3Client) Proxy.newProxyInstance(
                            S3Client.class.getClassLoader(), new Class<?>[] {S3Client.class}, (proxy, method, args) -> {
                                Object response;
                                try {
                                    response = method.invoke(s3, args);
                                } catch (InvocationTargetException failure) {
                                    throw failure.getCause();
                                }
                                if ("deleteObject".equals(method.getName())
                                        && args[0] instanceof DeleteObjectRequest request
                                        && body.identity().key().equals(request.key())
                                        && request.versionId() != null
                                        && loseDeleteAck.compareAndSet(true, false)) {
                                    throw SdkClientException.create("injected S3 delete acknowledgement loss");
                                }
                                return response;
                            });
                    var deletingBackend =
                            S3M5PhysicalNamespaceAuthorityV2.connect(lossyS3, BUCKET, backend.namespace());
                    try (var reader = deletingBackend.openDeleteIdentityReader(
                            route,
                            scope,
                            "m5-object-oxia",
                            "m5-object-oxia/objects/",
                            body.identity(),
                            version,
                            1_024 * 1_024,
                            8,
                            128,
                            64_000,
                            1_024,
                            2)) {
                        var resource = reader.resource();
                        Map<String, VersionedValue> seeded = new LinkedHashMap<>();
                        java.util.function.BiFunction<String, CanonicalBytes, AuthorityFactV1> fact = (key, value) -> {
                            VersionedValue stored = seeded.computeIfAbsent(key, ignored -> {
                                if (facts.read(key).toCompletableFuture().join().isEmpty()) {
                                    assertThat(facts.compareAndSet(Optional.empty(), key, value)
                                                    .toCompletableFuture()
                                                    .join())
                                            .isEqualTo(ExactMetadataTransactionStoreV1.MutationOutcome.APPLIED_EXACT);
                                }
                                return facts.read(key)
                                        .toCompletableFuture()
                                        .join()
                                        .orElseThrow();
                            });
                            assertThat(stored.canonicalStoredBytes()).isEqualTo(value);
                            return new AuthorityFactV1(key, stored.metadataVersion(), stored.canonicalStoredSha256());
                        };
                        var snapshot = SyntheticDeleteAuthorityFixturesV2.replacement(resource, 1, fact);
                        var context = new DeleteObservationContextV2(
                                1,
                                fact.apply(
                                        "/dispatch/owner/one",
                                        CanonicalUtf8.fromString("owner-one").bytes()),
                                fact.apply(
                                        "/dispatch/capability/one",
                                        CanonicalUtf8.fromString("capability-one")
                                                .bytes()),
                                Optional.empty());
                        var verifier = SyntheticDeleteAuthorityFixturesV2.syntheticObservationVerifier();
                        var coordinator = new M5TargetDeleteAuthorityCoordinatorV1(route, verifier, reader);
                        var open = M5TargetDeleteAuthorityStateMachineV1.open(
                                PhysicalDeleteTargetV1.create(resource),
                                new ProofBoundWriterEnrollmentV1(
                                        List.of(ProofBoundWriterClassV1.values()),
                                        SyntheticDeleteAuthorityFixturesV2.digest("writer-set"),
                                        SyntheticDeleteAuthorityFixturesV2.digest("admission"),
                                        SyntheticDeleteAuthorityFixturesV2.digest("native-synthetic")),
                                snapshot);
                        var exact = coordinator
                                .create(open)
                                .toCompletableFuture()
                                .join()
                                .observed()
                                .orElseThrow();
                        var exactFence = coordinator
                                .prepareIdentityRead(exact, SyntheticDeleteAuthorityFixturesV2.digest("read"), context)
                                .toCompletableFuture()
                                .join()
                                .observed()
                                .orElseThrow();
                        var aliasBackend = S3M5PhysicalNamespaceAuthorityV2.connect(s3, BUCKET, backend.namespace());
                        var aliasMetadata = OxiaPhysicalMetadataNamespaceV2.connect(aliasOxia, metadata.identity())
                                .toCompletableFuture()
                                .join();
                        var aliasRoute = aliasMetadata
                                .openAuthorityRoute(aliasBackend, new Oxia09ExactMetadataTransactionStoreV1(aliasOxia))
                                .toCompletableFuture()
                                .join();
                        assertThat(aliasRoute
                                        .read(resource.authorityKey())
                                        .toCompletableFuture()
                                        .join())
                                .contains(exactFence);
                        byte[] staleVersion =
                                exactFence.metadataVersion().value().toByteArray();
                        staleVersion[0] ^= 1;
                        var staleFence = ExactMetadataTransactionStoreV1.VersionedValue.of(
                                exactFence.key(),
                                exactFence.canonicalStoredBytes(),
                                new MetadataVersion(CanonicalBytes.copyOf(staleVersion)));
                        assertThatThrownBy(() -> reader.capture(staleFence)
                                        .toCompletableFuture()
                                        .join())
                                .hasRootCauseMessage("S3 identity capture lacks the exact persisted read fence");
                        var captured =
                                reader.capture(exactFence).toCompletableFuture().join();
                        assertThat(captured.observation()).isEqualTo(ExternalIdentityObservationV1.PRESENT_EXACT_V1);
                        assertThat(reader.rereadExact(captured)
                                        .toCompletableFuture()
                                        .join())
                                .isEqualTo(ExternalIdentityObservationV1.PRESENT_EXACT_V1);
                        var intent = coordinator
                                .bindDeleteIntent(
                                        exactFence, captured, SyntheticDeleteAuthorityFixturesV2.digest("delete"))
                                .toCompletableFuture()
                                .join()
                                .observed()
                                .orElseThrow();
                        assertThatThrownBy(() -> reader.deletePersistedIntent(exactFence, verifier)
                                        .toCompletableFuture()
                                        .join())
                                .hasRootCauseMessage("S3 physical delete requires this exact target INTENT");
                        byte[] staleIntentVersion =
                                intent.metadataVersion().value().toByteArray();
                        staleIntentVersion[0] ^= 1;
                        var staleIntent = VersionedValue.of(
                                intent.key(),
                                intent.canonicalStoredBytes(),
                                new MetadataVersion(CanonicalBytes.copyOf(staleIntentVersion)));
                        assertThatThrownBy(() -> reader.deletePersistedIntent(staleIntent, verifier)
                                        .toCompletableFuture()
                                        .join())
                                .hasRootCauseMessage("dispatch authority changed before native identity read");
                        assertThat(reader.rereadExact(captured)
                                        .toCompletableFuture()
                                        .join())
                                .isEqualTo(ExternalIdentityObservationV1.PRESENT_EXACT_V1);
                        String uploadId = s3.createMultipartUpload(CreateMultipartUploadRequest.builder()
                                        .bucket(BUCKET)
                                        .key(body.identity().key())
                                        .build())
                                .uploadId();
                        String unrelatedKey = "m5-object-oxia/objects/unrelated";
                        String unrelatedUploadId = s3.createMultipartUpload(CreateMultipartUploadRequest.builder()
                                        .bucket(BUCKET)
                                        .key(unrelatedKey)
                                        .build())
                                .uploadId();
                        var done = reader.deletePersistedIntent(intent, verifier)
                                .toCompletableFuture()
                                .join();
                        assertThat(loseDeleteAck).isFalse();
                        assertThat(done.exactTerminalIsAuthoritative()).isTrue();
                        assertThat(reader.deletePersistedIntent(intent, verifier)
                                        .toCompletableFuture()
                                        .join()
                                        .outcome())
                                .isEqualTo(M5TargetDeleteAuthorityCoordinatorV1.Outcome.EXISTING_EXACT);
                        var compact = coordinator
                                .compactDone(done.observed().orElseThrow())
                                .toCompletableFuture()
                                .join();
                        assertThat(compact.exactTerminalIsAuthoritative()).isTrue();
                        assertThat(reader.deletePersistedIntent(intent, verifier)
                                        .toCompletableFuture()
                                        .join()
                                        .outcome())
                                .isEqualTo(M5TargetDeleteAuthorityCoordinatorV1.Outcome.EXISTING_TERMINAL);
                        try (var uploadReader = backend.openMultipartDeleteIdentityReader(
                                route,
                                scope,
                                "m5-object-oxia",
                                body.identity().key(),
                                CanonicalBytes.copyOf(uploadId.getBytes(StandardCharsets.UTF_8)),
                                8,
                                128,
                                64_000,
                                1_024,
                                2)) {
                            var uploadResource = uploadReader.resource();
                            var uploadOpen = M5TargetDeleteAuthorityStateMachineV1.open(
                                    PhysicalDeleteTargetV1.create(uploadResource),
                                    open.writerEnrollment(),
                                    SyntheticDeleteAuthorityFixturesV2.replacement(uploadResource, 1, fact));
                            var uploadCoordinator =
                                    new M5TargetDeleteAuthorityCoordinatorV1(route, verifier, uploadReader);
                            var exactUpload = uploadCoordinator
                                    .create(uploadOpen)
                                    .toCompletableFuture()
                                    .join()
                                    .observed()
                                    .orElseThrow();
                            var uploadFence = uploadCoordinator
                                    .prepareIdentityRead(
                                            exactUpload,
                                            SyntheticDeleteAuthorityFixturesV2.digest("upload-read"),
                                            context)
                                    .toCompletableFuture()
                                    .join()
                                    .observed()
                                    .orElseThrow();
                            var uploadIdentity = uploadReader
                                    .capture(uploadFence)
                                    .toCompletableFuture()
                                    .join();
                            assertThat(uploadIdentity.observation())
                                    .isEqualTo(ExternalIdentityObservationV1.PRESENT_EXACT_V1);
                            var uploadIntent = uploadCoordinator
                                    .bindDeleteIntent(
                                            uploadFence,
                                            uploadIdentity,
                                            SyntheticDeleteAuthorityFixturesV2.digest("upload-delete"))
                                    .toCompletableFuture()
                                    .join()
                                    .observed()
                                    .orElseThrow();
                            String foreignUploadId = s3.createMultipartUpload(CreateMultipartUploadRequest.builder()
                                            .bucket(BUCKET)
                                            .key(body.identity().key())
                                            .build())
                                    .uploadId();
                            try {
                                assertThatThrownBy(() -> uploadReader
                                                .deletePersistedIntent(uploadIntent, verifier)
                                                .toCompletableFuture()
                                                .join())
                                        .hasRootCauseMessage("S3 exact upload inventory is foreign or incomplete");
                                assertThat(s3.listMultipartUploads(ListMultipartUploadsRequest.builder()
                                                        .bucket(BUCKET)
                                                        .prefix(body.identity().key())
                                                        .build())
                                                .uploads())
                                        .hasSize(2);
                            } finally {
                                s3.abortMultipartUpload(AbortMultipartUploadRequest.builder()
                                        .bucket(BUCKET)
                                        .key(body.identity().key())
                                        .uploadId(foreignUploadId)
                                        .build());
                            }
                            assertThat(uploadReader
                                            .deletePersistedIntent(uploadIntent, verifier)
                                            .toCompletableFuture()
                                            .join()
                                            .exactTerminalIsAuthoritative())
                                    .isTrue();
                            assertThat(s3.listMultipartUploads(ListMultipartUploadsRequest.builder()
                                                    .bucket(BUCKET)
                                                    .prefix(unrelatedKey)
                                                    .build())
                                            .uploads())
                                    .hasSize(1);
                        }
                        s3.abortMultipartUpload(AbortMultipartUploadRequest.builder()
                                .bucket(BUCKET)
                                .key(unrelatedKey)
                                .uploadId(unrelatedUploadId)
                                .build());
                        assertThat(new M5ObjectDeleteSessionV1(
                                                transport,
                                                scope,
                                                "m5-object-oxia",
                                                1_024 * 1_024,
                                                8,
                                                128,
                                                64_000,
                                                1_024)
                                        .reconcile(body.identity(), "m5-object-oxia/objects/", version)
                                        .kind())
                                .isEqualTo(M5ObjectDeleteSessionV1.ReconciliationKindV1.AUTHORITATIVELY_ABSENT);
                        Body successor = new Body(body.identity().key(), "new-native-body");
                        try (var create =
                                new C1ObjectProviderSession(transport, scope, "m5-object-oxia", 1_024 * 1_024, 1_024)) {
                            assertThat(create.conditionalCreate(successor)
                                            .outcome()
                                            .name())
                                    .isEqualTo("APPLIED_EXACT");
                        }
                        assertThat(new M5ObjectDeleteSessionV1(
                                                transport,
                                                scope,
                                                "m5-object-oxia",
                                                1_024 * 1_024,
                                                8,
                                                128,
                                                64_000,
                                                1_024)
                                        .reconcile(body.identity(), "m5-object-oxia/objects/", version)
                                        .kind())
                                .isEqualTo(
                                        M5ObjectDeleteSessionV1.ReconciliationKindV1
                                                .EXACT_OLD_VERSION_ABSENT_OTHER_CURRENT);
                        s3.deleteObject(DeleteObjectRequest.builder()
                                .bucket(BUCKET)
                                .key(S3M5PhysicalNamespaceAuthorityV2.MARKER_KEY)
                                .build());
                        assertThatThrownBy(() -> route.read(resource.authorityKey())
                                        .toCompletableFuture()
                                        .join())
                                .hasRootCauseMessage("S3 namespace marker history is truncated or deleted");
                        assertThatThrownBy(() -> reader.rereadExact(captured)
                                        .toCompletableFuture()
                                        .join())
                                .hasRootCauseMessage("S3 namespace marker history is truncated or deleted");
                    }
                }
            }
        }
    }

    private static S3Client s3(URI endpoint) {
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
                // The exact image is still starting.
            }
            Thread.sleep(100);
        }
        throw new IllegalStateException("MinIO did not become healthy within the bounded startup deadline");
    }

    private record Body(ObjectIdentity identity, byte[] body) implements RepeatableObjectBody {
        private Body(String key, String body) {
            this(
                    new ObjectIdentity(
                            key,
                            body.getBytes(StandardCharsets.US_ASCII).length,
                            Sha256Digest.hash(CanonicalBytes.copyOf(body.getBytes(StandardCharsets.US_ASCII)))),
                    body.getBytes(StandardCharsets.US_ASCII));
        }

        private Body {
            body = body.clone();
        }

        @Override
        public InputStream openStream() {
            return new ByteArrayInputStream(body);
        }
    }
}
