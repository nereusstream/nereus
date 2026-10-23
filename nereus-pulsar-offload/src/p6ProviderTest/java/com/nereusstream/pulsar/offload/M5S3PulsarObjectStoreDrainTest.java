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
import com.nereusstream.pulsar.offload.PulsarOffloadObjectStoreV1.ImmutableObject;
import com.nereusstream.pulsar.offload.PulsarOffloadObjectStoreV1.ObjectStoreException;
import java.io.ByteArrayInputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.PutBucketVersioningRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/** Actual versioned S3 I/O lifetime; this supplies no M5 deletion or protocol qualification. */
class M5S3PulsarObjectStoreDrainTest {
    private static final String BUCKET = "nereus-m5-pulsar-io-drain";
    private static final String ACCESS = "nereus-m5-drain";
    private static final String SECRET = "nereus-m5-drain-secret";
    private static S3Client independent;

    @BeforeAll
    static void provision() {
        independent = clientBuilder().build();
        independent.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
        independent.putBucketVersioning(PutBucketVersioningRequest.builder()
                .bucket(BUCKET)
                .versioningConfiguration(value -> value.status(BucketVersioningStatus.ENABLED))
                .build());
    }

    @AfterAll
    static void closeIndependent() {
        if (independent != null) {
            independent.close();
        }
    }

    @Test
    void closeWaitsForRealPutAfterObserverCancellationAndRejectsQueuedDispatch() throws Exception {
        String key = "drain/held-put";
        var received = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var executor = Executors.newSingleThreadExecutor();
        var actual = clientBuilder()
                .overrideConfiguration(config -> config.addExecutionInterceptor(new ExecutionInterceptor() {
                    @Override
                    public void afterExecution(Context.AfterExecution context, ExecutionAttributes attributes) {
                        if (context.request() instanceof PutObjectRequest put
                                && put.key().equals(key)) {
                            received.countDown();
                            try {
                                if (!release.await(30, TimeUnit.SECONDS)) {
                                    throw new IllegalStateException("held real PUT response was not released");
                                }
                            } catch (InterruptedException failure) {
                                Thread.currentThread().interrupt();
                                throw new IllegalStateException("held real PUT response was interrupted", failure);
                            }
                        }
                    }
                }))
                .build();
        var store = new S3PulsarOffloadObjectStoreV1(actual, BUCKET, executor, true);
        try {
            Body body = body("actual-drained-put");
            var observer = store.createImmutable(key, body).toCompletableFuture();
            assertThat(received.await(30, TimeUnit.SECONDS)).isTrue();
            var nativeHead = independent.headObject(
                    HeadObjectRequest.builder().bucket(BUCKET).key(key).build());
            assertThat(nativeHead.versionId()).isNotBlank().isNotEqualTo("null");
            assertThat(nativeHead.metadata().get("nereus-sha256")).isEqualTo(body.sha256());
            var queued = store.createImmutable("drain/queued-put", body).toCompletableFuture();
            assertThat(observer.cancel(true)).isTrue();
            var closeObserver = store.close().toCompletableFuture();
            assertThat(closeObserver).isNotDone();
            assertThat(store.close().toCompletableFuture()).isNotDone();
            assertThat(closeObserver.cancel(true)).isTrue();
            assertThat(store.close().toCompletableFuture()).isNotDone();
            assertThatThrownBy(() -> store.head(key).toCompletableFuture().join())
                    .hasRootCauseMessage("S3 Pulsar Object store is closed");
            release.countDown();
            store.close().toCompletableFuture().get(30, TimeUnit.SECONDS);
            assertThatThrownBy(queued::join).hasRootCauseMessage("S3 Pulsar Object store is closed");
            assertThat(observer).isCancelled();
            assertThat(executor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
            assertThat(independent
                            .headObject(HeadObjectRequest.builder()
                                    .bucket(BUCKET)
                                    .key(key)
                                    .build())
                            .versionId())
                    .isEqualTo(nativeHead.versionId());
            assertThatThrownBy(() -> independent.headObject(HeadObjectRequest.builder()
                            .bucket(BUCKET)
                            .key("drain/queued-put")
                            .build()))
                    .satisfies(failure -> assertThat(
                                    ((software.amazon.awssdk.services.s3.model.S3Exception) failure).statusCode())
                            .isEqualTo(404));
        } finally {
            release.countDown();
            store.close().toCompletableFuture().get(30, TimeUnit.SECONDS);
            assertThat(executor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void rejectedExecutorReturnsFailureAndLeavesNoProviderWorkToDrain() throws Exception {
        var executor = Executors.newSingleThreadExecutor();
        executor.shutdown();
        try (var actual = clientBuilder().build()) {
            var store = new S3PulsarOffloadObjectStoreV1(actual, BUCKET, executor, false);
            var result = store.createImmutable("drain/rejected-put", body("not-dispatched"));
            assertThatThrownBy(() -> result.toCompletableFuture().join())
                    .hasRootCauseInstanceOf(RejectedExecutionException.class);
            store.close().toCompletableFuture().get(30, TimeUnit.SECONDS);
            assertThatThrownBy(() -> independent.headObject(HeadObjectRequest.builder()
                            .bucket(BUCKET)
                            .key("drain/rejected-put")
                            .build()))
                    .satisfies(failure -> assertThat(
                                    ((software.amazon.awssdk.services.s3.model.S3Exception) failure).statusCode())
                            .isEqualTo(404));
        }
    }

    @Test
    void failedActualClientCloseCannotBecomeSuccessOnRetry() throws Exception {
        var executor = Executors.newSingleThreadExecutor();
        try (var actual = clientBuilder().build()) {
            S3Client lostCloseReply = (S3Client) Proxy.newProxyInstance(
                    S3Client.class.getClassLoader(), new Class<?>[] {S3Client.class}, (proxy, method, args) -> {
                        if (method.getName().equals("close")) {
                            actual.close();
                            throw new IllegalStateException("actual S3 client close reply is unavailable");
                        }
                        try {
                            return method.invoke(actual, args);
                        } catch (InvocationTargetException failure) {
                            throw failure.getCause();
                        }
                    });
            var store = new S3PulsarOffloadObjectStoreV1(lostCloseReply, BUCKET, executor, true);
            try {
                var written = store.createImmutable("drain/failed-close", body("actual-close-failure"))
                        .toCompletableFuture()
                        .get(30, TimeUnit.SECONDS);
                assertThat(written.immutableVersion()).startsWith("version:");
                assertThatThrownBy(() -> store.close().toCompletableFuture().join())
                        .hasRootCauseMessage("actual S3 client close reply is unavailable");
                assertThatThrownBy(() -> store.close().toCompletableFuture().join())
                        .hasRootCauseMessage("actual S3 client close reply is unavailable");
                assertThat(independent
                                .headObject(HeadObjectRequest.builder()
                                        .bucket(BUCKET)
                                        .key("drain/failed-close")
                                        .build())
                                .versionId())
                        .isEqualTo(written.immutableVersion().substring("version:".length()));
            } finally {
                executor.shutdown();
                assertThat(executor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    @Test
    void boundVersionRangeIgnoresNewCurrentVersionAndRejectsWrongProof() throws Exception {
        verifyLiveVersionDeletionPolicies();
        String key = "versions/captured-data";
        var executor = Executors.newSingleThreadExecutor();
        var store = new S3PulsarOffloadObjectStoreV1(clientBuilder().build(), BUCKET, executor, true);
        try {
            Body original = body("captured-object-body");
            var proof =
                    store.createImmutable(key, original).toCompletableFuture().get(30, TimeUnit.SECONDS);
            Body replacement = body("different-newer-body");
            var newer = independent.putObject(
                    PutObjectRequest.builder()
                            .bucket(BUCKET)
                            .key(key)
                            .metadata(Map.of("nereus-sha256", replacement.sha256()))
                            .build(),
                    RequestBody.fromString("different-newer-body"));
            assertThat(newer.versionId()).isNotEqualTo(proof.immutableVersion().substring("version:".length()));
            assertThat(store.head(key)
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS)
                            .immutableVersion())
                    .isEqualTo("version:" + newer.versionId());
            assertThat(store.readRange(key, proof, 2, 7).toCompletableFuture().get(30, TimeUnit.SECONDS))
                    .containsExactly("ptured-".getBytes(StandardCharsets.UTF_8));
            var wrongSha = new ImmutableObject(proof.immutableVersion(), proof.bytes(), "f".repeat(64));
            assertThatThrownBy(() -> store.readRange(key, wrongSha, 2, 7)
                            .toCompletableFuture()
                            .join())
                    .hasCauseInstanceOf(ObjectStoreException.class)
                    .satisfies(failure -> assertThat(((ObjectStoreException) failure.getCause()).kind())
                            .isEqualTo(FailureKind.INTEGRITY));
            assertThatThrownBy(() -> store.readRange(key, proof, proof.bytes(), 1)
                            .toCompletableFuture()
                            .join())
                    .hasRootCauseMessage("S3 range exceeds the captured Object extent");
        } finally {
            store.close().toCompletableFuture().get(30, TimeUnit.SECONDS);
            assertThat(executor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void unversionedRangeUsesConditionalEtagAndRejectsReplacement() throws Exception {
        String bucket = "nereus-m5-pulsar-etag-read";
        String key = "versions/captured-etag";
        independent.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
        var executor = Executors.newSingleThreadExecutor();
        var store = new S3PulsarOffloadObjectStoreV1(clientBuilder().build(), bucket, executor, true);
        try {
            Body original = body("captured-etag-body");
            var proof =
                    store.createImmutable(key, original).toCompletableFuture().get(30, TimeUnit.SECONDS);
            assertThat(proof.immutableVersion()).startsWith("etag:");
            assertThat(store.readRange(key, proof, 2, 7).toCompletableFuture().get(30, TimeUnit.SECONDS))
                    .containsExactly("ptured-".getBytes(StandardCharsets.UTF_8));
            Body replacement = body("different-newer-body");
            independent.putObject(
                    PutObjectRequest.builder()
                            .bucket(bucket)
                            .key(key)
                            .metadata(Map.of("nereus-sha256", replacement.sha256()))
                            .build(),
                    RequestBody.fromString("different-newer-body"));
            assertThatThrownBy(() -> store.readRange(key, proof, 2, 7)
                            .toCompletableFuture()
                            .join())
                    .hasCauseInstanceOf(ObjectStoreException.class)
                    .satisfies(failure -> assertThat(((ObjectStoreException) failure.getCause()).kind())
                            .isEqualTo(FailureKind.CONFLICT));
        } finally {
            store.close().toCompletableFuture().get(30, TimeUnit.SECONDS);
            assertThat(executor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static void verifyLiveVersionDeletionPolicies() throws Exception {
        String bucket = BUCKET + "-retention";
        independent.createBucket(CreateBucketRequest.builder()
                .bucket(bucket)
                .objectLockEnabledForBucket(true)
                .build());
        var empty = independent.putObject(
                PutObjectRequest.builder().bucket(bucket).key("policy/empty").build(),
                RequestBody.fromString("no-per-object-retention"));
        try (var policy = com.nereusstream.storage.object.s3.S3C1ObjectProviderTransport.admitVersionMatchDeleteV1(
                independent, bucket, "m5-live-retention-test", false, 10)) {
            var emptyVersion = com.nereusstream.domain.bytes.CanonicalUtf8.fromString(empty.versionId())
                    .bytes();
            policy.requireObjectDeletePolicyAllows("policy/empty", emptyVersion);
            var retained = independent.putObject(
                    PutObjectRequest.builder()
                            .bucket(bucket)
                            .key("policy/held")
                            .objectLockMode(software.amazon.awssdk.services.s3.model.ObjectLockMode.GOVERNANCE)
                            .objectLockRetainUntilDate(java.time.Instant.now().plusSeconds(4))
                            .build(),
                    RequestBody.fromString("actual-retained-version"));
            var version = com.nereusstream.domain.bytes.CanonicalUtf8.fromString(retained.versionId())
                    .bytes();
            assertThatThrownBy(() -> policy.requireObjectDeletePolicyAllows("policy/held", version))
                    .isInstanceOf(java.io.IOException.class)
                    .hasMessageContaining("retained by the provider");
            independent.putObjectLegalHold(value -> value.bucket(bucket)
                    .key("policy/held")
                    .versionId(retained.versionId())
                    .legalHold(hold ->
                            hold.status(software.amazon.awssdk.services.s3.model.ObjectLockLegalHoldStatus.ON)));
            assertThatThrownBy(() -> policy.requireObjectDeletePolicyAllows("policy/held", version))
                    .isInstanceOf(java.io.IOException.class)
                    .hasMessageContaining("legal hold");
            independent.putObjectLegalHold(value -> value.bucket(bucket)
                    .key("policy/held")
                    .versionId(retained.versionId())
                    .legalHold(hold ->
                            hold.status(software.amazon.awssdk.services.s3.model.ObjectLockLegalHoldStatus.OFF)));
            assertThatThrownBy(() -> policy.requireObjectDeletePolicyAllows("policy/held", version))
                    .isInstanceOf(java.io.IOException.class)
                    .hasMessageContaining("retained by the provider");
            TimeUnit.SECONDS.sleep(5);
            policy.requireObjectDeletePolicyAllows("policy/held", version);
            independent.deleteObject(
                    value -> value.bucket(bucket).key("policy/held").versionId(retained.versionId()));
            policy.requireObjectDeletePolicyAllows("policy/held", version);
            independent.deleteObject(
                    value -> value.bucket(bucket).key("policy/empty").versionId(empty.versionId()));
            independent.deleteBucket(value -> value.bucket(bucket));
            assertThatThrownBy(() -> policy.requireObjectDeletePolicyAllows("policy/held", version))
                    .isInstanceOf(java.io.IOException.class);
        }
        System.out.println("ACTUAL_PROVIDER_DELETE_POLICY emptyRetention=true legalHoldVeto=true retentionVeto=true"
                + " serverTimeExpiry=true absentIsNotDone=true missingBucketVeto=true wholeM5Authority=false");
    }

    private static software.amazon.awssdk.services.s3.S3ClientBuilder clientBuilder() {
        String endpoint = System.getProperty("nereus.m5.s3DrainEndpoint");
        if (endpoint == null || endpoint.isBlank()) {
            throw new IllegalStateException("real S3 drain endpoint is required");
        }
        return S3Client.builder()
                .endpointOverride(URI.create(endpoint))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(ACCESS, SECRET)))
                .region(Region.US_EAST_1)
                .forcePathStyle(true);
    }

    private static Body body(String value) throws Exception {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        String sha =
                HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        return new Body(bytes.length, sha, () -> new ByteArrayInputStream(bytes));
    }
}
