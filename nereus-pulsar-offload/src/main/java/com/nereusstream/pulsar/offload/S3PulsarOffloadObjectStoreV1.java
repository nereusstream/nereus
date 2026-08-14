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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsRequest;
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsResponse;
import software.amazon.awssdk.services.s3.model.MultipartUpload;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

/** Bounded S3 production adapter for immutable NPD1/NPO1 Objects. */
public final class S3PulsarOffloadObjectStoreV1 implements PulsarOffloadObjectStoreV1 {
    private static final String SHA256_METADATA = "nereus-sha256";
    private static final long FIVE_MIB = 5L * 1_024 * 1_024;
    private static final long FIVE_GIB = 5L * 1_024 * 1_024 * 1_024;
    private static final Capabilities CAPABILITIES = new Capabilities(
            PulsarOffloadLimitCandidateV1.FOUR_GIB, FIVE_MIB, FIVE_GIB, 10_000, true, true, true, true, true);

    private final S3Client client;
    private final String bucket;
    private final ExecutorService executor;
    private final boolean closeClient;
    private final AtomicBoolean closed = new AtomicBoolean();

    public S3PulsarOffloadObjectStoreV1(S3Client client, String bucket, ExecutorService executor, boolean closeClient) {
        this.client = Objects.requireNonNull(client, "client");
        this.bucket = Objects.requireNonNull(bucket, "bucket");
        this.executor = Objects.requireNonNull(executor, "executor");
        this.closeClient = closeClient;
        if (bucket.isBlank()) {
            throw new IllegalArgumentException("S3 bucket is blank");
        }
    }

    @Override
    public Capabilities capabilities() {
        return CAPABILITIES;
    }

    @Override
    public CompletionStage<ImmutableObject> createImmutable(String key, Body body) {
        Objects.requireNonNull(body, "body");
        return submit(() -> {
            validateKey(key);
            try (InputStream input = body.inputStreamFactory().open()) {
                PutObjectResponse response = client.putObject(
                        PutObjectRequest.builder()
                                .bucket(bucket)
                                .key(key)
                                .ifNoneMatch("*")
                                .metadata(Map.of(SHA256_METADATA, body.sha256()))
                                .build(),
                        RequestBody.fromInputStream(input, body.bytes()));
                return new ImmutableObject(version(response.versionId(), response.eTag()), body.bytes(), body.sha256());
            }
        });
    }

    @Override
    public CompletionStage<ImmutableObject> head(String key) {
        return submit(() -> headSync(key));
    }

    @Override
    public CompletionStage<byte[]> readRange(String key, long offset, int length) {
        return submit(() -> {
            validateKey(key);
            if (offset < 0 || length <= 0) {
                throw new IllegalArgumentException("S3 range is empty or negative");
            }
            long last = Math.addExact(offset, length - 1L);
            try (InputStream input = client.getObject(GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .range("bytes=" + offset + "-" + last)
                    .build())) {
                byte[] result = input.readNBytes(length);
                if (result.length != length || input.read() != -1) {
                    throw failure(FailureKind.SHORT_READ, "S3 range response differs from the exact requested length");
                }
                return result;
            }
        });
    }

    @Override
    public CompletionStage<Void> deleteAndProveAbsent(String key) {
        return submit(() -> {
            validateKey(key);
            client.deleteObject(
                    DeleteObjectRequest.builder().bucket(bucket).key(key).build());
            try {
                headSync(key);
            } catch (ObjectStoreException expected) {
                if (expected.kind() == FailureKind.NOT_FOUND) {
                    return null;
                }
                throw expected;
            }
            throw failure(FailureKind.CONFLICT, "S3 Object remains visible after delete");
        });
    }

    @Override
    public CompletionStage<Void> cleanupAttemptMultipartResidue(String attemptPrefix) {
        return submit(() -> {
            validateKey(attemptPrefix);
            for (MultipartUpload upload : listMultipartUploads(attemptPrefix)) {
                client.abortMultipartUpload(AbortMultipartUploadRequest.builder()
                        .bucket(bucket)
                        .key(upload.key())
                        .uploadId(upload.uploadId())
                        .build());
            }
            if (!listMultipartUploads(attemptPrefix).isEmpty()) {
                throw failure(FailureKind.CONFLICT, "S3 multipart uploads remain after deterministic cleanup");
            }
            return null;
        });
    }

    @Override
    public CompletionStage<Void> close() {
        if (!closed.compareAndSet(false, true)) {
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Void> result = new CompletableFuture<>();
        try {
            if (closeClient) {
                client.close();
            }
            executor.shutdown();
            result.complete(null);
        } catch (Throwable failure) {
            result.completeExceptionally(failure);
        }
        return result;
    }

    private ImmutableObject headSync(String key) throws ObjectStoreException {
        validateKey(key);
        HeadObjectResponse response;
        try {
            response = client.headObject(
                    HeadObjectRequest.builder().bucket(bucket).key(key).build());
        } catch (Throwable failure) {
            Throwable mapped = map(failure);
            if (mapped instanceof ObjectStoreException objectStoreFailure) {
                throw objectStoreFailure;
            }
            if (mapped instanceof RuntimeException runtimeFailure) {
                throw runtimeFailure;
            }
            throw new IllegalStateException("unexpected checked S3 HEAD failure", mapped);
        }
        String sha256 = response.metadata().get(SHA256_METADATA);
        if (sha256 == null || !sha256.matches("[0-9a-f]{64}") || response.contentLength() <= 0) {
            throw failure(FailureKind.INTEGRITY, "S3 HEAD lacks the exact Nereus length or SHA-256 proof");
        }
        return new ImmutableObject(version(response.versionId(), response.eTag()), response.contentLength(), sha256);
    }

    private List<MultipartUpload> listMultipartUploads(String prefix) {
        List<MultipartUpload> result = new ArrayList<>();
        String keyMarker = null;
        String uploadIdMarker = null;
        do {
            ListMultipartUploadsResponse response = client.listMultipartUploads(ListMultipartUploadsRequest.builder()
                    .bucket(bucket)
                    .prefix(prefix)
                    .keyMarker(keyMarker)
                    .uploadIdMarker(uploadIdMarker)
                    .build());
            result.addAll(response.uploads());
            if (!response.isTruncated()) {
                break;
            }
            keyMarker = response.nextKeyMarker();
            uploadIdMarker = response.nextUploadIdMarker();
        } while (true);
        return result;
    }

    private <T> CompletionStage<T> submit(ThrowingSupplier<T> operation) {
        if (closed.get()) {
            return CompletableFuture.failedFuture(new IllegalStateException("S3 Pulsar Object store is closed"));
        }
        return CompletableFuture.supplyAsync(
                () -> {
                    if (closed.get()) {
                        throw new IllegalStateException("S3 Pulsar Object store is closed");
                    }
                    try {
                        return operation.get();
                    } catch (Throwable failure) {
                        throw new java.util.concurrent.CompletionException(map(failure));
                    }
                },
                executor);
    }

    private static Throwable map(Throwable failure) {
        if (failure instanceof ObjectStoreException || failure instanceof IllegalArgumentException) {
            return failure;
        }
        if (failure instanceof CancellationException) {
            return failure(FailureKind.CANCELLED, "S3 operation was cancelled", failure);
        }
        if (failure instanceof NoSuchKeyException) {
            return failure(FailureKind.NOT_FOUND, "S3 Object is absent", failure);
        }
        if (failure instanceof ApiCallTimeoutException || failure instanceof ApiCallAttemptTimeoutException) {
            return failure(FailureKind.TIMEOUT, "S3 operation timed out", failure);
        }
        if (failure instanceof S3Exception s3Failure) {
            int status = s3Failure.statusCode();
            if (status == 404) {
                return failure(FailureKind.NOT_FOUND, "S3 Object is absent", failure);
            }
            if (status == 408 || status == 504) {
                return failure(FailureKind.TIMEOUT, "S3 operation timed out", failure);
            }
            if (status == 409 || status == 412) {
                return failure(FailureKind.CONFLICT, "S3 conditional or concurrent operation conflicted", failure);
            }
            if (status >= 500) {
                return failure(FailureKind.UNAVAILABLE, "S3 service is unavailable", failure);
            }
        }
        if (failure instanceof SdkClientException || failure instanceof IOException) {
            return failure(FailureKind.UNAVAILABLE, "S3 client operation failed", failure);
        }
        return failure;
    }

    private static String version(String versionId, String eTag) throws ObjectStoreException {
        if (versionId != null && !versionId.isBlank() && !"null".equals(versionId)) {
            return "version:" + versionId;
        }
        if (eTag == null || eTag.isBlank()) {
            throw failure(FailureKind.INTEGRITY, "S3 response lacks an immutable version identity");
        }
        return "etag:" + eTag.toLowerCase(Locale.ROOT);
    }

    private static void validateKey(String key) {
        Objects.requireNonNull(key, "key");
        if (key.isBlank() || key.startsWith("/") || key.contains("//")) {
            throw new IllegalArgumentException("S3 Object key is not canonical");
        }
    }

    private static ObjectStoreException failure(FailureKind kind, String message) {
        return new ObjectStoreException(kind, message);
    }

    private static ObjectStoreException failure(FailureKind kind, String message, Throwable cause) {
        return new ObjectStoreException(kind, message, cause);
    }

    @FunctionalInterface
    private interface ThrowingSupplier<T> {
        T get() throws Exception;
    }
}
