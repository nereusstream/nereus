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

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import com.nereusstream.storage.object.provider.ObjectProviderCapabilities;
import com.nereusstream.storage.object.provider.ObjectProviderTransport;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.GetBucketVersioningRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

/** AWS SDK v2 transport for the M3 C1 strategy. Content proof is performed by full GET, never ETag or metadata. */
public final class S3C1ObjectProviderTransport implements ObjectProviderTransport, AutoCloseable {
    public static final long EVIDENCED_MAXIMUM_OBJECT_BYTES = 64L * 1024 * 1024;
    public static final int EVIDENCED_MAXIMUM_RANGE_BYTES = 64 * 1024 * 1024;
    public static final int DEFAULT_MAXIMUM_LIST_PAGE_KEYS = 1_000;
    public static final int MAXIMUM_DELETE_VERSION_TOKEN_BYTES = 1_024;
    public static final String ADAPTER_VERSION = "nereus-s3-c1-v1/aws-sdk-s3-2.47.5";

    private final S3Client client;
    private final String bucket;
    private final boolean closeClient;
    private final ObjectProviderCapabilities capabilities;
    private final boolean versionMatchDeleteAdmitted;
    private final AtomicBoolean closed = new AtomicBoolean();

    public S3C1ObjectProviderTransport(
            S3Client client,
            String bucket,
            String exactProviderIdentity,
            boolean closeClient,
            int maximumListPageKeys) {
        this(client, bucket, exactProviderIdentity, closeClient, maximumListPageKeys, false);
    }

    private S3C1ObjectProviderTransport(
            S3Client client,
            String bucket,
            String exactProviderIdentity,
            boolean closeClient,
            int maximumListPageKeys,
            boolean versionMatchDeleteAdmitted) {
        this.client = Objects.requireNonNull(client, "client");
        this.bucket = requireNonBlank(bucket, "bucket");
        this.closeClient = closeClient;
        this.versionMatchDeleteAdmitted = versionMatchDeleteAdmitted;
        if (maximumListPageKeys <= 0 || maximumListPageKeys > DEFAULT_MAXIMUM_LIST_PAGE_KEYS) {
            throw new IllegalArgumentException("maximumListPageKeys must be in [1,1000]");
        }
        this.capabilities = new ObjectProviderCapabilities(
                requireNonBlank(exactProviderIdentity, "exactProviderIdentity"),
                true,
                true,
                true,
                true,
                true,
                EVIDENCED_MAXIMUM_OBJECT_BYTES,
                EVIDENCED_MAXIMUM_RANGE_BYTES,
                maximumListPageKeys);
    }

    /** Admits M5-D only after an exact live read proves bucket versioning is enabled. */
    public static S3C1ObjectProviderTransport admitVersionMatchDeleteV1(
            S3Client client, String bucket, String exactProviderIdentity, boolean closeClient, int maximumListPageKeys)
            throws IOException {
        Objects.requireNonNull(client, "client");
        String canonicalBucket = requireNonBlank(bucket, "bucket");
        try {
            BucketVersioningStatus status = client.getBucketVersioning(GetBucketVersioningRequest.builder()
                            .bucket(canonicalBucket)
                            .build())
                    .status();
            if (status != BucketVersioningStatus.ENABLED) {
                throw new S3C1ProviderException(
                        S3C1ProviderException.Kind.UNSUPPORTED_OPERATION,
                        "VERSION_MATCH_DELETE_V1 requires an enabled versioned bucket");
            }
        } catch (S3C1ProviderException failure) {
            throw failure;
        } catch (Throwable failure) {
            throw mapped("bucket-versioning admission", failure);
        }
        return new S3C1ObjectProviderTransport(
                client, canonicalBucket, exactProviderIdentity, closeClient, maximumListPageKeys, true);
    }

    @Override
    public ObjectProviderCapabilities capabilities() {
        return capabilities;
    }

    @Override
    public ObjectDeleteCapabilities deleteCapabilities() {
        return versionMatchDeleteAdmitted
                ? new ObjectDeleteCapabilities(
                        capabilities.providerIdentity(),
                        "VERSION_MATCH_DELETE_V1",
                        true,
                        true,
                        true,
                        MAXIMUM_DELETE_VERSION_TOKEN_BYTES)
                : ObjectDeleteCapabilities.unsupported(capabilities.providerIdentity());
    }

    @Override
    public ConditionalCreateResult putIfAbsent(ObjectIdentity identity, InputStream body) throws IOException {
        requireOpen();
        validateIdentity(identity);
        Objects.requireNonNull(body, "body");
        try {
            client.putObject(
                    PutObjectRequest.builder()
                            .bucket(bucket)
                            .key(identity.key())
                            .ifNoneMatch("*")
                            .checksumSHA256(Base64.getEncoder()
                                    .encodeToString(
                                            identity.bodySha256().bytes().toByteArray()))
                            .build(),
                    RequestBody.fromInputStream(body, identity.bodyLength()));
            return ConditionalCreateResult.CREATED;
        } catch (S3Exception failure) {
            return classifyConditionalCreateFailure(failure);
        } catch (SdkClientException failure) {
            return ConditionalCreateResult.RESPONSE_UNKNOWN;
        } catch (CancellationException failure) {
            throw new S3C1ProviderException(
                    S3C1ProviderException.Kind.OUTCOME_UNKNOWN, "conditional PUT was cancelled", failure);
        }
    }

    @Override
    public StreamingObject get(String key, Optional<CanonicalBytes> exactVersionToken) throws IOException {
        requireOpen();
        validateKey(key);
        GetObjectRequest.Builder request =
                GetObjectRequest.builder().bucket(bucket).key(key);
        exactVersionToken.ifPresent(token -> request.versionId(decodeVersionToken(token)));
        ResponseInputStream<GetObjectResponse> input;
        try {
            input = client.getObject(request.build());
        } catch (Throwable failure) {
            throw mapped("full GET", failure);
        }
        GetObjectResponse response = input.response();
        long length = response.contentLength();
        if (length <= 0 || length > capabilities.maximumObjectBytes()) {
            closeAfterInvalid(input);
            throw new S3C1ProviderException(
                    S3C1ProviderException.Kind.CAPACITY, "full GET length is outside the admitted Root cap");
        }
        return new StreamingObject(length, 0, length, versionToken(response.versionId()), input);
    }

    @Override
    public StreamingObject getRange(
            String key, long inclusiveStart, long exclusiveEnd, Optional<CanonicalBytes> exactVersionToken)
            throws IOException {
        requireOpen();
        validateKey(key);
        if (inclusiveStart < 0 || exclusiveEnd <= inclusiveStart) {
            throw new IllegalArgumentException("range is empty or negative");
        }
        long requested = Math.subtractExact(exclusiveEnd, inclusiveStart);
        if (requested > capabilities.maximumRangeBytes()) {
            throw new S3C1ProviderException(
                    S3C1ProviderException.Kind.CAPACITY, "range exceeds the admitted single-range cap");
        }
        GetObjectRequest.Builder request = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .range("bytes=" + inclusiveStart + "-" + (exclusiveEnd - 1));
        exactVersionToken.ifPresent(token -> request.versionId(decodeVersionToken(token)));
        ResponseInputStream<GetObjectResponse> input;
        try {
            input = client.getObject(request.build());
        } catch (Throwable failure) {
            throw mapped("range GET", failure);
        }
        GetObjectResponse response = input.response();
        ParsedContentRange range;
        try {
            range = ParsedContentRange.parse(response.contentRange());
        } catch (RuntimeException failure) {
            closeAfterInvalid(input);
            throw new S3C1ProviderException(
                    S3C1ProviderException.Kind.INTEGRITY, "range GET returned a malformed Content-Range", failure);
        }
        if (range.start() != inclusiveStart
                || range.endExclusive() != exclusiveEnd
                || response.contentLength() != requested) {
            closeAfterInvalid(input);
            throw new S3C1ProviderException(
                    S3C1ProviderException.Kind.INTEGRITY, "range GET differs from the exact requested interval");
        }
        return new StreamingObject(
                range.totalLength(), inclusiveStart, exclusiveEnd, versionToken(response.versionId()), input);
    }

    @Override
    public ListPage list(String prefix, Optional<CanonicalBytes> continuationToken, int maximumKeys)
            throws IOException {
        requireOpen();
        validateKey(prefix);
        if (maximumKeys <= 0 || maximumKeys > capabilities.maximumListPageKeys()) {
            throw new IllegalArgumentException("maximumKeys exceeds the admitted page cap");
        }
        ListObjectsV2Request.Builder request =
                ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).maxKeys(maximumKeys);
        continuationToken.ifPresent(token -> request.continuationToken(decodeOpaqueToken(token)));
        ListObjectsV2Response response;
        try {
            response = client.listObjectsV2(request.build());
        } catch (Throwable failure) {
            throw mapped("LIST", failure);
        }
        List<ListedObject> objects = new ArrayList<>(response.contents().size());
        response.contents()
                .forEach(object -> objects.add(new ListedObject(object.key(), object.size(), Optional.empty())));
        Optional<CanonicalBytes> next = response.isTruncated()
                ? Optional.of(
                        CanonicalBytes.copyOf(requireNonBlank(response.nextContinuationToken(), "nextContinuationToken")
                                .getBytes(StandardCharsets.UTF_8)))
                : Optional.empty();
        return new ListPage(objects, next);
    }

    @Override
    public ConditionalDeleteResult deleteExactVersion(String key, CanonicalBytes exactVersionToken) throws IOException {
        requireOpen();
        validateKey(key);
        Objects.requireNonNull(exactVersionToken, "exactVersionToken");
        if (!versionMatchDeleteAdmitted) {
            return ConditionalDeleteResult.UNSUPPORTED;
        }
        if (exactVersionToken.isEmpty() || exactVersionToken.length() > MAXIMUM_DELETE_VERSION_TOKEN_BYTES) {
            throw new IllegalArgumentException("delete version token is empty or oversized");
        }
        String versionId = decodeVersionToken(exactVersionToken);
        try {
            DeleteObjectResponse response = client.deleteObject(DeleteObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .versionId(versionId)
                    .build());
            if (response.versionId() != null
                    && !response.versionId().isBlank()
                    && !versionId.equals(response.versionId())) {
                throw new S3C1ProviderException(
                        S3C1ProviderException.Kind.INTEGRITY,
                        "conditional DELETE acknowledged a different immutable version");
            }
            return ConditionalDeleteResult.DELETED_EXACT;
        } catch (S3C1ProviderException failure) {
            throw failure;
        } catch (S3Exception failure) {
            return classifyConditionalDeleteFailure(failure);
        } catch (SdkClientException | CancellationException failure) {
            return ConditionalDeleteResult.RESPONSE_UNKNOWN;
        }
    }

    @Override
    public FailureKind classifyFailure(IOException failure) {
        if (!(failure instanceof S3C1ProviderException typed)) {
            return FailureKind.FATAL;
        }
        return switch (typed.kind()) {
            case NOT_FOUND -> FailureKind.NOT_FOUND;
            case RETRYABLE -> FailureKind.RETRYABLE;
            case OUTCOME_UNKNOWN -> FailureKind.OUTCOME_UNKNOWN;
            case INTEGRITY, CAPACITY, DEFINITIVE_CONFLICT, UNSUPPORTED_OPERATION -> FailureKind.FATAL;
        };
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true) && closeClient) {
            client.close();
        }
    }

    private void validateIdentity(ObjectIdentity identity) throws S3C1ProviderException {
        Objects.requireNonNull(identity, "identity");
        validateKey(identity.key());
        if (identity.bodyLength() > capabilities.maximumObjectBytes()) {
            throw new S3C1ProviderException(
                    S3C1ProviderException.Kind.CAPACITY, "Object body exceeds the evidenced 64 MiB Root cap");
        }
    }

    private static Optional<CanonicalBytes> versionToken(String versionId) {
        if (versionId == null || versionId.isBlank() || "null".equals(versionId)) {
            return Optional.empty();
        }
        return Optional.of(CanonicalBytes.copyOf(versionId.getBytes(StandardCharsets.UTF_8)));
    }

    private static String decodeVersionToken(CanonicalBytes token) {
        return decodeUtf8(token, "version token");
    }

    private static String decodeOpaqueToken(CanonicalBytes token) {
        return decodeUtf8(token, "continuation token");
    }

    private static String decodeUtf8(CanonicalBytes bytes, String label) {
        byte[] raw = bytes.toByteArray();
        String decoded = new String(raw, StandardCharsets.UTF_8);
        if (decoded.isEmpty()
                || decoded.indexOf('\0') >= 0
                || !Arrays.equals(raw, decoded.getBytes(StandardCharsets.UTF_8))) {
            throw new IllegalArgumentException(label + " is not canonical non-empty UTF-8");
        }
        return decoded;
    }

    static ConditionalCreateResult classifyConditionalCreateFailure(S3Exception failure) throws IOException {
        int status = failure.statusCode();
        if (status == 412) {
            return ConditionalCreateResult.ALREADY_EXISTS;
        }
        if (status == 408 || status == 409 || status == 429 || status >= 500) {
            return ConditionalCreateResult.RESPONSE_UNKNOWN;
        }
        if (status >= 400 && status < 500) {
            return ConditionalCreateResult.DEFINITIVE_CONFLICT;
        }
        throw mapped("conditional PUT", failure);
    }

    static ConditionalDeleteResult classifyConditionalDeleteFailure(S3Exception failure) throws IOException {
        int status = failure.statusCode();
        String code = failure.awsErrorDetails() == null
                ? null
                : failure.awsErrorDetails().errorCode();
        if (status == 404 && ("NoSuchVersion".equals(code) || "NoSuchKey".equals(code))) {
            return ConditionalDeleteResult.DEFINITIVELY_NOT_FOUND;
        }
        if (status == 412) {
            return ConditionalDeleteResult.VERSION_PRECONDITION_FAILED;
        }
        if (status == 429) {
            return ConditionalDeleteResult.RETRYABLE;
        }
        if (status == 408 || status == 409 || status >= 500) {
            return ConditionalDeleteResult.RESPONSE_UNKNOWN;
        }
        if (status >= 400 && status < 500) {
            return ConditionalDeleteResult.DEFINITIVE_CONFLICT;
        }
        throw mapped("conditional DELETE", failure);
    }

    static IOException mapped(String operation, Throwable failure) {
        if (failure instanceof S3C1ProviderException typed) {
            return typed;
        }
        if (failure instanceof NoSuchKeyException) {
            return new S3C1ProviderException(
                    S3C1ProviderException.Kind.NOT_FOUND, operation + " found no Object", failure);
        }
        if (failure instanceof S3Exception s3Failure) {
            int status = s3Failure.statusCode();
            if (status == 404 && hasExactNoSuchKeyCode(s3Failure)) {
                return new S3C1ProviderException(
                        S3C1ProviderException.Kind.NOT_FOUND, operation + " found no Object", failure);
            }
            if (status == 404) {
                return new S3C1ProviderException(
                        S3C1ProviderException.Kind.OUTCOME_UNKNOWN,
                        operation + " returned an untyped 404 response",
                        failure);
            }
            if (status == 408 || status == 429 || status >= 500) {
                return new S3C1ProviderException(
                        S3C1ProviderException.Kind.RETRYABLE, operation + " failed transiently", failure);
            }
            return new S3C1ProviderException(
                    S3C1ProviderException.Kind.DEFINITIVE_CONFLICT, operation + " was rejected", failure);
        }
        if (failure instanceof ApiCallTimeoutException
                || failure instanceof ApiCallAttemptTimeoutException
                || failure instanceof SdkClientException
                || failure instanceof CancellationException) {
            return new S3C1ProviderException(
                    S3C1ProviderException.Kind.RETRYABLE, operation + " did not complete definitively", failure);
        }
        if (failure instanceof IOException ioFailure) {
            return ioFailure;
        }
        return new S3C1ProviderException(
                S3C1ProviderException.Kind.RETRYABLE, operation + " failed unexpectedly", failure);
    }

    private static boolean hasExactNoSuchKeyCode(S3Exception failure) {
        return failure.awsErrorDetails() != null
                && "NoSuchKey".equals(failure.awsErrorDetails().errorCode());
    }

    private static void closeAfterInvalid(InputStream input) {
        try {
            input.close();
        } catch (IOException ignored) {
            // The integrity/capacity failure remains primary.
        }
    }

    private void requireOpen() {
        if (closed.get()) {
            throw new IllegalStateException("S3 C1 transport is closed");
        }
    }

    private static void validateKey(String key) {
        Objects.requireNonNull(key, "key");
        byte[] encoded = key.getBytes(StandardCharsets.UTF_8);
        if (key.isEmpty()
                || key.indexOf('\0') >= 0
                || encoded.length > 1_024
                || !key.equals(new String(encoded, StandardCharsets.UTF_8))) {
            throw new IllegalArgumentException(
                    "S3 Object key is empty, contains NUL/malformed Unicode, or exceeds 1024 UTF-8 bytes");
        }
    }

    private static String requireNonBlank(String value, String label) {
        Objects.requireNonNull(value, label);
        if (value.isBlank()) {
            throw new IllegalArgumentException(label + " must be non-blank");
        }
        return value;
    }

    private record ParsedContentRange(long start, long endExclusive, long totalLength) {
        private static ParsedContentRange parse(String header) {
            if (header == null || !header.toLowerCase(Locale.ROOT).startsWith("bytes ")) {
                throw new IllegalArgumentException("missing bytes unit");
            }
            int dash = header.indexOf('-', 6);
            int slash = header.indexOf('/', dash + 1);
            if (dash < 0 || slash < 0 || slash == header.length() - 1) {
                throw new IllegalArgumentException("malformed Content-Range");
            }
            long start = Long.parseLong(header.substring(6, dash));
            long inclusiveEnd = Long.parseLong(header.substring(dash + 1, slash));
            long total = Long.parseLong(header.substring(slash + 1));
            long endExclusive = Math.addExact(inclusiveEnd, 1);
            if (start < 0 || endExclusive <= start || total < endExclusive) {
                throw new IllegalArgumentException("invalid Content-Range values");
            }
            return new ParsedContentRange(start, endExclusive, total);
        }
    }
}
