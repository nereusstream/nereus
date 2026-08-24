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
    public static final String ADAPTER_VERSION = "nereus-s3-c1-v1/aws-sdk-s3-2.47.5";

    private final S3Client client;
    private final String bucket;
    private final boolean closeClient;
    private final ObjectProviderCapabilities capabilities;
    private final AtomicBoolean closed = new AtomicBoolean();

    public S3C1ObjectProviderTransport(
            S3Client client,
            String bucket,
            String exactProviderIdentity,
            boolean closeClient,
            int maximumListPageKeys) {
        this.client = Objects.requireNonNull(client, "client");
        this.bucket = requireNonBlank(bucket, "bucket");
        this.closeClient = closeClient;
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

    @Override
    public ObjectProviderCapabilities capabilities() {
        return capabilities;
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
