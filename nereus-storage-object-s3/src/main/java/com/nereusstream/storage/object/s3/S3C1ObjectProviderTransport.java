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
import java.nio.ByteBuffer;
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
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.GetBucketVersioningRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsRequest;
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsResponse;
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
    public static final int MAXIMUM_MULTIPART_UPLOAD_ID_BYTES = 2_048;
    public static final int MAXIMUM_MULTIPART_CONTINUATION_TOKEN_BYTES = 4_096;
    public static final String ADAPTER_VERSION = "nereus-s3-c1-v1/aws-sdk-s3-2.47.5";
    private static final byte[] MULTIPART_TOKEN_MAGIC = "M5MP1".getBytes(StandardCharsets.US_ASCII);

    private final S3Client client;
    private final String bucket;
    private final boolean closeClient;
    private final ObjectProviderCapabilities capabilities;
    private final boolean versionMatchDeleteAdmitted;
    private final boolean multipartCleanupAdmitted;
    private final AtomicBoolean closed = new AtomicBoolean();

    public S3C1ObjectProviderTransport(
            S3Client client,
            String bucket,
            String exactProviderIdentity,
            boolean closeClient,
            int maximumListPageKeys) {
        this(client, bucket, exactProviderIdentity, closeClient, maximumListPageKeys, false, false);
    }

    private S3C1ObjectProviderTransport(
            S3Client client,
            String bucket,
            String exactProviderIdentity,
            boolean closeClient,
            int maximumListPageKeys,
            boolean versionMatchDeleteAdmitted,
            boolean multipartCleanupAdmitted) {
        this.client = Objects.requireNonNull(client, "client");
        this.bucket = requireNonBlank(bucket, "bucket");
        this.closeClient = closeClient;
        this.versionMatchDeleteAdmitted = versionMatchDeleteAdmitted;
        this.multipartCleanupAdmitted = multipartCleanupAdmitted;
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
                client, canonicalBucket, exactProviderIdentity, closeClient, maximumListPageKeys, true, false);
    }

    /** Admits exact upload-id abort only after an exact live multipart LIST succeeds against the selected bucket. */
    public static S3C1ObjectProviderTransport admitMultipartCleanupV1(
            S3Client client, String bucket, String exactProviderIdentity, boolean closeClient, int maximumListPageKeys)
            throws IOException {
        Objects.requireNonNull(client, "client");
        String canonicalBucket = requireNonBlank(bucket, "bucket");
        requireNonBlank(exactProviderIdentity, "exactProviderIdentity");
        if (maximumListPageKeys <= 0 || maximumListPageKeys > DEFAULT_MAXIMUM_LIST_PAGE_KEYS) {
            throw new IllegalArgumentException("maximumListPageKeys must be in [1,1000]");
        }
        try {
            ListMultipartUploadsResponse response = client.listMultipartUploads(ListMultipartUploadsRequest.builder()
                    .bucket(canonicalBucket)
                    .maxUploads(1)
                    .build());
            if (response == null || response.uploads() == null) {
                throw new S3C1ProviderException(
                        S3C1ProviderException.Kind.INTEGRITY,
                        "EXACT_UPLOAD_ID_ABORT_V1 admission returned no multipart inventory");
            }
        } catch (S3C1ProviderException failure) {
            throw failure;
        } catch (Throwable failure) {
            throw mapped("multipart cleanup admission", failure);
        }
        return new S3C1ObjectProviderTransport(
                client, canonicalBucket, exactProviderIdentity, closeClient, maximumListPageKeys, false, true);
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
    public MultipartCleanupCapabilities multipartCleanupCapabilities() {
        return multipartCleanupAdmitted
                ? new MultipartCleanupCapabilities(
                        capabilities.providerIdentity(),
                        "EXACT_UPLOAD_ID_ABORT_V1",
                        true,
                        true,
                        true,
                        MAXIMUM_MULTIPART_UPLOAD_ID_BYTES,
                        MAXIMUM_MULTIPART_CONTINUATION_TOKEN_BYTES,
                        capabilities.maximumListPageKeys())
                : MultipartCleanupCapabilities.unsupported(capabilities.providerIdentity());
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
    public MultipartListPage listMultipartUploads(
            String prefix, Optional<CanonicalBytes> continuationToken, int maximumUploads) throws IOException {
        requireOpen();
        validateKey(prefix);
        Objects.requireNonNull(continuationToken, "continuationToken");
        if (!multipartCleanupAdmitted) {
            throw new S3C1ProviderException(
                    S3C1ProviderException.Kind.UNSUPPORTED_OPERATION, "multipart cleanup is not admitted");
        }
        if (maximumUploads <= 0 || maximumUploads > capabilities.maximumListPageKeys()) {
            throw new IllegalArgumentException("maximumUploads exceeds the admitted page cap");
        }
        ListMultipartUploadsRequest.Builder request = ListMultipartUploadsRequest.builder()
                .bucket(bucket)
                .prefix(prefix)
                .maxUploads(maximumUploads);
        continuationToken.ifPresent(token -> {
            MultipartMarkers markers = decodeMultipartContinuation(token);
            markers.keyMarker().ifPresent(request::keyMarker);
            markers.uploadIdMarker().ifPresent(request::uploadIdMarker);
        });
        ListMultipartUploadsResponse response;
        try {
            response = client.listMultipartUploads(request.build());
        } catch (Throwable failure) {
            throw mapped("multipart LIST", failure);
        }
        List<MultipartUploadIdentity> uploads =
                new ArrayList<>(response.uploads().size());
        response.uploads()
                .forEach(upload -> uploads.add(new MultipartUploadIdentity(
                        requireNonBlank(upload.key(), "multipart key"),
                        canonicalMultipartUploadId(upload.uploadId()))));
        Optional<CanonicalBytes> next;
        if (response.isTruncated()) {
            next = Optional.of(encodeMultipartContinuation(
                    optionalMarker(response.nextKeyMarker()), optionalMarker(response.nextUploadIdMarker())));
        } else {
            next = Optional.empty();
        }
        return new MultipartListPage(uploads, next);
    }

    @Override
    public ExactMultipartAbortResult abortMultipartUploadExact(String key, CanonicalBytes exactUploadId)
            throws IOException {
        requireOpen();
        validateKey(key);
        Objects.requireNonNull(exactUploadId, "exactUploadId");
        if (!multipartCleanupAdmitted) {
            return ExactMultipartAbortResult.UNSUPPORTED;
        }
        String uploadId = decodeMultipartUploadId(exactUploadId);
        try {
            client.abortMultipartUpload(AbortMultipartUploadRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .uploadId(uploadId)
                    .build());
            return ExactMultipartAbortResult.ABORT_ACCEPTED;
        } catch (S3Exception failure) {
            return classifyMultipartAbortFailure(failure);
        } catch (SdkClientException | CancellationException failure) {
            return ExactMultipartAbortResult.RESPONSE_UNKNOWN;
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

    private static CanonicalBytes canonicalMultipartUploadId(String uploadId) {
        String canonical = requireNonBlank(uploadId, "multipart upload id");
        CanonicalBytes encoded = CanonicalBytes.copyOf(canonical.getBytes(StandardCharsets.UTF_8));
        if (encoded.length() > MAXIMUM_MULTIPART_UPLOAD_ID_BYTES
                || !canonical.equals(new String(encoded.toByteArray(), StandardCharsets.UTF_8))) {
            throw new IllegalArgumentException("multipart upload id is malformed or oversized");
        }
        return encoded;
    }

    private static String decodeMultipartUploadId(CanonicalBytes token) {
        if (token.length() > MAXIMUM_MULTIPART_UPLOAD_ID_BYTES) {
            throw new IllegalArgumentException("multipart upload id is oversized");
        }
        return decodeUtf8(token, "multipart upload id");
    }

    private static CanonicalBytes encodeMultipartContinuation(
            Optional<String> keyMarker, Optional<String> uploadIdMarker) {
        Objects.requireNonNull(keyMarker, "keyMarker");
        Objects.requireNonNull(uploadIdMarker, "uploadIdMarker");
        if (keyMarker.isEmpty() && uploadIdMarker.isEmpty()) {
            throw new IllegalArgumentException("truncated multipart LIST returned no continuation markers");
        }
        byte[] key = keyMarker
                .map(value -> canonicalMarkerBytes(value, "key marker"))
                .orElseGet(() -> new byte[0]);
        byte[] upload = uploadIdMarker
                .map(value -> canonicalMarkerBytes(value, "upload-id marker"))
                .orElseGet(() -> new byte[0]);
        if (key.length > 1_024 || upload.length > MAXIMUM_MULTIPART_UPLOAD_ID_BYTES) {
            throw new IllegalArgumentException("multipart continuation marker exceeds its component hard cap");
        }
        int length = Math.addExact(MULTIPART_TOKEN_MAGIC.length + Integer.BYTES * 2, key.length + upload.length);
        if (length > MAXIMUM_MULTIPART_CONTINUATION_TOKEN_BYTES) {
            throw new IllegalArgumentException("multipart continuation markers exceed the hard cap");
        }
        ByteBuffer target = ByteBuffer.allocate(length);
        target.put(MULTIPART_TOKEN_MAGIC)
                .putInt(key.length)
                .put(key)
                .putInt(upload.length)
                .put(upload);
        return CanonicalBytes.copyOf(target.array());
    }

    private static MultipartMarkers decodeMultipartContinuation(CanonicalBytes token) {
        Objects.requireNonNull(token, "token");
        if (token.isEmpty() || token.length() > MAXIMUM_MULTIPART_CONTINUATION_TOKEN_BYTES) {
            throw new IllegalArgumentException("multipart continuation token is empty or oversized");
        }
        try {
            ByteBuffer source = ByteBuffer.wrap(token.toByteArray());
            byte[] magic = new byte[MULTIPART_TOKEN_MAGIC.length];
            source.get(magic);
            if (!Arrays.equals(magic, MULTIPART_TOKEN_MAGIC)) {
                throw new IllegalArgumentException("multipart continuation token has the wrong domain");
            }
            int keyLength = source.getInt();
            if (keyLength < 0 || keyLength > source.remaining() - Integer.BYTES) {
                throw new IllegalArgumentException("multipart continuation key marker length is invalid");
            }
            byte[] key = new byte[keyLength];
            source.get(key);
            int uploadLength = source.getInt();
            if (uploadLength < 0 || uploadLength != source.remaining()) {
                throw new IllegalArgumentException("multipart continuation upload marker length is invalid");
            }
            byte[] upload = new byte[uploadLength];
            source.get(upload);
            String keyMarker = new String(key, StandardCharsets.UTF_8);
            String uploadMarker = new String(upload, StandardCharsets.UTF_8);
            if (!Arrays.equals(key, keyMarker.getBytes(StandardCharsets.UTF_8))
                    || !Arrays.equals(upload, uploadMarker.getBytes(StandardCharsets.UTF_8))) {
                throw new IllegalArgumentException("multipart continuation markers are not canonical UTF-8");
            }
            Optional<String> optionalKey = keyLength == 0 ? Optional.empty() : Optional.of(keyMarker);
            Optional<String> optionalUpload = uploadLength == 0 ? Optional.empty() : Optional.of(uploadMarker);
            MultipartMarkers markers = new MultipartMarkers(optionalKey, optionalUpload);
            if (!encodeMultipartContinuation(optionalKey, optionalUpload).equals(token)) {
                throw new IllegalArgumentException("multipart continuation token is not canonical");
            }
            return markers;
        } catch (java.nio.BufferUnderflowException failure) {
            throw new IllegalArgumentException("multipart continuation token is truncated", failure);
        }
    }

    private static Optional<String> optionalMarker(String marker) {
        return marker == null || marker.isEmpty() ? Optional.empty() : Optional.of(marker);
    }

    private static byte[] canonicalMarkerBytes(String marker, String label) {
        byte[] encoded = marker.getBytes(StandardCharsets.UTF_8);
        if (marker.isBlank()
                || marker.indexOf('\0') >= 0
                || !marker.equals(new String(encoded, StandardCharsets.UTF_8))) {
            throw new IllegalArgumentException(label + " is not canonical non-empty UTF-8");
        }
        return encoded;
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

    static ExactMultipartAbortResult classifyMultipartAbortFailure(S3Exception failure) throws IOException {
        int status = failure.statusCode();
        String code = failure.awsErrorDetails() == null
                ? null
                : failure.awsErrorDetails().errorCode();
        if (status == 404 && "NoSuchUpload".equals(code)) {
            return ExactMultipartAbortResult.DEFINITIVELY_NOT_FOUND;
        }
        if (status == 429) {
            return ExactMultipartAbortResult.RETRYABLE;
        }
        if (status == 408 || status == 409 || status >= 500) {
            return ExactMultipartAbortResult.RESPONSE_UNKNOWN;
        }
        if (status >= 400 && status < 500) {
            return ExactMultipartAbortResult.DEFINITIVE_CONFLICT;
        }
        throw mapped("exact multipart abort", failure);
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

    private record MultipartMarkers(Optional<String> keyMarker, Optional<String> uploadIdMarker) {
        private MultipartMarkers {
            keyMarker = Objects.requireNonNull(keyMarker, "keyMarker");
            uploadIdMarker = Objects.requireNonNull(uploadIdMarker, "uploadIdMarker");
            if (keyMarker.isEmpty() && uploadIdMarker.isEmpty()) {
                throw new IllegalArgumentException("multipart continuation markers are both empty");
            }
        }
    }
}
