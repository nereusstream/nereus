/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKey;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.LongUnaryOperator;
import java.util.concurrent.Flow;
import java.util.zip.CRC32C;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

/** S3-compatible immutable object implementation with independent CRC32C validation. */
public final class S3CompatibleObjectStore implements ObjectStore {
    static final String CHECKSUM_TYPE_METADATA = "nereus-storage-checksum-type";
    static final String CHECKSUM_VALUE_METADATA = "nereus-storage-checksum-value";
    private static final int MAX_USER_METADATA_BYTES = 2_048;

    private final S3AsyncClient client;
    private final ScheduledExecutorService deadlineScheduler;
    private final String bucket;
    private final S3ObjectKeyMapper keys;
    private final Duration requestTimeout;
    private final ObjectPutRetryPolicy putRetryPolicy;
    private final LongUnaryOperator fullJitterMillis;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Set<CompletableFuture<?>> admitted = ConcurrentHashMap.newKeySet();
    private final Set<GuardedPutExecution> putExecutions = ConcurrentHashMap.newKeySet();

    S3CompatibleObjectStore(
            S3AsyncClient client,
            ScheduledExecutorService deadlineScheduler,
            String bucket,
            String prefix) {
        this(
                client,
                deadlineScheduler,
                bucket,
                prefix,
                Duration.ofSeconds(30),
                ObjectPutRetryPolicy.defaults());
    }

    S3CompatibleObjectStore(
            S3AsyncClient client,
            ScheduledExecutorService deadlineScheduler,
            String bucket,
            String prefix,
            Duration requestTimeout,
            ObjectPutRetryPolicy putRetryPolicy) {
        this(
                client,
                deadlineScheduler,
                bucket,
                prefix,
                requestTimeout,
                putRetryPolicy,
                S3CompatibleObjectStore::randomFullJitterMillis);
    }

    S3CompatibleObjectStore(
            S3AsyncClient client,
            ScheduledExecutorService deadlineScheduler,
            String bucket,
            String prefix,
            Duration requestTimeout,
            ObjectPutRetryPolicy putRetryPolicy,
            LongUnaryOperator fullJitterMillis) {
        this.client = Objects.requireNonNull(client, "client");
        this.deadlineScheduler = Objects.requireNonNull(deadlineScheduler, "deadlineScheduler");
        this.bucket = requireText(bucket, "bucket");
        this.keys = new S3ObjectKeyMapper(prefix);
        this.requestTimeout = Objects.requireNonNull(requestTimeout, "requestTimeout");
        this.putRetryPolicy = Objects.requireNonNull(putRetryPolicy, "putRetryPolicy");
        this.fullJitterMillis = Objects.requireNonNull(fullJitterMillis, "fullJitterMillis");
    }

    @Override
    public CompletableFuture<PutObjectResult> putObject(
            ObjectKey key,
            ByteBuffer payload,
            PutObjectOptions options) {
        ByteBufferObjectUpload source = new ByteBufferObjectUpload(payload);
        CompletableFuture<PutObjectResult> result = putObject(key, source, options);
        result.whenComplete((ignored, failure) -> source.close());
        return result;
    }

    @Override
    public CompletableFuture<PutObjectResult> putObject(
            ObjectKey key,
            ReplayableObjectUpload source,
            PutObjectOptions options) {
        return putObject(key, source, options, (ignored, attempt) -> CompletableFuture.completedFuture(null));
    }

    @Override
    public CompletableFuture<PutObjectResult> putObject(
            ObjectKey key,
            ReplayableObjectUpload source,
            PutObjectOptions options,
            PutObjectAttemptGuard attemptGuard) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(options, "options");
        Objects.requireNonNull(attemptGuard, "attemptGuard");
        if (closed.get()) {
            return failed(S3ObjectErrorMapper.closed(S3ObjectErrorMapper.Operation.PUT, bucket, key));
        }
        try {
            requireCrc32c(options.expectedChecksum(), "put checksum");
            if (source.contentLength() < 0) {
                throw new IllegalArgumentException("upload contentLength must be non-negative");
            }
            long deadline = Math.addExact(System.nanoTime(), options.timeout().toNanos());
            GuardedPutExecution execution = new GuardedPutExecution(
                    key, source, options, attemptGuard, deadline);
            putExecutions.add(execution);
            if (closed.get()) {
                execution.closeStore();
            } else {
                execution.start();
            }
            return execution.result();
        } catch (IllegalArgumentException | ArithmeticException error) {
            return failed(S3ObjectErrorMapper.invalid(
                    S3ObjectErrorMapper.Operation.PUT, bucket, key, "invalid request"));
        } catch (RuntimeException error) {
            return failed(S3ObjectErrorMapper.put(error, bucket, key));
        }
    }

    @Override
    public CompletableFuture<RangeReadResult> readRange(
            ObjectKey key,
            long offset,
            long length,
            RangeReadOptions options) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(options, "options");
        if (closed.get()) {
            return failed(S3ObjectErrorMapper.closed(S3ObjectErrorMapper.Operation.READ, bucket, key));
        }
        try {
            RangeChecks.requireNonNegativeNonOverflowingRange(offset, length, "S3 range read");
            if (length > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("S3 range length exceeds integer buffer limit");
            }
            options.expectedChecksum().ifPresent(checksum -> requireCrc32c(checksum, "range checksum"));
            if (length == 0) {
                return zeroLengthRead(key, offset, options);
            }
            long inclusiveEnd = Math.addExact(offset, length - 1);
            GetObjectRequest request = GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(keys.map(key))
                    .range("bytes=" + offset + "-" + inclusiveEnd)
                    .overrideConfiguration(operationOverride(options.timeout()).build())
                    .build();
            CompletableFuture<ResponseBytes<GetObjectResponse>> sdk = client.getObject(
                    request, AsyncResponseTransformer.toBytes());
            return link(sdk, options.timeout(), S3ObjectErrorMapper.Operation.READ, key,
                    response -> rangeResult(key, offset, length, inclusiveEnd, options, response));
        } catch (IllegalArgumentException | ArithmeticException error) {
            return failed(S3ObjectErrorMapper.invalid(
                    S3ObjectErrorMapper.Operation.READ, bucket, key, "invalid range"));
        } catch (RuntimeException error) {
            return failed(S3ObjectErrorMapper.read(error, bucket, key));
        }
    }

    @Override
    public CompletableFuture<HeadObjectResult> headObject(ObjectKey key, HeadObjectOptions options) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(options, "options");
        if (closed.get()) {
            return failed(S3ObjectErrorMapper.closed(S3ObjectErrorMapper.Operation.HEAD, bucket, key));
        }
        try {
            HeadObjectRequest request = HeadObjectRequest.builder()
                    .bucket(bucket)
                    .key(keys.map(key))
                    .overrideConfiguration(operationOverride(options.timeout()).build())
                    .build();
            CompletableFuture<HeadObjectResponse> sdk = client.headObject(request);
            return link(sdk, options.timeout(), S3ObjectErrorMapper.Operation.HEAD, key,
                    response -> headResult(key, response));
        } catch (IllegalArgumentException error) {
            return failed(S3ObjectErrorMapper.invalid(
                    S3ObjectErrorMapper.Operation.HEAD, bucket, key, "invalid request"));
        } catch (RuntimeException error) {
            return failed(S3ObjectErrorMapper.head(error, bucket, key));
        }
    }

    @Override
    public CompletableFuture<ListObjectsResult> listObjects(
            ObjectKeyPrefix prefix,
            Optional<String> continuationToken,
            ListObjectsOptions options) {
        Objects.requireNonNull(prefix, "prefix");
        Objects.requireNonNull(continuationToken, "continuationToken");
        Objects.requireNonNull(options, "options");
        if (closed.get()) {
            return failed(S3ObjectErrorMapper.closed(S3ObjectErrorMapper.Operation.LIST, bucket, null));
        }
        try {
            List<String> mappedPrefixes = keys.mapPrefixes(prefix);
            ListCursor cursor = continuationToken
                    .map(token -> decodeListCursor(token, mappedPrefixes.size()))
                    .orElseGet(() -> new ListCursor(0, Optional.empty()));
            long deadline = Math.addExact(System.nanoTime(), options.timeout().toNanos());
            return listPage(
                    prefix,
                    mappedPrefixes,
                    cursor,
                    options,
                    new ArrayList<>(),
                    new HashSet<>(),
                    new HashSet<>(),
                    deadline);
        } catch (IllegalArgumentException | ArithmeticException error) {
            return failed(S3ObjectErrorMapper.invalid(
                    S3ObjectErrorMapper.Operation.LIST, bucket, null, "invalid request"));
        } catch (RuntimeException error) {
            return failed(S3ObjectErrorMapper.list(error, bucket));
        }
    }

    @Override
    public CompletableFuture<DeleteObjectResult> deleteObject(
            ObjectKey key,
            DeleteObjectOptions options) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(options, "options");
        if (closed.get()) {
            return failed(S3ObjectErrorMapper.closed(S3ObjectErrorMapper.Operation.DELETE, bucket, key));
        }
        long deadlineNanos;
        Duration headTimeout;
        try {
            requireCrc32c(options.expectedStorageChecksum(), "delete checksum");
            deadlineNanos = Math.addExact(System.nanoTime(), options.timeout().toNanos());
            headTimeout = remainingTimeout(deadlineNanos);
        } catch (ArithmeticException | IllegalArgumentException failure) {
            return failed(S3ObjectErrorMapper.invalid(
                    S3ObjectErrorMapper.Operation.DELETE, bucket, key, "invalid request"));
        } catch (RuntimeException failure) {
            return failed(S3ObjectErrorMapper.timeout(S3ObjectErrorMapper.Operation.DELETE, bucket, key));
        }
        CompletableFuture<Optional<HeadObjectResult>> head = headObject(key, new HeadObjectOptions(headTimeout))
                .handle((value, failure) -> {
                    if (failure == null) {
                        return Optional.of(value);
                    }
                    NereusException mapped = S3ObjectErrorMapper.head(failure, bucket, key);
                    if (mapped.code() == ErrorCode.OBJECT_NOT_FOUND) {
                        return Optional.empty();
                    }
                    throw new java.util.concurrent.CompletionException(mapped);
                });
        return head.thenCompose(optional -> {
            if (optional.isEmpty()) {
                return CompletableFuture.completedFuture(
                        new DeleteObjectResult(key, DeleteObjectResult.Status.ALREADY_ABSENT));
            }
            HeadObjectResult value = optional.orElseThrow();
            if (value.objectLength() != options.expectedLength()
                    || !value.checksum().equals(options.expectedStorageChecksum())
                    || options.expectedEtag().filter(expected -> value.etag().filter(expected::equals).isEmpty()).isPresent()) {
                return failed(new NereusException(
                        ErrorCode.OBJECT_CHECKSUM_MISMATCH, false, "delete identity mismatch"));
            }
            Duration deleteTimeout;
            try {
                deleteTimeout = remainingTimeout(deadlineNanos);
            } catch (RuntimeException failure) {
                return failed(S3ObjectErrorMapper.timeout(
                        S3ObjectErrorMapper.Operation.DELETE, bucket, key));
            }
            CompletableFuture<DeleteObjectResponse> sdk = deleteWithConditionalFallback(
                    key, options.expectedEtag(), deadlineNanos, deleteTimeout);
            return link(sdk, deleteTimeout, S3ObjectErrorMapper.Operation.DELETE, key,
                    ignored -> new DeleteObjectResult(key, DeleteObjectResult.Status.DELETED))
                    .handle((result, failure) -> {
                        if (failure == null) {
                            return result;
                        }
                        NereusException mapped = S3ObjectErrorMapper.delete(failure, bucket, key);
                        if (mapped.code() == ErrorCode.OBJECT_NOT_FOUND) {
                            return new DeleteObjectResult(key, DeleteObjectResult.Status.ALREADY_ABSENT);
                        }
                        throw new java.util.concurrent.CompletionException(mapped);
                    });
        });
    }

    private CompletableFuture<DeleteObjectResponse> deleteWithConditionalFallback(
            ObjectKey key,
            Optional<String> expectedEtag,
            long deadlineNanos,
            Duration firstTimeout) {
        AtomicReference<CompletableFuture<DeleteObjectResponse>> active = new AtomicReference<>();
        CompletableFuture<DeleteObjectResponse> result = new CompletableFuture<>();
        result.whenComplete((ignored, failure) -> {
            if (result.isCancelled()) {
                CompletableFuture<DeleteObjectResponse> request = active.get();
                if (request != null) {
                    request.cancel(true);
                }
            }
        });
        CompletableFuture<DeleteObjectResponse> first;
        try {
            first = client.deleteObject(deleteRequest(key, expectedEtag, firstTimeout));
        } catch (RuntimeException failure) {
            result.completeExceptionally(failure);
            return result;
        }
        active.set(first);
        if (result.isCancelled()) {
            first.cancel(true);
            return result;
        }
        first.whenComplete((response, failure) -> {
            if (result.isDone()) {
                return;
            }
            if (failure == null) {
                result.complete(response);
                return;
            }
            if (expectedEtag.isEmpty() || !conditionalDeleteUnsupported(failure)) {
                result.completeExceptionally(failure);
                return;
            }
            Duration remaining;
            try {
                remaining = remainingTimeout(deadlineNanos);
            } catch (RuntimeException timeout) {
                result.completeExceptionally(
                        S3ObjectErrorMapper.timeout(S3ObjectErrorMapper.Operation.DELETE, bucket, key));
                return;
            }
            CompletableFuture<DeleteObjectResponse> fallback;
            try {
                fallback = client.deleteObject(deleteRequest(key, Optional.empty(), remaining));
            } catch (RuntimeException fallbackFailure) {
                result.completeExceptionally(fallbackFailure);
                return;
            }
            active.set(fallback);
            if (result.isCancelled()) {
                fallback.cancel(true);
                return;
            }
            fallback.whenComplete((fallbackResponse, fallbackFailure) -> {
                if (fallbackFailure == null) {
                    result.complete(fallbackResponse);
                } else {
                    result.completeExceptionally(fallbackFailure);
                }
            });
        });
        return result;
    }

    private DeleteObjectRequest deleteRequest(
            ObjectKey key, Optional<String> expectedEtag, Duration timeout) {
        DeleteObjectRequest.Builder request = DeleteObjectRequest.builder()
                .bucket(bucket)
                .key(keys.map(key))
                .overrideConfiguration(operationOverride(timeout).build());
        expectedEtag.ifPresent(request::ifMatch);
        return request.build();
    }

    private static boolean conditionalDeleteUnsupported(Throwable failure) {
        Throwable cause = unwrap(failure);
        if (!(cause instanceof software.amazon.awssdk.services.s3.model.S3Exception service)) {
            return false;
        }
        return service.statusCode() == 501 || service.statusCode() == 405;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        putExecutions.forEach(GuardedPutExecution::closeStore);
        putExecutions.clear();
        admitted.forEach(future -> future.cancel(true));
        admitted.clear();
        client.close();
        deadlineScheduler.shutdownNow();
    }

    private CompletableFuture<PutObjectResult> transmitPut(
            ObjectKey key,
            ReplayableObjectUpload source,
            PutObjectOptions options,
            Duration remaining) {
        try {
            String mappedKey = keys.map(key);
            Map<String, String> metadata = putMetadata(options.metadata(), options.expectedChecksum());
            AwsRequestOverrideConfiguration.Builder override = operationOverride(remaining);
            if (options.ifAbsent()) {
                override.putHeader("If-None-Match", "*");
            }
            PutObjectRequest request = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(mappedKey)
                    .contentType(options.contentType())
                    .contentLength(source.contentLength())
                    .metadata(metadata)
                    .overrideConfiguration(override.build())
                    .build();
            UploadAttemptBody body = new UploadAttemptBody(source);
            CompletableFuture<PutObjectResponse> sdk = client.putObject(request, body);
            CompletableFuture<CompletedPut> completed = awaitCompletedPut(sdk, body);
            return link(completed, remaining, S3ObjectErrorMapper.Operation.PUT, key, value -> {
                Checksum actual = value.checksum();
                if (!actual.equals(options.expectedChecksum())) {
                    throw new NereusException(
                            ErrorCode.OBJECT_CHECKSUM_MISMATCH, false, "put checksum mismatch");
                }
                return putResult(key, source.contentLength(), actual, value.response());
            });
        } catch (IllegalArgumentException failure) {
            return failed(S3ObjectErrorMapper.invalid(
                    S3ObjectErrorMapper.Operation.PUT, bucket, key, "invalid request"));
        } catch (RuntimeException failure) {
            return failed(S3ObjectErrorMapper.put(failure, bucket, key));
        }
    }

    private static CompletableFuture<CompletedPut> awaitCompletedPut(
            CompletableFuture<PutObjectResponse> sdk,
            UploadAttemptBody body) {
        CompletableFuture<CompletedPut> completed = new CompletableFuture<>();
        body.completion().whenComplete((checksum, failure) -> {
            if (failure != null && completed.completeExceptionally(failure)) {
                sdk.cancel(true);
            }
        });
        sdk.whenComplete((response, failure) -> {
            if (failure != null) {
                completed.completeExceptionally(failure);
                return;
            }
            body.completion().whenComplete((checksum, bodyFailure) -> {
                if (bodyFailure != null) {
                    completed.completeExceptionally(bodyFailure);
                } else {
                    try {
                        completed.complete(new CompletedPut(response, body.requireExactChecksum(checksum)));
                    } catch (Throwable verificationFailure) {
                        completed.completeExceptionally(verificationFailure);
                    }
                }
            });
        });
        completed.whenComplete((ignored, failure) -> {
            if (completed.isCancelled()) {
                sdk.cancel(true);
            }
        });
        return completed;
    }

    private Duration remainingTimeout(long deadlineNanos) {
        long remaining = deadlineNanos - System.nanoTime();
        if (remaining <= 0) {
            throw new IllegalStateException("deadline expired");
        }
        Duration exact = Duration.ofNanos(remaining);
        Duration bounded = exact.compareTo(requestTimeout) > 0 ? requestTimeout : exact;
        long millis = bounded.toMillis();
        if (millis <= 0) {
            throw new IllegalStateException("deadline has less than one millisecond remaining");
        }
        return Duration.ofMillis(millis);
    }

    private static boolean retryablePut(Throwable failure) {
        return failure instanceof NereusException nereus
                && nereus.retriable()
                && nereus.code() != ErrorCode.OBJECT_CHECKSUM_MISMATCH
                && nereus.code() != ErrorCode.STORAGE_CLOSED
                && nereus.code() != ErrorCode.CANCELLED;
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof java.util.concurrent.CompletionException
                || current instanceof java.util.concurrent.ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private CompletableFuture<RangeReadResult> zeroLengthRead(
            ObjectKey key,
            long offset,
            RangeReadOptions options) {
        HeadObjectRequest request = HeadObjectRequest.builder()
                .bucket(bucket)
                .key(keys.map(key))
                .overrideConfiguration(operationOverride(options.timeout()).build())
                .build();
        CompletableFuture<HeadObjectResponse> sdk = client.headObject(request);
        return link(sdk, options.timeout(), S3ObjectErrorMapper.Operation.READ, key, response -> {
            if (offset > response.contentLength()) {
                throw new NereusException(
                        ErrorCode.OBJECT_READ_FAILED, false, "zero-length range starts beyond object");
            }
            Checksum checksum = Crc32cChecksums.checksum(new byte[0]);
            if (options.expectedChecksum().isPresent()
                    && !options.expectedChecksum().orElseThrow().equals(checksum)) {
                throw new NereusException(
                        ErrorCode.OBJECT_CHECKSUM_MISMATCH, false, "range checksum mismatch");
            }
            return new RangeReadResult(
                    key, offset, 0, ByteBuffer.allocate(0).asReadOnlyBuffer(), Optional.of(checksum));
        });
    }

    private PutObjectResult putResult(
            ObjectKey key,
            long length,
            Checksum checksum,
            PutObjectResponse response) {
        String etag = response.eTag();
        if (etag == null || etag.isBlank()) {
            throw new NereusException(
                    ErrorCode.OBJECT_UPLOAD_FAILED, false, "S3 put response has no ETag");
        }
        if (response.checksumCRC32C() != null
                && !checksum.equals(decodeSdkCrc32c(response.checksumCRC32C()))) {
            throw new NereusException(
                    ErrorCode.OBJECT_CHECKSUM_MISMATCH, false, "S3 response CRC32C mismatch");
        }
        return new PutObjectResult(key, length, checksum, etag);
    }

    private RangeReadResult rangeResult(
            ObjectKey key,
            long offset,
            long length,
            long inclusiveEnd,
            RangeReadOptions options,
            ResponseBytes<GetObjectResponse> responseBytes) {
        GetObjectResponse response = responseBytes.response();
        int status = response.sdkHttpResponse().statusCode();
        String expectedContentRange = "bytes " + offset + "-" + inclusiveEnd + "/";
        String contentRange = response.sdkHttpResponse().firstMatchingHeader("Content-Range").orElse("");
        byte[] bytes = responseBytes.asByteArray();
        if (status != 206
                || !validContentRange(contentRange, expectedContentRange, inclusiveEnd)
                || (response.contentLength() != null && response.contentLength() != length)
                || bytes.length != length) {
            throw new NereusException(
                    ErrorCode.OBJECT_READ_FAILED, false, "S3 returned an invalid range response");
        }
        Checksum checksum = Crc32cChecksums.checksum(bytes);
        if (options.expectedChecksum().isPresent()
                && !options.expectedChecksum().orElseThrow().equals(checksum)) {
            throw new NereusException(
                    ErrorCode.OBJECT_CHECKSUM_MISMATCH, false, "range checksum mismatch");
        }
        return new RangeReadResult(
                key, offset, length, ByteBuffer.wrap(bytes).asReadOnlyBuffer(), Optional.of(checksum));
    }

    private HeadObjectResult headResult(ObjectKey key, HeadObjectResponse response) {
        Map<String, String> metadata = new HashMap<>(response.metadata());
        String type = metadata.remove(CHECKSUM_TYPE_METADATA);
        String value = metadata.remove(CHECKSUM_VALUE_METADATA);
        if (!ChecksumType.CRC32C.name().equals(type) || value == null) {
            throw new NereusException(
                    ErrorCode.OBJECT_CHECKSUM_MISMATCH, false, "S3 object lacks valid Nereus CRC32C metadata");
        }
        Checksum checksum;
        try {
            checksum = new Checksum(ChecksumType.CRC32C, value);
        } catch (IllegalArgumentException error) {
            throw new NereusException(
                    ErrorCode.OBJECT_CHECKSUM_MISMATCH, false, "S3 object has malformed Nereus CRC32C metadata");
        }
        return new HeadObjectResult(
                key,
                response.contentLength(),
                checksum,
                Optional.ofNullable(response.eTag()).filter(etag -> !etag.isBlank()),
                metadata);
    }

    private CompletableFuture<ListObjectsResult> listPage(
            ObjectKeyPrefix logicalPrefix,
            List<String> mappedPrefixes,
            ListCursor cursor,
            ListObjectsOptions options,
            List<ListedObject> accumulated,
            Set<String> seenCursors,
            Set<String> seenLogicalKeys,
            long deadlineNanos) {
        if (closed.get()) {
            return failed(S3ObjectErrorMapper.closed(S3ObjectErrorMapper.Operation.LIST, bucket, null));
        }
        String cursorIdentity = encodeListCursor(cursor);
        if (!seenCursors.add(cursorIdentity)) {
            return failed(new NereusException(
                    ErrorCode.OBJECT_READ_FAILED, false, "S3 list repeated an internal cursor"));
        }
        Duration remaining;
        try {
            remaining = remainingTimeout(deadlineNanos);
        } catch (RuntimeException failure) {
            return failed(S3ObjectErrorMapper.timeout(S3ObjectErrorMapper.Operation.LIST, bucket, null));
        }
        String mappedPrefix = mappedPrefixes.get(cursor.prefixIndex());
        int requested = options.maxKeys() - accumulated.size();
        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(mappedPrefix)
                .maxKeys(requested)
                .continuationToken(cursor.providerToken().orElse(null))
                .overrideConfiguration(operationOverride(remaining).build())
                .build();
        CompletableFuture<ListObjectsV2Response> sdk;
        try {
            sdk = client.listObjectsV2(request);
        } catch (RuntimeException failure) {
            return failed(S3ObjectErrorMapper.list(failure, bucket));
        }
        return link(sdk, remaining, S3ObjectErrorMapper.Operation.LIST, null,
                response -> parseListPage(
                        logicalPrefix, mappedPrefix, cursor.providerToken(), requested, response))
                .thenCompose(page -> {
                    for (ListedObject object : page.objects()) {
                        if (!seenLogicalKeys.add(object.key().value())) {
                            return failed(new NereusException(
                                    ErrorCode.OBJECT_READ_FAILED, false, "S3 list repeated a logical object"));
                        }
                        accumulated.add(object);
                    }
                    ListCursor next = null;
                    if (page.providerContinuation().isPresent()) {
                        next = new ListCursor(cursor.prefixIndex(), page.providerContinuation());
                    } else if (cursor.prefixIndex() + 1 < mappedPrefixes.size()) {
                        next = new ListCursor(cursor.prefixIndex() + 1, Optional.empty());
                    }
                    if (accumulated.size() == options.maxKeys() || next == null) {
                        accumulated.sort(java.util.Comparator.comparing(value -> value.key().value()));
                        return CompletableFuture.completedFuture(new ListObjectsResult(
                                logicalPrefix,
                                accumulated,
                                Optional.ofNullable(next).map(S3CompatibleObjectStore::encodeListCursor)));
                    }
                    return listPage(
                            logicalPrefix,
                            mappedPrefixes,
                            next,
                            options,
                            accumulated,
                            seenCursors,
                            seenLogicalKeys,
                            deadlineNanos);
                });
    }

    private ParsedListPage parseListPage(
            ObjectKeyPrefix logicalPrefix,
            String mappedPrefix,
            Optional<String> suppliedContinuation,
            int requested,
            ListObjectsV2Response response) {
        if (!mappedPrefix.equals(response.prefix())
                || response.contents().size() > requested
                || (response.keyCount() != null && response.keyCount() != response.contents().size())) {
            throw new NereusException(
                    ErrorCode.OBJECT_READ_FAILED, false, "S3 returned an invalid list page");
        }
        List<ListedObject> objects = new ArrayList<>();
        String previousMapped = null;
        for (software.amazon.awssdk.services.s3.model.S3Object item : response.contents()) {
            String mapped = item.key();
            if (mapped == null || !mapped.startsWith(mappedPrefix)
                    || (previousMapped != null && previousMapped.compareTo(mapped) >= 0)
                    || item.size() == null || item.size() < 0) {
                throw new NereusException(
                        ErrorCode.OBJECT_READ_FAILED, false, "S3 list page is not strictly ordered or bounded");
            }
            previousMapped = mapped;
            ObjectKey logical;
            try {
                logical = keys.unmap(mapped);
            } catch (IllegalArgumentException failure) {
                throw new NereusException(
                        ErrorCode.OBJECT_READ_FAILED, false, "S3 list returned a non-canonical Nereus key");
            }
            if (!logical.value().startsWith(logicalPrefix.value())) {
                throw new NereusException(
                        ErrorCode.OBJECT_READ_FAILED, false, "S3 list escaped the exact logical prefix expansion");
            }
            objects.add(new ListedObject(
                    logical,
                    item.size(),
                    Optional.ofNullable(item.eTag()).filter(value -> !value.isBlank()),
                    Optional.ofNullable(item.lastModified())));
        }
        objects.sort(java.util.Comparator.comparing(listed -> listed.key().value()));
        Optional<String> next;
        if (Boolean.TRUE.equals(response.isTruncated())) {
            String token = response.nextContinuationToken();
            if (token == null || token.isBlank() || suppliedContinuation.filter(token::equals).isPresent()) {
                throw new NereusException(
                        ErrorCode.OBJECT_READ_FAILED, false, "S3 list returned an invalid continuation token");
            }
            next = Optional.of(token);
        } else {
            if (response.nextContinuationToken() != null && !response.nextContinuationToken().isBlank()) {
                throw new NereusException(
                        ErrorCode.OBJECT_READ_FAILED, false, "non-terminal token appeared on a terminal list page");
            }
            next = Optional.empty();
        }
        return new ParsedListPage(objects, next);
    }

    private static String encodeListCursor(ListCursor cursor) {
        String raw = cursor.prefixIndex() + "\0" + cursor.providerToken().orElse("");
        return "nls1." + Base64.getUrlEncoder().withoutPadding()
                .encodeToString(raw.getBytes(StandardCharsets.UTF_8));
    }

    private static ListCursor decodeListCursor(String token, int prefixCount) {
        Objects.requireNonNull(token, "continuationToken");
        if (!token.startsWith("nls1.") || token.length() > 16_384) {
            throw new IllegalArgumentException("list continuation token is not canonical");
        }
        byte[] decoded;
        try {
            decoded = Base64.getUrlDecoder().decode(token.substring("nls1.".length()));
        } catch (IllegalArgumentException failure) {
            throw new IllegalArgumentException("list continuation token is not base64url", failure);
        }
        String raw = new String(decoded, StandardCharsets.UTF_8);
        int separator = raw.indexOf('\0');
        if (separator <= 0 || raw.indexOf('\0', separator + 1) >= 0) {
            throw new IllegalArgumentException("list continuation token has an invalid shape");
        }
        int prefixIndex;
        try {
            prefixIndex = Integer.parseInt(raw.substring(0, separator));
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException("list continuation prefix index is invalid", failure);
        }
        String provider = raw.substring(separator + 1);
        ListCursor result = new ListCursor(
                prefixIndex,
                provider.isEmpty() ? Optional.empty() : Optional.of(provider));
        if (prefixIndex < 0
                || prefixIndex >= prefixCount
                || !encodeListCursor(result).equals(token)) {
            throw new IllegalArgumentException("list continuation token is not canonical");
        }
        return result;
    }

    private <S, T> CompletableFuture<T> link(
            CompletableFuture<S> sdk,
            Duration timeout,
            S3ObjectErrorMapper.Operation operation,
            ObjectKey key,
            Function<S, T> success) {
        if (closed.get()) {
            sdk.cancel(true);
            return failed(S3ObjectErrorMapper.closed(operation, bucket, key));
        }
        OperationFuture<T> result = new OperationFuture<>(
                () -> sdk.cancel(true),
                () -> S3ObjectErrorMapper.cancelled(operation, bucket, key));
        admitted.add(result);
        if (closed.get()) {
            result.fail(S3ObjectErrorMapper.closed(operation, bucket, key));
            sdk.cancel(true);
            admitted.remove(result);
            return result;
        }
        ScheduledFuture<?> deadline;
        try {
            deadline = deadlineScheduler.schedule(() -> {
                if (result.fail(S3ObjectErrorMapper.timeout(operation, bucket, key))) {
                    sdk.cancel(true);
                }
            }, timeout.toNanos(), TimeUnit.NANOSECONDS);
        } catch (RuntimeException rejected) {
            sdk.cancel(true);
            admitted.remove(result);
            result.fail(S3ObjectErrorMapper.closed(operation, bucket, key));
            return result;
        }
        sdk.whenComplete((value, failure) -> {
            deadline.cancel(false);
            if (failure != null) {
                result.fail(map(operation, failure, key));
                return;
            }
            try {
                result.complete(success.apply(value));
            } catch (Throwable callbackFailure) {
                result.fail(map(operation, callbackFailure, key));
            }
        });
        result.whenComplete((ignored, failure) -> admitted.remove(result));
        return result;
    }

    private NereusException map(S3ObjectErrorMapper.Operation operation, Throwable failure, ObjectKey key) {
        return switch (operation) {
            case PUT -> S3ObjectErrorMapper.put(failure, bucket, key);
            case READ -> S3ObjectErrorMapper.read(failure, bucket, key);
            case HEAD -> S3ObjectErrorMapper.head(failure, bucket, key);
            case LIST -> S3ObjectErrorMapper.list(failure, bucket);
            case DELETE -> S3ObjectErrorMapper.delete(failure, bucket, key);
        };
    }

    private static AwsRequestOverrideConfiguration.Builder operationOverride(Duration timeout) {
        return AwsRequestOverrideConfiguration.builder()
                .apiCallTimeout(timeout)
                .apiCallAttemptTimeout(timeout);
    }

    private static Map<String, String> putMetadata(Map<String, String> caller, Checksum checksum) {
        Map<String, String> result = new HashMap<>();
        Set<String> canonicalKeys = new HashSet<>();
        for (Map.Entry<String, String> entry : caller.entrySet()) {
            String key = entry.getKey().toLowerCase(Locale.ROOT);
            if (!canonicalKeys.add(key)
                    || CHECKSUM_TYPE_METADATA.equals(key)
                    || CHECKSUM_VALUE_METADATA.equals(key)) {
                throw new IllegalArgumentException("caller metadata key collision");
            }
            result.put(key, entry.getValue());
        }
        result.put(CHECKSUM_TYPE_METADATA, ChecksumType.CRC32C.name());
        result.put(CHECKSUM_VALUE_METADATA, checksum.value());
        long bytes = 0;
        for (Map.Entry<String, String> entry : result.entrySet()) {
            bytes += entry.getKey().getBytes(StandardCharsets.UTF_8).length;
            bytes += entry.getValue().getBytes(StandardCharsets.UTF_8).length;
        }
        if (bytes > MAX_USER_METADATA_BYTES) {
            throw new IllegalArgumentException("S3 user metadata exceeds 2048 bytes");
        }
        return Map.copyOf(result);
    }

    private static Checksum decodeSdkCrc32c(String encoded) {
        try {
            byte[] bytes = Base64.getDecoder().decode(encoded);
            if (bytes.length != Integer.BYTES) {
                throw new IllegalArgumentException("CRC32C response length");
            }
            return Crc32cChecksums.checksum(ByteBuffer.wrap(bytes).getInt());
        } catch (RuntimeException error) {
            throw new NereusException(
                    ErrorCode.OBJECT_CHECKSUM_MISMATCH, false, "S3 response CRC32C is malformed");
        }
    }

    private static boolean validContentRange(String actual, String expectedPrefix, long inclusiveEnd) {
        if (!actual.startsWith(expectedPrefix)) {
            return false;
        }
        String total = actual.substring(expectedPrefix.length());
        if (total.isEmpty()) {
            return false;
        }
        try {
            return Long.parseLong(total) > inclusiveEnd;
        } catch (NumberFormatException error) {
            return false;
        }
    }

    private static void requireCrc32c(Checksum checksum, String name) {
        if (checksum.type() != ChecksumType.CRC32C) {
            throw new IllegalArgumentException(name + " must use CRC32C");
        }
    }

    private static byte[] copy(ByteBuffer source) {
        ByteBuffer duplicate = source.asReadOnlyBuffer();
        byte[] bytes = new byte[duplicate.remaining()];
        duplicate.get(bytes);
        return bytes;
    }

    private static String requireText(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " cannot be blank");
        }
        return value;
    }

    private static <T> CompletableFuture<T> failed(Throwable failure) {
        return CompletableFuture.failedFuture(failure);
    }

    private static long randomFullJitterMillis(long maximumInclusive) {
        if (maximumInclusive < 0) {
            throw new IllegalArgumentException("maximum jitter must be non-negative");
        }
        if (maximumInclusive == 0) {
            return 0;
        }
        if (maximumInclusive == Long.MAX_VALUE) {
            return ThreadLocalRandom.current().nextLong() & Long.MAX_VALUE;
        }
        return ThreadLocalRandom.current().nextLong(maximumInclusive + 1);
    }

    private final class GuardedPutExecution {
        private final ObjectKey key;
        private final ReplayableObjectUpload source;
        private final PutObjectOptions options;
        private final PutObjectAttemptGuard attemptGuard;
        private final long deadlineNanos;
        private final PutResultFuture result = new PutResultFuture(this);
        private volatile CompletableFuture<?> currentStage;
        private volatile ScheduledFuture<?> retryTask;
        private volatile ScheduledFuture<?> deadlineTask;

        private GuardedPutExecution(
                ObjectKey key,
                ReplayableObjectUpload source,
                PutObjectOptions options,
                PutObjectAttemptGuard attemptGuard,
                long deadlineNanos) {
            this.key = key;
            this.source = source;
            this.options = options;
            this.attemptGuard = attemptGuard;
            this.deadlineNanos = deadlineNanos;
            result.whenComplete((ignored, failure) -> cleanup());
        }

        private CompletableFuture<PutObjectResult> result() {
            return result;
        }

        private void start() {
            long remaining = deadlineNanos - System.nanoTime();
            if (remaining <= 0) {
                fail(S3ObjectErrorMapper.timeout(S3ObjectErrorMapper.Operation.PUT, bucket, key));
                return;
            }
            try {
                deadlineTask = deadlineScheduler.schedule(
                        () -> fail(S3ObjectErrorMapper.timeout(
                                S3ObjectErrorMapper.Operation.PUT, bucket, key)),
                        remaining,
                        TimeUnit.NANOSECONDS);
            } catch (RuntimeException rejected) {
                closeStore();
                return;
            }
            attempt(1);
        }

        private void attempt(int attemptNumber) {
            if (result.isDone()) {
                return;
            }
            if (closed.get()) {
                closeStore();
                return;
            }
            try {
                remainingTimeout(deadlineNanos);
            } catch (RuntimeException failure) {
                fail(S3ObjectErrorMapper.timeout(S3ObjectErrorMapper.Operation.PUT, bucket, key));
                return;
            }
            CompletableFuture<Void> authorization;
            try {
                authorization = Objects.requireNonNull(
                        attemptGuard.authorize(key, attemptNumber), "attemptGuard result");
            } catch (Throwable failure) {
                fail(failure);
                return;
            }
            track(authorization);
            authorization.whenComplete((ignored, authorizationFailure) -> {
                if (result.isDone()) {
                    return;
                }
                if (authorizationFailure != null) {
                    fail(unwrap(authorizationFailure));
                    return;
                }
                Duration remaining;
                try {
                    remaining = remainingTimeout(deadlineNanos);
                } catch (RuntimeException failure) {
                    fail(S3ObjectErrorMapper.timeout(S3ObjectErrorMapper.Operation.PUT, bucket, key));
                    return;
                }
                CompletableFuture<PutObjectResult> transmission = transmitPut(key, source, options, remaining);
                track(transmission);
                transmission.whenComplete((value, transmissionFailure) -> {
                    if (result.isDone()) {
                        return;
                    }
                    if (transmissionFailure == null) {
                        result.internalComplete(value);
                        return;
                    }
                    retryOrFail(unwrap(transmissionFailure), attemptNumber);
                });
            });
        }

        private void retryOrFail(Throwable failure, int attemptNumber) {
            if (!retryablePut(failure) || attemptNumber >= putRetryPolicy.maxAttempts()) {
                fail(failure);
                return;
            }
            int nextAttempt = attemptNumber + 1;
            long maximumDelay = putRetryPolicy.maximumBackoffMillis(nextAttempt);
            long delay;
            try {
                delay = fullJitterMillis.applyAsLong(maximumDelay);
            } catch (RuntimeException jitterFailure) {
                fail(new NereusException(
                        ErrorCode.OBJECT_UPLOAD_FAILED, false, "PUT jitter source failed"));
                return;
            }
            if (delay < 0 || delay > maximumDelay) {
                fail(new NereusException(
                        ErrorCode.OBJECT_UPLOAD_FAILED, false, "PUT jitter source exceeded its bound"));
                return;
            }
            long retryAt;
            try {
                retryAt = Math.addExact(System.nanoTime(), TimeUnit.MILLISECONDS.toNanos(delay));
            } catch (ArithmeticException overflow) {
                fail(S3ObjectErrorMapper.timeout(S3ObjectErrorMapper.Operation.PUT, bucket, key));
                return;
            }
            if (retryAt >= deadlineNanos) {
                fail(S3ObjectErrorMapper.timeout(S3ObjectErrorMapper.Operation.PUT, bucket, key));
                return;
            }
            try {
                ScheduledFuture<?> scheduled = deadlineScheduler.schedule(
                        () -> attempt(nextAttempt), delay, TimeUnit.MILLISECONDS);
                retryTask = scheduled;
                if (result.isDone()) {
                    scheduled.cancel(false);
                }
            } catch (RuntimeException rejected) {
                closeStore();
            }
        }

        private void track(CompletableFuture<?> stage) {
            currentStage = stage;
            if (result.isDone()) {
                stage.cancel(true);
            }
        }

        private boolean cancelUser() {
            boolean completed = result.internalFail(
                    S3ObjectErrorMapper.cancelled(S3ObjectErrorMapper.Operation.PUT, bucket, key));
            if (completed) {
                cancelOutstanding();
            }
            return completed;
        }

        private void closeStore() {
            boolean completed = result.internalFail(
                    S3ObjectErrorMapper.closed(S3ObjectErrorMapper.Operation.PUT, bucket, key));
            if (completed) {
                cancelOutstanding();
            }
        }

        private void fail(Throwable failure) {
            boolean completed = result.internalFail(failure);
            if (completed) {
                cancelOutstanding();
            }
        }

        private void cancelOutstanding() {
            CompletableFuture<?> stage = currentStage;
            if (stage != null) {
                stage.cancel(true);
            }
            ScheduledFuture<?> retry = retryTask;
            if (retry != null) {
                retry.cancel(false);
            }
        }

        private void cleanup() {
            ScheduledFuture<?> deadline = deadlineTask;
            if (deadline != null) {
                deadline.cancel(false);
            }
            ScheduledFuture<?> retry = retryTask;
            if (retry != null) {
                retry.cancel(false);
            }
            putExecutions.remove(this);
        }
    }

    private static final class PutResultFuture extends CompletableFuture<PutObjectResult> {
        private final GuardedPutExecution execution;

        private PutResultFuture(GuardedPutExecution execution) {
            this.execution = execution;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return execution.cancelUser();
        }

        private boolean internalComplete(PutObjectResult value) {
            return super.complete(value);
        }

        private boolean internalFail(Throwable failure) {
            return super.completeExceptionally(failure);
        }
    }

    private record ListCursor(int prefixIndex, Optional<String> providerToken) {
        private ListCursor {
            if (prefixIndex < 0) {
                throw new IllegalArgumentException("prefixIndex must be non-negative");
            }
            providerToken = Objects.requireNonNull(providerToken, "providerToken").map(value -> {
                if (value.isBlank() || value.indexOf('\0') >= 0) {
                    throw new IllegalArgumentException("provider continuation token is not canonical");
                }
                return value;
            });
        }
    }

    private record ParsedListPage(
            List<ListedObject> objects,
            Optional<String> providerContinuation) {
        private ParsedListPage {
            objects = List.copyOf(Objects.requireNonNull(objects, "objects"));
            providerContinuation = Objects.requireNonNull(providerContinuation, "providerContinuation");
        }
    }

    private static final class UploadAttemptBody implements AsyncRequestBody {
        private final ReplayableObjectUpload source;
        private final AtomicBoolean subscribed = new AtomicBoolean();
        private final CRC32C crc32c = new CRC32C();
        private final CompletableFuture<Checksum> completion = new CompletableFuture<>();
        private long emittedBytes;
        private Throwable terminalFailure;

        private UploadAttemptBody(ReplayableObjectUpload source) {
            this.source = source;
            if (source.contentLength() == 0) {
                completion.complete(Crc32cChecksums.checksum(0));
            }
        }

        @Override
        public Optional<Long> contentLength() {
            return Optional.of(source.contentLength());
        }

        @Override
        public void subscribe(Subscriber<? super ByteBuffer> downstream) {
            Objects.requireNonNull(downstream, "downstream");
            if (!subscribed.compareAndSet(false, true)) {
                IllegalStateException failure =
                        new IllegalStateException("one provider attempt body supports one subscription");
                downstream.onSubscribe(new Subscription() {
                    @Override
                    public void request(long count) {
                    }

                    @Override
                    public void cancel() {
                    }
                });
                downstream.onError(failure);
                completion.completeExceptionally(failure);
                return;
            }
            Flow.Publisher<ByteBuffer> publisher;
            try {
                publisher = source.openPublisher();
            } catch (Throwable failure) {
                synchronized (this) {
                    terminalFailure = failure;
                }
                downstream.onSubscribe(new Subscription() {
                    @Override
                    public void request(long count) {
                    }

                    @Override
                    public void cancel() {
                    }
                });
                downstream.onError(failure);
                completion.completeExceptionally(failure);
                return;
            }
            publisher.subscribe(new Flow.Subscriber<>() {
                private Flow.Subscription upstream;

                @Override
                public void onSubscribe(Flow.Subscription subscription) {
                    upstream = subscription;
                    downstream.onSubscribe(new Subscription() {
                        @Override
                        public void request(long count) {
                            upstream.request(count);
                        }

                        @Override
                        public void cancel() {
                            upstream.cancel();
                        }
                    });
                }

                @Override
                public void onNext(ByteBuffer value) {
                    ByteBuffer duplicate = Objects.requireNonNull(value, "value").asReadOnlyBuffer();
                    ByteBuffer checksumBytes = duplicate.asReadOnlyBuffer();
                    Checksum exactChecksum;
                    synchronized (UploadAttemptBody.this) {
                        emittedBytes = Math.addExact(emittedBytes, checksumBytes.remaining());
                        if (emittedBytes > source.contentLength()) {
                            upstream.cancel();
                            terminalFailure = new IllegalStateException("upload publisher exceeded declared length");
                            downstream.onError(terminalFailure);
                            completion.completeExceptionally(terminalFailure);
                            return;
                        }
                        crc32c.update(checksumBytes);
                        exactChecksum = emittedBytes == source.contentLength()
                                ? Crc32cChecksums.checksum((int) crc32c.getValue())
                                : null;
                    }
                    downstream.onNext(duplicate);
                    if (exactChecksum != null) {
                        completion.complete(exactChecksum);
                    }
                }

                @Override
                public void onError(Throwable failure) {
                    synchronized (UploadAttemptBody.this) {
                        terminalFailure = failure;
                    }
                    downstream.onError(failure);
                    completion.completeExceptionally(failure);
                }

                @Override
                public void onComplete() {
                    synchronized (UploadAttemptBody.this) {
                        if (emittedBytes != source.contentLength()) {
                            terminalFailure = new IllegalStateException(
                                    "upload publisher ended before its declared length");
                            downstream.onError(terminalFailure);
                            completion.completeExceptionally(terminalFailure);
                            return;
                        }
                    }
                    downstream.onComplete();
                    completion.complete(Crc32cChecksums.checksum((int) crc32c.getValue()));
                }
            });
        }

        private CompletableFuture<Checksum> completion() {
            return completion;
        }

        private synchronized Checksum requireExactChecksum(Checksum observed) {
            if (terminalFailure != null || emittedBytes != source.contentLength()) {
                throw new NereusException(
                        ErrorCode.OBJECT_UPLOAD_FAILED, false, "provider did not consume exact upload bytes");
            }
            Checksum actual = Crc32cChecksums.checksum((int) crc32c.getValue());
            if (!actual.equals(observed)) {
                throw new NereusException(
                        ErrorCode.OBJECT_UPLOAD_FAILED, false, "upload checksum completion changed unexpectedly");
            }
            return actual;
        }
    }

    private record CompletedPut(PutObjectResponse response, Checksum checksum) {
        private CompletedPut {
            Objects.requireNonNull(response, "response");
            Objects.requireNonNull(checksum, "checksum");
        }
    }

    private static final class OperationFuture<T> extends CompletableFuture<T> {
        private final Runnable cancelAction;
        private final java.util.function.Supplier<NereusException> cancellation;

        private OperationFuture(
                Runnable cancelAction,
                java.util.function.Supplier<NereusException> cancellation) {
            this.cancelAction = cancelAction;
            this.cancellation = cancellation;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            boolean completed = fail(cancellation.get());
            if (completed) {
                cancelAction.run();
            }
            return completed;
        }

        private boolean fail(Throwable failure) {
            return completeExceptionally(failure);
        }
    }
}
