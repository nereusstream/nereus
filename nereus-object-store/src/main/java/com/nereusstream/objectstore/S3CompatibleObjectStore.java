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
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
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
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Set<CompletableFuture<?>> admitted = ConcurrentHashMap.newKeySet();

    S3CompatibleObjectStore(
            S3AsyncClient client,
            ScheduledExecutorService deadlineScheduler,
            String bucket,
            String prefix) {
        this.client = Objects.requireNonNull(client, "client");
        this.deadlineScheduler = Objects.requireNonNull(deadlineScheduler, "deadlineScheduler");
        this.bucket = requireText(bucket, "bucket");
        this.keys = new S3ObjectKeyMapper(prefix);
    }

    @Override
    public CompletableFuture<PutObjectResult> putObject(
            ObjectKey key,
            ByteBuffer payload,
            PutObjectOptions options) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(options, "options");
        if (closed.get()) {
            return failed(S3ObjectErrorMapper.closed(S3ObjectErrorMapper.Operation.PUT, bucket, key));
        }
        try {
            requireCrc32c(options.expectedChecksum(), "put checksum");
            byte[] bytes = copy(payload);
            Checksum actual = Crc32cChecksums.checksum(bytes);
            if (!actual.equals(options.expectedChecksum())) {
                return failed(new NereusException(
                        ErrorCode.OBJECT_CHECKSUM_MISMATCH, false, "put checksum mismatch"));
            }
            String mappedKey = keys.map(key);
            Map<String, String> metadata = putMetadata(options.metadata(), actual);
            AwsRequestOverrideConfiguration.Builder override = operationOverride(options.timeout());
            if (options.ifAbsent()) {
                override.putHeader("If-None-Match", "*");
            }
            PutObjectRequest request = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(mappedKey)
                    .contentType(options.contentType())
                    .contentLength((long) bytes.length)
                    .metadata(metadata)
                    .overrideConfiguration(override.build())
                    .build();
            CompletableFuture<PutObjectResponse> sdk = client.putObject(request, AsyncRequestBody.fromBytes(bytes));
            return link(sdk, options.timeout(), S3ObjectErrorMapper.Operation.PUT, key,
                    response -> putResult(key, bytes.length, actual, response));
        } catch (IllegalArgumentException error) {
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
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        admitted.forEach(future -> future.cancel(true));
        admitted.clear();
        client.close();
        deadlineScheduler.shutdownNow();
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
            int length,
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
        return new HeadObjectResult(key, response.contentLength(), checksum, metadata);
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
