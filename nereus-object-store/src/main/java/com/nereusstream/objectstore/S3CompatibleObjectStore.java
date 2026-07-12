/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKey;
import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** S3-compatible immutable object implementation with independent CRC32C validation. */
public final class S3CompatibleObjectStore implements ObjectStore {
    static final String CRC32C_METADATA = "nereus-crc32c";
    private final AmazonS3 client;
    private final ExecutorService executor;
    private final String bucket;
    private final S3ObjectKeyMapper keys;
    private final AtomicBoolean closed = new AtomicBoolean();

    S3CompatibleObjectStore(AmazonS3 client, ExecutorService executor, String bucket, String prefix) {
        this.client = client; this.executor = executor; this.bucket = bucket; this.keys = new S3ObjectKeyMapper(prefix);
    }

    @Override public CompletableFuture<PutObjectResult> putObject(ObjectKey key, ByteBuffer payload, PutObjectOptions options) {
        byte[] bytes = copy(payload); Checksum actual = Crc32cChecksums.checksum(bytes);
        if (!actual.equals(options.expectedChecksum())) return CompletableFuture.failedFuture(failure(ErrorCode.OBJECT_CHECKSUM_MISMATCH, false, "put checksum mismatch", null));
        return supply(() -> {
            ObjectMetadata metadata = new ObjectMetadata(); metadata.setContentLength(bytes.length); metadata.setContentType(options.contentType());
            options.metadata().forEach(metadata::addUserMetadata); metadata.addUserMetadata(CRC32C_METADATA, actual.value());
            PutObjectRequest request = new PutObjectRequest(bucket, keys.map(key), new ByteArrayInputStream(bytes), metadata);
            if (options.ifAbsent()) request.putCustomRequestHeader("If-None-Match", "*");
            try { String etag = client.putObject(request).getETag(); return new PutObjectResult(key, bytes.length, actual, etag); }
            catch (Throwable error) { throw map(error, ErrorCode.OBJECT_UPLOAD_FAILED, "S3 put failed"); }
        }, options.timeout().toNanos());
    }

    @Override public CompletableFuture<RangeReadResult> readRange(ObjectKey key, long offset, long length, RangeReadOptions options) {
        RangeChecks.requireNonNegativeNonOverflowingRange(offset, length, "S3 range read");
        if (length == 0) return headObject(key, new HeadObjectOptions(options.timeout())).thenApply(head -> {
            if (offset > head.objectLength()) throw failure(ErrorCode.OBJECT_READ_FAILED, false, "zero-length range starts beyond object", null);
            return new RangeReadResult(key, offset, 0, ByteBuffer.allocate(0), Optional.of(Crc32cChecksums.checksum(new byte[0])));
        });
        return supply(() -> {
            GetObjectRequest request = new GetObjectRequest(bucket, keys.map(key)).withRange(offset, offset + length - 1);
            try (var object = client.getObject(request); var input = object.getObjectContent()) {
                byte[] bytes = input.readAllBytes(); if (bytes.length != length) throw failure(ErrorCode.OBJECT_READ_FAILED, true, "S3 returned a short range", null);
                Checksum actual = Crc32cChecksums.checksum(bytes);
                if (options.expectedChecksum().isPresent() && !options.expectedChecksum().orElseThrow().equals(actual)) throw failure(ErrorCode.OBJECT_CHECKSUM_MISMATCH, false, "range checksum mismatch", null);
                return new RangeReadResult(key, offset, length, ByteBuffer.wrap(bytes), Optional.of(actual));
            } catch (NereusException e) { throw e; } catch (Throwable error) { throw map(error, ErrorCode.OBJECT_READ_FAILED, "S3 range read failed"); }
        }, options.timeout().toNanos());
    }

    @Override public CompletableFuture<HeadObjectResult> headObject(ObjectKey key, HeadObjectOptions options) {
        return supply(() -> {
            try {
                ObjectMetadata metadata = client.getObjectMetadata(new GetObjectMetadataRequest(bucket, keys.map(key)));
                String crc = metadata.getUserMetaDataOf(CRC32C_METADATA);
                if (crc == null) throw failure(ErrorCode.OBJECT_CHECKSUM_MISMATCH, false, "S3 object lacks Nereus CRC32C", null);
                Map<String,String> user = new HashMap<>(metadata.getUserMetadata()); user.remove(CRC32C_METADATA);
                return new HeadObjectResult(key, metadata.getContentLength(), new Checksum(com.nereusstream.api.ChecksumType.CRC32C, crc), user);
            } catch (NereusException e) { throw e; } catch (Throwable error) { throw map(error, ErrorCode.OBJECT_READ_FAILED, "S3 head failed"); }
        }, options.timeout().toNanos());
    }

    @Override public void close() { if (closed.compareAndSet(false, true)) { client.shutdown(); executor.shutdownNow(); } }
    private <T> CompletableFuture<T> supply(java.util.concurrent.Callable<T> task, long timeoutNanos) {
        if (closed.get()) return CompletableFuture.failedFuture(failure(ErrorCode.STORAGE_CLOSED, false, "S3 object store is closed", null));
        return CompletableFuture.supplyAsync(() -> { try { return task.call(); } catch (RuntimeException e) { throw e; } catch (Exception e) { throw new RuntimeException(e); } }, executor).orTimeout(timeoutNanos, TimeUnit.NANOSECONDS);
    }
    private static byte[] copy(ByteBuffer source) { ByteBuffer copy = source.asReadOnlyBuffer(); byte[] bytes = new byte[copy.remaining()]; copy.get(bytes); return bytes; }
    private static RuntimeException map(Throwable error, ErrorCode fallback, String message) { if (error instanceof AmazonServiceException service && service.getStatusCode() == 404) return failure(ErrorCode.OBJECT_NOT_FOUND, false, message, error); if (error instanceof AmazonServiceException service && (service.getStatusCode() == 409 || service.getStatusCode() == 412)) return failure(ErrorCode.OBJECT_UPLOAD_FAILED, false, "conditional S3 put failed", error); return failure(fallback, true, message, error); }
    private static NereusException failure(ErrorCode code, boolean retry, String message, Throwable cause) { return new NereusException(code, retry, message, cause); }
}
