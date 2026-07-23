/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.kafka.checkpoint;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.objectstore.HeadObjectOptions;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.RangeReadOptions;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** Exact-key full-integrity NKC1 object reader; discovery by LIST is intentionally absent. */
public final class KafkaCheckpointReader {
    private final ObjectStore objectStore;
    private final KafkaCheckpointCodecV1 codec;

    public KafkaCheckpointReader(ObjectStore objectStore, KafkaCheckpointCodecV1 codec) {
        this.objectStore = Objects.requireNonNull(objectStore, "objectStore");
        this.codec = Objects.requireNonNull(codec, "codec");
    }

    public CompletableFuture<KafkaCheckpointObject> openAndVerify(
            ObjectKey key,
            long expectedLength,
            Checksum expectedStorageCrc32c,
            Checksum expectedObjectSha256,
            Duration timeout) {
        try {
            Objects.requireNonNull(key, "key");
            requireChecksum(expectedStorageCrc32c, ChecksumType.CRC32C, "expectedStorageCrc32c");
            requireChecksum(expectedObjectSha256, ChecksumType.SHA256, "expectedObjectSha256");
            Objects.requireNonNull(timeout, "timeout");
            if (expectedLength <= 0 || expectedLength > KafkaCheckpointFormatV1.MAX_OBJECT_BYTES
                    || timeout.isZero() || timeout.isNegative()) {
                throw new IllegalArgumentException("invalid NKC1 read bounds");
            }
        } catch (RuntimeException failure) {
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.INVALID_ARGUMENT, false, "invalid NKC1 open request", failure));
        }
        CompletableFuture<com.nereusstream.objectstore.HeadObjectResult> head = objectStore.headObject(
                key, new HeadObjectOptions(timeout));
        return head.thenCompose(actual -> {
            if (!actual.key().equals(key)
                    || actual.objectLength() != expectedLength
                    || !actual.checksum().equals(expectedStorageCrc32c)) {
                return CompletableFuture.failedFuture(corrupt("NKC1 HEAD identity mismatch", null));
            }
            return objectStore.readRange(
                            key, 0, expectedLength,
                            new RangeReadOptions(Optional.of(expectedStorageCrc32c), timeout))
                    .thenApply(range -> {
                        ByteBuffer payload = range.payload();
                        byte[] bytes = new byte[Math.toIntExact(expectedLength)];
                        payload.get(bytes);
                        KafkaCheckpointCodecV1.Decoded decoded;
                        try {
                            decoded = codec.decode(bytes);
                        } catch (KafkaCheckpointFormatException failure) {
                            throw corrupt("NKC1 structural verification failed", failure);
                        }
                        if (!decoded.storageCrc32c().equals(expectedStorageCrc32c)
                                || !decoded.objectSha256().equals(expectedObjectSha256)) {
                            throw corrupt("NKC1 whole-object checksum mismatch", null);
                        }
                        return new KafkaCheckpointObject(
                                decoded.header(), decoded.sections(), KafkaCheckpointFormatV1.objectId(key), key,
                                expectedLength, decoded.storageCrc32c(), decoded.objectSha256(),
                                decoded.contentSha256(), actual.etag());
                    });
        }).handle((value, failure) -> {
            if (failure == null) return value;
            Throwable cause = unwrap(failure);
            if (cause instanceof NereusException nereus) throw new CompletionException(nereus);
            throw new CompletionException(new NereusException(
                    ErrorCode.OBJECT_READ_FAILED, true, "failed to read NKC1 object", cause));
        });
    }

    private static void requireChecksum(Checksum value, ChecksumType type, String name) {
        Objects.requireNonNull(value, name);
        if (value.type() != type) throw new IllegalArgumentException(name + " has the wrong checksum type");
        HexFormat.of().parseHex(value.value());
    }

    private static NereusException corrupt(String message, Throwable cause) {
        return new NereusException(ErrorCode.OBJECT_CHECKSUM_MISMATCH, false, message, cause);
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) current = current.getCause();
        return current;
    }
}
