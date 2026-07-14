/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.metadata.oxia.CursorIds;
import com.nereusstream.metadata.oxia.OxiaKeyspace;
import com.nereusstream.objectstore.HeadObjectOptions;
import com.nereusstream.objectstore.HeadObjectResult;
import com.nereusstream.objectstore.ObjectAlreadyExistsException;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.PutObjectOptions;
import com.nereusstream.objectstore.PutObjectResult;
import com.nereusstream.objectstore.RangeReadOptions;
import com.nereusstream.objectstore.RangeReadResult;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** Generic ObjectStore-backed immutable NCS1 cursor snapshot persistence. */
public final class DefaultCursorSnapshotStore implements CursorSnapshotStore {
    private static final String CONTENT_TYPE = "application/vnd.nereus.cursor-snapshot-v1";
    private static final Map<String, String> METADATA_BASE = Map.of(
            "nereus-format", "NCS1",
            "nereus-object-type", "CURSOR_SNAPSHOT_OBJECT");

    private final String cluster;
    private final ObjectStore objectStore;
    private final CursorStorageConfig cursorConfig;
    private final Duration objectStoreRequestTimeout;
    private final Clock clock;
    private final Supplier<String> snapshotIdSupplier;
    private final LongSupplier nanoTime;
    private final AtomicBoolean closed = new AtomicBoolean();

    public DefaultCursorSnapshotStore(
            String cluster,
            ObjectStore objectStore,
            CursorStorageConfig cursorConfig,
            Duration objectStoreRequestTimeout,
            Clock clock) {
        this(
                cluster,
                objectStore,
                cursorConfig,
                objectStoreRequestTimeout,
                clock,
                secureRandomIdSupplier(),
                System::nanoTime);
    }

    DefaultCursorSnapshotStore(
            String cluster,
            ObjectStore objectStore,
            CursorStorageConfig cursorConfig,
            Duration objectStoreRequestTimeout,
            Clock clock,
            Supplier<String> snapshotIdSupplier,
            LongSupplier nanoTime) {
        this.cluster = new OxiaKeyspace(cluster).cluster();
        this.objectStore = Objects.requireNonNull(objectStore, "objectStore");
        this.cursorConfig = Objects.requireNonNull(cursorConfig, "cursorConfig");
        this.objectStoreRequestTimeout = requirePositive(
                objectStoreRequestTimeout, "objectStoreRequestTimeout");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.snapshotIdSupplier = Objects.requireNonNull(snapshotIdSupplier, "snapshotIdSupplier");
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
    }

    @Override
    public CompletableFuture<CursorSnapshotReference> write(CursorSnapshotWriteRequest request) {
        Objects.requireNonNull(request, "request");
        if (closed.get()) {
            return closedFuture();
        }
        Deadline deadline = Deadline.start(cursorConfig.cursorSnapshotOperationTimeout(), nanoTime);
        return writeAttempt(request, deadline, 0);
    }

    @Override
    public CompletableFuture<CursorAckState> read(
            CursorSnapshotReference reference, CursorIdentity expectedIdentity) {
        Objects.requireNonNull(reference, "reference");
        Objects.requireNonNull(expectedIdentity, "expectedIdentity");
        if (closed.get()) {
            return closedFuture();
        }
        try {
            validateReferenceKey(reference, expectedIdentity);
            Deadline deadline = Deadline.start(cursorConfig.cursorSnapshotOperationTimeout(), nanoTime);
            CompletableFuture<HeadObjectResult> head = objectStore.headObject(
                    reference.objectKey(), new HeadObjectOptions(deadline.callTimeout(objectStoreRequestTimeout)));
            return head.thenCompose(result -> {
                verifyHead(result, reference.objectKey(), reference.objectLength(), reference.storageChecksum(),
                        metadata(reference.snapshotId()));
                Duration timeout = deadline.callTimeout(objectStoreRequestTimeout);
                return objectStore.readRange(
                        reference.objectKey(),
                        0,
                        reference.objectLength(),
                        new RangeReadOptions(Optional.of(reference.storageChecksum()), timeout));
            }).thenApply(result -> {
                verifyRead(result, reference);
                return CursorSnapshotCodecV1.decode(
                        result.payload(), reference, expectedIdentity, cursorConfig);
            });
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
    }

    @Override
    public void close() {
        closed.set(true);
    }

    private CompletableFuture<CursorSnapshotReference> writeAttempt(
            CursorSnapshotWriteRequest request, Deadline deadline, int attempt) {
        if (closed.get()) {
            return closedFuture();
        }
        final String snapshotId;
        final ObjectKey objectKey;
        final CursorSnapshotCodecV1.EncodedSnapshot encoded;
        final Map<String, String> metadata;
        try {
            if (attempt >= cursorConfig.cursorSnapshotIdMaxAttempts()) {
                throw new NereusException(
                        ErrorCode.OBJECT_UPLOAD_FAILED,
                        false,
                        "cursor snapshot ID collision retry bound is exhausted");
            }
            snapshotId = CursorIds.requireRandomId(snapshotIdSupplier.get(), "snapshotId");
            objectKey = CursorSnapshotKeys.objectKey(cluster, request.identity(), snapshotId);
            encoded = CursorSnapshotCodecV1.encode(request, snapshotId, cursorConfig);
            metadata = metadata(snapshotId);
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }

        CompletableFuture<CursorSnapshotReference> attemptFuture;
        try {
            PutObjectOptions options = new PutObjectOptions(
                    CONTENT_TYPE,
                    encoded.storageChecksum(),
                    true,
                    metadata,
                    deadline.callTimeout(objectStoreRequestTimeout));
            attemptFuture = objectStore.putObject(objectKey, encoded.payload(), options)
                    .thenCompose(result -> {
                        verifyPut(result, objectKey, encoded.objectLength(), encoded.storageChecksum());
                        Duration timeout = deadline.callTimeout(objectStoreRequestTimeout);
                        return objectStore.headObject(objectKey, new HeadObjectOptions(timeout));
                    })
                    .thenApply(result -> {
                        verifyHead(result, objectKey, encoded.objectLength(), encoded.storageChecksum(), metadata);
                        return new CursorSnapshotReference(
                                objectKey,
                                snapshotId,
                                request.identity().cursorGeneration(),
                                request.sourceMutationSequence(),
                                request.fullState().markDeleteOffset(),
                                encoded.objectLength(),
                                encoded.storageChecksum(),
                                encoded.formatCrc32c(),
                                1,
                                request.createdAtMillis());
                    });
        } catch (Throwable error) {
            attemptFuture = CompletableFuture.failedFuture(error);
        }

        return attemptFuture.handle((result, error) -> {
            if (error == null) {
                return CompletableFuture.completedFuture(result);
            }
            Throwable cause = unwrap(error);
            if (isIfAbsentCollision(cause)
                    && attempt + 1 < cursorConfig.cursorSnapshotIdMaxAttempts()) {
                return writeAttempt(request, deadline, attempt + 1);
            }
            return CompletableFuture.<CursorSnapshotReference>failedFuture(cause);
        }).thenCompose(future -> future);
    }

    private void validateReferenceKey(
            CursorSnapshotReference reference, CursorIdentity expectedIdentity) {
        ObjectKey expected = CursorSnapshotKeys.objectKey(
                cluster, expectedIdentity, reference.snapshotId());
        if (!expected.equals(reference.objectKey())
                || reference.cursorGeneration() != expectedIdentity.cursorGeneration()) {
            throw new CursorSnapshotCodecV1.CursorSnapshotCorruptionException(
                    "cursor snapshot reference key or generation does not match the expected identity");
        }
        if (reference.objectLength() > cursorConfig.cursorSnapshotMaxBytes()) {
            throw new CursorSnapshotCodecV1.CursorSnapshotCorruptionException(
                    "cursor snapshot reference exceeds the configured object bound");
        }
    }

    private static void verifyPut(
            PutObjectResult result,
            ObjectKey key,
            long length,
            com.nereusstream.api.Checksum checksum) {
        if (!result.key().equals(key)
                || result.objectLength() != length
                || !result.checksum().equals(checksum)) {
            throw objectFailure(ErrorCode.OBJECT_UPLOAD_FAILED, "cursor snapshot PUT result mismatch");
        }
    }

    private static void verifyHead(
            HeadObjectResult result,
            ObjectKey key,
            long length,
            com.nereusstream.api.Checksum checksum,
            Map<String, String> metadata) {
        if (!result.key().equals(key)
                || result.objectLength() != length
                || !result.checksum().equals(checksum)
                || !result.metadata().equals(metadata)) {
            throw objectFailure(ErrorCode.OBJECT_CHECKSUM_MISMATCH, "cursor snapshot HEAD result mismatch");
        }
    }

    private static void verifyRead(RangeReadResult result, CursorSnapshotReference reference) {
        if (!result.key().equals(reference.objectKey())
                || result.offset() != 0
                || result.length() != reference.objectLength()
                || result.payload().remaining() != reference.objectLength()
                || result.checksum().isEmpty()
                || !result.checksum().orElseThrow().equals(reference.storageChecksum())) {
            throw objectFailure(ErrorCode.OBJECT_CHECKSUM_MISMATCH, "cursor snapshot range result mismatch");
        }
    }

    private static Map<String, String> metadata(String snapshotId) {
        return Map.of(
                "nereus-format", METADATA_BASE.get("nereus-format"),
                "nereus-object-type", METADATA_BASE.get("nereus-object-type"),
                "nereus-snapshot-id", snapshotId);
    }

    private static boolean isIfAbsentCollision(Throwable error) {
        return error instanceof ObjectAlreadyExistsException;
    }

    private static Throwable unwrap(Throwable error) {
        Throwable current = error;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static NereusException objectFailure(ErrorCode code, String message) {
        return new NereusException(code, false, message);
    }

    private static <T> CompletableFuture<T> closedFuture() {
        return NereusException.failedFuture(
                ErrorCode.STORAGE_CLOSED, false, "cursor snapshot store is closed");
    }

    private static Duration requirePositive(Duration value, String fieldName) {
        Objects.requireNonNull(value, fieldName);
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(fieldName + " must be positive");
        }
        return value;
    }

    private static Supplier<String> secureRandomIdSupplier() {
        SecureRandom random = new SecureRandom();
        return () -> {
            byte[] bytes = new byte[16];
            random.nextBytes(bytes);
            return HexFormat.of().formatHex(bytes);
        };
    }

    private static final class Deadline {
        private final long deadlineNanos;
        private final LongSupplier nanoTime;

        private Deadline(long deadlineNanos, LongSupplier nanoTime) {
            this.deadlineNanos = deadlineNanos;
            this.nanoTime = nanoTime;
        }

        static Deadline start(Duration timeout, LongSupplier nanoTime) {
            long now = nanoTime.getAsLong();
            long timeoutNanos;
            try {
                timeoutNanos = timeout.toNanos();
            } catch (ArithmeticException ignored) {
                timeoutNanos = Long.MAX_VALUE;
            }
            long deadline;
            try {
                deadline = Math.addExact(now, timeoutNanos);
            } catch (ArithmeticException ignored) {
                deadline = Long.MAX_VALUE;
            }
            return new Deadline(deadline, nanoTime);
        }

        Duration callTimeout(Duration perCallTimeout) {
            long remaining = deadlineNanos - nanoTime.getAsLong();
            if (remaining <= 0) {
                throw new NereusException(
                        ErrorCode.TIMEOUT, true, "cursor snapshot operation deadline expired", new TimeoutException());
            }
            long perCall;
            try {
                perCall = perCallTimeout.toNanos();
            } catch (ArithmeticException ignored) {
                perCall = Long.MAX_VALUE;
            }
            return Duration.ofNanos(Math.min(remaining, perCall));
        }
    }
}
