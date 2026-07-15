/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.objectstore.HeadObjectOptions;
import com.nereusstream.objectstore.HeadObjectResult;
import com.nereusstream.objectstore.ObjectStore;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Production verifier that composes exact object-store HEAD with the M3 format verifier. */
public final class DefaultMaterializationOutputVerifier implements MaterializationOutputVerifier {
    private final ObjectStore objectStore;
    private final MaterializationFormatVerifier formatVerifier;

    public DefaultMaterializationOutputVerifier(
            ObjectStore objectStore,
            MaterializationFormatVerifier formatVerifier) {
        this.objectStore = Objects.requireNonNull(objectStore, "objectStore");
        this.formatVerifier = Objects.requireNonNull(formatVerifier, "formatVerifier");
    }

    @Override
    public CompletableFuture<Void> verify(
            MaterializationTask task,
            MaterializationOutput output,
            Duration timeout) {
        MaterializationTask exactTask;
        MaterializationOutput exact;
        Deadline deadline;
        try {
            exactTask = Objects.requireNonNull(task, "task");
            exact = Objects.requireNonNull(output, "output");
            deadline = new Deadline(requirePositive(timeout));
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        CompletableFuture<HeadObjectResult> head;
        try {
            head = Objects.requireNonNull(
                    objectStore.headObject(
                            exact.objectKey(),
                            new HeadObjectOptions(deadline.remaining())),
                    "head future");
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return head.thenApply(value -> requireExactHead(exact, value))
                .thenCompose(ignored -> formatVerifier.verify(
                        exactTask,
                        exact,
                        deadline.remaining()));
    }

    private static Void requireExactHead(
            MaterializationOutput output,
            HeadObjectResult head) {
        if (!head.key().equals(output.objectKey())
                || head.objectLength() != output.objectLength()
                || !head.checksum().equals(output.storageCrc32c())
                || (!output.etag().isEmpty()
                        && (head.etag().isEmpty()
                                || !head.etag().orElseThrow().equals(output.etag())))) {
            throw new NereusException(
                    ErrorCode.OBJECT_CHECKSUM_MISMATCH,
                    false,
                    "materialization output HEAD does not match frozen output identity");
        }
        return null;
    }

    private static Duration requirePositive(Duration value) {
        Objects.requireNonNull(value, "timeout");
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
        return value;
    }

    private static final class Deadline {
        private final long deadlineNanos;

        private Deadline(Duration timeout) {
            long now = System.nanoTime();
            long nanos;
            try {
                nanos = timeout.toNanos();
            } catch (ArithmeticException failure) {
                nanos = Long.MAX_VALUE;
            }
            deadlineNanos = nanos >= Long.MAX_VALUE - now ? Long.MAX_VALUE : now + nanos;
        }

        private Duration remaining() {
            long nanos = deadlineNanos == Long.MAX_VALUE
                    ? Long.MAX_VALUE
                    : deadlineNanos - System.nanoTime();
            if (nanos <= 0) {
                throw new NereusException(
                        ErrorCode.TIMEOUT,
                        true,
                        "materialization output verification deadline expired");
            }
            return Duration.ofNanos(nanos);
        }
    }
}
