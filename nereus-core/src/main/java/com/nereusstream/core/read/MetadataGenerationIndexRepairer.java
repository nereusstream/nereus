/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.read;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.DerivedIndexRepairCursor;
import com.nereusstream.metadata.oxia.DerivedIndexRepairResult;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Compatibility adapter over the bounded live commit-chain repair contract. */
public final class MetadataGenerationIndexRepairer implements GenerationIndexRepairer {
    private final String cluster;
    private final OxiaMetadataStore metadataStore;
    private final int maxRepairCommits;

    public MetadataGenerationIndexRepairer(
            String cluster,
            OxiaMetadataStore metadataStore,
            int maxRepairCommits) {
        this.cluster = requireText(cluster, "cluster");
        this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
        if (maxRepairCommits <= 0) {
            throw new IllegalArgumentException("maxRepairCommits must be positive");
        }
        this.maxRepairCommits = maxRepairCommits;
    }

    @Override
    public CompletableFuture<GenerationIndexRepairResult> repair(
            StreamId streamId, long targetOffset, Duration timeout) {
        Objects.requireNonNull(streamId, "streamId");
        if (targetOffset < 0) {
            throw new IllegalArgumentException("targetOffset must be non-negative");
        }
        LiveRepairDeadline deadline = new LiveRepairDeadline(timeout);
        return repairPage(
                streamId,
                targetOffset,
                Optional.empty(),
                0,
                deadline);
    }

    private CompletableFuture<GenerationIndexRepairResult> repairPage(
            StreamId streamId,
            long targetOffset,
            Optional<DerivedIndexRepairCursor> continuation,
            int scannedBefore,
            LiveRepairDeadline deadline) {
        int remaining = maxRepairCommits - scannedBefore;
        if (remaining <= 0) {
            return failedResolution("generation-zero index repair exhausted its budget");
        }
        int pageSize = Math.min(remaining, 512);
        return deadline.bound(metadataStore.repairDerivedStreamIndexes(
                        cluster,
                        streamId,
                        targetOffset,
                        continuation,
                        pageSize),
                "repair generation-zero index")
                .thenCompose(result -> continueRepair(
                        streamId,
                        targetOffset,
                        result,
                        scannedBefore,
                        deadline));
    }

    private CompletableFuture<GenerationIndexRepairResult> continueRepair(
            StreamId streamId,
            long targetOffset,
            DerivedIndexRepairResult result,
            int scannedBefore,
            LiveRepairDeadline deadline) {
        if (!result.streamId().equals(streamId)) {
            return CompletableFuture.failedFuture(invariant(
                    "generation-zero repair returned another stream", null));
        }
        int scanned;
        try {
            scanned = Math.addExact(scannedBefore, result.scannedRecords());
        } catch (ArithmeticException overflow) {
            return CompletableFuture.failedFuture(invariant(
                    "generation-zero repair accounting overflowed", overflow));
        }
        if (result.targetCovered()) {
            return CompletableFuture.completedFuture(
                    GenerationIndexRepairResult.live(
                            streamId, targetOffset, scanned));
        }
        if (!result.repairBudgetExhausted()
                || result.continuation().isEmpty()
                || result.scannedRecords() <= 0) {
            return CompletableFuture.failedFuture(invariant(
                    "generation-zero repair made no resumable progress", null));
        }
        return repairPage(
                streamId,
                targetOffset,
                result.continuation(),
                scanned,
                deadline);
    }

    private static CompletableFuture<GenerationIndexRepairResult> failedResolution(
            String message) {
        return CompletableFuture.failedFuture(new NereusException(
                ErrorCode.READ_RESOLUTION_FAILED, true, message));
    }

    private static NereusException invariant(String message, Throwable cause) {
        return new NereusException(
                ErrorCode.METADATA_INVARIANT_VIOLATION, false, message, cause);
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }

    private static final class LiveRepairDeadline {
        private final long expiresAtNanos;

        private LiveRepairDeadline(Duration timeout) {
            Objects.requireNonNull(timeout, "timeout");
            if (timeout.isZero() || timeout.isNegative()) {
                throw new IllegalArgumentException("timeout must be positive");
            }
            long timeoutNanos;
            try {
                timeoutNanos = timeout.toNanos();
            } catch (ArithmeticException overflow) {
                timeoutNanos = Long.MAX_VALUE;
            }
            long now = System.nanoTime();
            expiresAtNanos = timeoutNanos >= Long.MAX_VALUE - now
                    ? Long.MAX_VALUE
                    : now + timeoutNanos;
        }

        private <T> CompletableFuture<T> bound(
                CompletableFuture<T> source, String action) {
            Objects.requireNonNull(source, "source");
            long remaining = expiresAtNanos - System.nanoTime();
            if (remaining <= 0) {
                return CompletableFuture.failedFuture(timeout(action));
            }
            return source.orTimeout(remaining, TimeUnit.NANOSECONDS)
                    .handle((value, failure) -> {
                        if (failure == null) {
                            return value;
                        }
                        Throwable exact = unwrap(failure);
                        if (exact instanceof TimeoutException) {
                            throw timeout(action);
                        }
                        if (exact instanceof RuntimeException runtime) {
                            throw runtime;
                        }
                        throw new CompletionException(exact);
                    });
        }

        private static NereusException timeout(String action) {
            return new NereusException(
                    ErrorCode.TIMEOUT, true, action + " exceeded its deadline");
        }

        private static Throwable unwrap(Throwable failure) {
            Throwable current = failure;
            while (current instanceof CompletionException
                    && current.getCause() != null) {
                current = current.getCause();
            }
            return current;
        }
    }
}
