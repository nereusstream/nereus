/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.recovery;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.kafka.checkpoint.KafkaCheckpointSourceState;
import com.nereusstream.kafka.partition.KafkaPartitionState;
import com.nereusstream.metadata.oxia.records.KafkaPartitionLifecycle;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

/** One-shot deterministic leader-open recovery from a verified checkpoint plus exact committed batch replay. */
public final class KafkaPartitionRecoveryCoordinator<S> {
    private static final String AUTHORITY_TYPE = "kafka-partition-leader-v1";

    private final KafkaCheckpointRecoveryCoordinator checkpoints;
    private final KafkaRecoveryBatchSource batches;
    private final KafkaRecoveryStateCodec<S> stateCodec;
    private final KafkaRecoveryPublisher<S> publisher;
    private final Executor recoveryExecutor;
    private final Clock clock;
    private final AtomicReference<KafkaPartitionState> state =
            new AtomicReference<>(KafkaPartitionState.NEW);
    private final AtomicReference<CompletableFuture<KafkaRecoveredPartition<S>>> inFlight =
            new AtomicReference<>();

    public KafkaPartitionRecoveryCoordinator(
            KafkaCheckpointRecoveryCoordinator checkpoints,
            KafkaRecoveryBatchSource batches,
            KafkaRecoveryStateCodec<S> stateCodec,
            KafkaRecoveryPublisher<S> publisher,
            Clock clock) {
        this(checkpoints, batches, stateCodec, publisher, Runnable::run, clock);
    }

    public KafkaPartitionRecoveryCoordinator(
            KafkaCheckpointRecoveryCoordinator checkpoints,
            KafkaRecoveryBatchSource batches,
            KafkaRecoveryStateCodec<S> stateCodec,
            KafkaRecoveryPublisher<S> publisher,
            Executor recoveryExecutor,
            Clock clock) {
        this.checkpoints = Objects.requireNonNull(checkpoints, "checkpoints");
        this.batches = Objects.requireNonNull(batches, "batches");
        this.stateCodec = Objects.requireNonNull(stateCodec, "stateCodec");
        this.publisher = Objects.requireNonNull(publisher, "publisher");
        this.recoveryExecutor = Objects.requireNonNull(recoveryExecutor, "recoveryExecutor");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    public KafkaPartitionState state() {
        return state.get();
    }

    public CompletableFuture<KafkaRecoveredPartition<S>> recover(
            KafkaPartitionRecoveryRequest request) {
        Objects.requireNonNull(request, "request");
        CompletableFuture<KafkaRecoveredPartition<S>> existing = inFlight.get();
        if (existing != null) return existing;
        CompletableFuture<KafkaRecoveredPartition<S>> result = new CompletableFuture<>();
        if (!inFlight.compareAndSet(null, result)) return inFlight.get();
        try {
            begin(request);
            run(request).whenComplete((value, failure) -> {
                if (failure == null) {
                    result.complete(value);
                } else {
                    failState(unwrap(failure));
                    result.completeExceptionally(unwrap(failure));
                }
            });
        } catch (Throwable failure) {
            failState(failure);
            result.completeExceptionally(failure);
        }
        return result;
    }

    private void begin(KafkaPartitionRecoveryRequest request) {
        transition(KafkaPartitionState.NEW, KafkaPartitionState.BINDING);
        var checkpoint = request.checkpointRequest();
        if (!checkpoint.binding().value().identity().equals(checkpoint.identity().durableId())
                || checkpoint.binding().value().lifecycle() != KafkaPartitionLifecycle.ACTIVE) {
            throw invariant("Kafka recovery requires the exact ACTIVE binding");
        }
        transition(KafkaPartitionState.BINDING, KafkaPartitionState.ACQUIRING_AUTHORITY);
        KafkaCheckpointSourceState source = checkpoint.currentSource();
        if (!source.authority().authorityType().equals(AUTHORITY_TYPE)
                || !source.authority().authorityId().equals(
                        checkpoint.identity().durableId().canonicalIdentity())
                || source.appendInFlight()
                || source.stateMapEndOffset() != source.endOffset()) {
            throw fenced("Kafka recovery source is not an exact quiescent leader authority");
        }
        transition(KafkaPartitionState.ACQUIRING_AUTHORITY, KafkaPartitionState.LOADING_HEAD);
        transition(KafkaPartitionState.LOADING_HEAD, KafkaPartitionState.LOADING_CHECKPOINT);
    }

    private CompletableFuture<KafkaRecoveredPartition<S>> run(
            KafkaPartitionRecoveryRequest request) {
        long deadline;
        try {
            deadline = Math.addExact(clock.millis(), request.timeout().toMillis());
        } catch (ArithmeticException failure) {
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                    "Kafka partition recovery deadline overflows", failure));
        }
        return checkpoints.recover(request.checkpointRequest()).thenComposeAsync(checkpoint -> {
            transition(KafkaPartitionState.LOADING_CHECKPOINT, KafkaPartitionState.REPLAYING);
            KafkaCheckpointSourceState frozen = request.checkpointRequest().currentSource();
            S fresh = stateCodec.freshState();
            long replayStart = checkpoint.checkpoint()
                    .map(value -> {
                        stateCodec.hydrateCheckpoint(
                                fresh, value.sections(), value.header().checkpointOffset());
                        return value.header().checkpointOffset();
                    })
                    .orElse(0L);
            return replayPages(fresh, replayStart, frozen.endOffset(), 0, deadline)
                    .thenCompose(count -> validateAndPublish(
                            request, checkpoint, fresh, replayStart, count, deadline));
        }, recoveryExecutor);
    }

    private CompletableFuture<Integer> replayPages(
            S fresh,
            long startOffset,
            long endOffset,
            int replayedBatchCount,
            long deadline) {
        if (startOffset == endOffset) {
            return CompletableFuture.completedFuture(replayedBatchCount);
        }
        Duration remaining = remaining(deadline);
        return batches.readCommittedPage(startOffset, endOffset, remaining)
                .thenComposeAsync(page -> {
                    Objects.requireNonNull(page, "Kafka recovery batch page");
                    if (page.requestedOffset() != startOffset
                            || page.nextOffset() <= startOffset
                            || page.nextOffset() > endOffset) {
                        throw invariant("Kafka recovery page is not exact, progressing, and bounded");
                    }
                    long cursor = startOffset;
                    int count = replayedBatchCount;
                    for (KafkaReplayBatch batch : page.batches()) {
                        if (batch.baseOffset() != cursor || batch.lastOffset() >= endOffset) {
                            throw invariant(
                                    "Kafka recovery batches are not exact, contiguous, and bounded");
                        }
                        stateCodec.replayBatch(fresh, batch);
                        try {
                            cursor = Math.addExact(batch.lastOffset(), 1);
                            count = Math.addExact(count, 1);
                        } catch (ArithmeticException failure) {
                            throw invariant("Kafka recovery batch offset/count overflows");
                        }
                    }
                    if (cursor != page.nextOffset()) {
                        throw invariant("Kafka recovery page cursor does not match replayed batches");
                    }
                    return replayPages(fresh, cursor, endOffset, count, deadline);
                }, recoveryExecutor);
    }

    private CompletableFuture<KafkaRecoveredPartition<S>> validateAndPublish(
            KafkaPartitionRecoveryRequest request,
            KafkaCheckpointRecoveryResult checkpoint,
            S fresh,
            long replayStart,
            int replayedBatchCount,
            long deadline) {
        transition(KafkaPartitionState.REPLAYING, KafkaPartitionState.VALIDATING);
        KafkaCheckpointSourceState frozen = request.checkpointRequest().currentSource();
        stateCodec.validateRecoveredState(fresh, frozen);
        return request.checkpointRequest().sourceValidator().loadCurrent().thenCompose(current -> {
            requireUnchanged(frozen, current);
            remaining(deadline);
            KafkaRecoveredPartition<S> recovered = new KafkaRecoveredPartition<>(
                    fresh,
                    frozen,
                    replayStart,
                    frozen.endOffset(),
                    replayedBatchCount,
                    checkpoint.checkpoint().map(value -> value.reference().objectId()));
            return publisher.publish(recovered)
                    .thenCompose(ignored -> request.checkpointRequest().sourceValidator().loadCurrent())
                    .thenApply(afterPublish -> {
                        requireUnchanged(frozen, afterPublish);
                        remaining(deadline);
                        transition(KafkaPartitionState.VALIDATING, KafkaPartitionState.LEADER_WRITABLE);
                        return recovered;
                    });
        });
    }

    private static void requireUnchanged(
            KafkaCheckpointSourceState frozen, KafkaCheckpointSourceState current) {
        if (!frozen.sameSession(current)
                || frozen.trimOffset() != current.trimOffset()
                || frozen.endOffset() != current.endOffset()
                || frozen.commitVersion() != current.commitVersion()
                || !frozen.lastCommitId().equals(current.lastCommitId())
                || !frozen.headSha256().equals(current.headSha256())
                || current.appendInFlight()
                || current.stateMapEndOffset() != current.endOffset()) {
            throw fenced("Kafka stream head or authority changed during recovery");
        }
    }

    private Duration remaining(long deadline) {
        long millis = deadline - clock.millis();
        if (millis <= 0) throw timeout();
        return Duration.ofMillis(millis);
    }

    private void failState(Throwable failure) {
        ErrorCode code = failure instanceof NereusException nereus
                ? nereus.code() : ErrorCode.METADATA_INVARIANT_VIOLATION;
        KafkaPartitionState target = switch (code) {
            case OBJECT_CHECKSUM_MISMATCH,
                    UNSUPPORTED_FORMAT,
                    METADATA_INVARIANT_VIOLATION -> KafkaPartitionState.CORRUPT_OFFLINE;
            default -> KafkaPartitionState.WRITE_FENCED_RECOVERY_REQUIRED;
        };
        state.updateAndGet(current -> current == KafkaPartitionState.LEADER_WRITABLE ? current : target);
    }

    private void transition(KafkaPartitionState expected, KafkaPartitionState target) {
        if (!state.compareAndSet(expected, target)) {
            throw invariant("illegal Kafka recovery state transition: " + state.get() + " -> " + target);
        }
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) current = current.getCause();
        return current;
    }

    private static NereusException timeout() {
        return new NereusException(ErrorCode.TIMEOUT, true, "Kafka partition recovery deadline expired");
    }

    private static NereusException fenced(String message) {
        return new NereusException(ErrorCode.FENCED_APPEND, false, message);
    }

    private static NereusException invariant(String message) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }
}
