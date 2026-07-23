/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.recovery;

import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamStorage;
import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/** Product-owned checkpoint and COMMITTED replay composition around fresh Kafka-fork derived state. */
public final class DefaultKafkaPartitionRecoveryLauncher implements KafkaPartitionRecoveryLauncher {
    private final KafkaCheckpointRecoveryCoordinator checkpoints;
    private final StreamStorage streams;
    private final KafkaRecoveryStateFactory stateFactory;
    private final int chunkRecords;
    private final int chunkBytes;
    private final Executor recoveryExecutor;
    private final Clock clock;

    public DefaultKafkaPartitionRecoveryLauncher(
            KafkaCheckpointRecoveryCoordinator checkpoints,
            StreamStorage streams,
            KafkaRecoveryStateFactory stateFactory,
            int chunkRecords,
            int chunkBytes,
            Executor recoveryExecutor,
            Clock clock) {
        this.checkpoints = Objects.requireNonNull(checkpoints, "checkpoints");
        this.streams = Objects.requireNonNull(streams, "streams");
        this.stateFactory = Objects.requireNonNull(stateFactory, "stateFactory");
        if (chunkRecords <= 0 || chunkBytes <= 0) {
            throw new IllegalArgumentException("Kafka recovery chunk limits must be positive");
        }
        this.chunkRecords = chunkRecords;
        this.chunkBytes = chunkBytes;
        this.recoveryExecutor = Objects.requireNonNull(recoveryExecutor, "recoveryExecutor");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public CompletableFuture<? extends KafkaRecoveredPartition<?>> recover(
            KafkaPartitionRecoveryRequest request) {
        Objects.requireNonNull(request, "request");
        KafkaRecoveryState<?> state;
        try {
            state = Objects.requireNonNull(
                    stateFactory.create(request), "Kafka recovery state factory returned null");
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return launch(request, state);
    }

    private <S> CompletableFuture<KafkaRecoveredPartition<S>> launch(
            KafkaPartitionRecoveryRequest request,
            KafkaRecoveryState<S> state) {
        StreamId streamId = new StreamId(
                request.checkpointRequest().binding().value().streamId());
        KafkaPartitionRecoveryCoordinator<S> coordinator =
                new KafkaPartitionRecoveryCoordinator<>(
                        checkpoints,
                        new DefaultKafkaRecoveryBatchSource(
                                streams, streamId, chunkRecords, chunkBytes),
                        state.codec(),
                        state.publisher(),
                        recoveryExecutor,
                        clock);
        return coordinator.recover(request);
    }
}
