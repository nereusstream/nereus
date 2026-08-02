/* Licensed under the Apache License, Version 2.0 */

package com.nereusstream.kafka.checkpoint;

import com.nereusstream.metadata.oxia.KafkaPartitionId;
import com.nereusstream.metadata.oxia.records.KafkaCheckpointFailureSource;
import com.nereusstream.metadata.oxia.records.KafkaCheckpointReferenceRecord;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/**
 * Durable decision boundary for an exact unusable NKC1 reference.
 *
 * <p>Fallback must wait for {@link #quarantine} to complete. A failed quarantine read or write
 * therefore fails recovery/retention closed instead of silently using an older checkpoint.
 */
public interface KafkaCheckpointFailureQuarantine {
    CompletableFuture<Boolean> isQuarantined(
            KafkaPartitionId identity, long partitionIncarnation, KafkaCheckpointReferenceRecord reference);

    CompletableFuture<Void> quarantine(
            KafkaPartitionId identity,
            long partitionIncarnation,
            KafkaCheckpointReferenceRecord reference,
            KafkaCheckpointFailureSource source,
            Throwable failure);

    /**
     * In-memory observer adapter for deterministic tests only.
     */
    static KafkaCheckpointFailureQuarantine transientObserver(
            BiConsumer<KafkaCheckpointReferenceRecord, Throwable> observer) {
        BiConsumer<KafkaCheckpointReferenceRecord, Throwable> exact = Objects.requireNonNull(observer, "observer");
        return new KafkaCheckpointFailureQuarantine() {
            @Override
            public CompletableFuture<Boolean> isQuarantined(
                    KafkaPartitionId identity, long partitionIncarnation, KafkaCheckpointReferenceRecord reference) {
                return CompletableFuture.completedFuture(false);
            }

            @Override
            public CompletableFuture<Void> quarantine(
                    KafkaPartitionId identity,
                    long partitionIncarnation,
                    KafkaCheckpointReferenceRecord reference,
                    KafkaCheckpointFailureSource source,
                    Throwable failure) {
                exact.accept(reference, failure);
                return CompletableFuture.completedFuture(null);
            }
        };
    }
}
