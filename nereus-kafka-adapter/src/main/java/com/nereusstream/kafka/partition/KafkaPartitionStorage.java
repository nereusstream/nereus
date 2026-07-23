/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.partition;

import com.nereusstream.api.StorageProfile;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/** Nereus-side partition log boundary consumed by the Kafka fork. */
public interface KafkaPartitionStorage extends AutoCloseable {
    KafkaPartitionIdentity identity();

    int leaderEpoch();

    StorageProfile storageProfile();

    KafkaPartitionState state();

    KafkaStableSnapshot stableSnapshot();

    /**
     * Publish Kafka-derived HW/LSO after the stock state machine has consumed the exact stable append.
     *
     * <p>The durable end may advance before these derived offsets. Implementations keep the previous visibility bounds
     * and do not dispatch the next same-partition append or publish its stable event until this method confirms the
     * matching stable end.
     */
    default KafkaStableSnapshot publishDerivedOffsets(
            long expectedStableEndOffset,
            long highWatermark,
            long lastStableOffset) {
        throw new UnsupportedOperationException(
                "Kafka partition storage does not support derived-offset publication");
    }

    CompletableFuture<KafkaStableAppendResult> append(
            ByteBuffer validatedRecords, KafkaAppendContext context);

    CompletableFuture<KafkaStorageReadResult> read(KafkaStorageReadRequest request);

    KafkaPartitionEventSubscription subscribe(KafkaPartitionEventListener listener);

    CompletableFuture<Void> resign();

    @Override
    void close();
}
