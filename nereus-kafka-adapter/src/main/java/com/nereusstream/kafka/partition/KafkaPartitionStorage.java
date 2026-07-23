/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.partition;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/** Nereus-side partition log boundary consumed by the Kafka fork. */
public interface KafkaPartitionStorage extends AutoCloseable {
    KafkaPartitionIdentity identity();

    int leaderEpoch();

    KafkaPartitionState state();

    KafkaStableSnapshot stableSnapshot();

    CompletableFuture<KafkaStableAppendResult> append(
            ByteBuffer validatedRecords, KafkaAppendContext context);

    CompletableFuture<KafkaStorageReadResult> read(KafkaStorageReadRequest request);

    KafkaPartitionEventSubscription subscribe(KafkaPartitionEventListener listener);

    CompletableFuture<Void> resign();

    @Override
    void close();
}
