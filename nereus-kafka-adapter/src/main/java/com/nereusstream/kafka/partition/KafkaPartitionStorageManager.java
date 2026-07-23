/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.partition;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Product-owned partition lifecycle boundary consumed by the Kafka fork. */
public interface KafkaPartitionStorageManager extends AutoCloseable {
    CompletableFuture<KafkaPartitionStorage> openLeader(KafkaPartitionLeaderOpenRequest request);

    CompletableFuture<Void> resign(
            KafkaPartitionIdentity identity, int observedLeaderEpoch, Duration timeout);

    CompletableFuture<Void> delete(
            KafkaPartitionIdentity identity, long metadataOffset, Duration timeout);

    Optional<KafkaPartitionStorage> current(KafkaPartitionIdentity identity);

    CompletableFuture<Void> shutdown();

    @Override
    void close();
}
