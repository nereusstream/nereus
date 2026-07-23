/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.runtime;

import com.nereusstream.kafka.partition.KafkaPartitionStorageManager;
import java.time.Duration;
import java.util.concurrent.CompletionStage;

/** Product-owned broker runtime boundary consumed by the Kafka fork. */
public interface NereusKafkaRuntime extends AutoCloseable {
    /** Starts connectivity, activation and local readiness work; completion means broker-local admission may open. */
    CompletionStage<Void> start();

    KafkaStorageAdmission admission();

    KafkaPartitionStorageManager partitionStorageManager();

    KafkaStorageHealth health();

    /** Stops new work and starts bounded drain without waiting for all work to finish. */
    CompletionStage<Void> beginDrain(DrainReason reason);

    /** Completes after accepted work and partition sessions are drained, or fails at the supplied timeout. */
    CompletionStage<Void> awaitDrained(Duration timeout);

    @Override
    void close();
}
