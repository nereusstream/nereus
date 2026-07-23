/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.recovery;

import java.util.concurrent.CompletableFuture;

/** Creates/executes one fresh per-open recovery coordinator with Kafka-fork state dependencies. */
@FunctionalInterface
public interface KafkaPartitionRecoveryLauncher {
    CompletableFuture<? extends KafkaRecoveredPartition<?>> recover(
            KafkaPartitionRecoveryRequest request);
}
