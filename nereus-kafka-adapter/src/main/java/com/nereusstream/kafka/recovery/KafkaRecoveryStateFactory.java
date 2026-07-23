/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.recovery;

/** Kafka-fork seam that creates fresh derived state and an exact partition publisher for one leader open. */
@FunctionalInterface
public interface KafkaRecoveryStateFactory {
    KafkaRecoveryState<?> create(KafkaPartitionRecoveryRequest request);
}
