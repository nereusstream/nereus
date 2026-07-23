/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.partition;

/** Idempotent listener registration owned by one Fetch operation. */
@FunctionalInterface
public interface KafkaPartitionEventSubscription extends AutoCloseable {
    @Override
    void close();
}
