/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.partition;

/** Stable partition facts that may change the outcome of a pending Kafka Fetch. */
public enum KafkaPartitionEventType {
    STABLE_APPEND,
    LOG_START_CHANGED,
    LEADERSHIP_LOST,
    CORRUPT_OFFLINE
}
