/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.partition;

/** Non-blocking listener; callback failures never reclassify partition I/O. */
@FunctionalInterface
public interface KafkaPartitionEventListener {
    void onPartitionEvent(KafkaPartitionEvent event);
}
