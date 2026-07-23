/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.fetch;

/** Observable state of one protocol-neutral multi-partition Fetch operation. */
public enum KafkaFetchOperationState {
    NEW,
    READING,
    WAITING,
    TIMED_READING,
    COMPLETE,
    CANCELLED
}
