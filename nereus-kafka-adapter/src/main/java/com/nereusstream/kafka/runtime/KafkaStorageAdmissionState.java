/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.runtime;

/** Process-local admission state for the native Kafka storage runtime. */
public enum KafkaStorageAdmissionState {
    STARTING,
    READY,
    NOT_READY,
    DRAINING,
    CLOSED
}
