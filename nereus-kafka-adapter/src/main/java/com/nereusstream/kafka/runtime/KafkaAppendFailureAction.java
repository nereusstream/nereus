/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.runtime;

/** Required partition-state action after a failed Kafka append path. */
public enum KafkaAppendFailureAction {
    REJECT_WITHOUT_FENCE,
    WRITE_FENCE_RECOVERY_REQUIRED,
    CORRUPT_OFFLINE
}
