/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.partition;

public enum KafkaPartitionState {
    NEW,
    BINDING,
    ACQUIRING_AUTHORITY,
    LOADING_HEAD,
    LOADING_CHECKPOINT,
    REPLAYING,
    VALIDATING,
    LEADER_WRITABLE,
    WRITE_FENCED_RECOVERY_REQUIRED,
    RESIGNING,
    CORRUPT_OFFLINE,
    CLOSED
}
