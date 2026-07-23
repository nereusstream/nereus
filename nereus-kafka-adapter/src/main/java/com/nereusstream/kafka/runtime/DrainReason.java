/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.runtime;

/** Stable reason for stopping new Kafka storage work before runtime shutdown. */
public enum DrainReason {
    BROKER_SHUTDOWN,
    STARTUP_FAILURE,
    BROKER_FENCED,
    OPERATOR_REQUEST
}
