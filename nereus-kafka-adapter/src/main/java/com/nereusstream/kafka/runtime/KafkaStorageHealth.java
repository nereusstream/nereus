/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.runtime;

import java.util.Objects;

/** Immutable bounded-label health snapshot for broker readiness and metrics. */
public record KafkaStorageHealth(
        KafkaStorageAdmissionState state,
        boolean ready,
        String detail) {
    public KafkaStorageHealth {
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(detail, "detail");
        if (detail.isBlank()) {
            throw new IllegalArgumentException("Kafka storage health detail must be nonblank");
        }
        if (ready != (state == KafkaStorageAdmissionState.READY)) {
            throw new IllegalArgumentException("Kafka storage health readiness must match the READY state");
        }
    }
}
