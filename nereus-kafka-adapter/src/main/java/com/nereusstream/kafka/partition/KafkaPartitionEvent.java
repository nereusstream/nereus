/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.partition;

import java.util.Objects;

/** Immutable listener event carrying only stable, already-published partition state. */
public record KafkaPartitionEvent(
        KafkaPartitionIdentity identity,
        KafkaPartitionEventType type,
        KafkaStableSnapshot stableSnapshot) {
    public KafkaPartitionEvent {
        Objects.requireNonNull(identity, "identity");
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(stableSnapshot, "stableSnapshot");
    }
}
