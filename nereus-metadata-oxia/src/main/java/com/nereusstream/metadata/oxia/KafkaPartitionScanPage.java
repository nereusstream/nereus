/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public record KafkaPartitionScanPage(
        List<VersionedKafkaPartitionRegistry> values,
        Optional<String> continuation) {
    public KafkaPartitionScanPage {
        values = List.copyOf(Objects.requireNonNull(values, "values"));
        continuation = Objects.requireNonNull(continuation, "continuation");
        String previous = null;
        for (VersionedKafkaPartitionRegistry value : values) {
            if (previous != null && previous.compareTo(value.key()) >= 0) {
                throw new IllegalArgumentException("registry page keys must strictly increase");
            }
            previous = value.key();
        }
        if (values.isEmpty() && continuation.isPresent()) {
            throw new IllegalArgumentException("empty registry page cannot carry continuation");
        }
    }
}
