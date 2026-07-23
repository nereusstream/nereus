/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.partition;

import com.nereusstream.kafka.codec.KafkaFetchAssembly;
import java.util.Objects;

/** One Fetch assembly and the stable partition snapshot that bounded its read. */
public record KafkaStorageReadResult(
        KafkaFetchAssembly fetchAssembly,
        KafkaStableSnapshot stableSnapshot) {
    public KafkaStorageReadResult {
        Objects.requireNonNull(fetchAssembly, "fetchAssembly");
        Objects.requireNonNull(stableSnapshot, "stableSnapshot");
        if (fetchAssembly.nextLogicalOffset() > stableSnapshot.stableEndOffset()) {
            throw new IllegalArgumentException("Kafka Fetch advanced beyond its stable snapshot");
        }
    }
}
