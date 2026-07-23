/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.checkpoint;

import com.nereusstream.kafka.partition.KafkaPartitionIdentity;
import com.nereusstream.metadata.oxia.VersionedKafkaPartitionBinding;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointWriteRequest;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;

/** Exact captured inputs for one restart-safe checkpoint publication attempt. */
public record KafkaCheckpointPublicationRequest(
        KafkaPartitionIdentity identity,
        VersionedKafkaPartitionBinding capturedBinding,
        KafkaCheckpointSourceState capturedSource,
        KafkaCheckpointWriteRequest objectRequest,
        KafkaCheckpointSourceValidator sourceValidator,
        Duration pendingProtectionTtl,
        String writerBuild) {
    public KafkaCheckpointPublicationRequest {
        Objects.requireNonNull(identity, "identity");
        Objects.requireNonNull(capturedBinding, "capturedBinding");
        Objects.requireNonNull(capturedSource, "capturedSource");
        Objects.requireNonNull(objectRequest, "objectRequest");
        Objects.requireNonNull(sourceValidator, "sourceValidator");
        Objects.requireNonNull(pendingProtectionTtl, "pendingProtectionTtl");
        Objects.requireNonNull(writerBuild, "writerBuild");
        if (pendingProtectionTtl.isZero() || pendingProtectionTtl.isNegative()
                || pendingProtectionTtl.toMillis() <= 0
                || writerBuild.isBlank()
                || writerBuild.getBytes(StandardCharsets.UTF_8).length > 64 * 1024) {
            throw new IllegalArgumentException("invalid Kafka checkpoint publication options");
        }
    }
}
