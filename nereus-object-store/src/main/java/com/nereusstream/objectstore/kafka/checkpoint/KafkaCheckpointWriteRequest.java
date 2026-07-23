/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.kafka.checkpoint;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

/** Immutable input to private-staging NKC1 publication. */
public record KafkaCheckpointWriteRequest(
        String nereusCluster,
        KafkaCheckpointHeader header,
        List<KafkaCheckpointSection> sections,
        Checksum contentPolicySha256,
        Duration timeout) {
    public KafkaCheckpointWriteRequest {
        Objects.requireNonNull(nereusCluster, "nereusCluster");
        if (nereusCluster.isBlank()
                || nereusCluster.getBytes(StandardCharsets.UTF_8).length
                > KafkaCheckpointFormatV1.MAX_STRING_BYTES) {
            throw new IllegalArgumentException("nereusCluster must be nonblank and bounded");
        }
        Objects.requireNonNull(header, "header");
        sections = List.copyOf(Objects.requireNonNull(sections, "sections"));
        Objects.requireNonNull(contentPolicySha256, "contentPolicySha256");
        if (contentPolicySha256.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException("contentPolicySha256 must use SHA256");
        }
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
    }
}
