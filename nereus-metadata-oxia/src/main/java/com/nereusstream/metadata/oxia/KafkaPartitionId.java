/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;

/** Protocol-neutral identity of one KRaft topic incarnation and partition. */
public record KafkaPartitionId(String kafkaClusterId, String topicId, int partitionId) {
    private static final String ZERO_UUID = "AAAAAAAAAAAAAAAAAAAAAA";

    public KafkaPartitionId {
        kafkaClusterId = text(kafkaClusterId, "kafkaClusterId");
        topicId = canonicalKafkaUuid(topicId);
        if (partitionId < 0) {
            throw new IllegalArgumentException("partitionId must be non-negative");
        }
    }

    public String canonicalIdentity() {
        return kafkaClusterId + "/" + topicId + "/" + partitionId;
    }

    public static String canonicalKafkaUuid(String value) {
        String exact = text(value, "topicId");
        if (exact.length() != 22 || exact.equals(ZERO_UUID)) {
            throw new IllegalArgumentException("topicId must be a non-zero canonical Kafka UUID");
        }
        try {
            byte[] decoded = Base64.getUrlDecoder().decode(exact + "==");
            if (decoded.length != 16
                    || !Base64.getUrlEncoder().withoutPadding().encodeToString(decoded).equals(exact)) {
                throw new IllegalArgumentException("topicId must be a canonical Kafka UUID");
            }
        } catch (IllegalArgumentException failure) {
            throw new IllegalArgumentException("topicId must be a canonical Kafka UUID", failure);
        }
        return exact;
    }

    private static String text(String value, String name) {
        Objects.requireNonNull(value, name);
        int bytes = value.getBytes(StandardCharsets.UTF_8).length;
        if (value.isBlank() || bytes > 65_535) {
            throw new IllegalArgumentException(name + " must be nonblank and at most 65535 UTF-8 bytes");
        }
        return value;
    }
}
