/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.metadata.oxia.records.TopicProjectionRecord;
import java.util.Objects;

/** Exact stored topic-projection authority with its Oxia version and envelope digest. */
public record VersionedTopicProjection(
        String key,
        TopicProjectionRecord value,
        long metadataVersion,
        Checksum durableValueSha256) {
    public VersionedTopicProjection {
        key = requireText(key, "key");
        Objects.requireNonNull(value, "value");
        if (value.metadataVersion() != 0 || metadataVersion < 0) {
            throw new IllegalArgumentException(
                    "topic projection wire version must be zero and Oxia version non-negative");
        }
        durableValueSha256 = requireSha256(durableValueSha256);
    }

    private static String requireText(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must be non-blank");
        }
        return value;
    }

    private static Checksum requireSha256(Checksum value) {
        Objects.requireNonNull(value, "durableValueSha256");
        if (value.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException("durableValueSha256 must use SHA256");
        }
        return value;
    }
}
