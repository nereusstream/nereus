/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.retention;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** Exact durable identity of one verified range-retention statistics value. */
public record RetentionStatsToken(
        String key,
        long metadataVersion,
        Checksum durableValueSha256) implements Comparable<RetentionStatsToken> {
    private static final int MAX_KEY_BYTES = 4_096;

    public RetentionStatsToken {
        Objects.requireNonNull(key, "key");
        if (key.isBlank()
                || key.getBytes(StandardCharsets.UTF_8).length > MAX_KEY_BYTES) {
            throw new IllegalArgumentException(
                    "retention stats key must be non-blank and at most 4096 UTF-8 bytes");
        }
        if (metadataVersion < 0) {
            throw new IllegalArgumentException(
                    "metadataVersion must be non-negative");
        }
        Objects.requireNonNull(durableValueSha256, "durableValueSha256");
        if (durableValueSha256.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException(
                    "durableValueSha256 must use SHA256");
        }
    }

    @Override
    public int compareTo(RetentionStatsToken other) {
        return key.compareTo(Objects.requireNonNull(other, "other").key);
    }
}
