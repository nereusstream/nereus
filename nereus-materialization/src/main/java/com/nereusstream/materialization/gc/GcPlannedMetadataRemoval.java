/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.api.Checksum;
import com.nereusstream.core.physical.GcReferenceQuery;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** Exact generic metadata key/version/envelope fact committed into a typed retirement action. */
public record GcPlannedMetadataRemoval(
        String removalType,
        String key,
        long metadataVersion,
        Checksum durableValueSha256) {
    public GcPlannedMetadataRemoval {
        removalType = requireCanonicalType(removalType);
        key = requireKey(key);
        if (metadataVersion < 0) {
            throw new IllegalArgumentException("metadataVersion must be non-negative");
        }
        durableValueSha256 = GcReferenceQuery.requireSha256(
                durableValueSha256, "durableValueSha256");
    }

    private static String requireCanonicalType(String value) {
        Objects.requireNonNull(value, "removalType");
        if (value.length() > 128 || !value.matches("[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?")) {
            throw new IllegalArgumentException("removalType is not canonical");
        }
        return value;
    }

    private static String requireKey(String value) {
        Objects.requireNonNull(value, "key");
        if (value.isBlank() || value.getBytes(StandardCharsets.UTF_8).length > 4_096) {
            throw new IllegalArgumentException("metadata removal key must be non-blank and bounded");
        }
        return value;
    }
}
