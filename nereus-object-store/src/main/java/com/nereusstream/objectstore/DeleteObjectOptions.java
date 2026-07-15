/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore;

import com.nereusstream.api.Checksum;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

public record DeleteObjectOptions(
        long expectedLength,
        Checksum expectedStorageChecksum,
        Optional<String> expectedEtag,
        Duration timeout) {
    public DeleteObjectOptions {
        if (expectedLength < 0) {
            throw new IllegalArgumentException("expectedLength must be non-negative");
        }
        Objects.requireNonNull(expectedStorageChecksum, "expectedStorageChecksum");
        expectedEtag = Objects.requireNonNull(expectedEtag, "expectedEtag").map(value -> {
            if (value.isBlank()) {
                throw new IllegalArgumentException("expectedEtag cannot be blank");
            }
            return value;
        });
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isZero() || timeout.isNegative() || timeout.toMillis() <= 0) {
            throw new IllegalArgumentException("timeout must be positive and millisecond-representable");
        }
    }
}
