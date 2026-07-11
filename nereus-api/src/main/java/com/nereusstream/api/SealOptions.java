/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.api;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;

/** Options for sealing an active stream. */
public record SealOptions(Duration timeout, String reason) {
    public SealOptions {
        validate(timeout, reason);
    }

    static void validate(Duration timeout, String reason) {
        Objects.requireNonNull(timeout, "timeout");
        Objects.requireNonNull(reason, "reason");
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
        if (reason.isBlank() || reason.getBytes(StandardCharsets.UTF_8).length
                > ApiLimits.MAX_STREAM_ATTRIBUTES_ENCODED_BYTES) {
            throw new IllegalArgumentException("reason must be nonblank and bounded");
        }
    }
}
