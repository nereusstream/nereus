/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore;

import java.time.Duration;
import java.util.Objects;

public record ListObjectsOptions(int maxKeys, Duration timeout) {
    public ListObjectsOptions {
        if (maxKeys < 1 || maxKeys > 1_000) {
            throw new IllegalArgumentException("maxKeys must be in [1, 1000]");
        }
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isZero() || timeout.isNegative() || timeout.toMillis() <= 0) {
            throw new IllegalArgumentException("timeout must be positive and millisecond-representable");
        }
    }
}
