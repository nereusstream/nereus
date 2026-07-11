/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.api;

import java.time.Duration;
import java.util.Objects;

/** Caller wait budget for exact append recovery. */
public record AppendRecoveryOptions(Duration timeout) {
    public AppendRecoveryOptions {
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
    }
}
