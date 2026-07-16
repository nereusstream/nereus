/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.backpressure;

import java.time.Duration;
import java.util.Objects;

/** Async-append lag limits. A zero threshold disables only that individual threshold. */
public record MaterializationLagThresholds(
        long throttleRecords,
        long rejectRecords,
        long throttleBytes,
        long rejectBytes,
        Duration rejectAge,
        Duration throttleDelay) {
    public MaterializationLagThresholds {
        if (throttleRecords < 0
                || rejectRecords < 0
                || throttleBytes < 0
                || rejectBytes < 0) {
            throw new IllegalArgumentException(
                    "materialization lag thresholds must be non-negative");
        }
        if (throttleRecords > 0
                && rejectRecords > 0
                && throttleRecords >= rejectRecords) {
            throw new IllegalArgumentException(
                    "record throttle threshold must be below the reject threshold");
        }
        if (throttleBytes > 0
                && rejectBytes > 0
                && throttleBytes >= rejectBytes) {
            throw new IllegalArgumentException(
                    "byte throttle threshold must be below the reject threshold");
        }
        rejectAge = requireNonNegative(rejectAge, "rejectAge");
        throttleDelay = requirePositive(throttleDelay, "throttleDelay");
    }

    public boolean allDisabled() {
        return throttleRecords == 0
                && rejectRecords == 0
                && throttleBytes == 0
                && rejectBytes == 0
                && rejectAge.isZero();
    }

    private static Duration requireNonNegative(
            Duration value,
            String field) {
        Objects.requireNonNull(value, field);
        if (value.isNegative()) {
            throw new IllegalArgumentException(field + " must be non-negative");
        }
        try {
            value.toNanos();
        } catch (ArithmeticException failure) {
            throw new IllegalArgumentException(
                    field + " is not nanosecond-representable",
                    failure);
        }
        return value;
    }

    private static Duration requirePositive(
            Duration value,
            String field) {
        Duration exact = requireNonNegative(value, field);
        if (exact.isZero()) {
            throw new IllegalArgumentException(field + " must be positive");
        }
        return exact;
    }
}
