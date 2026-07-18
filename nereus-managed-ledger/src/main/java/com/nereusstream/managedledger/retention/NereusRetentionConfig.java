/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.retention;

import java.time.Duration;
import java.util.Objects;

/** Bounded product-owned logical-retention planning lane configuration. */
public record NereusRetentionConfig(
        int statsScanPageSize,
        int maxConcurrentPlans,
        int maxQueuedPlans,
        Duration operationTimeout,
        Duration closeTimeout) {
    public static final int MAX_STATS_SCAN_PAGE_SIZE = 512;

    public NereusRetentionConfig {
        if (statsScanPageSize <= 0
                || statsScanPageSize > MAX_STATS_SCAN_PAGE_SIZE) {
            throw new IllegalArgumentException(
                    "statsScanPageSize must be in [1, 512]");
        }
        if (maxConcurrentPlans <= 0 || maxQueuedPlans <= 0) {
            throw new IllegalArgumentException(
                    "retention concurrency and queue limits must be positive");
        }
        operationTimeout = requirePositiveMillis(
                operationTimeout, "operationTimeout");
        closeTimeout = requirePositiveMillis(closeTimeout, "closeTimeout");
    }

    public static NereusRetentionConfig defaults() {
        return new NereusRetentionConfig(
                128,
                8,
                1_024,
                Duration.ofSeconds(30),
                Duration.ofSeconds(30));
    }

    private static Duration requirePositiveMillis(
            Duration value,
            String field) {
        Objects.requireNonNull(value, field);
        final long millis;
        try {
            millis = value.toMillis();
        } catch (ArithmeticException failure) {
            throw new IllegalArgumentException(
                    field + " must be millisecond-representable",
                    failure);
        }
        if (value.isNegative() || value.isZero() || millis <= 0) {
            throw new IllegalArgumentException(
                    field + " must be a positive whole-millisecond duration");
        }
        return value;
    }
}
