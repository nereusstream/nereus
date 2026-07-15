/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore;

import java.time.Duration;
import java.util.Objects;

/** Bounded retry/backoff policy for guarded provider PUT transmissions. */
public record ObjectPutRetryPolicy(
        int maxAttempts,
        Duration minBackoff,
        Duration maxBackoff) {
    public ObjectPutRetryPolicy {
        if (maxAttempts < 1 || maxAttempts > 10) {
            throw new IllegalArgumentException("maxAttempts must be in [1, 10]");
        }
        minBackoff = requirePositiveMillis(minBackoff, "minBackoff");
        maxBackoff = requirePositiveMillis(maxBackoff, "maxBackoff");
        if (minBackoff.compareTo(maxBackoff) > 0) {
            throw new IllegalArgumentException("minBackoff cannot exceed maxBackoff");
        }
    }

    public static ObjectPutRetryPolicy defaults() {
        return new ObjectPutRetryPolicy(3, Duration.ofMillis(25), Duration.ofSeconds(1));
    }

    public long maximumBackoffMillis(int providerAttemptNumber) {
        if (providerAttemptNumber <= 1 || providerAttemptNumber > maxAttempts) {
            throw new IllegalArgumentException("backoff exists only before attempts 2..maxAttempts");
        }
        int shifts = providerAttemptNumber - 2;
        long base = minBackoff.toMillis();
        long exponential = shifts >= Long.SIZE - 1 || base > (Long.MAX_VALUE >> shifts)
                ? Long.MAX_VALUE : base << shifts;
        return Math.min(maxBackoff.toMillis(), exponential);
    }

    private static Duration requirePositiveMillis(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isNegative() || value.isZero() || value.toMillis() <= 0) {
            throw new IllegalArgumentException(name + " must be positive and millisecond-representable");
        }
        return value;
    }
}
