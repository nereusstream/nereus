/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import java.time.Duration;
import java.util.Objects;

/** One bounded cluster-wide physical-root/cursor-root coverage attempt. */
public record PhysicalRootBackfillRequest(
        String runId,
        long expectedBrokerReadinessEpoch,
        int maxConcurrentStreams,
        Duration timeout) {
    private static final int MAX_CONCURRENT_STREAMS = 1_024;

    public PhysicalRootBackfillRequest {
        runId = requireBase32Id(runId);
        if (expectedBrokerReadinessEpoch < 0) {
            throw new IllegalArgumentException(
                    "expectedBrokerReadinessEpoch must be non-negative");
        }
        if (maxConcurrentStreams <= 0
                || maxConcurrentStreams > MAX_CONCURRENT_STREAMS) {
            throw new IllegalArgumentException(
                    "maxConcurrentStreams must be in [1, 1024]");
        }
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
    }

    private static String requireBase32Id(String value) {
        Objects.requireNonNull(value, "runId");
        if (value.length() < 26 || value.length() > 128) {
            throw new IllegalArgumentException(
                    "runId must encode at least 128 bits and contain at most 128 characters");
        }
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            if (!((character >= 'a' && character <= 'z')
                    || (character >= '2' && character <= '7'))) {
                throw new IllegalArgumentException(
                        "runId must be lowercase base32 without padding");
            }
        }
        return value;
    }
}
