/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.generation;

import java.time.Duration;
import java.util.Objects;

/** One bounded physical-root/cursor-root/capability activation attempt. */
public record ManagedLedgerPhysicalDeletionActivationRequest(
        String runId,
        int maxConcurrentStreams,
        Duration timeout) {
    public static final int MAX_CONCURRENT_STREAMS = 1_024;

    public ManagedLedgerPhysicalDeletionActivationRequest {
        runId = requireBase32Id(runId);
        if (maxConcurrentStreams <= 0
                || maxConcurrentStreams > MAX_CONCURRENT_STREAMS) {
            throw new IllegalArgumentException(
                    "maxConcurrentStreams must be in [1, 1024]");
        }
        timeout = Objects.requireNonNull(timeout, "timeout");
        if (timeout.isZero()
                || timeout.isNegative()
                || timeout.toMillis() <= 0) {
            throw new IllegalArgumentException(
                    "timeout must be positive and millisecond-representable");
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
