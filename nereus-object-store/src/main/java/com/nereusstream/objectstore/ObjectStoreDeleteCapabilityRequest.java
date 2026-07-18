/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore;

import java.time.Duration;
import java.util.Objects;

/** One bounded, isolated object-store list/head/exact-delete capability probe. */
public record ObjectStoreDeleteCapabilityRequest(String runId, Duration timeout) {
    public ObjectStoreDeleteCapabilityRequest {
        runId = requireBase32Id(runId);
        timeout = Objects.requireNonNull(timeout, "timeout");
        if (timeout.isZero() || timeout.isNegative() || timeout.toMillis() <= 0) {
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
