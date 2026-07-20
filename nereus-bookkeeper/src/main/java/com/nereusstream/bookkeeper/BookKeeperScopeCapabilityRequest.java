/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import java.time.Duration;
import java.util.Objects;
import java.util.regex.Pattern;

/** One explicit, bounded provider-scope canary run. */
public record BookKeeperScopeCapabilityRequest(
        String runId,
        BookKeeperBrokerReadiness readiness,
        Duration timeout) {
    private static final Pattern RUN_ID = Pattern.compile("[A-Za-z0-9_-]{8,128}");

    public BookKeeperScopeCapabilityRequest {
        runId = Objects.requireNonNull(runId, "runId");
        if (!RUN_ID.matcher(runId).matches()) {
            throw new IllegalArgumentException("runId must be 8-128 URL-safe characters");
        }
        Objects.requireNonNull(readiness, "readiness");
        timeout = Objects.requireNonNull(timeout, "timeout");
        if (timeout.isZero() || timeout.isNegative() || timeout.toMillis() <= 0) {
            throw new IllegalArgumentException("timeout must be positive and millisecond-representable");
        }
    }
}
