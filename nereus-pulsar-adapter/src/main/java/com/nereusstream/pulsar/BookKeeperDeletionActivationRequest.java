/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import java.time.Duration;
import java.util.Objects;
import java.util.regex.Pattern;

/** Operator request that names the observed activation version but never supplies its own proof digests. */
public record BookKeeperDeletionActivationRequest(
        String runId,
        long expectedActivationMetadataVersion,
        Duration timeout) {
    private static final Pattern RUN_ID = Pattern.compile("[A-Za-z0-9_-]{8,128}");

    public BookKeeperDeletionActivationRequest {
        runId = Objects.requireNonNull(runId, "runId");
        if (!RUN_ID.matcher(runId).matches()) {
            throw new IllegalArgumentException("runId must be 8-128 URL-safe characters");
        }
        if (expectedActivationMetadataVersion < 0) {
            throw new IllegalArgumentException("expected activation metadata version must be non-negative");
        }
        timeout = Objects.requireNonNull(timeout, "timeout");
        if (timeout.isZero() || timeout.isNegative() || timeout.toMillis() <= 0) {
            throw new IllegalArgumentException("timeout must be positive and millisecond-representable");
        }
    }
}
