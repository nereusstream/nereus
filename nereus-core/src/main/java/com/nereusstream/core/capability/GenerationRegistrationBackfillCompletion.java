/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.capability;

import com.nereusstream.api.Checksum;
import com.nereusstream.core.physical.GcReferenceQuery;
import java.util.Objects;

/**
 * Broker traversal result offered to the product-owned durable activation
 * authority.
 */
public record GenerationRegistrationBackfillCompletion(
        String runId,
        GenerationCapabilityReadiness readiness,
        Checksum coverageSha256,
        long failureCount) {
    public GenerationRegistrationBackfillCompletion {
        runId = requireBase32Id(runId);
        Objects.requireNonNull(readiness, "readiness");
        coverageSha256 = GcReferenceQuery.requireSha256(
                coverageSha256, "coverageSha256");
        if (failureCount < 0) {
            throw new IllegalArgumentException(
                    "failureCount must be non-negative");
        }
    }

    private static String requireBase32Id(String value) {
        Objects.requireNonNull(value, "runId");
        if (value.length() < 26
                || value.length() > 128
                || !value.matches("[a-z2-7]+")) {
            throw new IllegalArgumentException(
                    "runId must be lowercase base32 and encode at least 128 bits");
        }
        return value;
    }
}
