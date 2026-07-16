/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public record GcRetirementProtectionScanPage(
        List<VersionedGcRetirementProtection> values,
        Optional<F4ScanToken> continuation) {
    public GcRetirementProtectionScanPage {
        values = F4ValueValidation.orderedPage(
                values, VersionedGcRetirementProtection::key, 1_000, "values");
        continuation = Objects.requireNonNull(continuation, "continuation");
        if (values.isEmpty() && continuation.isPresent()) {
            throw new IllegalArgumentException(
                    "empty GC retirement-protection page cannot carry a continuation");
        }
    }
}
