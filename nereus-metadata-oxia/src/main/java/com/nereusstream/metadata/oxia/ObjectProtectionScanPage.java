/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public record ObjectProtectionScanPage(
        List<VersionedObjectProtection> values,
        Optional<F4ScanToken> continuation) {
    public ObjectProtectionScanPage {
        values = F4ValueValidation.orderedPage(values, VersionedObjectProtection::key, 1_000, "values");
        continuation = Objects.requireNonNull(continuation, "continuation");
        if (values.isEmpty() && continuation.isPresent()) {
            throw new IllegalArgumentException("empty protection page cannot carry a continuation");
        }
    }
}
