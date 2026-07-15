/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public record GenerationScanPage(
        List<VersionedGenerationCandidate> values,
        Optional<F4ScanToken> continuation) {
    public GenerationScanPage {
        values = F4ValueValidation.orderedPage(values, VersionedGenerationCandidate::key, 4_096, "values");
        continuation = Objects.requireNonNull(continuation, "continuation");
        if (values.isEmpty() && continuation.isPresent()) {
            throw new IllegalArgumentException("empty generation page cannot carry a continuation");
        }
    }
}
