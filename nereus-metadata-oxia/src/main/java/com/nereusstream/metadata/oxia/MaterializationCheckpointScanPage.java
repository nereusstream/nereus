/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** One bounded, key-ordered page of per-policy advisory materialization checkpoints. */
public record MaterializationCheckpointScanPage(
        List<VersionedMaterializationCheckpoint> values,
        Optional<F4ScanToken> continuation) {
    public MaterializationCheckpointScanPage {
        values = F4ValueValidation.orderedPage(
                values, VersionedMaterializationCheckpoint::key, 1_000, "values");
        continuation = Objects.requireNonNull(continuation, "continuation");
        if (values.isEmpty() && continuation.isPresent()) {
            throw new IllegalArgumentException(
                    "empty materialization-checkpoint page cannot carry a continuation");
        }
    }
}
