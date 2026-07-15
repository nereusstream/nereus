/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.checkpoint;

import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;

/** Bounded canonical publication-table page addressed by the next table index. */
public record RecoveryCheckpointPublicationPage(
        List<RecoveryCheckpointPublication> values,
        OptionalInt continuation) {
    public RecoveryCheckpointPublicationPage {
        values = List.copyOf(Objects.requireNonNull(values, "values"));
        continuation = Objects.requireNonNull(continuation, "continuation");
        if (values.isEmpty()) {
            throw new IllegalArgumentException("recovery checkpoint publication page cannot be empty");
        }
        if (continuation.isPresent() && continuation.getAsInt() <= 0) {
            throw new IllegalArgumentException("publication continuation must be positive");
        }
    }
}
