/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Immutable page of cursor roots in stable key order. */
public record CursorScanPage(
        List<VersionedCursorState> records,
        Optional<CursorScanToken> continuation) {
    public CursorScanPage {
        records = List.copyOf(Objects.requireNonNull(records, "records"));
        continuation = Objects.requireNonNull(continuation, "continuation");
        if (records.isEmpty() && continuation.isPresent()) {
            throw new IllegalArgumentException("an empty cursor page cannot have a continuation");
        }
    }
}
