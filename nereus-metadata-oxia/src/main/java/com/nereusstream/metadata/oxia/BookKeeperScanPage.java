/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public record BookKeeperScanPage<T>(List<T> values, Optional<BookKeeperScanToken> continuation) {
    public BookKeeperScanPage {
        Objects.requireNonNull(values, "values");
        if (values.size() > 1_024 || values.stream().anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException("scan page exceeds its bound or contains null");
        }
        values = List.copyOf(values);
        continuation = Objects.requireNonNull(continuation, "continuation");
        if (values.isEmpty() && continuation.isPresent()) {
            throw new IllegalArgumentException("empty scan page cannot carry a continuation");
        }
    }
}
