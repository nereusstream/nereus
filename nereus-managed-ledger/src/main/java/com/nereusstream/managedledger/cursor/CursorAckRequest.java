/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** One whole-entry or partial-batch acknowledgement in stream-offset coordinates. */
public record CursorAckRequest(
        long entryOffset,
        Optional<BatchAckState> batchAck,
        Map<String, Long> positionProperties) {
    public CursorAckRequest {
        if (entryOffset < 0) {
            throw new IllegalArgumentException("entryOffset must be non-negative");
        }
        batchAck = Objects.requireNonNull(batchAck, "batchAck");
        Objects.requireNonNull(positionProperties, "positionProperties");
        LinkedHashMap<String, Long> copy = new LinkedHashMap<>();
        positionProperties.forEach((key, value) -> copy.put(
                Objects.requireNonNull(key, "positionProperties contains null key"),
                Objects.requireNonNull(value, "positionProperties contains null value")));
        positionProperties = Collections.unmodifiableMap(copy);
    }

    public boolean wholeEntry() {
        return batchAck.isEmpty() || batchAck.orElseThrow().isWholeEntryAcknowledged();
    }
}
