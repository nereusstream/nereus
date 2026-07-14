/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Complete protocol-neutral input for opening or creating one durable cursor. */
public record CursorOpenRequest(
        InitialCursorPosition initialPosition,
        Map<String, Long> initialPositionProperties,
        Map<String, String> initialCursorProperties,
        long observedTrimOffset,
        long observedCommittedEndOffset) {
    public CursorOpenRequest {
        Objects.requireNonNull(initialPosition, "initialPosition");
        initialPositionProperties = immutableMap(
                initialPositionProperties, "initialPositionProperties");
        initialCursorProperties = immutableMap(
                initialCursorProperties, "initialCursorProperties");
        if (observedTrimOffset < 0
                || observedCommittedEndOffset < observedTrimOffset) {
            throw new IllegalArgumentException("observed cursor bounds are invalid");
        }
    }

    public long initialMarkDeleteOffset() {
        if (initialPosition instanceof InitialCursorPosition.Earliest) {
            return observedTrimOffset;
        }
        if (initialPosition instanceof InitialCursorPosition.Latest) {
            return observedCommittedEndOffset;
        }
        long requested = ((InitialCursorPosition.AtOffset) initialPosition).nextReadOffset();
        return Math.max(observedTrimOffset, Math.min(requested, observedCommittedEndOffset));
    }

    private static <K, V> Map<K, V> immutableMap(Map<K, V> values, String fieldName) {
        Objects.requireNonNull(values, fieldName);
        LinkedHashMap<K, V> copy = new LinkedHashMap<>();
        values.forEach((key, value) -> copy.put(
                Objects.requireNonNull(key, fieldName + " contains null key"),
                Objects.requireNonNull(value, fieldName + " contains null value")));
        return Collections.unmodifiableMap(copy);
    }
}
