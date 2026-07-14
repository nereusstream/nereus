/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

/** Canonical half-open whole-entry acknowledgement range. */
public record CursorAckRangeRecord(long startOffset, long endOffset) {
    public CursorAckRangeRecord {
        if (startOffset < 0 || endOffset <= startOffset) {
            throw new IllegalArgumentException("cursor ack range must be a non-negative non-empty interval");
        }
    }
}
