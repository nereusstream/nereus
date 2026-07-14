/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

/** Canonical half-open range of fully acknowledged persisted entry offsets. */
public record OffsetRange(long startOffset, long endOffset) {
    public OffsetRange {
        if (startOffset < 0 || endOffset <= startOffset) {
            throw new IllegalArgumentException("offset range must be a non-negative non-empty interval");
        }
    }

    public boolean contains(long offset) {
        return offset >= startOffset && offset < endOffset;
    }
}
