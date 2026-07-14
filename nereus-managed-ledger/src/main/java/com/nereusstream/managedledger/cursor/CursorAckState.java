/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

/** Full normalized durable acknowledgement truth for one cursor generation. */
public record CursorAckState(
        long markDeleteOffset,
        List<OffsetRange> wholeAckRanges,
        NavigableMap<Long, BatchAckState> partialBatchAcks) {
    public CursorAckState {
        AckNormalizer.Normalized normalized = AckNormalizer.normalize(
                markDeleteOffset,
                wholeAckRanges,
                partialBatchAcks);
        markDeleteOffset = normalized.markDeleteOffset();
        wholeAckRanges = normalized.wholeAckRanges();
        partialBatchAcks = normalized.partialBatchAcks();
    }

    @Override
    public NavigableMap<Long, BatchAckState> partialBatchAcks() {
        return Collections.unmodifiableNavigableMap(new TreeMap<>(partialBatchAcks));
    }

    public static CursorAckState empty(long markDeleteOffset) {
        return new CursorAckState(markDeleteOffset, List.of(), new TreeMap<>());
    }

    public boolean isWholeEntryAcknowledged(long offset) {
        if (offset < markDeleteOffset) {
            return true;
        }
        for (OffsetRange range : wholeAckRanges) {
            if (range.contains(offset)) {
                return true;
            }
            if (range.startOffset() > offset) {
                return false;
            }
        }
        return false;
    }

    public long firstUnackedOffset() {
        return markDeleteOffset;
    }
}
