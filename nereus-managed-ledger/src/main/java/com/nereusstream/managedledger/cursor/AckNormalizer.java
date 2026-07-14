/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

/** Canonical whole-range and partial-batch normalization shared by codecs and mutations. */
public final class AckNormalizer {
    private AckNormalizer() {
    }

    public static Normalized normalize(
            long markDeleteOffset,
            List<OffsetRange> ranges,
            Map<Long, BatchAckState> partials) {
        if (markDeleteOffset < 0) {
            throw new IllegalArgumentException("markDeleteOffset must be non-negative");
        }
        List<OffsetRange> normalizedRanges = mergeRanges(markDeleteOffset, ranges);
        TreeMap<Long, BatchAckState> normalizedPartials = new TreeMap<>();
        List<OffsetRange> promoted = new ArrayList<>(normalizedRanges);
        for (Map.Entry<Long, BatchAckState> entry : Objects.requireNonNull(partials, "partials").entrySet()) {
            Long offset = Objects.requireNonNull(entry.getKey(), "partials contains null offset");
            BatchAckState state = Objects.requireNonNull(entry.getValue(), "partials contains null state");
            if (offset < markDeleteOffset) {
                throw new IllegalArgumentException("partial ack offset is below markDeleteOffset");
            }
            if (state.isWholeEntryAcknowledged()) {
                promoted.add(new OffsetRange(offset, Math.addExact(offset, 1)));
            } else if (!state.isAllRemaining()) {
                if (normalizedPartials.put(offset, state) != null) {
                    throw new IllegalArgumentException("duplicate partial ack offset");
                }
            }
        }
        normalizedRanges = mergeRanges(markDeleteOffset, promoted);
        long foldedMarkDelete = markDeleteOffset;
        int firstRemainingRange = 0;
        while (firstRemainingRange < normalizedRanges.size()
                && normalizedRanges.get(firstRemainingRange).startOffset() == foldedMarkDelete) {
            foldedMarkDelete = normalizedRanges.get(firstRemainingRange).endOffset();
            firstRemainingRange++;
        }
        List<OffsetRange> finalRanges = List.copyOf(
                normalizedRanges.subList(firstRemainingRange, normalizedRanges.size()));
        normalizedPartials.headMap(foldedMarkDelete, false).clear();
        for (OffsetRange range : finalRanges) {
            normalizedPartials.subMap(range.startOffset(), true, range.endOffset(), false).clear();
        }
        return new Normalized(
                foldedMarkDelete,
                finalRanges,
                Collections.unmodifiableNavigableMap(new TreeMap<>(normalizedPartials)));
    }

    private static List<OffsetRange> mergeRanges(long markDeleteOffset, List<OffsetRange> ranges) {
        List<OffsetRange> sorted = new ArrayList<>(Objects.requireNonNull(ranges, "ranges"));
        sorted.forEach(range -> Objects.requireNonNull(range, "ranges contains null"));
        sorted.sort(java.util.Comparator.comparingLong(OffsetRange::startOffset)
                .thenComparingLong(OffsetRange::endOffset));
        List<OffsetRange> merged = new ArrayList<>();
        for (OffsetRange range : sorted) {
            if (range.startOffset() < markDeleteOffset) {
                throw new IllegalArgumentException("whole ack range starts below markDeleteOffset");
            }
            if (merged.isEmpty()) {
                merged.add(range);
                continue;
            }
            OffsetRange previous = merged.get(merged.size() - 1);
            if (range.startOffset() <= previous.endOffset()) {
                merged.set(merged.size() - 1, new OffsetRange(
                        previous.startOffset(), Math.max(previous.endOffset(), range.endOffset())));
            } else {
                merged.add(range);
            }
        }
        return merged;
    }

    public record Normalized(
            long markDeleteOffset,
            List<OffsetRange> wholeAckRanges,
            NavigableMap<Long, BatchAckState> partialBatchAcks) {
        public Normalized {
            wholeAckRanges = List.copyOf(wholeAckRanges);
            partialBatchAcks = Collections.unmodifiableNavigableMap(new TreeMap<>(partialBatchAcks));
        }
    }
}
