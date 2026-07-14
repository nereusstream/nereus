/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

class AckNormalizerTest {
    @Test
    void mergesRangesPromotesEmptyPartialsDropsAllRemainingAndFoldsMarkDelete() {
        TreeMap<Long, BatchAckState> partials = new TreeMap<>();
        partials.put(4L, new BatchAckState(3, new long[0]));
        partials.put(8L, new BatchAckState(3, new long[] {0b111}));
        partials.put(9L, new BatchAckState(3, new long[] {0b010}));

        CursorAckState state = new CursorAckState(
                2,
                List.of(
                        new OffsetRange(5, 7),
                        new OffsetRange(2, 4),
                        new OffsetRange(7, 8)),
                partials);

        assertThat(state.markDeleteOffset()).isEqualTo(8);
        assertThat(state.wholeAckRanges()).isEmpty();
        assertThat(state.partialBatchAcks()).containsOnlyKeys(9L);
    }

    @Test
    void andUsesMissingWordsAsZeroAndDefensivelyCopiesArrays() {
        long[] source = {-1L, 1L};
        BatchAckState state = new BatchAckState(65, source);
        source[0] = 0;
        long[] exposed = state.remainingWords();
        exposed[0] = 0;
        assertThat(state.remainingWords()).containsExactly(-1L, 1L);

        BatchAckState merged = state.and(new BatchAckState(65, new long[] {-2L, 1L}));
        assertThat(merged.remainingWords()).containsExactly(-2L, 1L);
    }

    @Test
    void rejectsBitsBeyondBatchAndRangesBelowMarkDelete() {
        assertThatThrownBy(() -> new BatchAckState(3, new long[] {0b1000}))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CursorAckState(
                5, List.of(new OffsetRange(4, 6)), new TreeMap<>()))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
