/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

class CursorStateMachinePropertyTest {
    private static final int END_OFFSET = 64;
    private static final String OWNER = "00112233445566778899aabbccddeeff";
    private static final String ATTEMPT = "11111111111111111111111111111111";

    @Test
    void randomizedIndividualAndCumulativeAcksMatchIndependentEntryModel()
            throws Exception {
        CursorStateMachine machine = new CursorStateMachine(CursorStorageConfig.defaults());
        for (int seed = 0; seed < 40; seed++) {
            Random random = new Random(0x4e45524555534cL + seed);
            CursorState state = machine.create(
                    new CursorOwnerSession(CursorTestSamples.identity().ledger(), OWNER),
                    CursorTestSamples.CURSOR,
                    1,
                    1,
                    ATTEMPT,
                    0,
                    Map.of(),
                    Map.of(),
                    100);
            AckModel model = new AckModel(END_OFFSET);
            for (int step = 0; step < 250; step++) {
                if (model.markDelete() < END_OFFSET && random.nextInt(4) == 0) {
                    long offset = model.markDelete()
                            + random.nextInt(
                                    END_OFFSET - Math.toIntExact(model.markDelete()));
                    CursorAckRequest request = request(random, offset, true, step);
                    state = machine.cumulativeAck(
                                    state, request, 0, END_OFFSET, 101L + step)
                            .state();
                    model.cumulative(request);
                } else {
                    int count = 1 + random.nextInt(6);
                    List<CursorAckRequest> requests = new ArrayList<>();
                    for (int index = 0; index < count; index++) {
                        requests.add(request(
                                random, random.nextInt(END_OFFSET), false, step));
                    }
                    state = machine.individualAck(
                                    state, requests, 0, END_OFFSET, 101L + step)
                            .state();
                    model.individual(requests);
                }
                assertMatches(model, state, seed, step);
            }
        }
    }

    private static CursorAckRequest request(
            Random random, long offset, boolean cumulative, int step) {
        Map<String, Long> properties = cumulative
                ? Map.of("revision", (long) step)
                : Map.of();
        if (random.nextInt(3) == 0) {
            return new CursorAckRequest(offset, Optional.empty(), properties);
        }
        long remaining = random.nextInt(256);
        return new CursorAckRequest(
                offset,
                Optional.of(new BatchAckState(8, new long[] {remaining})),
                properties);
    }

    private static void assertMatches(
            AckModel model, CursorState state, int seed, int step) {
        CursorAckState actual = state.acknowledgements();
        String description = "seed=" + seed + ", step=" + step;
        assertThat(actual.markDeleteOffset())
                .as(description + " mark-delete")
                .isEqualTo(model.markDelete());
        assertThat(actual.wholeAckRanges())
                .as(description + " whole ranges")
                .isEqualTo(model.ranges());
        assertThat(actual.partialBatchAcks())
                .as(description + " partials")
                .isEqualTo(model.partials());
        assertThat(state.positionProperties())
                .as(description + " position properties")
                .isEqualTo(model.positionProperties());
        for (int offset = 0; offset < END_OFFSET; offset++) {
            assertThat(actual.isWholeEntryAcknowledged(offset))
                    .as(description + " whole offset " + offset)
                    .isEqualTo(model.isWhole(offset));
        }
    }

    private static final class AckModel {
        private final boolean[] whole;
        private final int[] remaining;
        private int markDelete;
        private Map<String, Long> positionProperties = Map.of();

        private AckModel(int endOffset) {
            whole = new boolean[endOffset];
            remaining = new int[endOffset];
            java.util.Arrays.fill(remaining, 0xff);
        }

        private void individual(List<CursorAckRequest> requests) {
            for (CursorAckRequest request : requests) {
                applyEntry(request);
            }
            foldMarkDelete();
        }

        private void cumulative(CursorAckRequest request) {
            int offset = Math.toIntExact(request.entryOffset());
            for (int index = markDelete; index < offset; index++) {
                whole[index] = true;
                remaining[index] = 0;
            }
            markDelete = offset;
            applyEntry(request);
            foldMarkDelete();
            positionProperties = request.positionProperties();
        }

        private void applyEntry(CursorAckRequest request) {
            int offset = Math.toIntExact(request.entryOffset());
            if (offset < markDelete || whole[offset]) {
                return;
            }
            if (request.wholeEntry()) {
                whole[offset] = true;
                remaining[offset] = 0;
                return;
            }
            int requested = request.batchAck().orElseThrow().isWholeEntryAcknowledged()
                    ? 0
                    : Math.toIntExact(
                            request.batchAck().orElseThrow().remainingWords()[0]);
            remaining[offset] &= requested;
            if (remaining[offset] == 0) {
                whole[offset] = true;
            }
        }

        private void foldMarkDelete() {
            while (markDelete < whole.length && whole[markDelete]) {
                markDelete++;
            }
        }

        private long markDelete() {
            return markDelete;
        }

        private boolean isWhole(int offset) {
            return offset < markDelete || whole[offset];
        }

        private List<OffsetRange> ranges() {
            List<OffsetRange> result = new ArrayList<>();
            int offset = markDelete;
            while (offset < whole.length) {
                if (!whole[offset]) {
                    offset++;
                    continue;
                }
                int start = offset;
                while (offset < whole.length && whole[offset]) {
                    offset++;
                }
                result.add(new OffsetRange(start, offset));
            }
            return List.copyOf(result);
        }

        private TreeMap<Long, BatchAckState> partials() {
            TreeMap<Long, BatchAckState> result = new TreeMap<>();
            for (int offset = markDelete; offset < whole.length; offset++) {
                if (!whole[offset] && remaining[offset] != 0xff) {
                    result.put(
                            (long) offset,
                            new BatchAckState(8, new long[] {remaining[offset]}));
                }
            }
            return result;
        }

        private Map<String, Long> positionProperties() {
            return positionProperties;
        }
    }
}
