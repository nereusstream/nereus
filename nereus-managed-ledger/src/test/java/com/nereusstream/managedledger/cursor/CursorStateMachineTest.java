/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ObjectKey;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.junit.jupiter.api.Test;

class CursorStateMachineTest {
    private static final String OWNER = "00112233445566778899aabbccddeeff";
    private static final String NEXT_OWNER = "ffeeddccbbaa99887766554433221100";
    private static final String ATTEMPT = "11111111111111111111111111111111";
    private static final String NEXT_ATTEMPT = "22222222222222222222222222222222";

    private final CursorStateMachine machine = new CursorStateMachine(CursorStorageConfig.defaults());

    @Test
    void wholeCumulativeAckFoldsFollowingRangesAndPersistsExactProperties() throws Exception {
        CursorState current = fresh(5);
        CursorState withHoles = machine.individualAck(
                        current,
                        List.of(whole(8), whole(7)),
                        0,
                        20,
                        101)
                .state();

        CursorMutationResult result = machine.cumulativeAck(
                withHoles,
                new CursorAckRequest(6, Optional.empty(), Map.of("next", 2L)),
                0,
                20,
                102);

        assertThat(result.outcome()).isEqualTo(CursorMutationOutcome.APPLIED);
        assertThat(result.state().acknowledgements())
                .isEqualTo(CursorAckState.empty(9));
        assertThat(result.state().positionProperties()).containsExactly(entry("next", 2L));
        assertThat(result.state().ackStateEpoch()).isEqualTo(current.ackStateEpoch());
        assertThat(result.state().mutationSequence()).isEqualTo(3);
    }

    @Test
    void cumulativeAckBehindMarkDeleteIsIdempotentAndDoesNotOverwriteProperties() throws Exception {
        CursorState current = copy(
                fresh(9),
                4,
                1,
                CursorAckState.empty(9),
                Map.of("winner", 9L),
                Map.of("external", "old"),
                Optional.empty());

        CursorMutationResult result = machine.cumulativeAck(
                current,
                new CursorAckRequest(8, Optional.empty(), Map.of("stale", 1L)),
                0,
                20,
                110);

        assertThat(result.outcome()).isEqualTo(CursorMutationOutcome.ALREADY_APPLIED);
        assertThat(result.state()).isSameAs(current);
        assertThat(result.state().positionProperties()).containsExactly(entry("winner", 9L));
    }

    @Test
    void partialCumulativeAckAdvancesBoundaryAndMergesRemainingWords() throws Exception {
        CursorMutationResult first = machine.cumulativeAck(
                fresh(5),
                partial(7, 0x0fL, Map.of("stage", 1L)),
                0,
                20,
                101);
        assertThat(first.state().acknowledgements().markDeleteOffset()).isEqualTo(7);
        assertThat(first.state().acknowledgements().partialBatchAcks())
                .containsExactly(entry(7L, batch(0x0fL)));

        CursorMutationResult second = machine.cumulativeAck(
                first.state(),
                partial(7, 0x03L, Map.of("stage", 2L)),
                0,
                20,
                102);
        assertThat(second.state().acknowledgements().partialBatchAcks())
                .containsExactly(entry(7L, batch(0x03L)));
        assertThat(second.state().positionProperties()).containsExactly(entry("stage", 2L));

        CursorMutationResult retry = machine.cumulativeAck(
                second.state(),
                partial(7, 0x07L, Map.of("stage", 2L)),
                0,
                20,
                103);
        assertThat(retry.outcome()).isEqualTo(CursorMutationOutcome.ALREADY_APPLIED);
        assertThat(retry.state()).isSameAs(second.state());

        CursorMutationResult complete = machine.cumulativeAck(
                second.state(),
                new CursorAckRequest(7, Optional.of(new BatchAckState(8, new long[0])), Map.of()),
                0,
                20,
                104);
        assertThat(complete.state().acknowledgements()).isEqualTo(CursorAckState.empty(8));
    }

    @Test
    void individualAckCanonicalizesOrderDuplicatesAndWholeWins() throws Exception {
        CursorMutationResult result = machine.individualAck(
                fresh(5),
                List.of(
                        whole(8),
                        partial(7, 0x0fL, Map.of()),
                        whole(6),
                        partial(7, 0x03L, Map.of()),
                        whole(5)),
                0,
                20,
                101);

        assertThat(result.state().acknowledgements().markDeleteOffset()).isEqualTo(7);
        assertThat(result.state().acknowledgements().wholeAckRanges())
                .containsExactly(new OffsetRange(8, 9));
        assertThat(result.state().acknowledgements().partialBatchAcks())
                .containsExactly(entry(7L, batch(0x03L)));
        assertThat(result.state().positionProperties()).isEqualTo(fresh(5).positionProperties());

        CursorMutationResult wholeWins = machine.individualAck(
                result.state(), List.of(whole(7)), 0, 20, 102);
        assertThat(wholeWins.state().acknowledgements()).isEqualTo(CursorAckState.empty(9));
    }

    @Test
    void individualAckRejectsAmbiguousOrOversizeRequestsBeforeTransition() {
        assertThatThrownBy(() -> machine.canonicalIndividualRequests(List.of(
                        partial(7, 0x0fL, Map.of()),
                        new CursorAckRequest(7, Optional.of(new BatchAckState(9, new long[] {1})), Map.of()))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batch sizes");
        assertThatThrownBy(() -> machine.canonicalIndividualRequests(List.of(
                        partial(7, 0x0fL, Map.of("not", 1L)))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("position properties");
        assertThatThrownBy(() -> machine.cumulativeAck(
                        fresh(5),
                        new CursorAckRequest(
                                7,
                                Optional.of(new BatchAckState(131_073, new long[] {1})),
                                Map.of()),
                        0,
                        20,
                        101))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batch-index bound");
    }

    @Test
    void resetIsDestructiveAndClearBacklogUsesCapturedEnd() throws Exception {
        CursorState active = withSnapshot(copy(
                fresh(5),
                8,
                3,
                new CursorAckState(5, List.of(new OffsetRange(7, 9)), new TreeMap<>()),
                Map.of("position", 1L),
                Map.of("external", "keep", "#pulsar.internal.keep", "yes"),
                Optional.empty()));
        CursorResetRequest request = new CursorResetRequest(
                6, Optional.of(batch(0x03L)), true, 4, 20);

        CursorMutationResult reset = machine.reset(active, request, NEXT_ATTEMPT, 120);

        assertThat(reset.state().ackStateEpoch()).isEqualTo(4);
        assertThat(reset.state().mutationSequence()).isEqualTo(9);
        assertThat(reset.state().acknowledgements().markDeleteOffset()).isEqualTo(6);
        assertThat(reset.state().acknowledgements().partialBatchAcks())
                .containsExactly(entry(6L, batch(0x03L)));
        assertThat(reset.state().positionProperties()).isEmpty();
        assertThat(reset.state().cursorProperties()).isEqualTo(active.cursorProperties());
        assertThat(reset.state().snapshotReference()).isEmpty();
        assertThat(reset.state().lastProtectionAttemptId()).isEqualTo(NEXT_ATTEMPT);
        assertThat(machine.isExactResetResult(reset.state(), request, 3)).isTrue();

        CursorMutationResult cleared = machine.clearBacklog(reset.state(), 20, 121);
        assertThat(cleared.state().acknowledgements()).isEqualTo(CursorAckState.empty(20));
        assertThat(cleared.state().ackStateEpoch()).isEqualTo(5);
        assertThat(machine.isExactClearResult(cleared.state(), 20, 4)).isTrue();
    }

    @Test
    void cursorPropertyShapesAndPositionFlushPreserveIndependentState() throws Exception {
        CursorState current = fresh(5);
        CursorMutationResult replaced = machine.mutateCursorProperties(
                current,
                new CursorPropertyMutation.ReplaceExternal(Map.of("new", "value")),
                101);
        assertThat(replaced.state().cursorProperties())
                .containsExactlyInAnyOrderEntriesOf(Map.of(
                        "new", "value", "#pulsar.internal.keep", "yes"));
        assertThat(replaced.state().acknowledgements()).isEqualTo(current.acknowledgements());

        CursorMutationResult put = machine.mutateCursorProperties(
                replaced.state(), new CursorPropertyMutation.Put("other", "x"), 102);
        CursorMutationResult remove = machine.mutateCursorProperties(
                put.state(), new CursorPropertyMutation.Remove("new"), 103);
        assertThat(remove.state().cursorProperties())
                .containsExactlyInAnyOrderEntriesOf(Map.of(
                        "other", "x", "#pulsar.internal.keep", "yes"));

        CursorMutationResult flushed = machine.flushPositionProperties(
                remove.state(), Map.of("flushed", 7L), 104);
        assertThat(flushed.state().positionProperties()).containsExactly(entry("flushed", 7L));
        assertThat(flushed.state().cursorProperties()).isEqualTo(remove.state().cursorProperties());
    }

    @Test
    void claimAndDeletePreserveOnlyTheirSpecifiedFields() throws Exception {
        CursorState current = fresh(5);
        assertThat(machine.claim(current, OWNER, 101)).isSameAs(current);

        CursorState claimed = machine.claim(current, NEXT_OWNER, 101);
        assertThat(claimed.ownerSessionId()).isEqualTo(NEXT_OWNER);
        assertThat(claimed.mutationSequence()).isEqualTo(current.mutationSequence() + 1);
        assertThat(claimed.ackStateEpoch()).isEqualTo(current.ackStateEpoch());
        assertThat(claimed.acknowledgements()).isEqualTo(current.acknowledgements());
        assertThat(claimed.positionProperties()).isEqualTo(current.positionProperties());
        assertThat(claimed.cursorProperties()).isEqualTo(current.cursorProperties());

        CursorMutationResult deleted = machine.delete(claimed, 102);
        assertThat(deleted.state().lifecycle()).isEqualTo(CursorLifecycle.DELETED);
        assertThat(deleted.state().acknowledgements()).isEqualTo(CursorAckState.empty(5));
        assertThat(deleted.state().positionProperties()).isEmpty();
        assertThat(deleted.state().cursorProperties()).isEmpty();
        assertThat(deleted.state().snapshotReference()).isEmpty();
        assertThat(machine.delete(deleted.state(), 103).outcome())
                .isEqualTo(CursorMutationOutcome.ALREADY_APPLIED);
    }

    @Test
    void subsumptionAndExactResultChecksDistinguishDestructiveHistory() throws Exception {
        CursorState partial = machine.cumulativeAck(
                        fresh(5), partial(7, 0x03L, Map.of("p", 1L)), 0, 20, 101)
                .state();
        assertThat(machine.isCumulativeAckSubsumed(
                        partial, partial(7, 0x07L, Map.of("p", 1L))))
                .isTrue();
        assertThat(machine.isCumulativeAckSubsumed(
                        partial, partial(7, 0x01L, Map.of("p", 1L))))
                .isFalse();
        assertThat(machine.isCumulativeAckSubsumed(
                        partial, partial(7, 0x07L, Map.of("different", 1L))))
                .isFalse();
        assertThat(machine.isIndividualAckSubsumed(
                        partial, List.of(partial(7, 0x07L, Map.of()))))
                .isTrue();
        assertThat(machine.isIndividualAckSubsumed(
                        partial, List.of(whole(7))))
                .isFalse();
    }

    @Test
    void checkedCountersAndAuthoritativeBoundsFailClosed() {
        CursorState sequenceMax = copy(
                fresh(5), Long.MAX_VALUE, 1, CursorAckState.empty(5), Map.of(), Map.of(), Optional.empty());
        assertThatThrownBy(() -> machine.cumulativeAck(
                        sequenceMax, whole(5), 0, 20, 101))
                .isInstanceOf(ArithmeticException.class);

        CursorState epochMax = copy(
                fresh(5), 1, Long.MAX_VALUE, CursorAckState.empty(5), Map.of(), Map.of(), Optional.empty());
        assertThatThrownBy(() -> machine.reset(
                        epochMax,
                        new CursorResetRequest(5, Optional.empty(), false, 0, 20),
                        NEXT_ATTEMPT,
                        101))
                .isInstanceOf(ArithmeticException.class);

        assertThatThrownBy(() -> machine.cumulativeAck(fresh(5), whole(20), 0, 20, 101))
                .isInstanceOf(ManagedLedgerException.InvalidCursorPositionException.class);
        assertThatThrownBy(() -> machine.cumulativeAck(fresh(5), whole(5), 6, 20, 101))
                .isInstanceOf(ManagedLedgerException.class)
                .hasMessageContaining("bounds");
    }

    private CursorState fresh(long markDeleteOffset) {
        return machine.create(
                new CursorOwnerSession(CursorTestSamples.identity().ledger(), OWNER),
                CursorTestSamples.CURSOR,
                1,
                1,
                ATTEMPT,
                markDeleteOffset,
                Map.of("initial", 1L),
                Map.of("external", "old", "#pulsar.internal.keep", "yes"),
                100);
    }

    private static CursorAckRequest whole(long offset) {
        return new CursorAckRequest(offset, Optional.empty(), Map.of());
    }

    private static CursorAckRequest partial(
            long offset, long remainingWord, Map<String, Long> properties) {
        return new CursorAckRequest(offset, Optional.of(batch(remainingWord)), properties);
    }

    private static BatchAckState batch(long remainingWord) {
        return new BatchAckState(8, new long[] {remainingWord});
    }

    private static CursorState withSnapshot(CursorState state) {
        CursorSnapshotReference reference = new CursorSnapshotReference(
                new ObjectKey("cursor-snapshots/test.ncs"),
                "33333333333333333333333333333333",
                state.identity().cursorGeneration(),
                state.mutationSequence(),
                state.acknowledgements().markDeleteOffset(),
                1,
                new Checksum(ChecksumType.CRC32C, "00000000"),
                0,
                1,
                state.updatedAtMillis());
        return new CursorState(
                state.identity(),
                state.ownerSessionId(),
                state.lifecycle(),
                state.mutationSequence(),
                state.ackStateEpoch(),
                state.lastProtectionAttemptId(),
                state.acknowledgements(),
                state.positionProperties(),
                state.cursorProperties(),
                Optional.of(reference),
                state.createdAtMillis(),
                state.updatedAtMillis(),
                state.metadataVersion());
    }

    private static CursorState copy(
            CursorState state,
            long mutationSequence,
            long ackStateEpoch,
            CursorAckState acknowledgements,
            Map<String, Long> positionProperties,
            Map<String, String> cursorProperties,
            Optional<CursorSnapshotReference> snapshotReference) {
        return new CursorState(
                state.identity(),
                state.ownerSessionId(),
                state.lifecycle(),
                mutationSequence,
                ackStateEpoch,
                state.lastProtectionAttemptId(),
                acknowledgements,
                positionProperties,
                cursorProperties,
                snapshotReference,
                state.createdAtMillis(),
                state.updatedAtMillis(),
                state.metadataVersion());
    }

    private static <K, V> Map.Entry<K, V> entry(K key, V value) {
        return Map.entry(key, value);
    }
}
