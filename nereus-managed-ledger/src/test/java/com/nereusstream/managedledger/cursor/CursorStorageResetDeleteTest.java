/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.Optional;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.junit.jupiter.api.Test;

class CursorStorageResetDeleteTest {
    @Test
    void backwardResetUsesProtectionProofAndForwardResetPreservesIt() {
        try (CursorStorageTestSupport.Context context =
                new CursorStorageTestSupport.Context(0, 20)) {
            CursorOwnerSession owner = context.owner(CursorStorageTestSupport.OWNER_1);
            CursorHandle handle = context.storage.open(
                            owner,
                            "subscription-a",
                            new CursorOpenRequest(
                                    new InitialCursorPosition.Latest(),
                                    Map.of("position", 1L),
                                    Map.of("cursor", "keep"),
                                    0,
                                    20))
                    .join();
            String createAttempt = handle.state().lastProtectionAttemptId();

            CursorMutationResult backward = context.storage.reset(
                            handle,
                            new CursorResetRequest(
                                    5,
                                    Optional.of(new BatchAckState(8, new long[] {0x03L})),
                                    true,
                                    0,
                                    20))
                    .join();

            assertThat(backward.state().acknowledgements().markDeleteOffset()).isEqualTo(5);
            assertThat(backward.state().acknowledgements().partialBatchAcks())
                    .containsExactly(Map.entry(5L, new BatchAckState(8, new long[] {0x03L})));
            assertThat(backward.state().ackStateEpoch()).isEqualTo(2);
            assertThat(backward.state().lastProtectionAttemptId()).isNotEqualTo(createAttempt);
            assertThat(backward.state().positionProperties()).isEmpty();
            assertThat(backward.state().cursorProperties())
                    .containsExactly(Map.entry("cursor", "keep"));
            assertThat(backward.state().snapshotReference()).isEmpty();
            CursorRetentionView protectedView = context.storage.retentionView(owner).join();
            assertThat(protectedView.lifecycle()).isEqualTo(CursorRetentionView.Lifecycle.ACTIVE);
            assertThat(protectedView.protectedFloorOffset()).isEqualTo(5);

            String protectionAttempt = backward.state().lastProtectionAttemptId();
            CursorMutationResult forward = context.storage.reset(
                            handle,
                            new CursorResetRequest(10, Optional.empty(), false, 0, 20))
                    .join();
            assertThat(forward.state().acknowledgements()).isEqualTo(CursorAckState.empty(10));
            assertThat(forward.state().ackStateEpoch()).isEqualTo(3);
            assertThat(forward.state().lastProtectionAttemptId()).isEqualTo(protectionAttempt);
            assertThat(forward.state().cursorProperties())
                    .containsExactly(Map.entry("cursor", "keep"));
        }
    }

    @Test
    void clearBacklogUsesCapturedEndAndLeavesConcurrentAppendVisible() {
        try (CursorStorageTestSupport.Context context =
                new CursorStorageTestSupport.Context(0, 20)) {
            CursorOwnerSession owner = context.owner(CursorStorageTestSupport.OWNER_1);
            CursorHandle handle = context.storage.open(
                            owner,
                            "subscription-a",
                            new CursorOpenRequest(
                                    new InitialCursorPosition.Earliest(),
                                    Map.of(),
                                    Map.of(),
                                    0,
                                    20))
                    .join();
            context.streamStorage.setCommittedEnd(25);

            CursorMutationResult cleared = context.storage.clearBacklog(handle, 20).join();

            assertThat(cleared.state().acknowledgements()).isEqualTo(CursorAckState.empty(20));
            assertThat(cleared.state().ackStateEpoch()).isEqualTo(2);
            assertThat(cleared.state().acknowledgements().markDeleteOffset()).isLessThan(25);
        }
    }

    @Test
    void deleteIsIdempotentAndStaleGenerationCannotMutateRecreatedCursor() {
        try (CursorStorageTestSupport.Context context =
                new CursorStorageTestSupport.Context(0, 20)) {
            CursorOwnerSession owner = context.owner(CursorStorageTestSupport.OWNER_1);
            CursorHandle old = context.storage.open(
                            owner,
                            "subscription-a",
                            new CursorOpenRequest(
                                    new InitialCursorPosition.Earliest(),
                                    Map.of(),
                                    Map.of(),
                                    0,
                                    20))
                    .join();

            context.storage.delete(owner, "subscription-a").join();
            context.storage.delete(owner, "subscription-a").join();
            context.storage.delete(owner, "missing").join();
            CursorHandle recreated = context.storage.open(
                            owner,
                            "subscription-a",
                            new CursorOpenRequest(
                                    new InitialCursorPosition.Latest(),
                                    Map.of(),
                                    Map.of(),
                                    0,
                                    20))
                    .join();

            assertThat(recreated.identity().cursorGeneration()).isEqualTo(2);
            assertThatThrownBy(() -> context.storage.individualAck(
                            old,
                            java.util.List.of(new CursorAckRequest(
                                    1, Optional.empty(), Map.of())))
                    .join())
                    .hasCauseInstanceOf(ManagedLedgerException.CursorAlreadyClosedException.class);
        }
    }
}
