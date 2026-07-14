/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.StreamId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.junit.jupiter.api.Test;

class CursorStorageAckTest {
    @Test
    void cumulativeAndIndividualAckPersistCanonicalDurableTruth() {
        try (CursorStorageTestSupport.Context context =
                new CursorStorageTestSupport.Context(0, 20)) {
            CursorOwnerSession owner = context.owner(CursorStorageTestSupport.OWNER_1);
            CursorHandle handle = openEarliest(context, owner);

            CursorMutationResult individual = context.storage.individualAck(
                            handle,
                            List.of(
                                    whole(4),
                                    partial(3, 0x0fL),
                                    whole(2)))
                    .join();
            assertThat(individual.state().acknowledgements().wholeAckRanges())
                    .containsExactly(new OffsetRange(2, 3), new OffsetRange(4, 5));
            assertThat(individual.state().acknowledgements().partialBatchAcks())
                    .containsExactly(Map.entry(3L, batch(0x0fL)));

            CursorMutationResult cumulative = context.storage.cumulativeAck(
                            handle,
                            new CursorAckRequest(
                                    1, Optional.empty(), Map.of("mark-delete", 1L)))
                    .join();
            assertThat(cumulative.state().acknowledgements().markDeleteOffset()).isEqualTo(3);
            assertThat(cumulative.state().acknowledgements().wholeAckRanges())
                    .containsExactly(new OffsetRange(4, 5));
            assertThat(cumulative.state().acknowledgements().partialBatchAcks())
                    .containsExactly(Map.entry(3L, batch(0x0fL)));

            CursorMutationResult partial = context.storage.cumulativeAck(
                            handle,
                            new CursorAckRequest(
                                    3,
                                    Optional.of(batch(0x03L)),
                                    Map.of("mark-delete", 2L)))
                    .join();
            assertThat(partial.state().acknowledgements().markDeleteOffset()).isEqualTo(3);
            assertThat(partial.state().acknowledgements().partialBatchAcks())
                    .containsExactly(Map.entry(3L, batch(0x03L)));

            CursorMutationResult completed = context.storage.cumulativeAck(
                            handle,
                            new CursorAckRequest(3, Optional.empty(), Map.of()))
                    .join();
            assertThat(completed.state().acknowledgements())
                    .isEqualTo(CursorAckState.empty(5));
            long sequence = completed.state().mutationSequence();
            CursorMutationResult retry = context.storage.cumulativeAck(
                            handle,
                            new CursorAckRequest(3, Optional.empty(), Map.of()))
                    .join();
            assertThat(retry.outcome()).isEqualTo(CursorMutationOutcome.ALREADY_APPLIED);
            assertThat(retry.state().mutationSequence()).isEqualTo(sequence);

            CursorState durable = context.storage
                    .claimAndLoadActiveCursors(owner)
                    .join()
                    .getFirst()
                    .state();
            assertThat(durable.acknowledgements()).isEqualTo(CursorAckState.empty(5));
            assertThat(durable.positionProperties()).isEmpty();
        }
    }

    @Test
    void snapshotSpillPublishesFullStateThenKeepsBoundedInlineDelta() {
        try (CursorStorageTestSupport.Context context =
                new CursorStorageTestSupport.Context(0, 2_000)) {
            CursorOwnerSession owner = context.owner(CursorStorageTestSupport.OWNER_1);
            CursorHandle handle = openEarliest(context, owner);
            List<CursorAckRequest> firstBatch = disjointWholeAcks(1, 300);

            CursorState spilled = context.storage.individualAck(handle, firstBatch).join().state();

            assertThat(spilled.acknowledgements().wholeAckRanges()).hasSize(300);
            assertThat(spilled.snapshotReference()).isPresent();
            assertThat(context.snapshotStore.objectCount()).isEqualTo(1);
            var storedAfterSpill = context.metadataStore.getCursor(
                            CursorStorageTestSupport.CLUSTER,
                            new StreamId(context.ledger.projection().streamId()),
                            "subscription-a")
                    .join()
                    .orElseThrow()
                    .value();
            assertThat(storedAfterSpill.snapshotReference()).isPresent();
            assertThat(storedAfterSpill.inlineWholeAckDeltas()).isEmpty();

            CursorState withDelta = context.storage.individualAck(
                            handle, List.of(whole(1_000)))
                    .join()
                    .state();
            assertThat(withDelta.snapshotReference()).isEqualTo(spilled.snapshotReference());
            assertThat(context.snapshotStore.objectCount()).isEqualTo(1);
            var storedWithDelta = context.metadataStore.getCursor(
                            CursorStorageTestSupport.CLUSTER,
                            new StreamId(context.ledger.projection().streamId()),
                            "subscription-a")
                    .join()
                    .orElseThrow()
                    .value();
            assertThat(storedWithDelta.inlineWholeAckDeltas()).hasSize(1);

            CursorState rehydrated = context.storage
                    .claimAndLoadActiveCursors(owner)
                    .join()
                    .getFirst()
                    .state();
            assertThat(rehydrated.acknowledgements()).isEqualTo(withDelta.acknowledgements());
        }
    }

    @Test
    void staleEpochAckIsRejectedUnlessLatestStateAlreadySubsumesIt() {
        try (CursorStorageTestSupport.Context context =
                new CursorStorageTestSupport.Context(0, 20)) {
            CursorOwnerSession owner = context.owner(CursorStorageTestSupport.OWNER_1);
            CursorHandle first = openEarliest(context, owner);
            DefaultCursorStorage secondStorage = new DefaultCursorStorage(
                    CursorStorageTestSupport.CLUSTER,
                    context.streamStorage,
                    context.projectionStore,
                    context.metadataStore,
                    context.snapshotStore,
                    context.retention,
                    ignored -> java.util.concurrent.CompletableFuture.completedFuture(null),
                    new CursorStateMachine(context.config),
                    new CursorStatePersistencePlanner(
                            CursorStorageTestSupport.CLUSTER, context.config),
                    context.config,
                    CursorStorageTestSupport.CLOCK,
                    context.scheduler);
            try {
                CursorHandle stale = secondStorage.open(
                                owner,
                                "subscription-a",
                                new CursorOpenRequest(
                                        new InitialCursorPosition.Latest(),
                                        Map.of(),
                                        Map.of(),
                                        0,
                                        20))
                        .join();

                context.storage.reset(
                                first,
                                new CursorResetRequest(
                                        10, Optional.empty(), false, 0, 20))
                        .join();

                assertThatThrownBy(() -> secondStorage.cumulativeAck(
                                stale,
                                new CursorAckRequest(12, Optional.empty(), Map.of()))
                        .join())
                        .hasCauseInstanceOf(
                                ManagedLedgerException.ConcurrentFindCursorPositionException.class);
                CursorMutationResult subsumed = secondStorage.cumulativeAck(
                                stale,
                                new CursorAckRequest(5, Optional.empty(), Map.of()))
                        .join();
                assertThat(subsumed.outcome()).isEqualTo(CursorMutationOutcome.ALREADY_APPLIED);
                assertThat(subsumed.state().acknowledgements().markDeleteOffset()).isEqualTo(10);
            } finally {
                secondStorage.close();
            }
        }
    }

    private static CursorHandle openEarliest(
            CursorStorageTestSupport.Context context, CursorOwnerSession owner) {
        return context.storage.open(
                        owner,
                        "subscription-a",
                        new CursorOpenRequest(
                                new InitialCursorPosition.Earliest(),
                                Map.of(),
                                Map.of(),
                                0,
                                context.streamStorage
                                        .getStreamMetadata(new StreamId(context.ledger.projection().streamId()))
                                        .join()
                                        .committedEndOffset()))
                .join();
    }

    private static List<CursorAckRequest> disjointWholeAcks(int firstOffset, int count) {
        List<CursorAckRequest> requests = new ArrayList<>();
        for (int index = 0; index < count; index++) {
            requests.add(whole(firstOffset + index * 2L));
        }
        return List.copyOf(requests);
    }

    private static CursorAckRequest whole(long offset) {
        return new CursorAckRequest(offset, Optional.empty(), Map.of());
    }

    private static CursorAckRequest partial(long offset, long remaining) {
        return new CursorAckRequest(offset, Optional.of(batch(remaining)), Map.of());
    }

    private static BatchAckState batch(long remaining) {
        return new BatchAckState(8, new long[] {remaining});
    }
}
