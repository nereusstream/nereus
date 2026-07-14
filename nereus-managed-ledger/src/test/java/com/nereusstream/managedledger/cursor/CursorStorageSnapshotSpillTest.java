/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.CursorNames;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;

class CursorStorageSnapshotSpillTest {
    @Test
    void stableMissingSnapshotFailsAsMetadataInvariantWithoutFallback() {
        try (CursorStorageTestSupport.Context context =
                new CursorStorageTestSupport.Context(0, 2_000)) {
            CursorOwnerSession owner = context.owner(CursorStorageTestSupport.OWNER_1);
            CursorHandle handle = context.storage.open(
                            owner,
                            "subscription-a",
                            new CursorOpenRequest(
                                    new InitialCursorPosition.Earliest(),
                                    Map.of(),
                                    Map.of(),
                                    0,
                                    2_000))
                    .join();
            List<CursorAckRequest> disjoint = new ArrayList<>();
            for (int index = 0; index < 300; index++) {
                disjoint.add(new CursorAckRequest(
                        1 + index * 2L, Optional.empty(), Map.of()));
            }
            CursorState spilled = context.storage.individualAck(handle, disjoint).join().state();
            CursorSnapshotReference reference = spilled.snapshotReference().orElseThrow();
            context.snapshotStore.remove(reference.objectKey());

            assertThatThrownBy(() -> context.storage.claimAndLoadActiveCursors(owner).join())
                    .hasCauseInstanceOf(NereusException.class)
                    .cause()
                    .satisfies(error -> assertThat(((NereusException) error).code())
                            .isEqualTo(ErrorCode.METADATA_INVARIANT_VIOLATION));
        }
    }

    @Test
    void stableCorruptSnapshotFailsAsMetadataInvariantWithoutFallback() {
        try (CursorStorageTestSupport.Context context =
                new CursorStorageTestSupport.Context(0, 2_000)) {
            CursorOwnerSession owner = context.owner(CursorStorageTestSupport.OWNER_1);
            CursorState spilled = spill(context, owner, 2_000);
            context.snapshotStore.corrupt(
                    spilled.snapshotReference().orElseThrow().objectKey());

            assertThatThrownBy(() -> context.storage.claimAndLoadActiveCursors(owner).join())
                    .hasCauseInstanceOf(NereusException.class)
                    .cause()
                    .satisfies(error -> assertThat(((NereusException) error).code())
                            .isEqualTo(ErrorCode.METADATA_INVARIANT_VIOLATION));
        }
    }

    @Test
    void transientSnapshotReadFailurePreservesItsErrorAndCanBeRetried() {
        try (CursorStorageTestSupport.Context context =
                new CursorStorageTestSupport.Context(0, 2_000)) {
            CursorOwnerSession owner = context.owner(CursorStorageTestSupport.OWNER_1);
            spill(context, owner, 2_000);
            context.snapshotStore.failNextRead(new NereusException(
                    ErrorCode.OBJECT_READ_FAILED, true, "transient snapshot transport failure"));

            assertThatThrownBy(() -> context.storage.claimAndLoadActiveCursors(owner).join())
                    .hasCauseInstanceOf(NereusException.class)
                    .cause()
                    .satisfies(error -> {
                        NereusException nereus = (NereusException) error;
                        assertThat(nereus.code()).isEqualTo(ErrorCode.OBJECT_READ_FAILED);
                        assertThat(nereus.retriable()).isTrue();
                    });

            assertThat(context.storage.claimAndLoadActiveCursors(owner).join()).hasSize(1);
        }
    }

    @Test
    void concurrentReplacementSnapshotsLeaveOrphansButPublishTheAckUnion() {
        try (CursorStorageTestSupport.Context context =
                new CursorStorageTestSupport.Context(0, 5_000)) {
            CursorOwnerSession owner = context.owner(CursorStorageTestSupport.OWNER_1);
            CursorState base = spill(context, owner, 5_000);
            CursorHandle firstHandle = context.storage
                    .open(
                            owner,
                            "subscription-a",
                            new CursorOpenRequest(
                                    new InitialCursorPosition.Earliest(),
                                    Map.of(),
                                    Map.of(),
                                    0,
                                    5_000))
                    .join();
            DefaultCursorStorage secondStorage = secondStorage(context);
            ExecutorService executor = Executors.newFixedThreadPool(2);
            try {
                CursorHandle secondHandle = secondStorage
                        .open(
                                owner,
                                "subscription-a",
                                new CursorOpenRequest(
                                        new InitialCursorPosition.Earliest(),
                                        Map.of(),
                                        Map.of(),
                                        0,
                                        5_000))
                        .join();
                List<CursorAckRequest> left = disjointAcks(1_001, 4, 257);
                List<CursorAckRequest> right = disjointAcks(1_003, 4, 257);
                context.metadataStore.barrierNextCursorCas(2);

                CompletableFuture<CursorMutationResult> first = CompletableFuture.supplyAsync(
                        () -> context.storage.individualAck(firstHandle, left).join(),
                        executor);
                CompletableFuture<CursorMutationResult> second = CompletableFuture.supplyAsync(
                        () -> secondStorage.individualAck(secondHandle, right).join(),
                        executor);
                CompletableFuture.allOf(first, second).join();

                CursorState durable = context.storage
                        .claimAndLoadActiveCursors(owner)
                        .join()
                        .getFirst()
                        .state();
                assertThat(durable.acknowledgements().wholeAckRanges()).hasSize(814);
                assertThat(durable.snapshotReference()).isPresent();
                assertThat(durable.snapshotReference()).isNotEqualTo(base.snapshotReference());
                assertThat(context.snapshotStore.objectCount()).isGreaterThanOrEqualTo(3);
                left.forEach(request -> assertThat(durable
                                .acknowledgements()
                                .isWholeEntryAcknowledged(request.entryOffset()))
                        .isTrue());
                right.forEach(request -> assertThat(durable
                                .acknowledgements()
                                .isWholeEntryAcknowledged(request.entryOffset()))
                        .isTrue());

                var roots = context.metadataStore.scanCursors(
                                CursorStorageTestSupport.CLUSTER,
                                new StreamId(context.ledger.projection().streamId()),
                                Optional.empty(),
                                context.config.cursorScanPageSize())
                        .join()
                        .records();
                var retention = context.metadataStore.getRetention(
                                CursorStorageTestSupport.CLUSTER,
                                new StreamId(context.ledger.projection().streamId()))
                        .join()
                        .orElseThrow();
                CursorSnapshotInventory active = CursorSnapshotInventory.classify(
                        CursorStorageTestSupport.CLUSTER,
                        context.ledger,
                        retention,
                        roots,
                        context.snapshotStore.objectKeys());
                assertThat(active.liveReferences()).hasSize(1);
                assertThat(active.unreferencedCandidates())
                        .contains(base.snapshotReference().orElseThrow().objectKey())
                        .hasSizeGreaterThanOrEqualTo(2);
                assertThat(active.deletionVetoed()).isFalse();
                assertThat(active.stillMatches(retention, roots)).isTrue();

                context.retention.beginProtection(
                                owner,
                                new CursorRetentionCoordinator.ProtectionRequest(
                                        CursorRetentionView.PendingProtection.Kind.CREATE,
                                        "pending-cursor",
                                        CursorNames.cursorNameHash("pending-cursor"),
                                        0,
                                        1,
                                        0,
                                        Optional.empty(),
                                        Map.of(),
                                        Map.of()))
                        .join();
                var protectionPending = context.metadataStore.getRetention(
                                CursorStorageTestSupport.CLUSTER,
                                new StreamId(context.ledger.projection().streamId()))
                        .join()
                        .orElseThrow();
                CursorSnapshotInventory pending = CursorSnapshotInventory.classify(
                        CursorStorageTestSupport.CLUSTER,
                        context.ledger,
                        protectionPending,
                        roots,
                        context.snapshotStore.objectKeys());
                assertThat(pending.deletionVetoed()).isTrue();
                assertThat(active.stillMatches(protectionPending, roots)).isFalse();
            } finally {
                executor.shutdownNow();
                secondStorage.close();
            }
        }
    }

    private static CursorState spill(
            CursorStorageTestSupport.Context context,
            CursorOwnerSession owner,
            long endOffset) {
        CursorHandle handle = context.storage.open(
                        owner,
                        "subscription-a",
                        new CursorOpenRequest(
                                new InitialCursorPosition.Earliest(),
                                Map.of(),
                                Map.of(),
                                0,
                                endOffset))
                .join();
        CursorState spilled = context.storage
                .individualAck(handle, disjointAcks(1, 2, 300))
                .join()
                .state();
        assertThat(spilled.snapshotReference()).isPresent();
        return spilled;
    }

    private static List<CursorAckRequest> disjointAcks(
            long firstOffset, long step, int count) {
        List<CursorAckRequest> requests = new ArrayList<>();
        for (int index = 0; index < count; index++) {
            requests.add(new CursorAckRequest(
                    firstOffset + index * step, Optional.empty(), Map.of()));
        }
        return List.copyOf(requests);
    }

    private static DefaultCursorStorage secondStorage(
            CursorStorageTestSupport.Context context) {
        return new DefaultCursorStorage(
                CursorStorageTestSupport.CLUSTER,
                context.streamStorage,
                context.projectionStore,
                context.metadataStore,
                context.snapshotStore,
                context.retention,
                ignored -> CompletableFuture.completedFuture(null),
                new CursorStateMachine(context.config),
                new CursorStatePersistencePlanner(
                        CursorStorageTestSupport.CLUSTER, context.config),
                context.config,
                CursorStorageTestSupport.CLOCK,
                context.scheduler);
    }
}
