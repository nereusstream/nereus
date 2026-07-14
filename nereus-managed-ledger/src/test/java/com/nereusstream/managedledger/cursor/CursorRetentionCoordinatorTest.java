/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.ManagedLedgerCursorProtocol;
import com.nereusstream.metadata.oxia.records.CursorRecordLifecycle;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.junit.jupiter.api.Test;

class CursorRetentionCoordinatorTest {
    @Test
    void preactivationRootIsClaimedThenCursorlessTrimActivatesAndUsesAttemptReason() {
        try (CursorStorageTestSupport.Context context =
                new CursorStorageTestSupport.Context(3, 20)) {
            CursorOwnerSession first = context.owner(CursorStorageTestSupport.OWNER_1);
            CursorRetentionView initial = context.retention.claimAndRecover(first).join();

            assertThat(initial.lifecycle()).isEqualTo(CursorRetentionView.Lifecycle.ACTIVE);
            assertThat(initial.ownerSessionId()).isEqualTo(first.ownerSessionId());
            assertThat(initial.protectedFloorOffset()).isEqualTo(3);
            assertThat(initial.lastCompletedTrimOffset()).isEqualTo(3);
            assertThat(context.activationCalls).hasValue(0);

            CursorOwnerSession second = context.owner(CursorStorageTestSupport.OWNER_2);
            CursorRetentionView claimed = context.retention.claimAndRecover(second).join();
            assertThat(claimed.ownerSessionId()).isEqualTo(second.ownerSessionId());
            assertThat(claimed.mutationSequence()).isGreaterThan(initial.mutationSequence());
            assertThatThrownBy(() -> context.retention.reconcileFloor(first).join())
                    .hasCauseInstanceOf(ManagedLedgerException.ManagedLedgerFencedException.class);

            CursorRetentionView raised = context.retention.reconcileFloor(second).join();
            assertThat(raised.protectedFloorOffset()).isEqualTo(20);
            CursorRetentionView trimmed = context.retention
                    .requestTrim(second, 10, "policy-age")
                    .join();

            assertThat(trimmed.lifecycle()).isEqualTo(CursorRetentionView.Lifecycle.ACTIVE);
            assertThat(trimmed.lastCompletedTrimOffset()).isEqualTo(10);
            assertThat(context.activationCalls).hasValue(1);
            assertThat(context.projectionStore
                            .getProjection(
                                    CursorStorageTestSupport.CLUSTER,
                                    CursorStorageTestSupport.TOPIC)
                            .join())
                    .hasValueSatisfying(value -> assertThat(
                                    ManagedLedgerCursorProtocol.isActivated(value))
                            .isTrue());
            assertThat(context.streamStorage.trims()).hasSize(1);
            CursorStorageTestSupport.TrimCall call = context.streamStorage.trims().getFirst();
            assertThat(call.offset()).isEqualTo(10);
            assertThat(call.reason())
                    .isEqualTo("nereus-cursor-retention/00000000000000000000000000000001:policy-age");
        }
    }

    @Test
    void claimedPendingCreateIsRecoveredWithExactAttemptAndInitialState() {
        try (CursorStorageTestSupport.Context context =
                new CursorStorageTestSupport.Context(0, 20)) {
            CursorOwnerSession first = context.owner(CursorStorageTestSupport.OWNER_1);
            context.retention.claimAndRecover(first).join();
            activate(context);
            CursorRetentionCoordinator.ProtectionRequest request =
                    new CursorRetentionCoordinator.ProtectionRequest(
                            CursorRetentionView.PendingProtection.Kind.CREATE,
                            "subscription-a",
                            com.nereusstream.metadata.oxia.CursorNames.cursorNameHash(
                                    "subscription-a"),
                            0,
                            1,
                            5,
                            Optional.empty(),
                            Map.of("position", 7L),
                            Map.of("cursor", "value"));
            CursorRetentionCoordinator.ProtectionLease lease = context.retention
                    .beginProtection(first, request)
                    .join();
            assertThat(context.storage.retentionView(first).join().lifecycle())
                    .isEqualTo(CursorRetentionView.Lifecycle.PROTECTION_PENDING);

            CursorOwnerSession second = context.owner(CursorStorageTestSupport.OWNER_2);
            CursorRetentionView recovered = context.retention.claimAndRecover(second).join();

            assertThat(recovered.lifecycle()).isEqualTo(CursorRetentionView.Lifecycle.ACTIVE);
            assertThat(recovered.ownerSessionId()).isEqualTo(second.ownerSessionId());
            var root = context.metadataStore.getCursor(
                            CursorStorageTestSupport.CLUSTER,
                            new StreamId(context.ledger.projection().streamId()),
                            "subscription-a")
                    .join()
                    .orElseThrow()
                    .value();
            assertThat(root.lifecycle()).isEqualTo(CursorRecordLifecycle.ACTIVE);
            assertThat(root.ownerSessionId()).isEqualTo(second.ownerSessionId());
            assertThat(root.lastProtectionAttemptId()).isEqualTo(lease.attemptId());
            assertThat(root.cursorGeneration()).isEqualTo(1);
            assertThat(root.markDeleteOffset()).isEqualTo(5);
            assertThat(root.positionProperties()).containsExactly(Map.entry("position", 7L));
            assertThat(root.cursorProperties()).containsExactly(Map.entry("cursor", "value"));
            assertThatThrownBy(() -> context.retention.completeProtection(lease).join())
                    .hasCauseInstanceOf(ManagedLedgerException.ManagedLedgerFencedException.class);
        }
    }

    @Test
    void claimedPendingBackwardResetRebuildsFromLatestRootWithoutLosingCursorProperties() {
        try (CursorStorageTestSupport.Context context =
                new CursorStorageTestSupport.Context(0, 20)) {
            CursorOwnerSession first = context.owner(CursorStorageTestSupport.OWNER_1);
            CursorHandle handle = context.storage.open(
                            first,
                            "subscription-a",
                            new CursorOpenRequest(
                                    new InitialCursorPosition.Latest(),
                                    Map.of("position", 1L),
                                    Map.of("cursor", "keep"),
                                    0,
                                    20))
                    .join();
            CursorRetentionCoordinator.ProtectionRequest reset =
                    new CursorRetentionCoordinator.ProtectionRequest(
                            CursorRetentionView.PendingProtection.Kind.BACKWARD_RESET,
                            handle.identity().cursorName(),
                            handle.identity().cursorNameHash(),
                            handle.identity().cursorGeneration(),
                            handle.identity().cursorGeneration(),
                            5,
                            Optional.of(new BatchAckState(8, new long[] {0x03L})),
                            Map.of(),
                            Map.of());
            CursorRetentionCoordinator.ProtectionLease lease = context.retention
                    .beginProtection(first, reset)
                    .join();

            CursorOwnerSession second = context.owner(CursorStorageTestSupport.OWNER_2);
            CursorRetentionView recovered = context.retention.claimAndRecover(second).join();
            CursorHandle claimed = context.storage
                    .claimAndLoadActiveCursors(second)
                    .join()
                    .getFirst();

            assertThat(recovered.lifecycle()).isEqualTo(CursorRetentionView.Lifecycle.ACTIVE);
            assertThat(recovered.protectedFloorOffset()).isEqualTo(5);
            assertThat(claimed.state().lastProtectionAttemptId()).isEqualTo(lease.attemptId());
            assertThat(claimed.state().acknowledgements().markDeleteOffset()).isEqualTo(5);
            assertThat(claimed.state().acknowledgements().partialBatchAcks())
                    .containsExactly(Map.entry(5L, new BatchAckState(8, new long[] {0x03L})));
            assertThat(claimed.state().positionProperties()).isEmpty();
            assertThat(claimed.state().cursorProperties())
                    .containsExactly(Map.entry("cursor", "keep"));
            assertThat(claimed.state().ownerSessionId()).isEqualTo(second.ownerSessionId());
        }
    }

    @Test
    void reconciliationTracksTheSlowestActiveCursorAndDeletionReleasesItsFloor() {
        try (CursorStorageTestSupport.Context context =
                new CursorStorageTestSupport.Context(0, 30)) {
            CursorOwnerSession owner = context.owner(CursorStorageTestSupport.OWNER_1);
            CursorHandle first = openAt(context, owner, "subscription-a", 5);
            openAt(context, owner, "subscription-b", 10);
            assertThat(context.retention.reconcileFloor(owner).join().protectedFloorOffset())
                    .isEqualTo(5);

            context.storage.reset(
                            first,
                            new CursorResetRequest(15, Optional.empty(), false, 0, 30))
                    .join();
            assertThat(context.retention.reconcileFloor(owner).join().protectedFloorOffset())
                    .isEqualTo(10);

            context.storage.delete(owner, "subscription-b").join();
            assertThat(context.retention.reconcileFloor(owner).join().protectedFloorOffset())
                    .isEqualTo(15);
        }
    }

    @Test
    void cursorlessTrimAndFirstCreateResolveOneActivationRaceThroughTheOwnerRoot() {
        try (CursorStorageTestSupport.Context context =
                new CursorStorageTestSupport.Context(0, 20)) {
            CursorOwnerSession owner = context.owner(CursorStorageTestSupport.OWNER_1);
            context.retention.claimAndRecover(owner).join();
            assertThat(context.retention.reconcileFloor(owner).join().protectedFloorOffset())
                    .isEqualTo(20);
            context.blockActivations(2);

            CompletableFuture<CursorHandle> opened = context.storage.open(
                    owner,
                    "subscription-race",
                    new CursorOpenRequest(
                            new InitialCursorPosition.AtOffset(10),
                            Map.of(),
                            Map.of(),
                            0,
                            20));
            CompletableFuture<CursorRetentionView> trimmed = context.retention.requestTrim(
                    owner, 5, "concurrent-first-create");
            context.awaitBlockedActivations();
            context.releaseActivations();

            CursorHandle handle = opened.join();
            CursorRetentionView view = trimmed.join();
            assertThat(handle.state().acknowledgements().markDeleteOffset()).isEqualTo(10);
            assertThat(view.lastCompletedTrimOffset()).isEqualTo(5);
            assertThat(context.streamStorage.trims()).hasSize(1);
            assertThat(context.activationCalls).hasValue(2);
            assertThat(context.projectionStore
                            .getProjection(
                                    CursorStorageTestSupport.CLUSTER,
                                    CursorStorageTestSupport.TOPIC)
                            .join())
                    .hasValueSatisfying(projection -> assertThat(
                                    ManagedLedgerCursorProtocol.isActivated(projection))
                            .isTrue());

            int callsAfterActivationRace = context.activationCalls.get();
            context.storage.delete(owner, handle.identity().cursorName()).join();
            context.storage.open(
                            owner,
                            handle.identity().cursorName(),
                            new CursorOpenRequest(
                                    new InitialCursorPosition.Latest(),
                                    Map.of(),
                                    Map.of(),
                                    5,
                                    20))
                    .join();
            assertThat(context.activationCalls).hasValue(callsAfterActivationRace);
        }
    }

    @Test
    void protectionPendingFreezesFloorRaiseAndLogicalTrimUntilAttemptFinalizes() {
        try (CursorStorageTestSupport.Context context =
                new CursorStorageTestSupport.Context(0, 30)) {
            CursorOwnerSession owner = context.owner(CursorStorageTestSupport.OWNER_1);
            CursorHandle handle = openAt(context, owner, "subscription-protected", 20);
            assertThat(context.retention.reconcileFloor(owner).join().protectedFloorOffset())
                    .isEqualTo(20);

            CursorRetentionCoordinator.ProtectionLease lease = context.retention
                    .beginProtection(
                            owner,
                            new CursorRetentionCoordinator.ProtectionRequest(
                                    CursorRetentionView.PendingProtection.Kind.BACKWARD_RESET,
                                    handle.identity().cursorName(),
                                    handle.identity().cursorNameHash(),
                                    handle.identity().cursorGeneration(),
                                    handle.identity().cursorGeneration(),
                                    5,
                                    Optional.empty(),
                                    Map.of(),
                                    Map.of()))
                    .join();

            CursorRetentionView blocked = context.retention.reconcileFloor(owner).join();
            assertThat(blocked.lifecycle())
                    .isEqualTo(CursorRetentionView.Lifecycle.PROTECTION_PENDING);
            assertThat(blocked.protectedFloorOffset()).isEqualTo(5);
            assertThatThrownBy(() -> context.retention
                            .requestTrim(owner, 6, "must-wait-for-protection")
                            .join())
                    .hasCauseInstanceOf(
                            ManagedLedgerException.ConcurrentFindCursorPositionException.class);
            assertThat(context.streamStorage.trims()).isEmpty();

            CursorRetentionView completed = context.retention.completeProtection(lease).join();
            assertThat(completed.lifecycle()).isEqualTo(CursorRetentionView.Lifecycle.ACTIVE);
            assertThat(completed.protectedFloorOffset()).isEqualTo(5);
            assertThat(context.retention.reconcileFloor(owner).join().protectedFloorOffset())
                    .isEqualTo(5);
        }
    }

    private static CursorHandle openAt(
            CursorStorageTestSupport.Context context,
            CursorOwnerSession owner,
            String name,
            long offset) {
        return context.storage.open(
                        owner,
                        name,
                        new CursorOpenRequest(
                                new InitialCursorPosition.AtOffset(offset),
                                Map.of(),
                                Map.of(),
                                0,
                                30))
                .join();
    }

    private static void activate(CursorStorageTestSupport.Context context) {
        var current = context.projectionStore
                .getProjection(
                        CursorStorageTestSupport.CLUSTER,
                        CursorStorageTestSupport.TOPIC)
                .join()
                .orElseThrow();
        context.projectionStore.activateCursorProtocol(
                        CursorStorageTestSupport.CLUSTER,
                        CursorStorageTestSupport.TOPIC,
                        context.ledger.projection(),
                        current.metadataVersion())
                .join();
    }
}
