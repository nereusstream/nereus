/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.metadata.oxia.records.CursorRecordLifecycle;
import com.nereusstream.metadata.oxia.records.CursorRetentionLifecycle;
import com.nereusstream.metadata.oxia.records.CursorRetentionRecord;
import com.nereusstream.metadata.oxia.records.CursorStateRecord;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class CursorRetentionFailureInjectionTest {
    @Test
    void failedTrimCallLeavesPendingAndRecoveryReplaysByteIdenticalReason() {
        try (CursorStorageTestSupport.Context context =
                new CursorStorageTestSupport.Context(0, 20)) {
            CursorOwnerSession owner = context.owner(CursorStorageTestSupport.OWNER_1);
            context.retention.claimAndRecover(owner).join();
            context.retention.reconcileFloor(owner).join();
            context.streamStorage.failNextTrimBeforeApply(transientFailure("trim unavailable"));

            assertThatThrownBy(() -> context.retention
                            .requestTrim(owner, 10, "policy-size")
                            .join())
                    .hasCauseInstanceOf(NereusException.class);
            CursorRetentionView pending = context.storage.retentionView(owner).join();
            assertThat(pending.lifecycle()).isEqualTo(CursorRetentionView.Lifecycle.TRIM_PENDING);
            String persistedReason = pending.pendingTrim().orElseThrow().composedReason();
            assertThat(context.streamStorage.trims()).hasSize(1);
            assertThat(context.streamStorage.trims().getFirst().reason())
                    .isEqualTo(persistedReason);

            CursorRetentionView recovered = context.retention.claimAndRecover(owner).join();

            assertThat(recovered.lifecycle()).isEqualTo(CursorRetentionView.Lifecycle.ACTIVE);
            assertThat(recovered.lastCompletedTrimOffset()).isEqualTo(10);
            assertThat(context.streamStorage.trims()).hasSize(2);
            assertThat(context.streamStorage.trims().get(1).reason())
                    .isEqualTo(persistedReason);
        }
    }

    @Test
    void appliedTrimWithLostResponseIsFinalizedWithoutIssuingASecondTrim() {
        try (CursorStorageTestSupport.Context context =
                new CursorStorageTestSupport.Context(0, 20)) {
            CursorOwnerSession owner = context.owner(CursorStorageTestSupport.OWNER_1);
            context.retention.claimAndRecover(owner).join();
            context.retention.reconcileFloor(owner).join();
            context.streamStorage.failNextTrimAfterApply(transientFailure("trim response lost"));

            assertThatThrownBy(() -> context.retention
                            .requestTrim(owner, 10, "policy-time")
                            .join())
                    .hasCauseInstanceOf(NereusException.class);
            assertThat(context.storage.retentionView(owner).join().lifecycle())
                    .isEqualTo(CursorRetentionView.Lifecycle.TRIM_PENDING);
            assertThat(context.streamStorage.trims()).hasSize(1);

            CursorRetentionView recovered = context.retention.claimAndRecover(owner).join();

            assertThat(recovered.lifecycle()).isEqualTo(CursorRetentionView.Lifecycle.ACTIVE);
            assertThat(recovered.lastCompletedTrimOffset()).isEqualTo(10);
            assertThat(context.streamStorage.trims()).hasSize(1);
        }
    }

    @Test
    void committedMonotonicAckAndDestructiveResultsAreProvedAfterResponseLoss() {
        try (CursorStorageTestSupport.Context context =
                new CursorStorageTestSupport.Context(0, 20)) {
            CursorOwnerSession owner = context.owner(CursorStorageTestSupport.OWNER_1);
            CursorHandle handle = open(context, owner, new InitialCursorPosition.Earliest());

            context.metadataStore.failNext(
                    CursorStorageTestSupport.MetadataOperation.CAS_CURSOR,
                    CursorStorageTestSupport.FaultCut.AFTER,
                    transientFailure("ack response lost"));
            CursorMutationResult ack = context.storage.individualAck(
                            handle,
                            List.of(new CursorAckRequest(2, Optional.empty(), Map.of())))
                    .join();
            assertThat(ack.outcome()).isEqualTo(CursorMutationOutcome.ALREADY_APPLIED);
            assertThat(ack.state().acknowledgements().isWholeEntryAcknowledged(2)).isTrue();

            context.metadataStore.failNext(
                    CursorStorageTestSupport.MetadataOperation.CAS_CURSOR,
                    CursorStorageTestSupport.FaultCut.AFTER,
                    transientFailure("reset response lost"));
            CursorMutationResult reset = context.storage.reset(
                            handle,
                            new CursorResetRequest(5, Optional.empty(), false, 0, 20))
                    .join();
            assertThat(reset.outcome()).isEqualTo(CursorMutationOutcome.ALREADY_APPLIED);
            assertThat(reset.state().acknowledgements()).isEqualTo(CursorAckState.empty(5));
            assertThat(reset.state().ackStateEpoch()).isEqualTo(2);

            context.metadataStore.failNext(
                    CursorStorageTestSupport.MetadataOperation.CAS_CURSOR,
                    CursorStorageTestSupport.FaultCut.AFTER,
                    transientFailure("clear response lost"));
            CursorMutationResult clear = context.storage.clearBacklog(handle, 20).join();
            assertThat(clear.outcome()).isEqualTo(CursorMutationOutcome.ALREADY_APPLIED);
            assertThat(clear.state().acknowledgements()).isEqualTo(CursorAckState.empty(20));
            assertThat(clear.state().ackStateEpoch()).isEqualTo(3);
        }
    }

    @Test
    void committedDeleteResponseLossClosesTheOldGenerationOnTombstoneRetry() {
        try (CursorStorageTestSupport.Context context =
                new CursorStorageTestSupport.Context(0, 20)) {
            CursorOwnerSession owner = context.owner(CursorStorageTestSupport.OWNER_1);
            CursorHandle handle = open(context, owner, new InitialCursorPosition.Earliest());
            context.metadataStore.failNext(
                    CursorStorageTestSupport.MetadataOperation.CAS_CURSOR,
                    CursorStorageTestSupport.FaultCut.AFTER,
                    value -> ((CursorStateRecord) value).lifecycle()
                            == CursorRecordLifecycle.DELETED,
                    transientFailure("delete response lost"));

            context.storage.delete(owner, "subscription-a").join();

            assertThat(handle.isClosed()).isTrue();
            assertThatThrownBy(() -> context.storage.individualAck(
                            handle,
                            List.of(new CursorAckRequest(1, Optional.empty(), Map.of())))
                    .join())
                    .hasCauseInstanceOf(
                            org.apache.bookkeeper.mledger.ManagedLedgerException
                                    .CursorAlreadyClosedException.class);
        }
    }

    @Test
    void lostProtectionPendingResponseIsRecoveredBeforeRetryReturnsCursor() {
        try (CursorStorageTestSupport.Context context =
                new CursorStorageTestSupport.Context(0, 20)) {
            CursorOwnerSession owner = context.owner(CursorStorageTestSupport.OWNER_1);
            context.metadataStore.failNext(
                    CursorStorageTestSupport.MetadataOperation.CAS_RETENTION,
                    CursorStorageTestSupport.FaultCut.AFTER,
                    value -> ((CursorRetentionRecord) value).lifecycle()
                            == CursorRetentionLifecycle.PROTECTION_PENDING,
                    transientFailure("protection response lost"));

            assertThatThrownBy(() -> open(
                            context, owner, new InitialCursorPosition.AtOffset(5)))
                    .hasCauseInstanceOf(NereusException.class);
            CursorRetentionView pending = context.storage.retentionView(owner).join();
            assertThat(pending.lifecycle())
                    .isEqualTo(CursorRetentionView.Lifecycle.PROTECTION_PENDING);
            String attemptId = pending.pendingProtection().orElseThrow().attemptId();

            CursorHandle recovered = open(
                    context, owner, new InitialCursorPosition.AtOffset(5));

            assertThat(recovered.state().acknowledgements().markDeleteOffset()).isEqualTo(5);
            assertThat(recovered.state().lastProtectionAttemptId()).isEqualTo(attemptId);
            assertThat(context.storage.retentionView(owner).join().lifecycle())
                    .isEqualTo(CursorRetentionView.Lifecycle.ACTIVE);
            assertThat(context.activationCalls).hasValue(1);
        }
    }

    @Test
    void lostProtectedCursorCasResponseLeavesBarrierUntilExactAttemptRecovery() {
        try (CursorStorageTestSupport.Context context =
                new CursorStorageTestSupport.Context(0, 20)) {
            CursorOwnerSession owner = context.owner(CursorStorageTestSupport.OWNER_1);
            CursorHandle handle = open(context, owner, new InitialCursorPosition.Latest());
            String priorAttempt = handle.state().lastProtectionAttemptId();
            context.metadataStore.failNext(
                    CursorStorageTestSupport.MetadataOperation.CAS_CURSOR,
                    CursorStorageTestSupport.FaultCut.AFTER,
                    value -> {
                        CursorStateRecord state = (CursorStateRecord) value;
                        return state.lifecycle() == CursorRecordLifecycle.ACTIVE
                                && state.markDeleteOffset() == 5
                                && !state.lastProtectionAttemptId().equals(priorAttempt);
                    },
                    transientFailure("protected cursor response lost"));

            assertThatThrownBy(() -> context.storage.reset(
                            handle,
                            new CursorResetRequest(5, Optional.empty(), true, 0, 20))
                    .join())
                    .hasCauseInstanceOf(NereusException.class);
            CursorRetentionView pending = context.storage.retentionView(owner).join();
            assertThat(pending.lifecycle())
                    .isEqualTo(CursorRetentionView.Lifecycle.PROTECTION_PENDING);
            String attemptId = pending.pendingProtection().orElseThrow().attemptId();
            CursorState provedState = new CursorStateHydrator(
                            CursorStorageTestSupport.CLUSTER,
                            context.metadataStore,
                            context.snapshotStore,
                            new CursorStatePersistencePlanner(
                                    CursorStorageTestSupport.CLUSTER, context.config),
                            context.config)
                    .load(context.ledger, "subscription-a")
                    .join()
                    .state();
            CursorHandle provedHandle = new CursorHandle(
                    provedState,
                    owner,
                    context.config.cursorMutationQueueMax(),
                    context.scheduler);
            context.storage.individualAck(
                            provedHandle,
                            List.of(new CursorAckRequest(7, Optional.empty(), Map.of())))
                    .join();
            context.storage.mutateCursorProperties(
                            provedHandle,
                            new CursorPropertyMutation.Put("after-proof", "kept"))
                    .join();

            CursorRetentionView recovered = context.retention.claimAndRecover(owner).join();
            CursorHandle loaded = context.storage
                    .claimAndLoadActiveCursors(owner)
                    .join()
                    .getFirst();

            assertThat(recovered.lifecycle()).isEqualTo(CursorRetentionView.Lifecycle.ACTIVE);
            assertThat(loaded.state().acknowledgements().markDeleteOffset()).isEqualTo(5);
            assertThat(loaded.state().acknowledgements().isWholeEntryAcknowledged(7)).isTrue();
            assertThat(loaded.state().lastProtectionAttemptId()).isEqualTo(attemptId);
            assertThat(loaded.state().cursorProperties())
                    .containsEntry("after-proof", "kept");
        }
    }

    @Test
    void lostProtectionFinalizeResponseIsIdempotentlyObservedOnNextOpen() {
        try (CursorStorageTestSupport.Context context =
                new CursorStorageTestSupport.Context(0, 20)) {
            CursorOwnerSession owner = context.owner(CursorStorageTestSupport.OWNER_1);
            context.metadataStore.failNext(
                    CursorStorageTestSupport.MetadataOperation.CAS_RETENTION,
                    CursorStorageTestSupport.FaultCut.AFTER,
                    value -> ((CursorRetentionRecord) value).lifecycle()
                            == CursorRetentionLifecycle.ACTIVE,
                    transientFailure("protection finalize response lost"));

            assertThatThrownBy(() -> open(
                            context, owner, new InitialCursorPosition.AtOffset(7)))
                    .hasCauseInstanceOf(NereusException.class);
            assertThat(context.storage.retentionView(owner).join().lifecycle())
                    .isEqualTo(CursorRetentionView.Lifecycle.ACTIVE);

            CursorHandle recovered = open(
                    context, owner, new InitialCursorPosition.AtOffset(7));
            assertThat(recovered.state().acknowledgements().markDeleteOffset()).isEqualTo(7);
            assertThat(recovered.identity().cursorGeneration()).isEqualTo(1);
        }
    }

    @Test
    void lostTrimPendingAndFinalizeResponsesRecoverWithoutChangingTheAttempt() {
        try (CursorStorageTestSupport.Context context =
                new CursorStorageTestSupport.Context(0, 20)) {
            CursorOwnerSession owner = context.owner(CursorStorageTestSupport.OWNER_1);
            context.retention.claimAndRecover(owner).join();
            context.retention.reconcileFloor(owner).join();
            context.metadataStore.failNext(
                    CursorStorageTestSupport.MetadataOperation.CAS_RETENTION,
                    CursorStorageTestSupport.FaultCut.AFTER,
                    value -> ((CursorRetentionRecord) value).lifecycle()
                            == CursorRetentionLifecycle.TRIM_PENDING,
                    transientFailure("trim pending response lost"));

            assertThatThrownBy(() -> context.retention
                            .requestTrim(owner, 10, "policy-count")
                            .join())
                    .hasCauseInstanceOf(NereusException.class);
            CursorRetentionView pending = context.storage.retentionView(owner).join();
            String reason = pending.pendingTrim().orElseThrow().composedReason();
            assertThat(context.streamStorage.trims()).isEmpty();

            context.metadataStore.failNext(
                    CursorStorageTestSupport.MetadataOperation.CAS_RETENTION,
                    CursorStorageTestSupport.FaultCut.AFTER,
                    value -> {
                        CursorRetentionRecord record = (CursorRetentionRecord) value;
                        return record.lifecycle() == CursorRetentionLifecycle.ACTIVE
                                && record.lastCompletedTrimOffset() == 10;
                    },
                    transientFailure("trim finalize response lost"));
            assertThatThrownBy(() -> context.retention.claimAndRecover(owner).join())
                    .hasCauseInstanceOf(NereusException.class);

            CursorRetentionView recovered = context.retention.claimAndRecover(owner).join();
            assertThat(recovered.lifecycle()).isEqualTo(CursorRetentionView.Lifecycle.ACTIVE);
            assertThat(recovered.lastCompletedTrimOffset()).isEqualTo(10);
            assertThat(context.streamStorage.trims()).hasSize(1);
            assertThat(context.streamStorage.trims().getFirst().reason()).isEqualTo(reason);
        }
    }

    @Test
    void deleteWinnerFinalizesPendingBackwardResetWithoutResurrection() {
        try (CursorStorageTestSupport.Context context =
                new CursorStorageTestSupport.Context(0, 20)) {
            CursorOwnerSession owner = context.owner(CursorStorageTestSupport.OWNER_1);
            CursorHandle handle = open(context, owner, new InitialCursorPosition.Latest());
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

            context.storage.delete(owner, "subscription-a").join();
            context.metadataStore.failNext(
                    CursorStorageTestSupport.MetadataOperation.CAS_RETENTION,
                    CursorStorageTestSupport.FaultCut.AFTER,
                    value -> ((CursorRetentionRecord) value).lifecycle()
                            == CursorRetentionLifecycle.ACTIVE,
                    transientFailure("delete-winner finalize response lost"));
            assertThatThrownBy(() -> context.retention.claimAndRecover(owner).join())
                    .hasCauseInstanceOf(NereusException.class);
            CursorRetentionView recovered = context.retention.completeProtection(lease).join();
            var root = context.metadataStore.getCursor(
                            CursorStorageTestSupport.CLUSTER,
                            new com.nereusstream.api.StreamId(
                                    context.ledger.projection().streamId()),
                            "subscription-a")
                    .join()
                    .orElseThrow()
                    .value();

            assertThat(recovered.lifecycle()).isEqualTo(CursorRetentionView.Lifecycle.ACTIVE);
            assertThat(recovered.protectedFloorOffset()).isEqualTo(5);
            assertThat(root.lifecycle()).isEqualTo(CursorRecordLifecycle.DELETED);
            assertThat(root.lastProtectionAttemptId())
                    .isEqualTo(lease.attemptId());
        }
    }

    private static CursorHandle open(
            CursorStorageTestSupport.Context context,
            CursorOwnerSession owner,
            InitialCursorPosition position) {
        return context.storage.open(
                        owner,
                        "subscription-a",
                        new CursorOpenRequest(position, Map.of(), Map.of(), 0, 20))
                .join();
    }

    private static NereusException transientFailure(String message) {
        return new NereusException(ErrorCode.METADATA_UNAVAILABLE, true, message);
    }
}
