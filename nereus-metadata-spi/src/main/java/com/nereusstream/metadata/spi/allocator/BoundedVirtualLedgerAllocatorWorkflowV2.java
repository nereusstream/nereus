/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.metadata.spi.allocator;

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.registry.allocator.AllocatorModeV1;
import com.nereusstream.domain.registry.allocator.AllocatorProtocolException;
import com.nereusstream.domain.registry.allocator.AllocatorProtocolV1;
import com.nereusstream.domain.registry.allocator.CellAllocatorReservationV1;
import com.nereusstream.domain.registry.allocator.ManagedLedgerAllocatorHeadV1;
import com.nereusstream.domain.registry.allocator.ManagedLedgerIncarnationIdV1;
import com.nereusstream.domain.registry.allocator.VirtualLedgerCandidateNodeV1;
import com.nereusstream.domain.registry.allocator.VirtualLedgerCellAllocatorStateV1;
import com.nereusstream.metadata.spi.model.ConditionalCasResult;
import com.nereusstream.metadata.spi.model.CreateMutationResult;
import com.nereusstream.metadata.spi.model.VersionedVirtualLedgerSliceViewV1;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Lock-free full rollover workflow. Every retry preserves request/descriptor identity and re-enters through exact
 * store reads; no Java lock or caller-owned aggregate can serialize independent coordinators.
 */
public final class BoundedVirtualLedgerAllocatorWorkflowV2 {
    private final ProductionVirtualLedgerAllocator allocator;
    private final PulsarVirtualLedgerAllocatorStore store;
    private final Bounds bounds;
    private final RetryScheduler retryScheduler;

    BoundedVirtualLedgerAllocatorWorkflowV2(
            ProductionVirtualLedgerAllocator allocator,
            PulsarVirtualLedgerAllocatorStore store,
            Bounds bounds,
            RetryScheduler retryScheduler) {
        this.allocator = Objects.requireNonNull(allocator, "allocator");
        this.store = Objects.requireNonNull(store, "store");
        this.bounds = Objects.requireNonNull(bounds, "bounds");
        this.retryScheduler = Objects.requireNonNull(retryScheduler, "retryScheduler");
    }

    public CompletionStage<Result> allocate(Request request) {
        Objects.requireNonNull(request, "request");
        State state = new State(bounds, request.requestId());
        PulsarVirtualLedgerAllocatorStore guardedStore = new DeadlineGuardedStore(store, state);
        BoundedVirtualLedgerAllocatorWorkflowV2 execution = new BoundedVirtualLedgerAllocatorWorkflowV2(
                allocator.withStore(guardedStore), guardedStore, bounds, retryScheduler);
        return withinWorkflowDeadline(
                invoke(() -> execution.acquireGrant(request, state)),
                state,
                AllocatorProtocolException.Code.WORKFLOW_DEADLINE_EXCEEDED,
                "allocator workflow exceeded its source-governed elapsed deadline");
    }

    public Bounds bounds() {
        return bounds;
    }

    private CompletionStage<Result> acquireGrant(Request request, State state) {
        return readAuthorities(request).thenCompose(authorities -> {
            try {
                requireOriginalHead(request, authorities.head());
            } catch (AllocatorProtocolException failure) {
                if (failure.code() == AllocatorProtocolException.Code.HEAD_STATE_DRIFT
                        && state.canRebaseExpectedHead()) {
                    Request rebased = new Request(
                            request.requestId(), request.descriptorDigest(), request.currentView(), authorities.head());
                    return retry(state, RetryReason.HEAD_REREAD, () -> acquireGrant(rebased, state));
                }
                return failed(failure);
            }
            Optional<CellAllocatorReservationV1> reservation =
                    authorities.cell().value().reservation();
            if (canUseIndependentInstalledRangeGrant(authorities, reservation)) {
                // One Cell reservation installs or renews exactly one Head grant. It must not serialize
                // independent Heads that already own an unconsumed RANGE grant. The exact Cell and Head read above,
                // followed by the request-bound node create and Head CAS, remain the allocation authority proofs.
                return createCandidate(request, state, authorities.cell(), authorities.head(), true);
            }
            if (reservation.isPresent()) {
                if (!reservationMatches(request, reservation.orElseThrow())) {
                    return retry(state, RetryReason.RESERVATION_BUSY, () -> acquireGrant(request, state));
                }
                return afterReserved(request, state, authorities.cell(), authorities.head());
            }
            return handleReserve(
                    request,
                    state,
                    authorities.cell(),
                    authorities.head(),
                    invoke(() -> allocator.reserve(
                            authorities.cell(), authorities.head(), request.currentView(), request.requestId())));
        });
    }

    private CompletionStage<Result> handleReserve(
            Request request,
            State state,
            VersionedAllocatorCellStateV1 predecessor,
            VersionedManagedLedgerAllocatorHeadV1 head,
            CompletionStage<ConditionalCasResult<VersionedAllocatorCellStateV1>> stage) {
        return stage.<CompletionStage<Result>>handle((result, failure) -> {
                    if (failure != null) {
                        Throwable exactFailure = unwrap(failure);
                        if (retryableAcquireFailure(exactFailure)) {
                            return retry(state, RetryReason.CELL_REREAD, () -> acquireGrant(request, state));
                        }
                        return failed(exactFailure);
                    }
                    return switch (result.outcome()) {
                        case APPLIED_EXACT ->
                            afterReserved(request, state, requireExact(result.exactSnapshot(), "reserved Cell"), head);
                        case PREDECESSOR_UNCHANGED, INDETERMINATE ->
                            retry(
                                    state,
                                    RetryReason.CELL_CAS_UNRESOLVED,
                                    () -> handleReserve(
                                            request,
                                            state,
                                            predecessor,
                                            head,
                                            invoke(() -> allocator.reserve(
                                                    predecessor, head, request.currentView(), request.requestId()))));
                        case DEFINITIVE_CONFLICT ->
                            retry(state, RetryReason.CELL_CAS_CONFLICT, () -> acquireGrant(request, state));
                    };
                })
                .thenCompose(value -> value);
    }

    private CompletionStage<Result> afterReserved(
            Request request,
            State state,
            VersionedAllocatorCellStateV1 reserved,
            VersionedManagedLedgerAllocatorHeadV1 head) {
        CellAllocatorReservationV1 reservation = reserved.value()
                .reservation()
                .orElseThrow(() -> failure(
                        AllocatorProtocolException.Code.REQUEST_CONTEXT_DRIFT,
                        "allocator reserve reconciliation lost the request-bound reservation"));
        if (!reservationMatches(request, reservation)) {
            return failed(failure(
                    AllocatorProtocolException.Code.REQUEST_CONTEXT_DRIFT,
                    "allocator reservation request/incarnation/Head context changed"));
        }
        if (reserved.value().mode() == AllocatorModeV1.STRICT_SERIALIZED) {
            return createCandidate(request, state, reserved, head);
        }
        return installRangeGrant(
                request,
                state,
                reserved,
                head,
                invoke(() -> allocator.installRangeReservedGrantAfterStoreObservedAuthorities(
                        reserved, head, request.currentView())));
    }

    private CompletionStage<Result> installRangeGrant(
            Request request,
            State state,
            VersionedAllocatorCellStateV1 reserved,
            VersionedManagedLedgerAllocatorHeadV1 predecessor,
            CompletionStage<ConditionalCasResult<VersionedManagedLedgerAllocatorHeadV1>> stage) {
        return stage.<CompletionStage<Result>>handle((result, failure) -> {
                    if (failure != null) {
                        Throwable exactFailure = unwrap(failure);
                        if (isCode(exactFailure, AllocatorProtocolException.Code.CELL_STATE_DRIFT)) {
                            return retry(
                                    state,
                                    RetryReason.CELL_REREAD,
                                    () -> reconcileInstalledRange(request, state, reserved, predecessor));
                        }
                        return failed(exactFailure);
                    }
                    return switch (result.outcome()) {
                        case APPLIED_EXACT ->
                            clearReservation(
                                    request,
                                    state,
                                    reserved,
                                    requireExact(result.exactSnapshot(), "installed RANGE Head"));
                        case PREDECESSOR_UNCHANGED, INDETERMINATE ->
                            retry(
                                    state,
                                    RetryReason.HEAD_CAS_UNRESOLVED,
                                    () -> installRangeGrant(
                                            request,
                                            state,
                                            reserved,
                                            predecessor,
                                            invoke(() -> allocator.installRangeReservedGrant(
                                                    reserved, predecessor, request.currentView()))));
                        case DEFINITIVE_CONFLICT ->
                            retry(
                                    state,
                                    RetryReason.HEAD_CAS_CONFLICT,
                                    () -> reconcileInstalledRange(request, state, reserved, predecessor));
                    };
                })
                .thenCompose(value -> value);
    }

    private CompletionStage<Result> reconcileInstalledRange(
            Request request,
            State state,
            VersionedAllocatorCellStateV1 reserved,
            VersionedManagedLedgerAllocatorHeadV1 predecessor) {
        return readAuthorities(request).thenCompose(authorities -> {
            CellAllocatorReservationV1 reservation =
                    reserved.value().reservation().orElseThrow();
            if (!headContainsGrant(authorities.head().value(), predecessor.value(), reservation)) {
                return failed(headDrift(request, authorities.head()));
            }
            if (authorities.cell().equals(reserved)) {
                return clearReservation(request, state, reserved, authorities.head());
            }
            if (reservationStillPresent(authorities.cell(), request)) {
                return retry(
                        state,
                        RetryReason.CELL_REREAD,
                        () -> reconcileInstalledRange(request, state, reserved, predecessor));
            }
            requireRefreshedRangeCell(request, authorities.cell(), authorities.head());
            return createCandidate(request, state, authorities.cell(), authorities.head());
        });
    }

    private CompletionStage<Result> clearReservation(
            Request request,
            State state,
            VersionedAllocatorCellStateV1 reserved,
            VersionedManagedLedgerAllocatorHeadV1 terminalHead) {
        CompletionStage<ConditionalCasResult<VersionedAllocatorCellStateV1>> stage =
                reserved.value().mode() == AllocatorModeV1.RANGE_LEASED
                        ? invoke(() ->
                                allocator.clearRangeReservationAfterStoreObservedInstalledHead(reserved, terminalHead))
                        : invoke(() -> allocator.clearReservation(reserved, terminalHead));
        return clearReservation(request, state, reserved, terminalHead, stage);
    }

    private CompletionStage<Result> clearReservation(
            Request request,
            State state,
            VersionedAllocatorCellStateV1 reserved,
            VersionedManagedLedgerAllocatorHeadV1 terminalHead,
            CompletionStage<ConditionalCasResult<VersionedAllocatorCellStateV1>> stage) {
        return stage.<CompletionStage<Result>>handle((result, failure) -> {
                    if (failure != null) {
                        Throwable exactFailure = unwrap(failure);
                        if (isCode(exactFailure, AllocatorProtocolException.Code.CELL_STATE_DRIFT)) {
                            return retry(
                                    state,
                                    RetryReason.CELL_REREAD,
                                    () -> reconcileCleared(request, state, reserved, terminalHead));
                        }
                        return failed(exactFailure);
                    }
                    return switch (result.outcome()) {
                        case APPLIED_EXACT ->
                            afterClear(
                                    request, state, requireExact(result.exactSnapshot(), "cleared Cell"), terminalHead);
                        case PREDECESSOR_UNCHANGED, INDETERMINATE ->
                            retry(
                                    state,
                                    RetryReason.CELL_CAS_UNRESOLVED,
                                    () -> clearReservation(
                                            request,
                                            state,
                                            reserved,
                                            terminalHead,
                                            invoke(() -> allocator.clearReservation(reserved, terminalHead))));
                        case DEFINITIVE_CONFLICT ->
                            retry(
                                    state,
                                    RetryReason.CELL_CAS_CONFLICT,
                                    () -> reconcileCleared(request, state, reserved, terminalHead));
                    };
                })
                .thenCompose(value -> value);
    }

    private CompletionStage<Result> reconcileCleared(
            Request request,
            State state,
            VersionedAllocatorCellStateV1 reserved,
            VersionedManagedLedgerAllocatorHeadV1 terminalHead) {
        return store.readCell(
                        reserved.value().ledgerIdCompatibilityNamespaceId(),
                        reserved.value().sliceAssignmentId())
                .thenCompose(observed -> {
                    VersionedAllocatorCellStateV1 current = observed.orElseThrow(() -> failure(
                            AllocatorProtocolException.Code.CELL_STATE_DRIFT,
                            "allocator Cell disappeared while reconciling reservation clear"));
                    if (reservationStillPresent(current, request)) {
                        return retry(
                                state,
                                RetryReason.CELL_REREAD,
                                () -> clearReservation(request, state, reserved, terminalHead));
                    }
                    if (reserved.value().mode() == AllocatorModeV1.RANGE_LEASED) {
                        requireRefreshedRangeCell(request, current, terminalHead);
                    } else {
                        requireSameConsumedPrefix(reserved, current);
                    }
                    return afterClear(request, state, current, terminalHead);
                });
    }

    private CompletionStage<Result> afterClear(
            Request request,
            State state,
            VersionedAllocatorCellStateV1 cell,
            VersionedManagedLedgerAllocatorHeadV1 head) {
        if (cell.value().mode() == AllocatorModeV1.STRICT_SERIALIZED) {
            if (state.exactNode == null) {
                return failed(failure(
                        AllocatorProtocolException.Code.CANDIDATE_OCCUPANCY_NOT_PROVEN,
                        "STRICT terminal clear completed without the exact request node"));
            }
            return CompletableFuture.completedFuture(new Result(cell, head, state.exactNode, state.reconcileRetries));
        }
        return createCandidate(request, state, cell, head, true);
    }

    private CompletionStage<Result> createCandidate(
            Request request,
            State state,
            VersionedAllocatorCellStateV1 cell,
            VersionedManagedLedgerAllocatorHeadV1 head) {
        return createCandidate(request, state, cell, head, false);
    }

    private CompletionStage<Result> createCandidate(
            Request request,
            State state,
            VersionedAllocatorCellStateV1 cell,
            VersionedManagedLedgerAllocatorHeadV1 head,
            boolean storeObservedRangeAuthorities) {
        VirtualLedgerCandidateNodeV1 expected = expectedCandidate(request, cell, head);
        if (state.candidateValue != null && !state.candidateValue.equals(expected)) {
            return failed(failure(
                    AllocatorProtocolException.Code.DESCRIPTOR_MISMATCH,
                    "allocator candidate identity changed while reconciling one request"));
        }
        state.candidateValue = expected;
        return createCandidate(
                request,
                state,
                cell,
                head,
                storeObservedRangeAuthorities,
                invoke(() -> storeObservedRangeAuthorities
                        ? allocator.createCandidateAfterStoreObservedRangeAuthorities(
                                cell, head, request.currentView(), request.descriptorDigest())
                        : allocator.createCandidate(cell, head, request.currentView(), request.descriptorDigest())));
    }

    private CompletionStage<Result> createCandidate(
            Request request,
            State state,
            VersionedAllocatorCellStateV1 cell,
            VersionedManagedLedgerAllocatorHeadV1 head,
            boolean storeObservedRangeAuthorities,
            CompletionStage<CreateMutationResult<VersionedVirtualLedgerCandidateNodeV1>> stage) {
        return stage.<CompletionStage<Result>>handle((result, failure) -> {
                    if (failure != null) {
                        Throwable exactFailure = unwrap(failure);
                        if (cell.value().mode() == AllocatorModeV1.RANGE_LEASED
                                && isCode(exactFailure, AllocatorProtocolException.Code.HEAD_STATE_DRIFT)
                                && state.canAbandonUnpersistedCandidate()) {
                            state.abandonUnpersistedCandidate();
                            return retry(state, RetryReason.HEAD_REREAD, () -> acquireGrant(request, state));
                        }
                        if (cell.value().mode() == AllocatorModeV1.RANGE_LEASED
                                && isCode(exactFailure, AllocatorProtocolException.Code.CELL_STATE_DRIFT)) {
                            return retry(
                                    state,
                                    RetryReason.CELL_REREAD,
                                    () -> refreshRangeCellForCandidate(request, state, head));
                        }
                        return failed(exactFailure);
                    }
                    return switch (result.outcome()) {
                        case CREATED, EXISTING_EXACT -> {
                            state.candidateMutationMayHavePersisted();
                            yield publishCandidate(
                                    request,
                                    state,
                                    cell,
                                    head,
                                    storeObservedRangeAuthorities,
                                    requireExactNode(result.exactSnapshot(), state.candidateValue));
                        }
                        case INDETERMINATE -> {
                            state.candidateMutationMayHavePersisted();
                            yield retry(
                                    state,
                                    RetryReason.NODE_CREATE_UNRESOLVED,
                                    () -> createCandidate(
                                            request,
                                            state,
                                            cell,
                                            head,
                                            storeObservedRangeAuthorities,
                                            invoke(() -> storeObservedRangeAuthorities
                                                    ? allocator.createCandidateAfterStoreObservedRangeAuthorities(
                                                            cell,
                                                            head,
                                                            request.currentView(),
                                                            request.descriptorDigest())
                                                    : allocator.createCandidate(
                                                            cell,
                                                            head,
                                                            request.currentView(),
                                                            request.descriptorDigest()))));
                        }
                        case DEFINITIVE_CONFLICT -> {
                            state.candidateMutationDefinitivelyDidNotPersist();
                            yield retry(
                                    state,
                                    RetryReason.NODE_CREATE_CONFLICT,
                                    () -> reconcileNode(request, state, cell, head));
                        }
                    };
                })
                .thenCompose(value -> value);
    }

    private CompletionStage<Result> refreshRangeCellForCandidate(
            Request request, State state, VersionedManagedLedgerAllocatorHeadV1 head) {
        return store.readCell(
                        request.currentView().value().ledgerIdCompatibilityNamespaceId(),
                        request.currentView().value().assignment().sliceAssignmentId())
                .thenCompose(observed -> {
                    VersionedAllocatorCellStateV1 current = observed.orElseThrow(() -> failure(
                            AllocatorProtocolException.Code.CELL_STATE_DRIFT,
                            "allocator Cell disappeared while refreshing RANGE candidate proof"));
                    requireRefreshedRangeCell(request, current, head);
                    return createCandidate(request, state, current, head);
                });
    }

    private CompletionStage<Result> reconcileNode(
            Request request,
            State state,
            VersionedAllocatorCellStateV1 cell,
            VersionedManagedLedgerAllocatorHeadV1 head) {
        VirtualLedgerCandidateNodeV1 expected = Objects.requireNonNull(state.candidateValue, "candidateValue");
        return store.readNode(
                        cell.value().ledgerIdCompatibilityNamespaceId(),
                        cell.value().sliceAssignmentId(),
                        expected.managedLedgerIncarnation(),
                        expected.ledgerId())
                .thenCompose(observed -> {
                    if (observed.isEmpty()) {
                        return retry(state, RetryReason.NODE_REREAD, () -> createCandidate(request, state, cell, head));
                    }
                    VersionedVirtualLedgerCandidateNodeV1 exact = observed.orElseThrow();
                    if (!exact.value().equals(expected)) {
                        if (cell.value().mode() != AllocatorModeV1.RANGE_LEASED) {
                            return failed(descriptorMismatch());
                        }
                        return reconcileCompetingRangeCandidate(request, state, cell, head, exact, false);
                    }
                    return publishCandidate(request, state, cell, head, exact);
                });
    }

    private CompletionStage<Result> reconcileCompetingRangeCandidate(
            Request request,
            State state,
            VersionedAllocatorCellStateV1 cell,
            VersionedManagedLedgerAllocatorHeadV1 predecessor,
            VersionedVirtualLedgerCandidateNodeV1 competingNode,
            boolean publicationRereadUsed) {
        ManagedLedgerAllocatorHeadV1 competingSuccessor;
        try {
            competingSuccessor = AllocatorProtocolV1.publish(predecessor.value(), competingNode.value());
        } catch (RuntimeException invalidCompetingNode) {
            return failed(descriptorMismatch());
        }
        return store.readHead(
                        cell.value().ledgerIdCompatibilityNamespaceId(),
                        cell.value().sliceAssignmentId(),
                        predecessor.value().managedLedgerIncarnation())
                .thenCompose(observed -> {
                    VersionedManagedLedgerAllocatorHeadV1 current = observed.orElseThrow(() -> failure(
                            AllocatorProtocolException.Code.HEAD_STATE_DRIFT,
                            "allocator Head disappeared while reconciling a competing RANGE candidate"));
                    if (current.value().equals(competingSuccessor)) {
                        state.abandonUnpersistedCandidate();
                        Request rebased = new Request(
                                request.requestId(), request.descriptorDigest(), request.currentView(), current);
                        return retry(state, RetryReason.HEAD_REREAD, () -> acquireGrant(rebased, state));
                    }
                    if (!publicationRereadUsed && current.value().equals(predecessor.value())) {
                        return retry(
                                state,
                                RetryReason.HEAD_REREAD,
                                () -> reconcileCompetingRangeCandidate(
                                        request, state, cell, predecessor, competingNode, true));
                    }
                    return failed(descriptorMismatch());
                });
    }

    private CompletionStage<Result> publishCandidate(
            Request request,
            State state,
            VersionedAllocatorCellStateV1 cell,
            VersionedManagedLedgerAllocatorHeadV1 predecessor,
            VersionedVirtualLedgerCandidateNodeV1 node) {
        return publishCandidate(request, state, cell, predecessor, false, node);
    }

    private CompletionStage<Result> publishCandidate(
            Request request,
            State state,
            VersionedAllocatorCellStateV1 cell,
            VersionedManagedLedgerAllocatorHeadV1 predecessor,
            boolean storeObservedRangeAuthorities,
            VersionedVirtualLedgerCandidateNodeV1 node) {
        state.exactNode = node;
        ManagedLedgerAllocatorHeadV1 expectedHead = cell.value().mode() == AllocatorModeV1.STRICT_SERIALIZED
                ? AllocatorProtocolV1.publishStrictReserved(cell.value(), predecessor.value(), node.value())
                : AllocatorProtocolV1.publish(predecessor.value(), node.value());
        return publishCandidate(
                request,
                state,
                cell,
                predecessor,
                node,
                expectedHead,
                storeObservedRangeAuthorities,
                invoke(() -> storeObservedRangeAuthorities
                        ? allocator.publishCandidateAfterStoreObservedRangeNode(
                                cell, predecessor, node, request.currentView())
                        : allocator.publishCandidate(cell, predecessor, node, request.currentView())));
    }

    private CompletionStage<Result> publishCandidate(
            Request request,
            State state,
            VersionedAllocatorCellStateV1 cell,
            VersionedManagedLedgerAllocatorHeadV1 predecessor,
            VersionedVirtualLedgerCandidateNodeV1 node,
            ManagedLedgerAllocatorHeadV1 expectedHead,
            boolean storeObservedRangeAuthorities,
            CompletionStage<ConditionalCasResult<VersionedManagedLedgerAllocatorHeadV1>> stage) {
        return stage.<CompletionStage<Result>>handle((result, failure) -> {
                    if (failure != null) {
                        Throwable exactFailure = unwrap(failure);
                        if (cell.value().mode() == AllocatorModeV1.RANGE_LEASED
                                && isCode(exactFailure, AllocatorProtocolException.Code.CELL_STATE_DRIFT)) {
                            return retry(
                                    state,
                                    RetryReason.CELL_REREAD,
                                    () -> refreshRangeCellForPublish(request, state, predecessor, node, expectedHead));
                        }
                        return failed(exactFailure);
                    }
                    return switch (result.outcome()) {
                        case APPLIED_EXACT ->
                            afterPublish(
                                    request, state, cell, requireExactHead(result.exactSnapshot(), expectedHead), node);
                        case PREDECESSOR_UNCHANGED, INDETERMINATE ->
                            retry(
                                    state,
                                    RetryReason.HEAD_CAS_UNRESOLVED,
                                    () -> publishCandidate(
                                            request,
                                            state,
                                            cell,
                                            predecessor,
                                            node,
                                            expectedHead,
                                            storeObservedRangeAuthorities,
                                            invoke(() -> storeObservedRangeAuthorities
                                                    ? allocator.publishCandidateAfterStoreObservedRangeNode(
                                                            cell, predecessor, node, request.currentView())
                                                    : allocator.publishCandidate(
                                                            cell, predecessor, node, request.currentView()))));
                        case DEFINITIVE_CONFLICT ->
                            retry(
                                    state,
                                    RetryReason.HEAD_CAS_CONFLICT,
                                    () -> reconcilePublishedHead(request, state, cell, node, expectedHead));
                    };
                })
                .thenCompose(value -> value);
    }

    private CompletionStage<Result> refreshRangeCellForPublish(
            Request request,
            State state,
            VersionedManagedLedgerAllocatorHeadV1 predecessor,
            VersionedVirtualLedgerCandidateNodeV1 node,
            ManagedLedgerAllocatorHeadV1 expectedHead) {
        return store.readCell(
                        request.currentView().value().ledgerIdCompatibilityNamespaceId(),
                        request.currentView().value().assignment().sliceAssignmentId())
                .thenCompose(observed -> {
                    VersionedAllocatorCellStateV1 current = observed.orElseThrow(() -> failure(
                            AllocatorProtocolException.Code.CELL_STATE_DRIFT,
                            "allocator Cell disappeared while refreshing RANGE publish proof"));
                    requireRefreshedRangeCell(request, current, predecessor);
                    return publishCandidate(
                            request,
                            state,
                            current,
                            predecessor,
                            node,
                            expectedHead,
                            false,
                            invoke(() ->
                                    allocator.publishCandidate(current, predecessor, node, request.currentView())));
                });
    }

    private CompletionStage<Result> reconcilePublishedHead(
            Request request,
            State state,
            VersionedAllocatorCellStateV1 cell,
            VersionedVirtualLedgerCandidateNodeV1 node,
            ManagedLedgerAllocatorHeadV1 expectedHead) {
        return store.readHead(
                        cell.value().ledgerIdCompatibilityNamespaceId(),
                        cell.value().sliceAssignmentId(),
                        node.value().managedLedgerIncarnation())
                .thenCompose(
                        observed -> afterPublish(request, state, cell, requireExactHead(observed, expectedHead), node));
    }

    private CompletionStage<Result> afterPublish(
            Request request,
            State state,
            VersionedAllocatorCellStateV1 cell,
            VersionedManagedLedgerAllocatorHeadV1 published,
            VersionedVirtualLedgerCandidateNodeV1 node) {
        if (cell.value().mode() == AllocatorModeV1.STRICT_SERIALIZED) {
            return clearReservation(request, state, cell, published);
        }
        return CompletableFuture.completedFuture(new Result(cell, published, node, state.reconcileRetries));
    }

    private CompletionStage<Authorities> readAuthorities(Request request) {
        CompletionStage<Optional<VersionedAllocatorCellStateV1>> cell = store.readCell(
                request.currentView().value().ledgerIdCompatibilityNamespaceId(),
                request.currentView().value().assignment().sliceAssignmentId());
        CompletionStage<Optional<VersionedManagedLedgerAllocatorHeadV1>> head = store.readHead(
                request.expectedHead().ledgerIdCompatibilityNamespaceId(),
                request.expectedHead().sliceAssignmentId(),
                request.expectedHead().value().managedLedgerIncarnation());
        return cell.thenCombine(
                head,
                (exactCell, exactHead) -> new Authorities(
                        exactCell.orElseThrow(() -> failure(
                                AllocatorProtocolException.Code.CELL_STATE_DRIFT,
                                "allocator Cell authority is absent")),
                        exactHead.orElseThrow(() -> failure(
                                AllocatorProtocolException.Code.HEAD_STATE_DRIFT,
                                "allocator Head authority is absent"))));
    }

    private CompletionStage<Result> retry(State state, RetryReason reason, Supplier<CompletionStage<Result>> retry) {
        if (state.reconcileRetries == bounds.maximumReconcileRetries()) {
            return failed(failure(
                    AllocatorProtocolException.Code.RECONCILE_RETRY_EXHAUSTED,
                    "allocator bounded reconcile retry budget exhausted at " + reason));
        }
        long remainingNanos = state.remainingNanos();
        if (remainingNanos <= 0) {
            return failed(failure(
                    AllocatorProtocolException.Code.WORKFLOW_DEADLINE_EXCEEDED,
                    "allocator workflow deadline elapsed before retry " + reason));
        }
        state.reconcileRetries++;
        long backoffNanos =
                Math.min(remainingNanos, bounds.maximumRetryBackoff().toNanos());
        return withinDeadline(
                        invoke(() -> retryScheduler.beforeRetry(state.requestId, state.reconcileRetries, reason)),
                        backoffNanos,
                        AllocatorProtocolException.Code.RETRY_BACKOFF_EXCEEDED,
                        "allocator retry backoff exceeded its source-governed bound at " + reason)
                .thenCompose(ignored -> invoke(retry));
    }

    private static void requireOriginalHead(Request request, VersionedManagedLedgerAllocatorHeadV1 observed) {
        if (!observed.value()
                .managedLedgerIncarnation()
                .equals(request.expectedHead().value().managedLedgerIncarnation())) {
            throw failure(
                    AllocatorProtocolException.Code.REQUEST_CONTEXT_DRIFT,
                    "allocator ManagedLedger incarnation changed");
        }
        if (observed.value().ownerEpoch() != request.expectedHead().value().ownerEpoch()) {
            throw failure(AllocatorProtocolException.Code.OWNER_FENCED, "allocator request owner is stale");
        }
        if (!observed.equals(request.expectedHead())) {
            throw failure(
                    AllocatorProtocolException.Code.HEAD_STATE_DRIFT,
                    "allocator request Head predecessor changed before reservation");
        }
    }

    private static AllocatorProtocolException headDrift(
            Request request, VersionedManagedLedgerAllocatorHeadV1 observed) {
        if (observed.value().ownerEpoch() != request.expectedHead().value().ownerEpoch()) {
            return failure(AllocatorProtocolException.Code.OWNER_FENCED, "allocator request owner is stale");
        }
        return failure(
                AllocatorProtocolException.Code.HEAD_STATE_DRIFT,
                "allocator Head changed to a different candidate while reconciling");
    }

    private static boolean reservationMatches(Request request, CellAllocatorReservationV1 reservation) {
        return reservation
                        .managedLedgerIncarnation()
                        .equals(request.expectedHead().value().managedLedgerIncarnation())
                && reservation.requestId().equals(request.requestId())
                && reservation
                        .expectedAllocationState()
                        .equals(request.expectedHead().value().allocationState());
    }

    private static boolean reservationStillPresent(VersionedAllocatorCellStateV1 cell, Request request) {
        return cell.value()
                .reservation()
                .filter(value -> reservationMatches(request, value))
                .isPresent();
    }

    private static boolean hasUsableGrant(ManagedLedgerAllocatorHeadV1 head) {
        return head.grantId() != 0 && head.nextLedgerId() < head.rangeEndExclusive();
    }

    private static boolean canUseIndependentInstalledRangeGrant(
            Authorities authorities, Optional<CellAllocatorReservationV1> reservation) {
        return authorities.cell().value().mode() == AllocatorModeV1.RANGE_LEASED
                && hasUsableGrant(authorities.head().value())
                && reservation
                        .map(value -> !value.managedLedgerIncarnation()
                                .equals(authorities.head().value().managedLedgerIncarnation()))
                        .orElse(true);
    }

    private static boolean headContainsGrant(
            ManagedLedgerAllocatorHeadV1 observed,
            ManagedLedgerAllocatorHeadV1 predecessor,
            CellAllocatorReservationV1 reservation) {
        return observed.managedLedgerIncarnation().equals(predecessor.managedLedgerIncarnation())
                && observed.ownerEpoch() == predecessor.ownerEpoch()
                && observed.visibleChainHead().equals(predecessor.visibleChainHead())
                && observed.grantId() == reservation.grantId()
                && observed.rangeStartInclusive() == reservation.rangeStartInclusive()
                && observed.rangeEndExclusive() == reservation.rangeEndExclusive()
                && observed.nextLedgerId() == reservation.rangeStartInclusive();
    }

    private static VirtualLedgerCandidateNodeV1 expectedCandidate(
            Request request, VersionedAllocatorCellStateV1 cell, VersionedManagedLedgerAllocatorHeadV1 head) {
        return cell.value().mode() == AllocatorModeV1.STRICT_SERIALIZED
                ? AllocatorProtocolV1.strictCandidateFromReservation(
                        cell.value(), head.value(), request.descriptorDigest())
                : AllocatorProtocolV1.candidate(head.value(), request.descriptorDigest());
    }

    private static void requireRefreshedRangeCell(
            Request request, VersionedAllocatorCellStateV1 cell, VersionedManagedLedgerAllocatorHeadV1 head) {
        if (cell.value().mode() != AllocatorModeV1.RANGE_LEASED) {
            throw failure(
                    AllocatorProtocolException.Code.MODE_MISMATCH, "allocator RANGE reconcile observed another mode");
        }
        AllocatorProtocolV1.requireCurrentActiveSlice(
                cell.value(), request.currentView().value());
        AllocatorProtocolV1.requireHeadWithinConsumedSlicePrefix(cell.value(), head.value());
    }

    private static void requireSameConsumedPrefix(
            VersionedAllocatorCellStateV1 predecessor, VersionedAllocatorCellStateV1 current) {
        if (predecessor.value().mode() != current.value().mode()
                || !predecessor
                        .value()
                        .ledgerIdCompatibilityNamespaceId()
                        .equals(current.value().ledgerIdCompatibilityNamespaceId())
                || !predecessor
                        .value()
                        .sliceAssignmentId()
                        .equals(current.value().sliceAssignmentId())
                || predecessor.value().sliceStartInclusive() != current.value().sliceStartInclusive()
                || predecessor.value().sliceEndInclusive() != current.value().sliceEndInclusive()
                || current.value().nextSliceLedgerId() < predecessor.value().nextSliceLedgerId()
                || current.value().nextGrantId() < predecessor.value().nextGrantId()) {
            throw failure(
                    AllocatorProtocolException.Code.REQUEST_CONTEXT_DRIFT,
                    "allocator Cell slice/context changed while reconciling clear");
        }
    }

    private static VersionedVirtualLedgerCandidateNodeV1 requireExactNode(
            Optional<VersionedVirtualLedgerCandidateNodeV1> snapshot, VirtualLedgerCandidateNodeV1 expected) {
        VersionedVirtualLedgerCandidateNodeV1 exact = requireExact(snapshot, "candidate node");
        if (!exact.value().equals(expected)) {
            throw descriptorMismatch();
        }
        return exact;
    }

    private static AllocatorProtocolException descriptorMismatch() {
        return failure(
                AllocatorProtocolException.Code.DESCRIPTOR_MISMATCH,
                "allocator existing candidate has another descriptor or request identity");
    }

    private static VersionedManagedLedgerAllocatorHeadV1 requireExactHead(
            Optional<VersionedManagedLedgerAllocatorHeadV1> snapshot, ManagedLedgerAllocatorHeadV1 expected) {
        VersionedManagedLedgerAllocatorHeadV1 exact = requireExact(snapshot, "published Head");
        if (!exact.value().equals(expected)) {
            throw failure(
                    AllocatorProtocolException.Code.HEAD_STATE_DRIFT,
                    "allocator Head contains another candidate or context");
        }
        return exact;
    }

    private static <T> T requireExact(Optional<T> snapshot, String label) {
        return snapshot.orElseThrow(
                () -> new IllegalStateException("allocator exact " + label + " snapshot is absent"));
    }

    private static boolean retryableAcquireFailure(Throwable failure) {
        return isCode(failure, AllocatorProtocolException.Code.CELL_STATE_DRIFT)
                || isCode(failure, AllocatorProtocolException.Code.RESERVATION_BUSY);
    }

    private static boolean isCode(Throwable failure, AllocatorProtocolException.Code code) {
        return failure instanceof AllocatorProtocolException protocolFailure && protocolFailure.code() == code;
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static <T> CompletionStage<T> invoke(Supplier<CompletionStage<T>> invocation) {
        try {
            return Objects.requireNonNull(invocation.get(), "allocator asynchronous stage");
        } catch (RuntimeException failure) {
            return failed(failure);
        }
    }

    private static <T> CompletionStage<T> withinDeadline(
            CompletionStage<T> source, long timeoutNanos, AllocatorProtocolException.Code code, String message) {
        if (timeoutNanos <= 0) {
            return failed(failure(code, message));
        }
        CompletableFuture<T> bounded = new CompletableFuture<>();
        source.whenComplete((value, failure) -> {
            if (failure == null) {
                bounded.complete(value);
            } else {
                bounded.completeExceptionally(unwrap(failure));
            }
        });
        CompletableFuture.delayedExecutor(timeoutNanos, TimeUnit.NANOSECONDS)
                .execute(() -> bounded.completeExceptionally(failure(code, message)));
        return bounded;
    }

    private static <T> CompletionStage<T> withinWorkflowDeadline(
            CompletionStage<T> source, State state, AllocatorProtocolException.Code code, String message) {
        long timeoutNanos = state.remainingNanos();
        if (timeoutNanos <= 0) {
            state.terminate();
            return failed(failure(code, message));
        }
        CompletableFuture<T> bounded = new CompletableFuture<>();
        source.whenComplete((value, sourceFailure) -> {
            if (state.terminate()) {
                if (sourceFailure == null) {
                    bounded.complete(value);
                } else {
                    bounded.completeExceptionally(unwrap(sourceFailure));
                }
            }
        });
        CompletableFuture.delayedExecutor(timeoutNanos, TimeUnit.NANOSECONDS).execute(() -> {
            if (state.terminate()) {
                bounded.completeExceptionally(failure(code, message));
            }
        });
        return bounded;
    }

    private static <T> CompletionStage<T> failed(Throwable failure) {
        return CompletableFuture.failedFuture(failure);
    }

    private static AllocatorProtocolException failure(AllocatorProtocolException.Code code, String message) {
        return new AllocatorProtocolException(code, message);
    }

    public record Request(
            Sha256Digest requestId,
            Sha256Digest descriptorDigest,
            VersionedVirtualLedgerSliceViewV1 currentView,
            VersionedManagedLedgerAllocatorHeadV1 expectedHead) {
        public Request {
            Objects.requireNonNull(requestId, "requestId");
            Objects.requireNonNull(descriptorDigest, "descriptorDigest");
            Objects.requireNonNull(currentView, "currentView");
            Objects.requireNonNull(expectedHead, "expectedHead");
            if (requestId.isZero() || descriptorDigest.isZero()) {
                throw new IllegalArgumentException("allocator request and descriptor digests must be non-zero");
            }
            if (!currentView
                            .value()
                            .ledgerIdCompatibilityNamespaceId()
                            .equals(expectedHead.ledgerIdCompatibilityNamespaceId())
                    || !currentView.value().assignment().sliceAssignmentId().equals(expectedHead.sliceAssignmentId())) {
                throw failure(
                        AllocatorProtocolException.Code.REQUEST_CONTEXT_DRIFT,
                        "allocator request slice view and Head provenance differ");
            }
        }
    }

    public record Result(
            VersionedAllocatorCellStateV1 exactCell,
            VersionedManagedLedgerAllocatorHeadV1 exactHead,
            VersionedVirtualLedgerCandidateNodeV1 exactNode,
            int reconcileRetries) {
        public Result {
            Objects.requireNonNull(exactCell, "exactCell");
            Objects.requireNonNull(exactHead, "exactHead");
            Objects.requireNonNull(exactNode, "exactNode");
            if (reconcileRetries < 0
                    || !exactHead
                            .value()
                            .visibleChainHead()
                            .equals(exactNode.value().pointer())) {
                throw new IllegalArgumentException("allocator workflow result is not one exact published candidate");
            }
        }
    }

    public enum RetryReason {
        RESERVATION_BUSY,
        HEAD_REREAD,
        CELL_REREAD,
        CELL_CAS_UNRESOLVED,
        CELL_CAS_CONFLICT,
        HEAD_CAS_UNRESOLVED,
        HEAD_CAS_CONFLICT,
        NODE_CREATE_UNRESOLVED,
        NODE_CREATE_CONFLICT,
        NODE_REREAD
    }

    @FunctionalInterface
    public interface RetryScheduler {
        CompletionStage<Void> beforeRetry(Sha256Digest requestId, int retryNumber, RetryReason reason);

        static RetryScheduler immediate() {
            return (requestId, retryNumber, reason) -> CompletableFuture.completedFuture(null);
        }
    }

    /** Source-governed bounds used by every formal actor coordinator. */
    public record Bounds(int maximumReconcileRetries, Duration totalElapsedDeadline, Duration maximumRetryBackoff) {
        private static final Bounds FORMAL = new Bounds(64, Duration.ofSeconds(4), Duration.ofMillis(25));

        public Bounds {
            Objects.requireNonNull(totalElapsedDeadline, "totalElapsedDeadline");
            Objects.requireNonNull(maximumRetryBackoff, "maximumRetryBackoff");
            if (maximumReconcileRetries < 0
                    || maximumReconcileRetries > 64
                    || totalElapsedDeadline.isZero()
                    || totalElapsedDeadline.isNegative()
                    || totalElapsedDeadline.compareTo(Duration.ofSeconds(4)) > 0
                    || maximumRetryBackoff.isZero()
                    || maximumRetryBackoff.isNegative()
                    || maximumRetryBackoff.compareTo(Duration.ofMillis(25)) > 0
                    || maximumRetryBackoff.compareTo(totalElapsedDeadline) > 0) {
                throw new IllegalArgumentException("allocator workflow bounds differ from the source-governed caps");
            }
        }

        public static Bounds formal() {
            return FORMAL;
        }
    }

    private record Authorities(VersionedAllocatorCellStateV1 cell, VersionedManagedLedgerAllocatorHeadV1 head) {}

    private static final class State {
        private final Sha256Digest requestId;
        private final long deadlineNanos;
        private final AtomicBoolean terminal = new AtomicBoolean();
        private int reconcileRetries;
        private VirtualLedgerCandidateNodeV1 candidateValue;
        private VersionedVirtualLedgerCandidateNodeV1 exactNode;
        private boolean candidateMutationMayHavePersisted;

        private State(Bounds bounds, Sha256Digest requestId) {
            this.requestId = Objects.requireNonNull(requestId, "requestId");
            deadlineNanos = Math.addExact(
                    System.nanoTime(), bounds.totalElapsedDeadline().toNanos());
        }

        private long remainingNanos() {
            return deadlineNanos - System.nanoTime();
        }

        private boolean canRebaseExpectedHead() {
            return candidateValue == null && exactNode == null;
        }

        private boolean canAbandonUnpersistedCandidate() {
            return candidateValue != null && exactNode == null && !candidateMutationMayHavePersisted;
        }

        private void candidateMutationMayHavePersisted() {
            candidateMutationMayHavePersisted = true;
        }

        private void candidateMutationDefinitivelyDidNotPersist() {
            candidateMutationMayHavePersisted = false;
        }

        private void abandonUnpersistedCandidate() {
            if (!canAbandonUnpersistedCandidate()) {
                throw new IllegalStateException("allocator candidate rebase did not prove an unpersisted request");
            }
            candidateValue = null;
        }

        private void requireAuthorized() {
            if (terminal.get() || remainingNanos() <= 0) {
                throw failure(
                        AllocatorProtocolException.Code.WORKFLOW_DEADLINE_EXCEEDED,
                        "allocator workflow is no longer authorized after its elapsed deadline");
            }
        }

        private boolean terminate() {
            return terminal.compareAndSet(false, true);
        }
    }

    private static final class DeadlineGuardedStore implements PulsarVirtualLedgerAllocatorStore {
        private final PulsarVirtualLedgerAllocatorStore delegate;
        private final State state;

        private DeadlineGuardedStore(PulsarVirtualLedgerAllocatorStore delegate, State state) {
            this.delegate = Objects.requireNonNull(delegate, "delegate");
            this.state = Objects.requireNonNull(state, "state");
        }

        @Override
        public CompletionStage<Optional<VersionedAllocatorCellStateV1>> readCell(
                Sha256Digest namespaceId, Sha256Digest sliceAssignmentId) {
            return authorized(() -> delegate.readCell(namespaceId, sliceAssignmentId));
        }

        @Override
        public CompletionStage<CreateMutationResult<VersionedAllocatorCellStateV1>> createCell(
                VirtualLedgerCellAllocatorStateV1 candidate) {
            return authorized(() -> delegate.createCell(candidate));
        }

        @Override
        public CompletionStage<ConditionalCasResult<VersionedAllocatorCellStateV1>> compareAndSetCell(
                VersionedAllocatorCellStateV1 exactPredecessor, VirtualLedgerCellAllocatorStateV1 candidate) {
            return authorized(() -> delegate.compareAndSetCell(exactPredecessor, candidate));
        }

        @Override
        public CompletionStage<Optional<VersionedManagedLedgerAllocatorHeadV1>> readHead(
                Sha256Digest namespaceId,
                Sha256Digest sliceAssignmentId,
                ManagedLedgerIncarnationIdV1 managedLedgerIncarnation) {
            return authorized(() -> delegate.readHead(namespaceId, sliceAssignmentId, managedLedgerIncarnation));
        }

        @Override
        public CompletionStage<CreateMutationResult<VersionedManagedLedgerAllocatorHeadV1>> createHead(
                Sha256Digest namespaceId, Sha256Digest sliceAssignmentId, ManagedLedgerAllocatorHeadV1 candidate) {
            return authorized(() -> delegate.createHead(namespaceId, sliceAssignmentId, candidate));
        }

        @Override
        public CompletionStage<ConditionalCasResult<VersionedManagedLedgerAllocatorHeadV1>> compareAndSetHead(
                Sha256Digest namespaceId,
                Sha256Digest sliceAssignmentId,
                VersionedManagedLedgerAllocatorHeadV1 exactPredecessor,
                ManagedLedgerAllocatorHeadV1 candidate) {
            return authorized(
                    () -> delegate.compareAndSetHead(namespaceId, sliceAssignmentId, exactPredecessor, candidate));
        }

        @Override
        public CompletionStage<ConditionalCasResult<VersionedManagedLedgerAllocatorHeadV1>>
                compareAndSetHeadAfterStoreObservedRangeNode(
                        Sha256Digest namespaceId,
                        Sha256Digest sliceAssignmentId,
                        VersionedManagedLedgerAllocatorHeadV1 exactPredecessor,
                        ManagedLedgerAllocatorHeadV1 candidate) {
            return authorized(() -> delegate.compareAndSetHeadAfterStoreObservedRangeNode(
                    namespaceId, sliceAssignmentId, exactPredecessor, candidate));
        }

        @Override
        public CompletionStage<Optional<VersionedVirtualLedgerCandidateNodeV1>> readNode(
                Sha256Digest namespaceId,
                Sha256Digest sliceAssignmentId,
                ManagedLedgerIncarnationIdV1 managedLedgerIncarnation,
                long ledgerId) {
            return authorized(
                    () -> delegate.readNode(namespaceId, sliceAssignmentId, managedLedgerIncarnation, ledgerId));
        }

        @Override
        public CompletionStage<CreateMutationResult<VersionedVirtualLedgerCandidateNodeV1>> createNode(
                Sha256Digest namespaceId, Sha256Digest sliceAssignmentId, VirtualLedgerCandidateNodeV1 candidate) {
            return authorized(() -> delegate.createNode(namespaceId, sliceAssignmentId, candidate));
        }

        @Override
        public CompletionStage<CreateMutationResult<VersionedVirtualLedgerCandidateNodeV1>>
                createNodeAfterStoreObservedRangeAuthorities(
                        Sha256Digest namespaceId,
                        Sha256Digest sliceAssignmentId,
                        VirtualLedgerCandidateNodeV1 candidate) {
            return authorized(() ->
                    delegate.createNodeAfterStoreObservedRangeAuthorities(namespaceId, sliceAssignmentId, candidate));
        }

        private <T> CompletionStage<T> authorized(Supplier<CompletionStage<T>> operation) {
            state.requireAuthorized();
            return Objects.requireNonNull(operation.get(), "allocator guarded store stage");
        }
    }
}
