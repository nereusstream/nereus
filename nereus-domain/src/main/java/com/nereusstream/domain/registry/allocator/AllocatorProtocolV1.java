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

package com.nereusstream.domain.registry.allocator;

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.registry.VirtualLedgerSliceAssignmentV1;
import com.nereusstream.domain.registry.VirtualLedgerSliceViewV1;
import java.util.Objects;
import java.util.Optional;

/** Pure exact-predecessor transition kernel shared by production adapters and deterministic evidence runners. */
public final class AllocatorProtocolV1 {
    private AllocatorProtocolV1() {}

    public static void requireCurrentActiveSlice(
            VirtualLedgerCellAllocatorStateV1 state, VirtualLedgerSliceViewV1 currentView) {
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(currentView, "currentView");
        VirtualLedgerSliceAssignmentV1 assignment = currentView.assignment();
        if (!currentView.allocationAllowed()) {
            throw failure(AllocatorProtocolException.Code.SLICE_NOT_ACTIVE, "Registry slice is not ACTIVE");
        }
        if (!state.ledgerIdCompatibilityNamespaceId().equals(currentView.ledgerIdCompatibilityNamespaceId())
                || !state.sliceAssignmentId().equals(assignment.sliceAssignmentId())
                || state.sliceStartInclusive() != assignment.startInclusive()
                || state.sliceEndInclusive() != assignment.endInclusive()) {
            throw failure(AllocatorProtocolException.Code.SLICE_IDENTITY_DRIFT, "Registry slice authority drifted");
        }
    }

    public static VirtualLedgerCellAllocatorStateV1 reserve(
            VirtualLedgerCellAllocatorStateV1 state,
            ManagedLedgerAllocatorHeadV1 exactHead,
            Sha256Digest requestId,
            long selectedRangeSize) {
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(exactHead, "exactHead");
        Objects.requireNonNull(requestId, "requestId");
        requireHeadWithinConsumedSlicePrefix(state, exactHead);
        if (state.reservation().isPresent()) {
            CellAllocatorReservationV1 existing = state.reservation().orElseThrow();
            if (existing.managedLedgerIncarnation().equals(exactHead.managedLedgerIncarnation())
                    && existing.requestId().equals(requestId)
                    && existing.expectedAllocationState().equals(exactHead.allocationState())) {
                return state;
            }
            throw failure(AllocatorProtocolException.Code.RESERVATION_BUSY, "another exact Cell grant is RESERVED");
        }
        if (selectedRangeSize <= 0
                || (state.mode() == AllocatorModeV1.STRICT_SERIALIZED && selectedRangeSize != 1)
                || (state.mode() == AllocatorModeV1.RANGE_LEASED && selectedRangeSize <= 1)) {
            throw failure(AllocatorProtocolException.Code.MODE_MISMATCH, "selected range size does not match mode");
        }
        if (state.mode() == AllocatorModeV1.RANGE_LEASED
                && exactHead.grantId() != 0
                && exactHead.nextLedgerId() < exactHead.rangeEndExclusive()) {
            throw failure(
                    AllocatorProtocolException.Code.RANGE_TAIL_NOT_EXHAUSTED,
                    "normal RANGE reserve cannot abandon or regrant an installed unused tail");
        }
        long end;
        try {
            end = Math.addExact(state.nextSliceLedgerId(), selectedRangeSize);
        } catch (ArithmeticException error) {
            throw failure(AllocatorProtocolException.Code.SLICE_EXHAUSTED, "allocator range overflows");
        }
        if (end > state.sliceEndInclusive() + 1) {
            throw failure(AllocatorProtocolException.Code.SLICE_EXHAUSTED, "allocator slice is exhausted");
        }
        CellAllocatorReservationV1 reservation = new CellAllocatorReservationV1(
                exactHead.managedLedgerIncarnation(),
                state.nextGrantId(),
                state.nextSliceLedgerId(),
                end,
                requestId,
                exactHead.allocationState());
        return new VirtualLedgerCellAllocatorStateV1(
                state.mode(),
                state.allocatorProtocolVersion(),
                state.ledgerIdCompatibilityNamespaceId(),
                state.sliceAssignmentId(),
                state.sliceStartInclusive(),
                state.sliceEndInclusive(),
                end,
                Math.addExact(state.nextGrantId(), 1),
                Optional.of(reservation));
    }

    public static ManagedLedgerAllocatorHeadV1 installReservedRange(
            VirtualLedgerCellAllocatorStateV1 state, ManagedLedgerAllocatorHeadV1 exactHead) {
        if (state.mode() != AllocatorModeV1.RANGE_LEASED) {
            throw failure(
                    AllocatorProtocolException.Code.MODE_MISMATCH,
                    "separate grant installation exists only for RANGE mode");
        }
        requireHeadWithinConsumedSlicePrefix(state, exactHead);
        CellAllocatorReservationV1 reservation = state.reservation()
                .orElseThrow(() ->
                        failure(AllocatorProtocolException.Code.GRANT_NOT_INSTALLED, "no Cell reservation is present"));
        requireIncarnation(reservation, exactHead);
        if (headContainsGrant(exactHead, reservation)) {
            return exactHead;
        }
        if (!reservation.expectedAllocationState().equals(exactHead.allocationState())) {
            throw failure(
                    AllocatorProtocolException.Code.HEAD_STATE_DRIFT,
                    "head allocation state differs from the RESERVED predecessor");
        }
        return new ManagedLedgerAllocatorHeadV1(
                exactHead.allocatorProtocolVersion(),
                exactHead.managedLedgerIncarnation(),
                exactHead.ownerEpoch(),
                exactHead.visibleChainHead(),
                reservation.grantId(),
                reservation.rangeStartInclusive(),
                reservation.rangeEndExclusive(),
                reservation.rangeStartInclusive());
    }

    public static ManagedLedgerAllocatorHeadV1 takeover(ManagedLedgerAllocatorHeadV1 exactHead, long newOwnerEpoch) {
        if (newOwnerEpoch == exactHead.ownerEpoch()) {
            return exactHead;
        }
        if (newOwnerEpoch <= exactHead.ownerEpoch()) {
            throw failure(AllocatorProtocolException.Code.OWNER_FENCED, "takeover owner epoch must increase");
        }
        return new ManagedLedgerAllocatorHeadV1(
                exactHead.allocatorProtocolVersion(),
                exactHead.managedLedgerIncarnation(),
                newOwnerEpoch,
                exactHead.visibleChainHead(),
                exactHead.grantId(),
                exactHead.rangeStartInclusive(),
                exactHead.rangeEndExclusive(),
                exactHead.nextLedgerId());
    }

    public static VirtualLedgerCandidateNodeV1 candidate(
            ManagedLedgerAllocatorHeadV1 exactHead, Sha256Digest ledgerDescriptorDigest) {
        if (exactHead.grantId() == 0) {
            throw failure(AllocatorProtocolException.Code.GRANT_NOT_INSTALLED, "head has no installed grant");
        }
        if (exactHead.nextLedgerId() >= exactHead.rangeEndExclusive()) {
            throw failure(AllocatorProtocolException.Code.RANGE_EXHAUSTED, "installed range is exhausted");
        }
        return AllocatorWireV1.createNode(
                exactHead.managedLedgerIncarnation(),
                exactHead.nextLedgerId(),
                exactHead.grantId(),
                exactHead.ownerEpoch(),
                exactHead.visibleChainHead(),
                ledgerDescriptorDigest);
    }

    /** STRICT's candidate follows Cell reserve directly, keeping its protocol at four successful writes. */
    public static VirtualLedgerCandidateNodeV1 strictCandidateFromReservation(
            VirtualLedgerCellAllocatorStateV1 state,
            ManagedLedgerAllocatorHeadV1 exactHead,
            Sha256Digest ledgerDescriptorDigest) {
        if (state.mode() != AllocatorModeV1.STRICT_SERIALIZED) {
            throw failure(
                    AllocatorProtocolException.Code.MODE_MISMATCH, "STRICT reservation path requires STRICT mode");
        }
        CellAllocatorReservationV1 reservation = state.reservation()
                .orElseThrow(() ->
                        failure(AllocatorProtocolException.Code.GRANT_NOT_INSTALLED, "no Cell reservation is present"));
        requireIncarnation(reservation, exactHead);
        if (!reservation.expectedAllocationState().equals(exactHead.allocationState())
                || reservation.rangeEndExclusive() != reservation.rangeStartInclusive() + 1) {
            throw failure(
                    AllocatorProtocolException.Code.HEAD_STATE_DRIFT,
                    "STRICT Head differs from the RESERVED predecessor or one-ID range");
        }
        return AllocatorWireV1.createNode(
                exactHead.managedLedgerIncarnation(),
                reservation.rangeStartInclusive(),
                reservation.grantId(),
                exactHead.ownerEpoch(),
                exactHead.visibleChainHead(),
                ledgerDescriptorDigest);
    }

    public static ManagedLedgerAllocatorHeadV1 publish(
            ManagedLedgerAllocatorHeadV1 exactHead, VirtualLedgerCandidateNodeV1 exactNode) {
        if (alreadyPublished(exactHead, exactNode)) {
            return exactHead;
        }
        requireCandidateAtCursor(exactHead, exactNode);
        if (exactNode.creatorOwnerEpoch() != exactHead.ownerEpoch()) {
            throw failure(AllocatorProtocolException.Code.OWNER_FENCED, "candidate creator is not the current owner");
        }
        if (!exactNode.expectedPredecessor().equals(exactHead.visibleChainHead())) {
            throw failure(AllocatorProtocolException.Code.CANDIDATE_CONFLICT, "candidate predecessor differs");
        }
        return new ManagedLedgerAllocatorHeadV1(
                exactHead.allocatorProtocolVersion(),
                exactHead.managedLedgerIncarnation(),
                exactHead.ownerEpoch(),
                exactNode.pointer(),
                exactHead.grantId(),
                exactHead.rangeStartInclusive(),
                exactHead.rangeEndExclusive(),
                Math.addExact(exactHead.nextLedgerId(), 1));
    }

    /** STRICT atomically installs and consumes its single-ID grant while publishing the node. */
    public static ManagedLedgerAllocatorHeadV1 publishStrictReserved(
            VirtualLedgerCellAllocatorStateV1 state,
            ManagedLedgerAllocatorHeadV1 exactHead,
            VirtualLedgerCandidateNodeV1 exactNode) {
        if (state.mode() != AllocatorModeV1.STRICT_SERIALIZED) {
            throw failure(AllocatorProtocolException.Code.MODE_MISMATCH, "STRICT publish requires STRICT mode");
        }
        CellAllocatorReservationV1 reservation = state.reservation()
                .orElseThrow(() ->
                        failure(AllocatorProtocolException.Code.GRANT_NOT_INSTALLED, "no Cell reservation is present"));
        requireIncarnation(reservation, exactHead);
        if (alreadyPublished(exactHead, exactNode)
                && headContainsGrant(exactHead, reservation)
                && strictReservationMatchesNode(reservation, exactNode)
                && exactHead.nextLedgerId() == reservation.rangeEndExclusive()) {
            return exactHead;
        }
        if (exactNode.creatorOwnerEpoch() != exactHead.ownerEpoch()) {
            throw failure(AllocatorProtocolException.Code.OWNER_FENCED, "candidate creator is not the current owner");
        }
        if (!reservation.expectedAllocationState().equals(exactHead.allocationState())
                || reservation.rangeEndExclusive() != reservation.rangeStartInclusive() + 1
                || exactNode.ledgerId() != reservation.rangeStartInclusive()
                || exactNode.grantId() != reservation.grantId()
                || !exactNode.expectedPredecessor().equals(exactHead.visibleChainHead())) {
            throw failure(
                    AllocatorProtocolException.Code.CANDIDATE_CONFLICT,
                    "STRICT candidate does not match its exact RESERVED grant and Head predecessor");
        }
        return new ManagedLedgerAllocatorHeadV1(
                exactHead.allocatorProtocolVersion(),
                exactHead.managedLedgerIncarnation(),
                exactHead.ownerEpoch(),
                exactNode.pointer(),
                reservation.grantId(),
                reservation.rangeStartInclusive(),
                reservation.rangeEndExclusive(),
                reservation.rangeEndExclusive());
    }

    public static ManagedLedgerAllocatorHeadV1 burnOneStaleCandidate(
            ManagedLedgerAllocatorHeadV1 exactHead, VirtualLedgerCandidateNodeV1 staleNode) {
        if (exactHead.managedLedgerIncarnation().equals(staleNode.managedLedgerIncarnation())
                && exactHead.grantId() == staleNode.grantId()
                && exactHead.nextLedgerId() == staleNode.ledgerId() + 1
                && exactHead.visibleChainHead().equals(staleNode.expectedPredecessor())
                && staleNode.creatorOwnerEpoch() < exactHead.ownerEpoch()) {
            return exactHead;
        }
        requireCandidateAtCursor(exactHead, staleNode);
        if (staleNode.creatorOwnerEpoch() >= exactHead.ownerEpoch()) {
            throw failure(
                    AllocatorProtocolException.Code.STALE_CANDIDATE_REQUIRED,
                    "only one exact candidate from an older owner may be burned");
        }
        return new ManagedLedgerAllocatorHeadV1(
                exactHead.allocatorProtocolVersion(),
                exactHead.managedLedgerIncarnation(),
                exactHead.ownerEpoch(),
                exactHead.visibleChainHead(),
                exactHead.grantId(),
                exactHead.rangeStartInclusive(),
                exactHead.rangeEndExclusive(),
                Math.addExact(exactHead.nextLedgerId(), 1));
    }

    /** STRICT installs and consumes, but never publishes, one exact RESERVED node left by a fenced prior owner. */
    public static ManagedLedgerAllocatorHeadV1 burnStrictReservedStaleCandidate(
            VirtualLedgerCellAllocatorStateV1 state,
            ManagedLedgerAllocatorHeadV1 exactHead,
            VirtualLedgerCandidateNodeV1 staleNode) {
        if (state.mode() != AllocatorModeV1.STRICT_SERIALIZED) {
            throw failure(AllocatorProtocolException.Code.MODE_MISMATCH, "STRICT stale burn requires STRICT mode");
        }
        CellAllocatorReservationV1 reservation = state.reservation()
                .orElseThrow(() ->
                        failure(AllocatorProtocolException.Code.GRANT_NOT_INSTALLED, "no Cell reservation is present"));
        requireIncarnation(reservation, exactHead);
        if (headContainsGrant(exactHead, reservation)
                && exactHead.nextLedgerId() == reservation.rangeEndExclusive()
                && exactHead.visibleChainHead().equals(staleNode.expectedPredecessor())
                && strictReservationMatchesNode(reservation, staleNode)
                && staleNode.ledgerId() == reservation.rangeStartInclusive()
                && staleNode.grantId() == reservation.grantId()
                && staleNode.creatorOwnerEpoch() < exactHead.ownerEpoch()) {
            return exactHead;
        }
        if (!reservation.expectedAllocationState().equals(exactHead.allocationState())
                || reservation.rangeEndExclusive() != reservation.rangeStartInclusive() + 1
                || staleNode.ledgerId() != reservation.rangeStartInclusive()
                || staleNode.grantId() != reservation.grantId()
                || !staleNode.managedLedgerIncarnation().equals(exactHead.managedLedgerIncarnation())
                || !staleNode.expectedPredecessor().equals(exactHead.visibleChainHead())) {
            throw failure(
                    AllocatorProtocolException.Code.CANDIDATE_CONFLICT,
                    "STRICT stale candidate does not match the exact RESERVED grant and Head predecessor");
        }
        if (staleNode.creatorOwnerEpoch() >= exactHead.ownerEpoch()) {
            throw failure(
                    AllocatorProtocolException.Code.STALE_CANDIDATE_REQUIRED,
                    "STRICT burn requires one exact candidate from an older owner");
        }
        return new ManagedLedgerAllocatorHeadV1(
                exactHead.allocatorProtocolVersion(),
                exactHead.managedLedgerIncarnation(),
                exactHead.ownerEpoch(),
                exactHead.visibleChainHead(),
                reservation.grantId(),
                reservation.rangeStartInclusive(),
                reservation.rangeEndExclusive(),
                reservation.rangeEndExclusive());
    }

    public static VirtualLedgerCellAllocatorStateV1 clearInstalledReservation(
            VirtualLedgerCellAllocatorStateV1 state, ManagedLedgerAllocatorHeadV1 exactHead) {
        if (state.reservation().isEmpty()) {
            return state;
        }
        if (state.mode() == AllocatorModeV1.STRICT_SERIALIZED) {
            throw failure(
                    AllocatorProtocolException.Code.CANDIDATE_OCCUPANCY_NOT_PROVEN,
                    "STRICT clear requires its exact published or stale-burned node proof");
        }
        return clearInstalledReservationAfterProof(state, exactHead);
    }

    /** STRICT clear accepts only the exact node proven published or consumed by the stale-owner burn transition. */
    public static VirtualLedgerCellAllocatorStateV1 clearStrictTerminalReservation(
            VirtualLedgerCellAllocatorStateV1 state,
            ManagedLedgerAllocatorHeadV1 exactHead,
            VirtualLedgerCandidateNodeV1 exactNode) {
        Objects.requireNonNull(exactNode, "exactNode");
        if (state.reservation().isEmpty()) {
            return state;
        }
        if (state.mode() != AllocatorModeV1.STRICT_SERIALIZED) {
            throw failure(AllocatorProtocolException.Code.MODE_MISMATCH, "STRICT clear requires STRICT mode");
        }
        CellAllocatorReservationV1 reservation = state.reservation().orElseThrow();
        requireIncarnation(reservation, exactHead);
        boolean exactNodeIdentity = exactNode.managedLedgerIncarnation().equals(exactHead.managedLedgerIncarnation())
                && exactNode.ledgerId() == reservation.rangeStartInclusive()
                && exactNode.grantId() == reservation.grantId()
                && strictReservationMatchesNode(reservation, exactNode);
        boolean published = exactHead.visibleChainHead().equals(exactNode.pointer())
                && exactNode.creatorOwnerEpoch() <= exactHead.ownerEpoch();
        boolean staleBurned = exactHead.visibleChainHead().equals(exactNode.expectedPredecessor())
                && exactNode.creatorOwnerEpoch() < exactHead.ownerEpoch();
        if (!exactNodeIdentity || (!published && !staleBurned)) {
            throw failure(
                    AllocatorProtocolException.Code.CANDIDATE_OCCUPANCY_NOT_PROVEN,
                    "STRICT clear node is neither the exact published node nor exact stale-burn proof");
        }
        return clearInstalledReservationAfterProof(state, exactHead);
    }

    private static VirtualLedgerCellAllocatorStateV1 clearInstalledReservationAfterProof(
            VirtualLedgerCellAllocatorStateV1 state, ManagedLedgerAllocatorHeadV1 exactHead) {
        CellAllocatorReservationV1 reservation = state.reservation().orElseThrow();
        requireIncarnation(reservation, exactHead);
        if (exactHead.grantId() != reservation.grantId()
                || exactHead.rangeStartInclusive() != reservation.rangeStartInclusive()
                || exactHead.rangeEndExclusive() != reservation.rangeEndExclusive()) {
            throw failure(
                    AllocatorProtocolException.Code.GRANT_NOT_INSTALLED,
                    "head does not contain the exact RESERVED grant");
        }
        if (state.mode() == AllocatorModeV1.STRICT_SERIALIZED
                && exactHead.nextLedgerId() != reservation.rangeEndExclusive()) {
            throw failure(
                    AllocatorProtocolException.Code.GRANT_NOT_INSTALLED,
                    "STRICT reservation clears only after its exact node was published or stale-burn consumed");
        }
        return new VirtualLedgerCellAllocatorStateV1(
                state.mode(),
                state.allocatorProtocolVersion(),
                state.ledgerIdCompatibilityNamespaceId(),
                state.sliceAssignmentId(),
                state.sliceStartInclusive(),
                state.sliceEndInclusive(),
                state.nextSliceLedgerId(),
                state.nextGrantId(),
                Optional.empty());
    }

    /**
     * Accounts for an unused installed RANGE tail only at a terminal retirement/incompatibility/corruption boundary.
     * The result is not allocator state and cannot authorize another reservation.
     */
    public static TerminalInstalledRangeAbandonmentV1 abandonInstalledRangeTerminal(
            VirtualLedgerCellAllocatorStateV1 state,
            ManagedLedgerAllocatorHeadV1 exactHead,
            InstalledRangeAbandonmentAuthorityV1 authority) {
        Objects.requireNonNull(authority, "authority");
        if (state.mode() != AllocatorModeV1.RANGE_LEASED) {
            throw failure(AllocatorProtocolException.Code.MODE_MISMATCH, "terminal abandonment requires RANGE mode");
        }
        requireHeadWithinConsumedSlicePrefix(state, exactHead);
        if (exactHead.grantId() == 0 || exactHead.nextLedgerId() >= exactHead.rangeEndExclusive()) {
            throw failure(
                    AllocatorProtocolException.Code.RANGE_TAIL_NOT_EXHAUSTED,
                    "terminal abandonment requires one installed unused RANGE tail");
        }
        return new TerminalInstalledRangeAbandonmentV1(
                state.ledgerIdCompatibilityNamespaceId(),
                state.sliceAssignmentId(),
                exactHead.managedLedgerIncarnation(),
                exactHead.grantId(),
                exactHead.rangeStartInclusive(),
                exactHead.rangeEndExclusive(),
                exactHead.nextLedgerId(),
                exactHead.ownerEpoch(),
                authority);
    }

    /** Validates that every Head grant/cursor lies inside the Cell's already-consumed slice prefix. */
    public static void requireHeadWithinConsumedSlicePrefix(
            VirtualLedgerCellAllocatorStateV1 state, ManagedLedgerAllocatorHeadV1 head) {
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(head, "head");
        if (head.nextLedgerId() < state.sliceStartInclusive()
                || head.nextLedgerId() > state.nextSliceLedgerId()
                || (head.grantId() != 0
                        && (head.rangeStartInclusive() < state.sliceStartInclusive()
                                || head.rangeEndExclusive() > state.nextSliceLedgerId()
                                || head.nextLedgerId() < head.rangeStartInclusive()
                                || head.nextLedgerId() > head.rangeEndExclusive()))) {
            throw failure(
                    AllocatorProtocolException.Code.HEAD_GEOMETRY,
                    "allocator Head range/cursor lies outside the Cell consumed slice prefix");
        }
    }

    private static void requireIncarnation(CellAllocatorReservationV1 reservation, ManagedLedgerAllocatorHeadV1 head) {
        if (!reservation.managedLedgerIncarnation().equals(head.managedLedgerIncarnation())) {
            throw failure(AllocatorProtocolException.Code.HEAD_IDENTITY, "ManagedLedger incarnation differs");
        }
    }

    private static void requireCandidateAtCursor(ManagedLedgerAllocatorHeadV1 head, VirtualLedgerCandidateNodeV1 node) {
        if (!node.managedLedgerIncarnation().equals(head.managedLedgerIncarnation())
                || node.grantId() != head.grantId()
                || node.ledgerId() != head.nextLedgerId()) {
            throw failure(
                    AllocatorProtocolException.Code.CANDIDATE_CONFLICT,
                    "candidate identity/grant/ledger does not match the exact Head cursor");
        }
    }

    private static boolean headContainsGrant(
            ManagedLedgerAllocatorHeadV1 head, CellAllocatorReservationV1 reservation) {
        return head.grantId() == reservation.grantId()
                && head.rangeStartInclusive() == reservation.rangeStartInclusive()
                && head.rangeEndExclusive() == reservation.rangeEndExclusive()
                && head.nextLedgerId() >= reservation.rangeStartInclusive()
                && head.nextLedgerId() <= reservation.rangeEndExclusive();
    }

    private static boolean strictReservationMatchesNode(
            CellAllocatorReservationV1 reservation, VirtualLedgerCandidateNodeV1 node) {
        return reservation.rangeEndExclusive() == reservation.rangeStartInclusive() + 1
                && node.expectedPredecessor()
                        .equals(reservation.expectedAllocationState().visibleChainHead());
    }

    private static boolean alreadyPublished(ManagedLedgerAllocatorHeadV1 head, VirtualLedgerCandidateNodeV1 node) {
        return head.managedLedgerIncarnation().equals(node.managedLedgerIncarnation())
                && head.grantId() == node.grantId()
                && head.visibleChainHead().equals(node.pointer())
                && head.nextLedgerId() == node.ledgerId() + 1;
    }

    private static AllocatorProtocolException failure(AllocatorProtocolException.Code code, String message) {
        return new AllocatorProtocolException(code, message);
    }
}
