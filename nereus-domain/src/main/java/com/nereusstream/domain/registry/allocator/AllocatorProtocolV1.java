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
                && exactHead.nextLedgerId() == reservation.rangeEndExclusive()) {
            return exactHead;
        }
        if (!reservation.expectedAllocationState().equals(exactHead.allocationState())
                || reservation.rangeEndExclusive() != reservation.rangeStartInclusive() + 1
                || exactNode.ledgerId() != reservation.rangeStartInclusive()
                || exactNode.grantId() != reservation.grantId()
                || exactNode.creatorOwnerEpoch() != exactHead.ownerEpoch()
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

    public static VirtualLedgerCellAllocatorStateV1 clearInstalledReservation(
            VirtualLedgerCellAllocatorStateV1 state, ManagedLedgerAllocatorHeadV1 exactHead) {
        if (state.reservation().isEmpty()) {
            return state;
        }
        CellAllocatorReservationV1 reservation = state.reservation().orElseThrow();
        requireIncarnation(reservation, exactHead);
        if (exactHead.grantId() != reservation.grantId()
                || exactHead.rangeStartInclusive() != reservation.rangeStartInclusive()
                || exactHead.rangeEndExclusive() != reservation.rangeEndExclusive()) {
            throw failure(
                    AllocatorProtocolException.Code.GRANT_NOT_INSTALLED,
                    "head does not contain the exact RESERVED grant");
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
