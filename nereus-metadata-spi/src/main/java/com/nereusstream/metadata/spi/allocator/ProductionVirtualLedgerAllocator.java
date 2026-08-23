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
import com.nereusstream.domain.registry.allocator.AllocatorActivationV1;
import com.nereusstream.domain.registry.allocator.AllocatorEvidenceCandidateV1;
import com.nereusstream.domain.registry.allocator.AllocatorModeV1;
import com.nereusstream.domain.registry.allocator.AllocatorProtocolException;
import com.nereusstream.domain.registry.allocator.AllocatorProtocolV1;
import com.nereusstream.domain.registry.allocator.ManagedLedgerAllocatorHeadV1;
import com.nereusstream.domain.registry.allocator.ManagedLedgerIncarnationIdV1;
import com.nereusstream.domain.registry.allocator.VirtualLedgerCandidateNodeV1;
import com.nereusstream.domain.registry.allocator.VirtualLedgerCellAllocatorStateV1;
import com.nereusstream.metadata.spi.model.ConditionalCasResult;
import com.nereusstream.metadata.spi.model.CreateMutationResult;
import com.nereusstream.metadata.spi.model.VersionedVirtualLedgerSliceViewV1;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Receipt-gated production coordinator. Every dispatched mutation retains an exact typed predecessor. */
public final class ProductionVirtualLedgerAllocator {
    private final AllocatorModeV1 selectedMode;
    private final int allocatorProtocolVersion;
    private final long selectedRangeSize;
    private final boolean runtimeActivated;
    private final PulsarVirtualLedgerAllocatorStore store;

    public ProductionVirtualLedgerAllocator(AllocatorActivationV1 activation, PulsarVirtualLedgerAllocatorStore store) {
        Objects.requireNonNull(activation, "activation");
        this.selectedMode = activation.selectedMode();
        this.allocatorProtocolVersion = activation.allocatorProtocolVersion();
        this.selectedRangeSize = activation.selectedRangeSize();
        this.runtimeActivated = true;
        this.store = Objects.requireNonNull(store, "store");
    }

    private ProductionVirtualLedgerAllocator(
            AllocatorEvidenceCandidateV1 candidate, PulsarVirtualLedgerAllocatorStore store) {
        Objects.requireNonNull(candidate, "candidate");
        this.selectedMode = candidate.mode();
        this.allocatorProtocolVersion = candidate.allocatorProtocolVersion();
        this.selectedRangeSize = candidate.rangeSize();
        this.runtimeActivated = false;
        this.store = Objects.requireNonNull(store, "store");
    }

    /** Formal evidence seam: exact production coordinator and store adapter, without runtime activation authority. */
    public static ProductionVirtualLedgerAllocator forEvidenceCandidate(
            AllocatorEvidenceCandidateV1 candidate, PulsarVirtualLedgerAllocatorStore store) {
        return new ProductionVirtualLedgerAllocator(candidate, store);
    }

    public CompletionStage<CreateMutationResult<VersionedAllocatorCellStateV1>> createCell(
            VersionedVirtualLedgerSliceViewV1 currentView) {
        Objects.requireNonNull(currentView, "currentView");
        if (!currentView.value().allocationAllowed()) {
            throw failure(AllocatorProtocolException.Code.SLICE_NOT_ACTIVE, "Registry slice is not ACTIVE");
        }
        return store.createCell(VirtualLedgerCellAllocatorStateV1.initial(
                selectedMode, currentView.value().assignment()));
    }

    public CompletionStage<CreateMutationResult<VersionedManagedLedgerAllocatorHeadV1>> createHead(
            VersionedAllocatorCellStateV1 exactCell,
            VersionedVirtualLedgerSliceViewV1 currentView,
            ManagedLedgerIncarnationIdV1 incarnation,
            long ownerEpoch) {
        Objects.requireNonNull(exactCell, "exactCell");
        requireCurrentActiveCell(exactCell.value(), currentView);
        ManagedLedgerAllocatorHeadV1 head = ManagedLedgerAllocatorHeadV1.initial(
                incarnation, ownerEpoch, exactCell.value().nextSliceLedgerId());
        return requireStoredCell(exactCell)
                .thenCompose(ignored -> store.createHead(
                        exactCell.value().ledgerIdCompatibilityNamespaceId(),
                        exactCell.value().sliceAssignmentId(),
                        head));
    }

    public CompletionStage<ConditionalCasResult<VersionedAllocatorCellStateV1>> reserve(
            VersionedAllocatorCellStateV1 exactCell,
            VersionedManagedLedgerAllocatorHeadV1 exactHead,
            VersionedVirtualLedgerSliceViewV1 currentView,
            Sha256Digest requestId) {
        requireActivation(exactCell.value());
        AllocatorProtocolV1.requireCurrentActiveSlice(exactCell.value(), currentView.value());
        requireHeadBoundToCell(exactCell.value(), exactHead);
        VirtualLedgerCellAllocatorStateV1 successor =
                AllocatorProtocolV1.reserve(exactCell.value(), exactHead.value(), requestId, selectedRangeSize);
        return successor.equals(exactCell.value())
                ? unchanged(exactCell)
                : store.compareAndSetCell(exactCell, successor);
    }

    public CompletionStage<ConditionalCasResult<VersionedManagedLedgerAllocatorHeadV1>> installRangeReservedGrant(
            VersionedAllocatorCellStateV1 exactCell,
            VersionedManagedLedgerAllocatorHeadV1 exactHead,
            VersionedVirtualLedgerSliceViewV1 currentView) {
        requireCurrentActiveCell(exactCell.value(), currentView);
        requireHeadBoundToCell(exactCell.value(), exactHead);
        ManagedLedgerAllocatorHeadV1 successor =
                AllocatorProtocolV1.installReservedRange(exactCell.value(), exactHead.value());
        return requireStoredCell(exactCell)
                .thenCompose(ignored -> successor.equals(exactHead.value())
                        ? unchanged(exactHead)
                        : store.compareAndSetHead(
                                exactCell.value().ledgerIdCompatibilityNamespaceId(),
                                exactCell.value().sliceAssignmentId(),
                                exactHead,
                                successor));
    }

    public CompletionStage<ConditionalCasResult<VersionedManagedLedgerAllocatorHeadV1>> takeover(
            VersionedAllocatorCellStateV1 exactCell,
            VersionedManagedLedgerAllocatorHeadV1 exactHead,
            long newOwnerEpoch) {
        requireActivation(exactCell.value());
        requireHeadBoundToCell(exactCell.value(), exactHead);
        ManagedLedgerAllocatorHeadV1 successor = AllocatorProtocolV1.takeover(exactHead.value(), newOwnerEpoch);
        return requireStoredCell(exactCell)
                .thenCompose(ignored -> successor.equals(exactHead.value())
                        ? unchanged(exactHead)
                        : store.compareAndSetHead(
                                exactCell.value().ledgerIdCompatibilityNamespaceId(),
                                exactCell.value().sliceAssignmentId(),
                                exactHead,
                                successor));
    }

    public CompletionStage<CreateMutationResult<VersionedVirtualLedgerCandidateNodeV1>> createCandidate(
            VersionedAllocatorCellStateV1 exactCell,
            VersionedManagedLedgerAllocatorHeadV1 exactHead,
            VersionedVirtualLedgerSliceViewV1 currentView,
            Sha256Digest ledgerDescriptorDigest) {
        requireCurrentActiveCell(exactCell.value(), currentView);
        requireHeadBoundToCell(exactCell.value(), exactHead);
        VirtualLedgerCandidateNodeV1 candidate =
                exactCell.value().mode() == com.nereusstream.domain.registry.allocator.AllocatorModeV1.STRICT_SERIALIZED
                        ? AllocatorProtocolV1.strictCandidateFromReservation(
                                exactCell.value(), exactHead.value(), ledgerDescriptorDigest)
                        : AllocatorProtocolV1.candidate(exactHead.value(), ledgerDescriptorDigest);
        return requireStoredCell(exactCell)
                .thenCompose(ignored -> requireStoredHead(exactCell.value(), exactHead))
                .thenCompose(ignored -> store.createNode(
                        exactCell.value().ledgerIdCompatibilityNamespaceId(),
                        exactCell.value().sliceAssignmentId(),
                        candidate));
    }

    public CompletionStage<ConditionalCasResult<VersionedManagedLedgerAllocatorHeadV1>> publishCandidate(
            VersionedAllocatorCellStateV1 exactCell,
            VersionedManagedLedgerAllocatorHeadV1 exactHead,
            VersionedVirtualLedgerCandidateNodeV1 exactNode,
            VersionedVirtualLedgerSliceViewV1 currentView) {
        requireCurrentActiveCell(exactCell.value(), currentView);
        requireHeadBoundToCell(exactCell.value(), exactHead);
        requireNodeBoundToCell(exactCell.value(), exactNode);
        ManagedLedgerAllocatorHeadV1 successor = exactCell.value().mode()
                        == com.nereusstream.domain.registry.allocator.AllocatorModeV1.STRICT_SERIALIZED
                ? AllocatorProtocolV1.publishStrictReserved(exactCell.value(), exactHead.value(), exactNode.value())
                : AllocatorProtocolV1.publish(exactHead.value(), exactNode.value());
        return requireStoredCell(exactCell)
                .thenCompose(ignored -> requireStoredNode(exactCell.value(), exactNode))
                .thenCompose(ignored -> successor.equals(exactHead.value())
                        ? unchanged(exactHead)
                        : store.compareAndSetHead(
                                exactCell.value().ledgerIdCompatibilityNamespaceId(),
                                exactCell.value().sliceAssignmentId(),
                                exactHead,
                                successor));
    }

    public CompletionStage<ConditionalCasResult<VersionedManagedLedgerAllocatorHeadV1>> burnStaleCandidate(
            VersionedAllocatorCellStateV1 exactCell,
            VersionedManagedLedgerAllocatorHeadV1 exactHead,
            VersionedVirtualLedgerCandidateNodeV1 exactStaleNode,
            VersionedVirtualLedgerSliceViewV1 currentView) {
        requireCurrentActiveCell(exactCell.value(), currentView);
        requireHeadBoundToCell(exactCell.value(), exactHead);
        requireNodeBoundToCell(exactCell.value(), exactStaleNode);
        ManagedLedgerAllocatorHeadV1 successor =
                AllocatorProtocolV1.burnOneStaleCandidate(exactHead.value(), exactStaleNode.value());
        return requireStoredCell(exactCell)
                .thenCompose(ignored -> requireStoredNode(exactCell.value(), exactStaleNode))
                .thenCompose(ignored -> successor.equals(exactHead.value())
                        ? unchanged(exactHead)
                        : store.compareAndSetHead(
                                exactCell.value().ledgerIdCompatibilityNamespaceId(),
                                exactCell.value().sliceAssignmentId(),
                                exactHead,
                                successor));
    }

    public CompletionStage<ConditionalCasResult<VersionedAllocatorCellStateV1>> clearReservation(
            VersionedAllocatorCellStateV1 exactCell, VersionedManagedLedgerAllocatorHeadV1 exactHead) {
        requireActivation(exactCell.value());
        requireHeadBoundToCell(exactCell.value(), exactHead);
        VirtualLedgerCellAllocatorStateV1 successor =
                AllocatorProtocolV1.clearInstalledReservation(exactCell.value(), exactHead.value());
        return requireStoredHead(exactCell.value(), exactHead)
                .thenCompose(ignored -> successor.equals(exactCell.value())
                        ? unchanged(exactCell)
                        : store.compareAndSetCell(exactCell, successor));
    }

    private void requireActivation(VirtualLedgerCellAllocatorStateV1 cell) {
        if (cell.mode() != selectedMode || cell.allocatorProtocolVersion() != allocatorProtocolVersion) {
            throw failure(
                    AllocatorProtocolException.Code.MODE_MISMATCH,
                    "persisted allocator mode/version differs from the selection receipt");
        }
    }

    /** True only for a canonical selection-receipt activation; formal candidate coordinators always return false. */
    public boolean runtimeActivated() {
        return runtimeActivated;
    }

    private void requireCurrentActiveCell(
            VirtualLedgerCellAllocatorStateV1 cell, VersionedVirtualLedgerSliceViewV1 currentView) {
        Objects.requireNonNull(currentView, "currentView");
        requireActivation(cell);
        AllocatorProtocolV1.requireCurrentActiveSlice(cell, currentView.value());
    }

    private static void requireHeadBoundToCell(
            VirtualLedgerCellAllocatorStateV1 cell, VersionedManagedLedgerAllocatorHeadV1 head) {
        Objects.requireNonNull(head, "head");
        if (!cell.ledgerIdCompatibilityNamespaceId().equals(head.ledgerIdCompatibilityNamespaceId())
                || !cell.sliceAssignmentId().equals(head.sliceAssignmentId())) {
            throw failure(AllocatorProtocolException.Code.HEAD_IDENTITY, "Head authority provenance differs from Cell");
        }
        AllocatorProtocolV1.requireHeadWithinConsumedSlicePrefix(cell, head.value());
    }

    private static void requireNodeBoundToCell(
            VirtualLedgerCellAllocatorStateV1 cell, VersionedVirtualLedgerCandidateNodeV1 node) {
        Objects.requireNonNull(node, "node");
        if (!cell.ledgerIdCompatibilityNamespaceId().equals(node.ledgerIdCompatibilityNamespaceId())
                || !cell.sliceAssignmentId().equals(node.sliceAssignmentId())
                || node.value().ledgerId() < cell.sliceStartInclusive()
                || node.value().ledgerId() >= cell.nextSliceLedgerId()) {
            throw failure(
                    AllocatorProtocolException.Code.CANDIDATE_OCCUPANCY_NOT_PROVEN,
                    "candidate authority provenance/geometry differs from the consumed Cell prefix");
        }
    }

    private CompletionStage<Void> requireStoredHead(
            VirtualLedgerCellAllocatorStateV1 cell, VersionedManagedLedgerAllocatorHeadV1 exactHead) {
        return store.readHead(
                        cell.ledgerIdCompatibilityNamespaceId(),
                        cell.sliceAssignmentId(),
                        exactHead.value().managedLedgerIncarnation())
                .thenApply(observed -> {
                    if (observed.isEmpty() || !observed.orElseThrow().equals(exactHead)) {
                        throw failure(
                                AllocatorProtocolException.Code.HEAD_STATE_DRIFT,
                                "exact versioned Head is not the current same-key store snapshot");
                    }
                    return null;
                });
    }

    private CompletionStage<Void> requireStoredCell(VersionedAllocatorCellStateV1 exactCell) {
        return store.readCell(
                        exactCell.value().ledgerIdCompatibilityNamespaceId(),
                        exactCell.value().sliceAssignmentId())
                .thenApply(observed -> {
                    if (observed.isEmpty() || !observed.orElseThrow().equals(exactCell)) {
                        throw failure(
                                AllocatorProtocolException.Code.CELL_STATE_DRIFT,
                                "exact versioned Cell is not the current same-key store snapshot");
                    }
                    return null;
                });
    }

    private CompletionStage<Void> requireStoredNode(
            VirtualLedgerCellAllocatorStateV1 cell, VersionedVirtualLedgerCandidateNodeV1 exactNode) {
        return store.readNode(
                        cell.ledgerIdCompatibilityNamespaceId(),
                        cell.sliceAssignmentId(),
                        exactNode.value().managedLedgerIncarnation(),
                        exactNode.value().ledgerId())
                .thenApply(observed -> {
                    if (observed.isEmpty() || !observed.orElseThrow().equals(exactNode)) {
                        throw failure(
                                AllocatorProtocolException.Code.CANDIDATE_OCCUPANCY_NOT_PROVEN,
                                "candidate burn/publish requires the current exact same-key versioned node snapshot");
                    }
                    return null;
                });
    }

    private static AllocatorProtocolException failure(AllocatorProtocolException.Code code, String message) {
        return new AllocatorProtocolException(code, message);
    }

    private static <T> CompletionStage<ConditionalCasResult<T>> unchanged(T exactPredecessor) {
        return CompletableFuture.completedFuture(ConditionalCasResult.predecessorUnchanged(exactPredecessor));
    }
}
