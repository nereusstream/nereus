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
    private final AllocatorActivationV1 activation;
    private final PulsarVirtualLedgerAllocatorStore store;

    public ProductionVirtualLedgerAllocator(AllocatorActivationV1 activation, PulsarVirtualLedgerAllocatorStore store) {
        this.activation = Objects.requireNonNull(activation, "activation");
        this.store = Objects.requireNonNull(store, "store");
    }

    public CompletionStage<CreateMutationResult<VersionedAllocatorCellStateV1>> createCell(
            VersionedVirtualLedgerSliceViewV1 currentView) {
        Objects.requireNonNull(currentView, "currentView");
        if (!currentView.value().allocationAllowed()) {
            throw failure(AllocatorProtocolException.Code.SLICE_NOT_ACTIVE, "Registry slice is not ACTIVE");
        }
        return store.createCell(VirtualLedgerCellAllocatorStateV1.initial(
                activation.selectedMode(), currentView.value().assignment()));
    }

    public CompletionStage<CreateMutationResult<VersionedManagedLedgerAllocatorHeadV1>> createHead(
            VirtualLedgerCellAllocatorStateV1 exactCell, ManagedLedgerIncarnationIdV1 incarnation, long ownerEpoch) {
        requireActivation(exactCell);
        ManagedLedgerAllocatorHeadV1 head =
                ManagedLedgerAllocatorHeadV1.initial(incarnation, ownerEpoch, exactCell.nextSliceLedgerId());
        return store.createHead(exactCell.ledgerIdCompatibilityNamespaceId(), exactCell.sliceAssignmentId(), head);
    }

    public CompletionStage<ConditionalCasResult<VersionedAllocatorCellStateV1>> reserve(
            VersionedAllocatorCellStateV1 exactCell,
            VersionedManagedLedgerAllocatorHeadV1 exactHead,
            VersionedVirtualLedgerSliceViewV1 currentView,
            Sha256Digest requestId) {
        requireActivation(exactCell.value());
        AllocatorProtocolV1.requireCurrentActiveSlice(exactCell.value(), currentView.value());
        VirtualLedgerCellAllocatorStateV1 successor = AllocatorProtocolV1.reserve(
                exactCell.value(), exactHead.value(), requestId, activation.selectedRangeSize());
        return successor.equals(exactCell.value())
                ? unchanged(exactCell)
                : store.compareAndSetCell(exactCell, successor);
    }

    public CompletionStage<ConditionalCasResult<VersionedManagedLedgerAllocatorHeadV1>> installReservedRange(
            VersionedAllocatorCellStateV1 exactCell, VersionedManagedLedgerAllocatorHeadV1 exactHead) {
        requireActivation(exactCell.value());
        ManagedLedgerAllocatorHeadV1 successor =
                AllocatorProtocolV1.installReservedRange(exactCell.value(), exactHead.value());
        return successor.equals(exactHead.value())
                ? unchanged(exactHead)
                : store.compareAndSetHead(
                        exactCell.value().ledgerIdCompatibilityNamespaceId(),
                        exactCell.value().sliceAssignmentId(),
                        exactHead,
                        successor);
    }

    public CompletionStage<ConditionalCasResult<VersionedManagedLedgerAllocatorHeadV1>> takeover(
            VirtualLedgerCellAllocatorStateV1 exactCell,
            VersionedManagedLedgerAllocatorHeadV1 exactHead,
            long newOwnerEpoch) {
        requireActivation(exactCell);
        ManagedLedgerAllocatorHeadV1 successor = AllocatorProtocolV1.takeover(exactHead.value(), newOwnerEpoch);
        return successor.equals(exactHead.value())
                ? unchanged(exactHead)
                : store.compareAndSetHead(
                        exactCell.ledgerIdCompatibilityNamespaceId(),
                        exactCell.sliceAssignmentId(),
                        exactHead,
                        successor);
    }

    public CompletionStage<CreateMutationResult<VersionedVirtualLedgerCandidateNodeV1>> createCandidate(
            VirtualLedgerCellAllocatorStateV1 exactCell,
            ManagedLedgerAllocatorHeadV1 exactHead,
            Sha256Digest ledgerDescriptorDigest) {
        requireActivation(exactCell);
        VirtualLedgerCandidateNodeV1 candidate = exactCell.mode()
                        == com.nereusstream.domain.registry.allocator.AllocatorModeV1.STRICT_SERIALIZED
                ? AllocatorProtocolV1.strictCandidateFromReservation(exactCell, exactHead, ledgerDescriptorDigest)
                : AllocatorProtocolV1.candidate(exactHead, ledgerDescriptorDigest);
        return store.createNode(exactCell.ledgerIdCompatibilityNamespaceId(), exactCell.sliceAssignmentId(), candidate);
    }

    public CompletionStage<ConditionalCasResult<VersionedManagedLedgerAllocatorHeadV1>> publishCandidate(
            VirtualLedgerCellAllocatorStateV1 exactCell,
            VersionedManagedLedgerAllocatorHeadV1 exactHead,
            VirtualLedgerCandidateNodeV1 exactNode) {
        requireActivation(exactCell);
        ManagedLedgerAllocatorHeadV1 successor =
                exactCell.mode() == com.nereusstream.domain.registry.allocator.AllocatorModeV1.STRICT_SERIALIZED
                        ? AllocatorProtocolV1.publishStrictReserved(exactCell, exactHead.value(), exactNode)
                        : AllocatorProtocolV1.publish(exactHead.value(), exactNode);
        return successor.equals(exactHead.value())
                ? unchanged(exactHead)
                : store.compareAndSetHead(
                        exactCell.ledgerIdCompatibilityNamespaceId(),
                        exactCell.sliceAssignmentId(),
                        exactHead,
                        successor);
    }

    public CompletionStage<ConditionalCasResult<VersionedManagedLedgerAllocatorHeadV1>> burnStaleCandidate(
            VirtualLedgerCellAllocatorStateV1 exactCell,
            VersionedManagedLedgerAllocatorHeadV1 exactHead,
            VirtualLedgerCandidateNodeV1 exactStaleNode) {
        requireActivation(exactCell);
        ManagedLedgerAllocatorHeadV1 successor =
                AllocatorProtocolV1.burnOneStaleCandidate(exactHead.value(), exactStaleNode);
        return successor.equals(exactHead.value())
                ? unchanged(exactHead)
                : store.compareAndSetHead(
                        exactCell.ledgerIdCompatibilityNamespaceId(),
                        exactCell.sliceAssignmentId(),
                        exactHead,
                        successor);
    }

    public CompletionStage<ConditionalCasResult<VersionedAllocatorCellStateV1>> clearReservation(
            VersionedAllocatorCellStateV1 exactCell, ManagedLedgerAllocatorHeadV1 exactHead) {
        requireActivation(exactCell.value());
        VirtualLedgerCellAllocatorStateV1 successor =
                AllocatorProtocolV1.clearInstalledReservation(exactCell.value(), exactHead);
        return successor.equals(exactCell.value())
                ? unchanged(exactCell)
                : store.compareAndSetCell(exactCell, successor);
    }

    private void requireActivation(VirtualLedgerCellAllocatorStateV1 cell) {
        if (cell.mode() != activation.selectedMode()
                || cell.allocatorProtocolVersion() != activation.allocatorProtocolVersion()) {
            throw failure(
                    AllocatorProtocolException.Code.MODE_MISMATCH,
                    "persisted allocator mode/version differs from the selection receipt");
        }
    }

    private static AllocatorProtocolException failure(AllocatorProtocolException.Code code, String message) {
        return new AllocatorProtocolException(code, message);
    }

    private static <T> CompletionStage<ConditionalCasResult<T>> unchanged(T exactPredecessor) {
        return CompletableFuture.completedFuture(ConditionalCasResult.predecessorUnchanged(exactPredecessor));
    }
}
