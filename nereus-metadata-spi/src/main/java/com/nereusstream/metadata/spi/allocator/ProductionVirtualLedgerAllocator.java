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
import com.nereusstream.domain.registry.allocator.AllocatorEvidenceCandidateV1;
import com.nereusstream.domain.registry.allocator.AllocatorModeV1;
import com.nereusstream.domain.registry.allocator.AllocatorProtocolException;
import com.nereusstream.domain.registry.allocator.AllocatorProtocolV1;
import com.nereusstream.domain.registry.allocator.AllocatorSelectionReceiptV1;
import com.nereusstream.domain.registry.allocator.ManagedLedgerAllocatorHeadV1;
import com.nereusstream.domain.registry.allocator.ManagedLedgerIncarnationIdV1;
import com.nereusstream.domain.registry.allocator.VirtualLedgerCandidateNodeV1;
import com.nereusstream.domain.registry.allocator.VirtualLedgerCellAllocatorStateV1;
import com.nereusstream.metadata.spi.model.ConditionalCasResult;
import com.nereusstream.metadata.spi.model.CreateMutationResult;
import com.nereusstream.metadata.spi.model.VersionedVirtualLedgerSliceViewV1;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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

    private ProductionVirtualLedgerAllocator(
            AllocatorEvidenceCandidateV1 candidate, PulsarVirtualLedgerAllocatorStore store, boolean runtimeActivated) {
        Objects.requireNonNull(candidate, "candidate");
        this.selectedMode = candidate.mode();
        this.allocatorProtocolVersion = candidate.allocatorProtocolVersion();
        this.selectedRangeSize = candidate.rangeSize();
        this.runtimeActivated = runtimeActivated;
        this.store = Objects.requireNonNull(store, "store");
    }

    /** Formal evidence seam: exact production coordinator and store adapter, without runtime activation authority. */
    public static ProductionVirtualLedgerAllocator forEvidenceCandidate(
            AllocatorEvidenceCandidateV1 candidate, PulsarVirtualLedgerAllocatorStore store) {
        return new ProductionVirtualLedgerAllocator(candidate, store, false);
    }

    /**
     * Runtime activation verifies the actual packaged domain, SPI, and concrete Oxia adapter artifacts against the raw
     * evidence-derived receipt. No caller supplies an artifact digest or eligibility Boolean.
     */
    public static ProductionVirtualLedgerAllocator activate(
            AllocatorSelectionReceiptV1 receipt, PulsarVirtualLedgerAllocatorStore store) {
        Objects.requireNonNull(receipt, "receipt");
        Objects.requireNonNull(store, "store");
        return activateExactArtifacts(
                receipt,
                store,
                packagedArtifactSha256(AllocatorSelectionReceiptV1.class),
                packagedArtifactSha256(ProductionVirtualLedgerAllocator.class),
                packagedArtifactSha256(store.getClass()));
    }

    static ProductionVirtualLedgerAllocator activateExactArtifacts(
            AllocatorSelectionReceiptV1 receipt,
            PulsarVirtualLedgerAllocatorStore store,
            Sha256Digest domainArtifact,
            Sha256Digest spiArtifact,
            Sha256Digest oxiaArtifact) {
        Objects.requireNonNull(receipt, "receipt");
        Objects.requireNonNull(store, "store");
        if (!receipt.sourceTuple().runtimeDomainArtifactSha256().equals(domainArtifact)
                || !receipt.sourceTuple().runtimeMetadataSpiArtifactSha256().equals(spiArtifact)
                || !receipt.sourceTuple().runtimeMetadataOxiaArtifactSha256().equals(oxiaArtifact)) {
            throw failure(
                    AllocatorProtocolException.Code.SOURCE_MISMATCH,
                    "allocator receipt differs from the exact running domain/SPI/Oxia artifacts");
        }
        AllocatorEvidenceCandidateV1 selected = receipt.selectedMode() == AllocatorModeV1.STRICT_SERIALIZED
                ? AllocatorEvidenceCandidateV1.strict()
                : AllocatorEvidenceCandidateV1.range(receipt.selectedRangeSize());
        return new ProductionVirtualLedgerAllocator(selected, store, true);
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
                exactCell.value().mode() == com.nereusstream.domain.registry.allocator.AllocatorModeV1.STRICT_SERIALIZED
                        ? AllocatorProtocolV1.burnStrictReservedStaleCandidate(
                                exactCell.value(), exactHead.value(), exactStaleNode.value())
                        : AllocatorProtocolV1.burnOneStaleCandidate(exactHead.value(), exactStaleNode.value());
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
        return requireStoredHead(exactCell.value(), exactHead)
                .thenCompose(ignored -> clearSuccessor(exactCell, exactHead))
                .thenCompose(successor -> successor.equals(exactCell.value())
                        ? unchanged(exactCell)
                        : store.compareAndSetCell(exactCell, successor));
    }

    private CompletionStage<VirtualLedgerCellAllocatorStateV1> clearSuccessor(
            VersionedAllocatorCellStateV1 exactCell, VersionedManagedLedgerAllocatorHeadV1 exactHead) {
        if (exactCell.value().reservation().isEmpty()
                || exactCell.value().mode() != AllocatorModeV1.STRICT_SERIALIZED) {
            return CompletableFuture.completedFuture(
                    AllocatorProtocolV1.clearInstalledReservation(exactCell.value(), exactHead.value()));
        }
        long ledgerId = exactCell.value().reservation().orElseThrow().rangeStartInclusive();
        return store.readNode(
                        exactCell.value().ledgerIdCompatibilityNamespaceId(),
                        exactCell.value().sliceAssignmentId(),
                        exactHead.value().managedLedgerIncarnation(),
                        ledgerId)
                .thenApply(observed -> {
                    VersionedVirtualLedgerCandidateNodeV1 exactNode = observed.orElseThrow(() -> failure(
                            AllocatorProtocolException.Code.CANDIDATE_OCCUPANCY_NOT_PROVEN,
                            "STRICT clear requires its exact store-observed node"));
                    requireNodeBoundToCell(exactCell.value(), exactNode);
                    return AllocatorProtocolV1.clearStrictTerminalReservation(
                            exactCell.value(), exactHead.value(), exactNode.value());
                });
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

    /** Creates one independent lock-free coordinator whose retries are serialized only by exact store authority. */
    public BoundedVirtualLedgerAllocatorWorkflowV2 boundedWorkflow(
            int maximumReconcileRetries, BoundedVirtualLedgerAllocatorWorkflowV2.RetryScheduler retryScheduler) {
        return boundedWorkflow(
                new BoundedVirtualLedgerAllocatorWorkflowV2.Bounds(
                        maximumReconcileRetries, java.time.Duration.ofSeconds(4), java.time.Duration.ofMillis(25)),
                retryScheduler);
    }

    /** Creates one coordinator with an exact source-governed attempts/elapsed/backoff envelope. */
    public BoundedVirtualLedgerAllocatorWorkflowV2 boundedWorkflow(
            BoundedVirtualLedgerAllocatorWorkflowV2.Bounds bounds,
            BoundedVirtualLedgerAllocatorWorkflowV2.RetryScheduler retryScheduler) {
        return new BoundedVirtualLedgerAllocatorWorkflowV2(this, store, bounds, retryScheduler);
    }

    ProductionVirtualLedgerAllocator withStore(PulsarVirtualLedgerAllocatorStore replacementStore) {
        AllocatorEvidenceCandidateV1 candidate = selectedMode == AllocatorModeV1.STRICT_SERIALIZED
                ? AllocatorEvidenceCandidateV1.strict()
                : AllocatorEvidenceCandidateV1.range(selectedRangeSize);
        return new ProductionVirtualLedgerAllocator(candidate, replacementStore, runtimeActivated);
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

    private static Sha256Digest packagedArtifactSha256(Class<?> type) {
        try {
            var source = type.getProtectionDomain().getCodeSource();
            if (source == null) {
                throw failure(AllocatorProtocolException.Code.SOURCE_MISMATCH, "allocator code source is absent");
            }
            Path artifact = Path.of(source.getLocation().toURI());
            if (!Files.isRegularFile(artifact, LinkOption.NOFOLLOW_LINKS) || Files.size(artifact) == 0) {
                throw failure(
                        AllocatorProtocolException.Code.SOURCE_MISMATCH,
                        "allocator runtime activation requires packaged regular-file artifacts");
            }
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (InputStream input = Files.newInputStream(artifact, LinkOption.NOFOLLOW_LINKS)) {
                byte[] buffer = new byte[8192];
                int read;
                while ((read = input.read(buffer)) >= 0) {
                    if (read > 0) {
                        digest.update(buffer, 0, read);
                    }
                }
            }
            return Sha256Digest.copyOf(digest.digest());
        } catch (IOException | URISyntaxException | NoSuchAlgorithmException error) {
            throw new AllocatorProtocolException(
                    AllocatorProtocolException.Code.SOURCE_MISMATCH,
                    "allocator packaged runtime artifact could not be hashed",
                    error);
        }
    }

    private static AllocatorProtocolException failure(AllocatorProtocolException.Code code, String message) {
        return new AllocatorProtocolException(code, message);
    }

    private static <T> CompletionStage<ConditionalCasResult<T>> unchanged(T exactPredecessor) {
        return CompletableFuture.completedFuture(ConditionalCasResult.predecessorUnchanged(exactPredecessor));
    }
}
