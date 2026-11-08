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

package com.nereusstream.pulsar.offload.objectwal;

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.registry.allocator.AllocatorModeV1;
import com.nereusstream.domain.registry.allocator.CellAllocatorReservationV1;
import com.nereusstream.domain.registry.allocator.ManagedLedgerIncarnationIdV1;
import com.nereusstream.domain.registry.allocator.VirtualLedgerCandidateNodeV1;
import com.nereusstream.metadata.spi.allocator.ProductionVirtualLedgerAllocator;
import com.nereusstream.metadata.spi.allocator.PulsarVirtualLedgerAllocatorStore;
import com.nereusstream.metadata.spi.allocator.VersionedAllocatorCellStateV1;
import com.nereusstream.metadata.spi.allocator.VersionedManagedLedgerAllocatorHeadV1;
import com.nereusstream.metadata.spi.allocator.VersionedVirtualLedgerCandidateNodeV1;
import com.nereusstream.metadata.spi.model.ConditionalCasOutcome;
import com.nereusstream.metadata.spi.model.ConditionalCasResult;
import com.nereusstream.metadata.spi.model.CreateMutationOutcome;
import com.nereusstream.metadata.spi.model.CreateMutationResult;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import com.nereusstream.metadata.spi.model.VersionedVirtualLedgerSliceViewV1;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Fenced control-plane bridge for the Pulsar Object-WAL virtual-ledger chain.
 *
 * <p>The metadata and allocator implementations remain outside this module. This class enforces the fixed 0.2 slice,
 * exact predecessor linkage, response-loss reconciliation, and fail-closed exhaustion boundary around those narrow
 * production SPIs. Normal entry append does not call this controller.
 */
public final class PulsarVirtualLedgerChainControllerV1 {
    public static final long RESERVED_START_INCLUSIVE = 1L << 62;
    public static final long RESERVED_END_INCLUSIVE = Long.MAX_VALUE - 1;
    public static final long SLICE_SIZE = 1L << 40;
    private static final int MAX_RESPONSE_LOSS_REDRIVES = 8;

    private final FixedSlice slice;
    private final LedgerIdAllocator allocator;
    private final LedgerChainAuthority authority;

    public PulsarVirtualLedgerChainControllerV1(
            FixedSlice slice, LedgerIdAllocator allocator, LedgerChainAuthority authority) {
        this.slice = Objects.requireNonNull(slice, "slice");
        this.allocator = Objects.requireNonNull(allocator, "allocator");
        this.authority = Objects.requireNonNull(authority, "authority");
    }

    /** Opens the exact durable head, or creates the first ledger through one fenced head mutation. */
    public CompletionStage<OpenedLedger> open(PulsarBindingKey binding, long ownerEpoch) {
        try {
            requireOwnerEpoch(ownerEpoch);
            Objects.requireNonNull(binding, "binding");
            return authority.readHead(binding).thenCompose(existing -> {
                if (existing.isPresent()) {
                    return completed(validateOpened(binding, ownerEpoch, existing.orElseThrow()));
                }
                return allocator.allocate(binding, slice, ownerEpoch).thenCompose(ledgerId -> {
                    slice.requireContains(ledgerId);
                    LedgerNode candidate = LedgerNode.first(binding, ledgerId, ownerEpoch);
                    return installCandidate(Optional.empty(), candidate);
                });
            });
        } catch (Throwable error) {
            return failed(error);
        }
    }

    /** Returns the exact sealed predecessors, newest first, rooted in the opened active ledger. */
    public CompletionStage<List<SealedLedger>> readSealedHistory(OpenedLedger opened, int maximumNodes) {
        if (maximumNodes <= 0) {
            return failed(new IllegalArgumentException("Object WAL history bound must be positive"));
        }
        return authority.readSealedHistory(opened, maximumNodes);
    }

    /** The same durable Head must still be current after provider recovery. */
    public CompletionStage<Void> requireUnchangedHead(OpenedLedger opened) {
        return authority.readHead(opened.node().binding()).thenAccept(actual -> {
            if (!actual.equals(
                    Optional.of(new HeadSnapshot(opened.authorityVersion(), opened.node(), opened.ownerEpoch())))) {
                throw rejected(ChainRejectionCode.HEAD_CONFLICT, "Object WAL Head changed during recovery");
            }
        });
    }

    public record SealedLedger(LedgerNode node, long terminalEntryId) {
        public SealedLedger {
            Objects.requireNonNull(node, "node");
            if (terminalEntryId < -1) {
                throw rejected(ChainRejectionCode.INVALID_TERMINAL_ENTRY, "invalid recovered terminal entry");
            }
        }
    }

    /**
     * Atomically seals the current ledger at {@code terminalEntryId} and installs one exact successor head.
     * Allocation exhaustion is propagated and never searches another slice or wraps the ledger ID.
     */
    public CompletionStage<OpenedLedger> sealAndOpenSuccessor(
            OpenedLedger current, long terminalEntryId, long ownerEpoch) {
        try {
            Objects.requireNonNull(current, "current");
            requireOwnerEpoch(ownerEpoch);
            if (current.ownerEpoch() != ownerEpoch) {
                throw rejected(ChainRejectionCode.STALE_OWNER, "owner epoch differs from the opened ledger");
            }
            if (terminalEntryId < -1) {
                throw rejected(ChainRejectionCode.INVALID_TERMINAL_ENTRY, "terminal entry must be -1 or non-negative");
            }
            return allocator
                    .allocate(current.node().binding(), slice, ownerEpoch)
                    .thenCompose(ledgerId -> {
                        slice.requireContains(ledgerId);
                        if (ledgerId <= current.node().virtualLedgerId()) {
                            throw rejected(
                                    ChainRejectionCode.LEDGER_ID_NOT_MONOTONIC,
                                    "successor ledger ID must be strictly greater than its predecessor");
                        }
                        LedgerNode candidate = LedgerNode.successor(
                                current.node().binding(),
                                ledgerId,
                                current.node().virtualLedgerId(),
                                terminalEntryId,
                                ownerEpoch);
                        return installCandidate(
                                Optional.of(new HeadSnapshot(current.authorityVersion(), current.node(), ownerEpoch)),
                                candidate);
                    });
        } catch (Throwable error) {
            return failed(error);
        }
    }

    private CompletionStage<OpenedLedger> installCandidate(
            Optional<HeadSnapshot> exactPredecessor, LedgerNode candidate) {
        return installCandidate(exactPredecessor, candidate, MAX_RESPONSE_LOSS_REDRIVES);
    }

    private CompletionStage<OpenedLedger> installCandidate(
            Optional<HeadSnapshot> exactPredecessor, LedgerNode candidate, int remainingRedrives) {
        return authority
                .compareAndSetHead(candidate.binding(), exactPredecessor, candidate)
                .thenCompose(outcome -> {
                    Objects.requireNonNull(outcome, "chain mutation outcome");
                    return switch (outcome.kind()) {
                        case APPLIED_EXACT, EXISTING_EXACT ->
                            completed(requireExactCandidate(candidate, outcome.observed()));
                        case OUTCOME_UNKNOWN ->
                            authority.readHead(candidate.binding()).thenCompose(observed -> {
                                if (observed.isPresent()
                                        && candidate.equals(
                                                observed.orElseThrow().node())) {
                                    return completed(requireExactCandidate(candidate, observed));
                                }
                                if (!observed.equals(exactPredecessor) || remainingRedrives == 0) {
                                    return failed(rejected(
                                            ChainRejectionCode.MUTATION_NOT_RECONCILED,
                                            "Head reread proves neither the exact candidate "
                                                    + "nor its exact predecessor"));
                                }
                                return installCandidate(exactPredecessor, candidate, remainingRedrives - 1);
                            });
                        case DEFINITIVE_CONFLICT ->
                            failed(rejected(
                                    ChainRejectionCode.HEAD_CONFLICT,
                                    "virtual-ledger head mutation lost its fenced comparison"));
                    };
                });
    }

    private OpenedLedger requireExactCandidate(LedgerNode candidate, Optional<HeadSnapshot> observed) {
        HeadSnapshot exact = observed.orElseThrow(() ->
                rejected(ChainRejectionCode.MUTATION_NOT_RECONCILED, "head mutation has no exact observed value"));
        if (!candidate.equals(exact.node())) {
            throw rejected(
                    ChainRejectionCode.MUTATION_NOT_RECONCILED,
                    "head mutation response does not match the exact candidate");
        }
        return validateOpened(candidate.binding(), candidate.ownerEpoch(), exact);
    }

    private OpenedLedger validateOpened(PulsarBindingKey binding, long ownerEpoch, HeadSnapshot snapshot) {
        if (!snapshot.node().binding().equals(binding)) {
            throw rejected(ChainRejectionCode.BINDING_MISMATCH, "head belongs to another Pulsar binding");
        }
        slice.requireContains(snapshot.node().virtualLedgerId());
        if (snapshot.ownerEpoch() != ownerEpoch) {
            throw rejected(ChainRejectionCode.STALE_OWNER, "durable head belongs to another owner epoch");
        }
        return new OpenedLedger(snapshot.authorityVersion(), snapshot.node(), ownerEpoch);
    }

    private static void requireOwnerEpoch(long ownerEpoch) {
        if (ownerEpoch <= 0) {
            throw rejected(ChainRejectionCode.STALE_OWNER, "owner epoch must be positive");
        }
    }

    private static <T> CompletionStage<T> completed(T value) {
        return CompletableFuture.completedFuture(value);
    }

    private static <T> CompletionStage<T> failed(Throwable error) {
        return CompletableFuture.failedFuture(error);
    }

    private static ChainException rejected(ChainRejectionCode code, String message) {
        return new ChainException(code, message);
    }

    /** One immutable aligned 2^40 assignment. A second interval is deliberately unrepresentable. */
    public record FixedSlice(long startInclusive, long endInclusive) {
        public FixedSlice {
            if (startInclusive < RESERVED_START_INCLUSIVE
                    || endInclusive > RESERVED_END_INCLUSIVE
                    || endInclusive < startInclusive
                    || startInclusive - RESERVED_START_INCLUSIVE < 0
                    || (startInclusive - RESERVED_START_INCLUSIVE) % SLICE_SIZE != 0
                    || endInclusive - startInclusive != SLICE_SIZE - 1) {
                throw rejected(
                        ChainRejectionCode.INVALID_SLICE, "slice must be one aligned 2^40 reserved-domain interval");
            }
        }

        public void requireContains(long ledgerId) {
            if (ledgerId < startInclusive || ledgerId > endInclusive) {
                throw rejected(ChainRejectionCode.SLICE_EXHAUSTED, "ledger ID lies outside the immutable Cell slice");
            }
        }
    }

    /** Stable protocol-binding and incarnation identity; Object identity is intentionally absent. */
    public record PulsarBindingKey(
            String protocolCellId,
            String topicBindingId,
            String topicIncarnation,
            String storageEpochId,
            String positionDomainId) {
        public PulsarBindingKey {
            protocolCellId = requireIdentifier(protocolCellId, "protocolCellId");
            topicBindingId = requireIdentifier(topicBindingId, "topicBindingId");
            topicIncarnation = requireIdentifier(topicIncarnation, "topicIncarnation");
            storageEpochId = requireIdentifier(storageEpochId, "storageEpochId");
            positionDomainId = requireIdentifier(positionDomainId, "positionDomainId");
        }

        private static String requireIdentifier(String value, String name) {
            Objects.requireNonNull(value, name);
            if (value.isBlank() || value.length() > 256 || value.indexOf('\0') >= 0) {
                throw new IllegalArgumentException(name + " must be non-blank bounded text without NUL");
            }
            return value;
        }
    }

    /** Current append-admitting ledger and the exact CAS version that installed it. */
    public record OpenedLedger(MetadataVersion authorityVersion, LedgerNode node, long ownerEpoch) {
        public OpenedLedger(MetadataVersion authorityVersion, LedgerNode node) {
            this(authorityVersion, node, node.ownerEpoch());
        }

        public OpenedLedger {
            Objects.requireNonNull(authorityVersion, "authorityVersion");
            Objects.requireNonNull(node, "node");
            if (ownerEpoch < node.ownerEpoch()) {
                throw new IllegalArgumentException("Active owner predates the immutable ledger creator");
            }
        }
    }

    /**
     * In-memory projection of the ManagedLedger descriptor bound by one NVAN1 ledgerDescriptorDigest. Production
     * persistence remains ADR 0091 NVAH1/NVAN1; this record defines no key or wire encoding.
     */
    public record LedgerNode(
            PulsarBindingKey binding,
            long virtualLedgerId,
            OptionalLong predecessorLedgerId,
            long predecessorTerminalEntryId,
            long ownerEpoch) {
        public LedgerNode {
            Objects.requireNonNull(binding, "binding");
            Objects.requireNonNull(predecessorLedgerId, "predecessorLedgerId");
            if (virtualLedgerId <= 0 || ownerEpoch <= 0) {
                throw new IllegalArgumentException("ledger ID and owner epoch must be positive");
            }
            if (predecessorLedgerId.isEmpty() && predecessorTerminalEntryId != -1) {
                throw new IllegalArgumentException("first ledger cannot carry a predecessor terminal entry");
            }
            if (predecessorLedgerId.isPresent()
                    && (predecessorLedgerId.getAsLong() <= 0
                            || predecessorLedgerId.getAsLong() >= virtualLedgerId
                            || predecessorTerminalEntryId < -1)) {
                throw new IllegalArgumentException("successor predecessor fields are invalid");
            }
        }

        static LedgerNode first(PulsarBindingKey binding, long ledgerId, long ownerEpoch) {
            return new LedgerNode(binding, ledgerId, OptionalLong.empty(), -1, ownerEpoch);
        }

        static LedgerNode successor(
                PulsarBindingKey binding,
                long ledgerId,
                long predecessorLedgerId,
                long predecessorTerminalEntryId,
                long ownerEpoch) {
            return new LedgerNode(
                    binding, ledgerId, OptionalLong.of(predecessorLedgerId), predecessorTerminalEntryId, ownerEpoch);
        }
    }

    public record HeadSnapshot(MetadataVersion authorityVersion, LedgerNode node, long ownerEpoch) {
        public HeadSnapshot(MetadataVersion authorityVersion, LedgerNode node) {
            this(authorityVersion, node, node.ownerEpoch());
        }

        public HeadSnapshot {
            Objects.requireNonNull(authorityVersion, "authorityVersion");
            Objects.requireNonNull(node, "node");
            if (ownerEpoch < node.ownerEpoch()) {
                throw new IllegalArgumentException("Head owner predates the immutable ledger creator");
            }
        }
    }

    public enum MutationKind {
        APPLIED_EXACT,
        EXISTING_EXACT,
        OUTCOME_UNKNOWN,
        DEFINITIVE_CONFLICT
    }

    public record MutationOutcome(MutationKind kind, Optional<HeadSnapshot> observed) {
        public MutationOutcome {
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(observed, "observed");
        }
    }

    public interface LedgerIdAllocator {
        /** Returns one never-reused candidate or fails with {@link SliceExhaustedException}. */
        CompletionStage<Long> allocate(PulsarBindingKey binding, FixedSlice slice, long ownerEpoch);
    }

    public interface LedgerChainAuthority {
        CompletionStage<Optional<HeadSnapshot>> readHead(PulsarBindingKey binding);

        default CompletionStage<List<SealedLedger>> readSealedHistory(OpenedLedger opened, int maximumNodes) {
            return opened.node().predecessorLedgerId().isEmpty()
                    ? completed(List.of())
                    : failed(rejected(
                            ChainRejectionCode.DESCRIPTOR_AUTHORITY_MISMATCH,
                            "sealed predecessor authority is unavailable"));
        }

        /**
         * Atomically installs the first head when {@code exactPredecessor} is empty, or seals the exact predecessor and
         * installs the successor otherwise. The authority must persist explicit predecessor order, not numeric order.
         */
        CompletionStage<MutationOutcome> compareAndSetHead(
                PulsarBindingKey binding, Optional<HeadSnapshot> exactPredecessor, LedgerNode candidate);
    }

    /** Exact current Registry/ManagedLedger identity used to address ADR 0091 NVAC1/NVAH1/NVAN1 authority. */
    public record NvBindingContext(
            PulsarBindingKey binding,
            VersionedVirtualLedgerSliceViewV1 currentSliceView,
            ManagedLedgerIncarnationIdV1 managedLedgerIncarnation) {
        public NvBindingContext {
            Objects.requireNonNull(binding, "binding");
            Objects.requireNonNull(currentSliceView, "currentSliceView");
            Objects.requireNonNull(managedLedgerIncarnation, "managedLedgerIncarnation");
        }
    }

    /** Registry/current-incarnation lookup; allocation requires ACTIVE, while existing-chain reads require geometry. */
    public interface NvBindingContextAuthority {
        CompletionStage<NvBindingContext> load(PulsarBindingKey binding);
    }

    /** The immutable descriptor is bound by NVAN1. Only the NVAH1 pointer can make a candidate visible. */
    public interface NvLedgerDescriptorAuthority {
        Sha256Digest requireDescriptorDigest(LedgerNode candidate);

        CompletionStage<Void> persistCandidate(VersionedVirtualLedgerCandidateNodeV1 exactNode, LedgerNode candidate);

        CompletionStage<ResolvedNvHead> resolveVisibleHead(
                PulsarBindingKey binding, VersionedManagedLedgerAllocatorHeadV1 exactHead);

        default CompletionStage<ResolvedNvHead> resolvePredecessor(ResolvedNvHead child) {
            return failed(rejected(
                    ChainRejectionCode.DESCRIPTOR_AUTHORITY_MISMATCH, "NVAN1 predecessor lookup is unavailable"));
        }
    }

    /** Stable operation identity supplied by the owning allocator workflow, not derived from a transient future. */
    public interface NvAllocationRequestAuthority {
        Sha256Digest requestId(
                PulsarBindingKey binding,
                long ownerEpoch,
                VersionedManagedLedgerAllocatorHeadV1 exactHead,
                VersionedVirtualLedgerSliceViewV1 currentSliceView);
    }

    public record ResolvedNvHead(VersionedVirtualLedgerCandidateNodeV1 exactNode, LedgerNode ledgerNode) {
        public ResolvedNvHead {
            Objects.requireNonNull(exactNode, "exactNode");
            Objects.requireNonNull(ledgerNode, "ledgerNode");
        }
    }

    /**
     * Production adapter over the receipt-activated ADR 0091 coordinator and its injected Oxia-backed store.
     * Allocation reserves exactly one NV grant/cursor value; Head publication creates NVAN1 then CASes NVAH1. No
     * logical chain bytes or keys are persisted by this class.
     */
    public static final class ProductionNvAllocatorHeadAdapter implements LedgerIdAllocator, LedgerChainAuthority {
        private final FixedSlice fixedSlice;
        private final ProductionVirtualLedgerAllocator allocator;
        private final PulsarVirtualLedgerAllocatorStore store;
        private final NvBindingContextAuthority contexts;
        private final NvLedgerDescriptorAuthority descriptors;
        private final NvAllocationRequestAuthority requestAuthority;
        private final Object monitor = new Object();
        private final Map<PulsarBindingKey, PendingNvAllocation> pending = new HashMap<>();

        public ProductionNvAllocatorHeadAdapter(
                FixedSlice fixedSlice,
                ProductionVirtualLedgerAllocator allocator,
                PulsarVirtualLedgerAllocatorStore store,
                NvBindingContextAuthority contexts,
                NvLedgerDescriptorAuthority descriptors,
                NvAllocationRequestAuthority requestAuthority) {
            this.fixedSlice = Objects.requireNonNull(fixedSlice, "fixedSlice");
            this.allocator = Objects.requireNonNull(allocator, "allocator");
            if (!allocator.runtimeActivated()) {
                throw new IllegalArgumentException("Pulsar runtime requires a receipt-activated production allocator");
            }
            this.store = Objects.requireNonNull(store, "store");
            this.contexts = Objects.requireNonNull(contexts, "contexts");
            this.descriptors = Objects.requireNonNull(descriptors, "descriptors");
            this.requestAuthority = Objects.requireNonNull(requestAuthority, "requestAuthority");
        }

        @Override
        public CompletionStage<Long> allocate(PulsarBindingKey binding, FixedSlice slice, long ownerEpoch) {
            try {
                Objects.requireNonNull(binding, "binding");
                requireOwnerEpoch(ownerEpoch);
                if (!fixedSlice.equals(slice)) {
                    throw rejected(ChainRejectionCode.INVALID_SLICE, "allocator request differs from the fixed slice");
                }
                synchronized (monitor) {
                    if (pending.containsKey(binding)) {
                        throw rejected(
                                ChainRejectionCode.ALLOCATION_ALREADY_PENDING,
                                "one binding already owns an unpublished NVAN candidate allocation");
                    }
                }
                return loadAuthority(binding, ownerEpoch).thenCompose(original -> prepareUsableGrant(original)
                        .thenCompose(this::settleOccupiedCursor)
                        .thenApply(state -> {
                            if (!state.head()
                                    .value()
                                    .visibleChainHead()
                                    .equals(original.head().value().visibleChainHead())) {
                                throw rejected(
                                        ChainRejectionCode.HEAD_CONFLICT,
                                        "Grant preparation changed the original visible chain head");
                            }
                            long ledgerId = nextLedgerId(state);
                            fixedSlice.requireContains(ledgerId);
                            synchronized (monitor) {
                                if (pending.putIfAbsent(
                                                binding,
                                                new PendingNvAllocation(
                                                        state.context(),
                                                        state.cell(),
                                                        state.head(),
                                                        original.head().metadataVersion(),
                                                        ledgerId))
                                        != null) {
                                    throw rejected(
                                            ChainRejectionCode.ALLOCATION_ALREADY_PENDING,
                                            "one binding already owns an unpublished NVAN candidate allocation");
                                }
                            }
                            return ledgerId;
                        }));
            } catch (Throwable error) {
                return failed(error);
            }
        }

        @Override
        public CompletionStage<Optional<HeadSnapshot>> readHead(PulsarBindingKey binding) {
            try {
                Objects.requireNonNull(binding, "binding");
                return contexts.load(binding).thenCompose(context -> {
                    requireContext(binding, context);
                    var assignment = context.currentSliceView().value().assignment();
                    requireFixedGeometry(context.currentSliceView());
                    return store.readHead(
                                    assignment.ledgerIdCompatibilityNamespaceId(),
                                    assignment.sliceAssignmentId(),
                                    context.managedLedgerIncarnation())
                            .thenCompose(head -> head.isEmpty()
                                            || head.orElseThrow()
                                                    .value()
                                                    .visibleChainHead()
                                                    .isAbsent()
                                    ? completed(Optional.empty())
                                    : resolveExactHead(binding, head.orElseThrow())
                                            .thenApply(snapshot -> {
                                                synchronized (monitor) {
                                                    PendingNvAllocation allocation = pending.get(binding);
                                                    if (allocation != null
                                                            && allocation.matchesPublished(snapshot.node())) {
                                                        pending.remove(binding, allocation);
                                                    }
                                                }
                                                return Optional.of(snapshot);
                                            }));
                });
            } catch (Throwable error) {
                return failed(error);
            }
        }

        @Override
        public CompletionStage<MutationOutcome> compareAndSetHead(
                PulsarBindingKey binding, Optional<HeadSnapshot> exactPredecessor, LedgerNode candidate) {
            PendingNvAllocation allocation;
            try {
                Objects.requireNonNull(binding, "binding");
                Objects.requireNonNull(exactPredecessor, "exactPredecessor");
                Objects.requireNonNull(candidate, "candidate");
                if (!candidate.binding().equals(binding)) {
                    throw rejected(ChainRejectionCode.BINDING_MISMATCH, "candidate belongs to another binding");
                }
                synchronized (monitor) {
                    allocation = Optional.ofNullable(pending.get(binding))
                            .orElseThrow(() -> rejected(
                                    ChainRejectionCode.ALLOCATION_NOT_PENDING,
                                    "Head publication has no exact pending NV allocation"));
                }
                requireCandidateMatchesAllocation(allocation, exactPredecessor, candidate);
            } catch (Throwable error) {
                return failed(error);
            }

            final Sha256Digest descriptorDigest;
            try {
                descriptorDigest = Objects.requireNonNull(
                        descriptors.requireDescriptorDigest(candidate), "ledger descriptor digest");
                if (descriptorDigest.isZero()) {
                    throw rejected(
                            ChainRejectionCode.DESCRIPTOR_AUTHORITY_MISMATCH,
                            "ledger descriptor digest must be non-zero");
                }
            } catch (Throwable error) {
                return failed(error);
            }
            synchronized (monitor) {
                PendingNvAllocation current = pending.get(binding);
                if (current != allocation) {
                    return failed(rejected(
                            ChainRejectionCode.ALLOCATION_NOT_PENDING,
                            "pending NV allocation changed before candidate intent installation"));
                }
                allocation = allocation.withIntent(candidate, descriptorDigest);
                pending.put(binding, allocation);
            }
            PendingNvAllocation exactAllocation = allocation;
            return allocator
                    .createCandidate(
                            exactAllocation.cell(),
                            exactAllocation.head(),
                            exactAllocation.context().currentSliceView(),
                            descriptorDigest)
                    .thenCompose(created -> publishCreatedCandidate(exactAllocation, candidate, created))
                    .whenComplete((outcome, error) -> {
                        if (error == null && outcome.kind() != MutationKind.OUTCOME_UNKNOWN) {
                            synchronized (monitor) {
                                pending.remove(binding, exactAllocation);
                            }
                        }
                    });
        }

        private CompletionStage<AuthorityState> loadAuthority(PulsarBindingKey binding, long ownerEpoch) {
            return contexts.load(binding)
                    .thenCompose(context -> {
                        requireContext(binding, context);
                        requireFixedAssignment(context.currentSliceView());
                        var assignment = context.currentSliceView().value().assignment();
                        return store.readCell(
                                        assignment.ledgerIdCompatibilityNamespaceId(), assignment.sliceAssignmentId())
                                .thenApply(cell -> cell.orElseThrow(() -> rejected(
                                        ChainRejectionCode.MUTATION_NOT_RECONCILED,
                                        "NV allocator Cell must already be provisioned")))
                                .thenCompose(cell -> store.readHead(
                                                assignment.ledgerIdCompatibilityNamespaceId(),
                                                assignment.sliceAssignmentId(),
                                                context.managedLedgerIncarnation())
                                        .thenApply(head -> new AuthorityState(
                                                context,
                                                cell,
                                                head.orElseThrow(() -> rejected(
                                                        ChainRejectionCode.MUTATION_NOT_RECONCILED,
                                                        "NV allocator Head must already be provisioned")))));
                    })
                    .thenApply(state -> {
                        if (state.head().value().ownerEpoch() != ownerEpoch) {
                            throw rejected(ChainRejectionCode.STALE_OWNER, "NVAH1 belongs to another owner epoch");
                        }
                        return state;
                    });
        }

        private CompletionStage<AuthorityState> prepareUsableGrant(AuthorityState state) {
            if (state.cell().value().mode() != AllocatorModeV1.RANGE_LEASED) {
                return reserveAndInstall(state);
            }
            var reservation = state.cell().value().reservation();
            if (reservation.isPresent()
                    && reservation
                            .orElseThrow()
                            .managedLedgerIncarnation()
                            .equals(state.head().value().managedLedgerIncarnation())) {
                var exact = reservation.orElseThrow();
                if (state.head().value().grantId() != exact.grantId()) {
                    return installAndClearRange(state).thenCompose(this::prepareUsableGrant);
                }
                return clearInstalledRange(state).thenCompose(this::prepareUsableGrant);
            }
            return hasUsableInstalledRange(state.head()) ? completed(state) : reserveAndInstall(state);
        }

        private CompletionStage<AuthorityState> reserveAndInstall(AuthorityState state) {
            Sha256Digest requestId = Objects.requireNonNull(
                    requestAuthority.requestId(
                            state.context().binding(),
                            state.head().value().ownerEpoch(),
                            state.head(),
                            state.context().currentSliceView()),
                    "allocation request ID");
            if (requestId.isZero()) {
                return failed(rejected(
                        ChainRejectionCode.DESCRIPTOR_AUTHORITY_MISMATCH, "allocation request ID must be non-zero"));
            }
            return requireExactCas(
                            allocator.reserve(
                                    state.cell(), state.head(), state.context().currentSliceView(), requestId),
                            "NVAC1 reserve")
                    .thenCompose(reservedCell -> {
                        var reserved = new AuthorityState(state.context(), reservedCell, state.head());
                        return reservedCell.value().mode() == AllocatorModeV1.STRICT_SERIALIZED
                                ? completed(reserved)
                                : installAndClearRange(reserved);
                    });
        }

        private CompletionStage<AuthorityState> installAndClearRange(AuthorityState reserved) {
            return requireExactCas(
                            allocator.installRangeReservedGrant(
                                    reserved.cell(),
                                    reserved.head(),
                                    reserved.context().currentSliceView()),
                            "NVAH1 range install")
                    .thenCompose(
                            head -> clearInstalledRange(new AuthorityState(reserved.context(), reserved.cell(), head)));
        }

        private CompletionStage<AuthorityState> clearInstalledRange(AuthorityState state) {
            return requireExactCas(allocator.clearReservation(state.cell(), state.head()), "NVAC1 range clear")
                    .thenApply(cleared -> new AuthorityState(state.context(), cleared, state.head()));
        }

        /** Only the exact stale node at the occupied cursor can be consumed; no node is rewritten or removed. */
        private CompletionStage<AuthorityState> settleOccupiedCursor(AuthorityState state) {
            if (state.cell().value().mode() != AllocatorModeV1.RANGE_LEASED) {
                return completed(state);
            }
            var assignment = state.context().currentSliceView().value().assignment();
            return store.readNode(
                            assignment.ledgerIdCompatibilityNamespaceId(),
                            assignment.sliceAssignmentId(),
                            state.context().managedLedgerIncarnation(),
                            state.head().value().nextLedgerId())
                    .thenCompose(found -> {
                        if (found.isEmpty()) {
                            return completed(state);
                        }
                        var occupied = found.orElseThrow();
                        if (occupied.value().creatorOwnerEpoch()
                                >= state.head().value().ownerEpoch()) {
                            return failed(rejected(
                                    ChainRejectionCode.MUTATION_NOT_RECONCILED,
                                    "current Object WAL allocator cursor has an unresolved same-owner candidate"));
                        }
                        return requireExactCas(
                                        allocator.burnStaleCandidate(
                                                state.cell(),
                                                state.head(),
                                                occupied,
                                                state.context().currentSliceView()),
                                        "NVAH1 burn stale candidate")
                                .thenCompose(burned ->
                                        prepareUsableGrant(new AuthorityState(state.context(), state.cell(), burned)));
                    });
        }

        private CompletionStage<MutationOutcome> publishCreatedCandidate(
                PendingNvAllocation allocation,
                LedgerNode logicalCandidate,
                CreateMutationResult<VersionedVirtualLedgerCandidateNodeV1> created) {
            if (created.outcome() == CreateMutationOutcome.DEFINITIVE_CONFLICT) {
                return completed(new MutationOutcome(MutationKind.DEFINITIVE_CONFLICT, Optional.empty()));
            }
            if (created.outcome() == CreateMutationOutcome.INDETERMINATE) {
                return completed(new MutationOutcome(MutationKind.OUTCOME_UNKNOWN, Optional.empty()));
            }
            VersionedVirtualLedgerCandidateNodeV1 exactNode =
                    created.exactSnapshot().orElseThrow();
            requireExactNode(allocation, logicalCandidate, exactNode);
            return descriptors
                    .persistCandidate(exactNode, logicalCandidate)
                    .thenCompose(unused -> allocator.publishCandidate(
                            allocation.cell(),
                            allocation.head(),
                            exactNode,
                            allocation.context().currentSliceView()))
                    .thenCompose(published -> finishPublishedHead(allocation, logicalCandidate, exactNode, published));
        }

        private CompletionStage<MutationOutcome> finishPublishedHead(
                PendingNvAllocation allocation,
                LedgerNode logicalCandidate,
                VersionedVirtualLedgerCandidateNodeV1 exactNode,
                ConditionalCasResult<VersionedManagedLedgerAllocatorHeadV1> published) {
            if (published.outcome() == ConditionalCasOutcome.DEFINITIVE_CONFLICT) {
                return completed(new MutationOutcome(MutationKind.DEFINITIVE_CONFLICT, Optional.empty()));
            }
            if (published.outcome() == ConditionalCasOutcome.INDETERMINATE) {
                return completed(new MutationOutcome(MutationKind.OUTCOME_UNKNOWN, Optional.empty()));
            }
            VersionedManagedLedgerAllocatorHeadV1 exactHead =
                    published.exactSnapshot().orElseThrow();
            if (!exactHead.value().visibleChainHead().equals(exactNode.value().pointer())) {
                throw rejected(
                        ChainRejectionCode.MUTATION_NOT_RECONCILED,
                        "exact NVAH1 does not publish the created NVAN1 pointer");
            }
            HeadSnapshot snapshot = new HeadSnapshot(exactHead.metadataVersion(), logicalCandidate);
            if (!installedReservationExhausted(allocation.cell(), exactHead)) {
                return completed(new MutationOutcome(kindFor(published.outcome()), Optional.of(snapshot)));
            }
            return allocator.clearReservation(allocation.cell(), exactHead).thenApply(cleared -> {
                if (cleared.outcome() == ConditionalCasOutcome.INDETERMINATE) {
                    return new MutationOutcome(MutationKind.OUTCOME_UNKNOWN, Optional.empty());
                }
                if (cleared.outcome() == ConditionalCasOutcome.DEFINITIVE_CONFLICT) {
                    throw rejected(
                            ChainRejectionCode.MUTATION_NOT_RECONCILED,
                            "published NVAH1 has an unreconciled NVAC1 reservation clear");
                }
                return new MutationOutcome(kindFor(published.outcome()), Optional.of(snapshot));
            });
        }

        @Override
        public CompletionStage<List<SealedLedger>> readSealedHistory(OpenedLedger opened, int maximumNodes) {
            PulsarBindingKey binding = opened.node().binding();
            return contexts.load(binding).thenCompose(context -> {
                requireContext(binding, context);
                requireFixedGeometry(context.currentSliceView());
                var assignment = context.currentSliceView().value().assignment();
                return store.readHead(
                                assignment.ledgerIdCompatibilityNamespaceId(),
                                assignment.sliceAssignmentId(),
                                context.managedLedgerIncarnation())
                        .thenCompose(found -> {
                            var head = found.orElseThrow(
                                    () -> rejected(ChainRejectionCode.HEAD_CONFLICT, "Object WAL Head is absent"));
                            if (!head.metadataVersion().equals(opened.authorityVersion())
                                    || head.value().ownerEpoch() != opened.ownerEpoch()) {
                                return failed(rejected(
                                        ChainRejectionCode.HEAD_CONFLICT,
                                        "Object WAL Head changed before reading its history"));
                            }
                            return descriptors.resolveVisibleHead(binding, head).thenCompose(active -> {
                                if (!active.exactNode()
                                                .value()
                                                .pointer()
                                                .equals(head.value().visibleChainHead())
                                        || !active.ledgerNode().equals(opened.node())) {
                                    return failed(rejected(
                                            ChainRejectionCode.DESCRIPTOR_AUTHORITY_MISMATCH,
                                            "Object WAL lineage is not rooted in its active Head"));
                                }
                                requireLogicalNode(binding, active);
                                return readPredecessors(binding, active, maximumNodes, new ArrayList<>());
                            });
                        });
            });
        }

        private CompletionStage<List<SealedLedger>> readPredecessors(
                PulsarBindingKey binding, ResolvedNvHead child, int remaining, List<SealedLedger> history) {
            var expected = child.exactNode().value().expectedPredecessor();
            if (expected.isAbsent()) {
                if (child.ledgerNode().predecessorLedgerId().isPresent()) {
                    return failed(rejected(
                            ChainRejectionCode.DESCRIPTOR_AUTHORITY_MISMATCH,
                            "logical predecessor differs from the root NVAN1"));
                }
                return completed(List.copyOf(history));
            }
            if (remaining == 0) {
                return failed(rejected(
                        ChainRejectionCode.HISTORY_BOUND_EXCEEDED,
                        "Object WAL ledger history exceeds its recovery bound"));
            }
            return descriptors.resolvePredecessor(child).thenCompose(parent -> {
                requireLogicalNode(binding, parent);
                if (!parent.exactNode().value().pointer().equals(expected)
                        || !child.ledgerNode()
                                .predecessorLedgerId()
                                .equals(OptionalLong.of(parent.ledgerNode().virtualLedgerId()))
                        || parent.ledgerNode().ownerEpoch() > child.ledgerNode().ownerEpoch()) {
                    throw rejected(
                            ChainRejectionCode.DESCRIPTOR_AUTHORITY_MISMATCH,
                            "typed predecessor pointer or logical predecessor differs");
                }
                history.add(
                        new SealedLedger(parent.ledgerNode(), child.ledgerNode().predecessorTerminalEntryId()));
                return readPredecessors(binding, parent, remaining - 1, history);
            });
        }

        private void requireLogicalNode(PulsarBindingKey binding, ResolvedNvHead resolved) {
            VirtualLedgerCandidateNodeV1 node = resolved.exactNode().value();
            LedgerNode logical = resolved.ledgerNode();
            if (!logical.binding().equals(binding)
                    || logical.virtualLedgerId() != node.ledgerId()
                    || logical.ownerEpoch() != node.creatorOwnerEpoch()
                    || !descriptors.requireDescriptorDigest(logical).equals(node.ledgerDescriptorDigest())) {
                throw rejected(
                        ChainRejectionCode.DESCRIPTOR_AUTHORITY_MISMATCH,
                        "resolved ManagedLedger descriptor differs from exact NVAN1 authority");
            }
            fixedSlice.requireContains(logical.virtualLedgerId());
        }

        private CompletionStage<HeadSnapshot> resolveExactHead(
                PulsarBindingKey binding, VersionedManagedLedgerAllocatorHeadV1 exactHead) {
            return descriptors.resolveVisibleHead(binding, exactHead).thenApply(resolved -> {
                requireLogicalNode(binding, resolved);
                if (!exactHead
                        .value()
                        .visibleChainHead()
                        .equals(resolved.exactNode().value().pointer())) {
                    throw rejected(
                            ChainRejectionCode.DESCRIPTOR_AUTHORITY_MISMATCH,
                            "resolved NVAN1 pointer differs from exact NVAH1 authority");
                }
                return new HeadSnapshot(
                        exactHead.metadataVersion(),
                        resolved.ledgerNode(),
                        exactHead.value().ownerEpoch());
            });
        }

        private void requireCandidateMatchesAllocation(
                PendingNvAllocation allocation, Optional<HeadSnapshot> exactPredecessor, LedgerNode candidate) {
            if (allocation.ledgerId() != candidate.virtualLedgerId()
                    || allocation.head().value().ownerEpoch() != candidate.ownerEpoch()) {
                throw rejected(
                        ChainRejectionCode.MUTATION_NOT_RECONCILED,
                        "logical candidate differs from the exact NV allocation");
            }
            boolean nvPredecessorAbsent =
                    allocation.head().value().visibleChainHead().isAbsent();
            if (nvPredecessorAbsent != exactPredecessor.isEmpty()) {
                throw rejected(ChainRejectionCode.HEAD_CONFLICT, "logical predecessor differs from NVAH1");
            }
            exactPredecessor.ifPresent(predecessor -> {
                if (!predecessor.authorityVersion().equals(allocation.originalHeadVersion())
                        || predecessor.ownerEpoch() != allocation.head().value().ownerEpoch()
                        || !candidate
                                .predecessorLedgerId()
                                .equals(OptionalLong.of(predecessor.node().virtualLedgerId()))) {
                    throw rejected(
                            ChainRejectionCode.HEAD_CONFLICT,
                            "logical predecessor is not the exact versioned NVAH1 snapshot");
                }
            });
        }

        private static void requireExactNode(
                PendingNvAllocation allocation,
                LedgerNode logicalCandidate,
                VersionedVirtualLedgerCandidateNodeV1 exactNode) {
            VirtualLedgerCandidateNodeV1 node = exactNode.value();
            if (node.ledgerId() != allocation.ledgerId()
                    || node.creatorOwnerEpoch() != logicalCandidate.ownerEpoch()
                    || !node.managedLedgerIncarnation()
                            .equals(allocation.context().managedLedgerIncarnation())
                    || !node.expectedPredecessor()
                            .equals(allocation.head().value().visibleChainHead())) {
                throw rejected(
                        ChainRejectionCode.DESCRIPTOR_AUTHORITY_MISMATCH,
                        "created NVAN1 differs from the exact allocation and predecessor");
            }
        }

        private void requireContext(PulsarBindingKey binding, NvBindingContext context) {
            Objects.requireNonNull(context, "NV binding context");
            if (!context.binding().equals(binding)) {
                throw rejected(ChainRejectionCode.BINDING_MISMATCH, "NV context belongs to another binding");
            }
        }

        private void requireFixedAssignment(VersionedVirtualLedgerSliceViewV1 currentView) {
            requireFixedGeometry(currentView);
            if (!currentView.value().allocationAllowed()) {
                throw rejected(
                        ChainRejectionCode.INVALID_SLICE,
                        "current Registry assignment does not permit new virtual-ledger allocation");
            }
        }

        private void requireFixedGeometry(VersionedVirtualLedgerSliceViewV1 currentView) {
            var assignment = currentView.value().assignment();
            if (assignment.startInclusive() != fixedSlice.startInclusive()
                    || assignment.endInclusive() != fixedSlice.endInclusive()) {
                throw rejected(
                        ChainRejectionCode.INVALID_SLICE,
                        "current Registry assignment differs from the immutable fixed slice");
            }
        }

        private static boolean hasUsableInstalledRange(VersionedManagedLedgerAllocatorHeadV1 head) {
            return head.value().grantId() != 0
                    && head.value().nextLedgerId() < head.value().rangeEndExclusive();
        }

        private static boolean installedReservationExhausted(
                VersionedAllocatorCellStateV1 cell, VersionedManagedLedgerAllocatorHeadV1 head) {
            Optional<CellAllocatorReservationV1> reservation = cell.value().reservation();
            return reservation.isPresent()
                    && head.value().grantId() == reservation.orElseThrow().grantId()
                    && head.value().rangeStartInclusive()
                            == reservation.orElseThrow().rangeStartInclusive()
                    && head.value().rangeEndExclusive()
                            == reservation.orElseThrow().rangeEndExclusive()
                    && head.value().nextLedgerId() == reservation.orElseThrow().rangeEndExclusive();
        }

        private static long nextLedgerId(AuthorityState state) {
            if (state.cell().value().mode() == AllocatorModeV1.STRICT_SERIALIZED) {
                return state.cell()
                        .value()
                        .reservation()
                        .orElseThrow(() -> rejected(
                                ChainRejectionCode.MUTATION_NOT_RECONCILED,
                                "STRICT allocation lacks its exact NVAC1 reservation"))
                        .rangeStartInclusive();
            }
            return state.head().value().nextLedgerId();
        }

        private static MutationKind kindFor(ConditionalCasOutcome outcome) {
            return outcome == ConditionalCasOutcome.APPLIED_EXACT
                    ? MutationKind.APPLIED_EXACT
                    : MutationKind.EXISTING_EXACT;
        }

        private static <T> CompletionStage<T> requireExactCas(
                CompletionStage<ConditionalCasResult<T>> stage, String authorityKind) {
            return stage.thenApply(result -> {
                if (result.outcome() == ConditionalCasOutcome.DEFINITIVE_CONFLICT) {
                    throw rejected(ChainRejectionCode.HEAD_CONFLICT, authorityKind + " conflicted");
                }
                if (result.outcome() == ConditionalCasOutcome.INDETERMINATE) {
                    throw rejected(
                            ChainRejectionCode.MUTATION_NOT_RECONCILED, authorityKind + " outcome is indeterminate");
                }
                return result.exactSnapshot().orElseThrow();
            });
        }

        private record AuthorityState(
                NvBindingContext context,
                VersionedAllocatorCellStateV1 cell,
                VersionedManagedLedgerAllocatorHeadV1 head) {}

        private record PendingNvAllocation(
                NvBindingContext context,
                VersionedAllocatorCellStateV1 cell,
                VersionedManagedLedgerAllocatorHeadV1 head,
                MetadataVersion originalHeadVersion,
                long ledgerId,
                Optional<LedgerNode> logicalCandidate,
                Optional<Sha256Digest> descriptorDigest) {
            private PendingNvAllocation(
                    NvBindingContext context,
                    VersionedAllocatorCellStateV1 cell,
                    VersionedManagedLedgerAllocatorHeadV1 head,
                    MetadataVersion originalHeadVersion,
                    long ledgerId) {
                this(context, cell, head, originalHeadVersion, ledgerId, Optional.empty(), Optional.empty());
            }

            private PendingNvAllocation {
                Objects.requireNonNull(context, "context");
                Objects.requireNonNull(cell, "cell");
                Objects.requireNonNull(head, "head");
                Objects.requireNonNull(originalHeadVersion, "originalHeadVersion");
                Objects.requireNonNull(logicalCandidate, "logicalCandidate");
                Objects.requireNonNull(descriptorDigest, "descriptorDigest");
                if (logicalCandidate.isPresent() != descriptorDigest.isPresent()) {
                    throw new IllegalArgumentException("pending NV candidate and descriptor intent must be atomic");
                }
            }

            private PendingNvAllocation withIntent(LedgerNode candidate, Sha256Digest digest) {
                if (logicalCandidate.isPresent()
                        && (!logicalCandidate.orElseThrow().equals(candidate)
                                || !descriptorDigest.orElseThrow().equals(digest))) {
                    throw rejected(
                            ChainRejectionCode.DESCRIPTOR_AUTHORITY_MISMATCH,
                            "response-loss redrive changed the exact NV candidate or descriptor digest");
                }
                return new PendingNvAllocation(
                        context,
                        cell,
                        head,
                        originalHeadVersion,
                        ledgerId,
                        Optional.of(candidate),
                        Optional.of(digest));
            }

            private boolean matchesPublished(LedgerNode observed) {
                return logicalCandidate.filter(observed::equals).isPresent();
            }
        }
    }

    public enum ChainRejectionCode {
        INVALID_SLICE,
        SLICE_EXHAUSTED,
        STALE_OWNER,
        BINDING_MISMATCH,
        INVALID_TERMINAL_ENTRY,
        LEDGER_ID_NOT_MONOTONIC,
        HEAD_CONFLICT,
        MUTATION_NOT_RECONCILED,
        ALLOCATION_ALREADY_PENDING,
        ALLOCATION_NOT_PENDING,
        DESCRIPTOR_AUTHORITY_MISMATCH,
        HISTORY_BOUND_EXCEEDED
    }

    public static final class ChainException extends IllegalStateException {
        private final ChainRejectionCode code;

        ChainException(ChainRejectionCode code, String message) {
            super(message);
            this.code = Objects.requireNonNull(code, "code");
        }

        public ChainRejectionCode code() {
            return code;
        }
    }

    public static final class SliceExhaustedException extends IllegalStateException {
        public SliceExhaustedException(String message) {
            super(message);
        }
    }
}
