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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.registry.allocator.AllocatorEvidenceCandidateV1;
import com.nereusstream.domain.registry.allocator.ManagedLedgerAllocatorHeadV1;
import com.nereusstream.domain.registry.allocator.ManagedLedgerIncarnationIdV1;
import com.nereusstream.domain.registry.allocator.VirtualLedgerCandidateNodeV1;
import com.nereusstream.domain.registry.allocator.VirtualLedgerCellAllocatorStateV1;
import com.nereusstream.metadata.spi.allocator.ProductionVirtualLedgerAllocator;
import com.nereusstream.metadata.spi.allocator.PulsarVirtualLedgerAllocatorStore;
import com.nereusstream.metadata.spi.allocator.VersionedAllocatorCellStateV1;
import com.nereusstream.metadata.spi.allocator.VersionedManagedLedgerAllocatorHeadV1;
import com.nereusstream.metadata.spi.allocator.VersionedVirtualLedgerCandidateNodeV1;
import com.nereusstream.metadata.spi.model.ConditionalCasResult;
import com.nereusstream.metadata.spi.model.CreateMutationResult;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import com.nereusstream.pulsar.offload.objectwal.PulsarVirtualLedgerChainControllerV1.ChainException;
import com.nereusstream.pulsar.offload.objectwal.PulsarVirtualLedgerChainControllerV1.ChainRejectionCode;
import com.nereusstream.pulsar.offload.objectwal.PulsarVirtualLedgerChainControllerV1.FixedSlice;
import com.nereusstream.pulsar.offload.objectwal.PulsarVirtualLedgerChainControllerV1.HeadSnapshot;
import com.nereusstream.pulsar.offload.objectwal.PulsarVirtualLedgerChainControllerV1.LedgerChainAuthority;
import com.nereusstream.pulsar.offload.objectwal.PulsarVirtualLedgerChainControllerV1.LedgerIdAllocator;
import com.nereusstream.pulsar.offload.objectwal.PulsarVirtualLedgerChainControllerV1.LedgerNode;
import com.nereusstream.pulsar.offload.objectwal.PulsarVirtualLedgerChainControllerV1.MutationKind;
import com.nereusstream.pulsar.offload.objectwal.PulsarVirtualLedgerChainControllerV1.MutationOutcome;
import com.nereusstream.pulsar.offload.objectwal.PulsarVirtualLedgerChainControllerV1.OpenedLedger;
import com.nereusstream.pulsar.offload.objectwal.PulsarVirtualLedgerChainControllerV1.PulsarBindingKey;
import com.nereusstream.pulsar.offload.objectwal.PulsarVirtualLedgerChainControllerV1.SliceExhaustedException;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class PulsarVirtualLedgerChainControllerV1Test {
    private static final long START = PulsarVirtualLedgerChainControllerV1.RESERVED_START_INCLUSIVE;
    private static final FixedSlice SLICE =
            new FixedSlice(START, START + PulsarVirtualLedgerChainControllerV1.SLICE_SIZE - 1);
    private static final PulsarBindingKey BINDING =
            new PulsarBindingKey("cell-1", "binding-1", "incarnation-1", "storage-epoch-1", "position-domain-1");

    @Test
    void opensFirstLedgerAndPublishesExplicitSuccessorLink() {
        FakeAllocator allocator = new FakeAllocator(START, START + 1);
        FakeAuthority authority = new FakeAuthority();
        PulsarVirtualLedgerChainControllerV1 controller =
                new PulsarVirtualLedgerChainControllerV1(SLICE, allocator, authority);

        OpenedLedger first = controller.open(BINDING, 7).toCompletableFuture().join();
        OpenedLedger successor = controller
                .sealAndOpenSuccessor(first, 41, 7)
                .toCompletableFuture()
                .join();

        assertThat(first.node().virtualLedgerId()).isEqualTo(START);
        assertThat(successor.node().virtualLedgerId()).isEqualTo(START + 1);
        assertThat(successor.node().predecessorLedgerId()).hasValue(START);
        assertThat(successor.node().predecessorTerminalEntryId()).isEqualTo(41);
        assertThat(authority.mutations).isEqualTo(2);
        assertThat(allocator.allocations).isEqualTo(2);
    }

    @Test
    void reconcilesUnknownMutationOnlyByExactReread() {
        FakeAllocator allocator = new FakeAllocator(START);
        FakeAuthority authority = new FakeAuthority();
        authority.nextMutationKind = MutationKind.OUTCOME_UNKNOWN;
        PulsarVirtualLedgerChainControllerV1 controller =
                new PulsarVirtualLedgerChainControllerV1(SLICE, allocator, authority);

        OpenedLedger opened = controller.open(BINDING, 11).toCompletableFuture().join();

        assertThat(opened.node().virtualLedgerId()).isEqualTo(START);
        assertThat(authority.reads).isEqualTo(2);
    }

    @Test
    void redrivesTheSameCandidateWhenUnknownRereadStillShowsExactPredecessor() {
        FakeAllocator allocator = new FakeAllocator(START);
        FakeAuthority authority = new FakeAuthority();
        authority.unknownWithoutApply = 2;
        PulsarVirtualLedgerChainControllerV1 controller =
                new PulsarVirtualLedgerChainControllerV1(SLICE, allocator, authority);

        OpenedLedger opened = controller.open(BINDING, 11).toCompletableFuture().join();

        assertThat(opened.node().virtualLedgerId()).isEqualTo(START);
        assertThat(authority.mutations).isEqualTo(3);
        assertThat(authority.candidates).containsOnly(opened.node());
        assertThat(allocator.allocations).isEqualTo(1);
    }

    @Test
    void rejectsUnknownMutationWhoseRereadIsAnotherWinner() {
        FakeAllocator allocator = new FakeAllocator(START);
        FakeAuthority authority = new FakeAuthority();
        authority.nextMutationKind = MutationKind.OUTCOME_UNKNOWN;
        authority.replaceUnknownWithDifferentOwner = true;
        PulsarVirtualLedgerChainControllerV1 controller =
                new PulsarVirtualLedgerChainControllerV1(SLICE, allocator, authority);

        assertThatThrownBy(
                        () -> controller.open(BINDING, 11).toCompletableFuture().join())
                .hasRootCauseInstanceOf(ChainException.class)
                .rootCause()
                .extracting(error -> ((ChainException) error).code())
                .isEqualTo(ChainRejectionCode.MUTATION_NOT_RECONCILED);
    }

    @Test
    void failsClosedWhenAllocatorExhaustsInsteadOfSearchingAnotherSlice() {
        LedgerIdAllocator exhausted = (binding, slice, ownerEpoch) ->
                CompletableFuture.failedFuture(new SliceExhaustedException("fixed slice exhausted"));
        PulsarVirtualLedgerChainControllerV1 controller =
                new PulsarVirtualLedgerChainControllerV1(SLICE, exhausted, new FakeAuthority());

        assertThatThrownBy(
                        () -> controller.open(BINDING, 1).toCompletableFuture().join())
                .hasRootCauseInstanceOf(SliceExhaustedException.class)
                .hasRootCauseMessage("fixed slice exhausted");
    }

    @Test
    void rejectsAllocatorValueOutsideTheImmutableSlice() {
        FakeAllocator allocator = new FakeAllocator(SLICE.endInclusive() + 1);
        PulsarVirtualLedgerChainControllerV1 controller =
                new PulsarVirtualLedgerChainControllerV1(SLICE, allocator, new FakeAuthority());

        assertThatThrownBy(
                        () -> controller.open(BINDING, 1).toCompletableFuture().join())
                .hasRootCauseInstanceOf(ChainException.class)
                .rootCause()
                .extracting(error -> ((ChainException) error).code())
                .isEqualTo(ChainRejectionCode.SLICE_EXHAUSTED);
    }

    @Test
    void productionNvAdapterRejectsEvidenceOnlyCoordinatorAsRuntimeAuthority() {
        PulsarVirtualLedgerAllocatorStore store = new NeverCalledAllocatorStore();
        ProductionVirtualLedgerAllocator evidenceOnly =
                ProductionVirtualLedgerAllocator.forEvidenceCandidate(AllocatorEvidenceCandidateV1.strict(), store);

        assertThatThrownBy(() -> new PulsarVirtualLedgerChainControllerV1.ProductionNvAllocatorHeadAdapter(
                        SLICE,
                        evidenceOnly,
                        store,
                        binding -> CompletableFuture.failedFuture(new AssertionError("context must not be called")),
                        new PulsarVirtualLedgerChainControllerV1.NvLedgerDescriptorAuthority() {
                            @Override
                            public Sha256Digest requireDescriptorDigest(LedgerNode candidate) {
                                throw new AssertionError("descriptor must not be called");
                            }

                            @Override
                            public CompletableFuture<PulsarVirtualLedgerChainControllerV1.ResolvedNvHead>
                                    resolveVisibleHead(
                                            PulsarBindingKey binding, VersionedManagedLedgerAllocatorHeadV1 exactHead) {
                                return CompletableFuture.failedFuture(
                                        new AssertionError("descriptor must not be called"));
                            }
                        },
                        (binding, ownerEpoch, head, view) -> digest("request")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("receipt-activated");
    }

    private static final class FakeAllocator implements LedgerIdAllocator {
        private final Queue<Long> values = new ArrayDeque<>();
        private int allocations;

        private FakeAllocator(long... values) {
            for (long value : values) {
                this.values.add(value);
            }
        }

        @Override
        public CompletableFuture<Long> allocate(PulsarBindingKey binding, FixedSlice slice, long ownerEpoch) {
            allocations++;
            Long value = values.poll();
            if (value == null) {
                return CompletableFuture.failedFuture(new SliceExhaustedException("fixed slice exhausted"));
            }
            return CompletableFuture.completedFuture(value);
        }
    }

    private static final class FakeAuthority implements LedgerChainAuthority {
        private HeadSnapshot head;
        private MutationKind nextMutationKind = MutationKind.APPLIED_EXACT;
        private boolean replaceUnknownWithDifferentOwner;
        private int unknownWithoutApply;
        private int reads;
        private int mutations;
        private long nextVersion;
        private final java.util.List<LedgerNode> candidates = new java.util.ArrayList<>();

        @Override
        public CompletableFuture<Optional<HeadSnapshot>> readHead(PulsarBindingKey binding) {
            reads++;
            return CompletableFuture.completedFuture(Optional.ofNullable(head));
        }

        @Override
        public CompletableFuture<MutationOutcome> compareAndSetHead(
                PulsarBindingKey binding, Optional<HeadSnapshot> exactPredecessor, LedgerNode candidate) {
            mutations++;
            candidates.add(candidate);
            if (!Objects.equals(head, exactPredecessor.orElse(null))) {
                return CompletableFuture.completedFuture(
                        new MutationOutcome(MutationKind.DEFINITIVE_CONFLICT, Optional.ofNullable(head)));
            }
            if (unknownWithoutApply > 0) {
                unknownWithoutApply--;
                return CompletableFuture.completedFuture(
                        new MutationOutcome(MutationKind.OUTCOME_UNKNOWN, Optional.empty()));
            }
            if (nextMutationKind == MutationKind.OUTCOME_UNKNOWN && replaceUnknownWithDifferentOwner) {
                LedgerNode other = new LedgerNode(
                        candidate.binding(),
                        candidate.virtualLedgerId(),
                        candidate.predecessorLedgerId(),
                        candidate.predecessorTerminalEntryId(),
                        candidate.ownerEpoch() + 1);
                head = new HeadSnapshot(version(nextVersion++), other);
                return CompletableFuture.completedFuture(new MutationOutcome(nextMutationKind, Optional.empty()));
            }
            head = new HeadSnapshot(version(nextVersion++), candidate);
            Optional<HeadSnapshot> observed =
                    nextMutationKind == MutationKind.OUTCOME_UNKNOWN ? Optional.empty() : Optional.of(head);
            return CompletableFuture.completedFuture(new MutationOutcome(nextMutationKind, observed));
        }
    }

    private static MetadataVersion version(long value) {
        return new MetadataVersion(CanonicalBytes.copyOf(
                java.nio.ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
    }

    private static Sha256Digest digest(String value) {
        return Sha256Digest.hash(CanonicalBytes.copyOf(value.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
    }

    private static final class NeverCalledAllocatorStore implements PulsarVirtualLedgerAllocatorStore {
        private static <T> CompletableFuture<T> unexpected() {
            return CompletableFuture.failedFuture(new AssertionError("allocator store must not be called"));
        }

        @Override
        public CompletableFuture<Optional<VersionedAllocatorCellStateV1>> readCell(
                Sha256Digest namespaceId, Sha256Digest sliceAssignmentId) {
            return unexpected();
        }

        @Override
        public CompletableFuture<CreateMutationResult<VersionedAllocatorCellStateV1>> createCell(
                VirtualLedgerCellAllocatorStateV1 candidate) {
            return unexpected();
        }

        @Override
        public CompletableFuture<ConditionalCasResult<VersionedAllocatorCellStateV1>> compareAndSetCell(
                VersionedAllocatorCellStateV1 exactPredecessor, VirtualLedgerCellAllocatorStateV1 candidate) {
            return unexpected();
        }

        @Override
        public CompletableFuture<Optional<VersionedManagedLedgerAllocatorHeadV1>> readHead(
                Sha256Digest namespaceId,
                Sha256Digest sliceAssignmentId,
                ManagedLedgerIncarnationIdV1 managedLedgerIncarnation) {
            return unexpected();
        }

        @Override
        public CompletableFuture<CreateMutationResult<VersionedManagedLedgerAllocatorHeadV1>> createHead(
                Sha256Digest namespaceId, Sha256Digest sliceAssignmentId, ManagedLedgerAllocatorHeadV1 candidate) {
            return unexpected();
        }

        @Override
        public CompletableFuture<ConditionalCasResult<VersionedManagedLedgerAllocatorHeadV1>> compareAndSetHead(
                Sha256Digest namespaceId,
                Sha256Digest sliceAssignmentId,
                VersionedManagedLedgerAllocatorHeadV1 exactPredecessor,
                ManagedLedgerAllocatorHeadV1 candidate) {
            return unexpected();
        }

        @Override
        public CompletableFuture<Optional<VersionedVirtualLedgerCandidateNodeV1>> readNode(
                Sha256Digest namespaceId,
                Sha256Digest sliceAssignmentId,
                ManagedLedgerIncarnationIdV1 managedLedgerIncarnation,
                long ledgerId) {
            return unexpected();
        }

        @Override
        public CompletableFuture<CreateMutationResult<VersionedVirtualLedgerCandidateNodeV1>> createNode(
                Sha256Digest namespaceId, Sha256Digest sliceAssignmentId, VirtualLedgerCandidateNodeV1 candidate) {
            return unexpected();
        }
    }
}
