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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.DeploymentId;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.PulsarCellId;
import com.nereusstream.domain.identity.ReservationDomainId;
import com.nereusstream.domain.registry.VirtualLedgerSliceAssignmentV1;
import com.nereusstream.domain.registry.VirtualLedgerSliceLifecycleV1;
import com.nereusstream.domain.registry.VirtualLedgerSliceViewV1;
import com.nereusstream.domain.registry.allocator.AllocatorEvidenceCandidateV1;
import com.nereusstream.domain.registry.allocator.AllocatorProtocolException;
import com.nereusstream.domain.registry.allocator.AllocatorProtocolV1;
import com.nereusstream.domain.registry.allocator.AllocatorWireV1;
import com.nereusstream.domain.registry.allocator.ManagedLedgerAllocatorHeadV1;
import com.nereusstream.domain.registry.allocator.ManagedLedgerIncarnationIdV1;
import com.nereusstream.domain.registry.allocator.VirtualLedgerCandidateNodeV1;
import com.nereusstream.domain.registry.allocator.VirtualLedgerCellAllocatorStateV1;
import com.nereusstream.metadata.spi.model.ConditionalCasResult;
import com.nereusstream.metadata.spi.model.CreateMutationResult;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import com.nereusstream.metadata.spi.model.VersionedVirtualLedgerSliceViewV1;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ProductionVirtualLedgerAllocatorTest {
    private InMemoryExactStore store;
    private ProductionVirtualLedgerAllocator allocator;
    private VersionedAllocatorCellStateV1 cell;
    private VersionedManagedLedgerAllocatorHeadV1 head;

    @BeforeEach
    void setUp() {
        store = new InMemoryExactStore();
        allocator =
                ProductionVirtualLedgerAllocator.forEvidenceCandidate(AllocatorEvidenceCandidateV1.range(16), store);
        VirtualLedgerCellAllocatorStateV1 initialCell = VirtualLedgerCellAllocatorStateV1.initial(
                AllocatorEvidenceCandidateV1.range(16).mode(), assignment());
        cell = store.createCell(initialCell)
                .toCompletableFuture()
                .join()
                .exactSnapshot()
                .orElseThrow();
        ManagedLedgerAllocatorHeadV1 initialHead =
                ManagedLedgerAllocatorHeadV1.initial(incarnation(), 10, initialCell.nextSliceLedgerId());
        head = store.createHead(namespace(), assignment().sliceAssignmentId(), initialHead)
                .toCompletableFuture()
                .join()
                .exactSnapshot()
                .orElseThrow();
    }

    @Test
    void evidenceCandidateUsesProductionTransitionsButNeverRuntimeActivation() {
        assertThat(allocator.runtimeActivated()).isFalse();

        cell = exact(allocator.reserve(cell, head, activeView(), digest("reserve")));
        head = exact(allocator.installRangeReservedGrant(cell, head, activeView()));
        VersionedVirtualLedgerCandidateNodeV1 node =
                exactCreate(allocator.createCandidate(cell, head, activeView(), digest("descriptor")));
        head = exact(allocator.publishCandidate(cell, head, node, activeView()));
        cell = exact(allocator.clearReservation(cell, head));

        assertThat(head.value().visibleChainHead()).isEqualTo(node.value().pointer());
        assertThat(cell.value().reservation()).isEmpty();
    }

    @Test
    void burnRequiresCurrentStoreObservedExactNodeAndIsIdempotentForOneIdOnly() {
        cell = exact(allocator.reserve(cell, head, activeView(), digest("reserve")));
        head = exact(allocator.installRangeReservedGrant(cell, head, activeView()));
        VersionedVirtualLedgerCandidateNodeV1 stale =
                exactCreate(allocator.createCandidate(cell, head, activeView(), digest("stale")));
        head = exact(allocator.takeover(cell, head, 11));

        VersionedManagedLedgerAllocatorHeadV1 burned =
                exact(allocator.burnStaleCandidate(cell, head, stale, activeView()));
        var repeated = allocator
                .burnStaleCandidate(cell, burned, stale, activeView())
                .toCompletableFuture()
                .join();
        assertThat(repeated.exactSnapshot()).contains(burned);

        VirtualLedgerCandidateNodeV1 fabricatedValue = AllocatorWireV1.createNode(
                incarnation(),
                burned.value().nextLedgerId(),
                burned.value().grantId(),
                10,
                burned.value().visibleChainHead(),
                digest("fabricated"));
        VersionedVirtualLedgerCandidateNodeV1 fabricated = new VersionedVirtualLedgerCandidateNodeV1(
                namespace(),
                assignment().sliceAssignmentId(),
                InMemoryExactStore.nodeKey(fabricatedValue.ledgerId()),
                fabricatedValue,
                version(99));

        assertThatThrownBy(() -> allocator
                        .burnStaleCandidate(cell, burned, fabricated, activeView())
                        .toCompletableFuture()
                        .join())
                .hasRootCauseInstanceOf(AllocatorProtocolException.class)
                .rootCause()
                .extracting(value -> ((AllocatorProtocolException) value).code())
                .isEqualTo(AllocatorProtocolException.Code.CANDIDATE_OCCUPANCY_NOT_PROVEN);
    }

    @Test
    void retiringSliceStopsEveryAllocationCutButAllowsTakeoverAndClearReconciliation() {
        cell = exact(allocator.reserve(cell, head, activeView(), digest("reserve")));
        head = exact(allocator.installRangeReservedGrant(cell, head, activeView()));
        VersionedVirtualLedgerCandidateNodeV1 node =
                exactCreate(allocator.createCandidate(cell, head, activeView(), digest("node")));
        VersionedVirtualLedgerSliceViewV1 retiring = retiringView();

        assertSliceInactive(() -> allocator.createHead(cell, retiring, incarnation(), 12));
        assertSliceInactive(() -> allocator.reserve(cell, head, retiring, digest("another")));
        assertSliceInactive(() -> allocator.installRangeReservedGrant(cell, head, retiring));
        assertSliceInactive(() -> allocator.createCandidate(cell, head, retiring, digest("another-node")));
        assertSliceInactive(() -> allocator.publishCandidate(cell, head, node, retiring));
        VersionedManagedLedgerAllocatorHeadV1 staleOwner = exact(allocator.takeover(cell, head, 11));
        assertSliceInactive(() -> allocator.burnStaleCandidate(cell, staleOwner, node, retiring));

        assertThat(allocator.takeover(cell, staleOwner, 12)).isNotNull();
        assertThat(allocator.clearReservation(cell, head)).isNotNull();
    }

    @Test
    void headProvenanceAndConsumedSliceGeometryFailClosed() {
        VersionedManagedLedgerAllocatorHeadV1 wrongSlice = new VersionedManagedLedgerAllocatorHeadV1(
                namespace(), digest("other-slice"), "/allocator/other/head", head.value(), head.metadataVersion());
        assertThatThrownBy(() -> allocator.reserve(cell, wrongSlice, activeView(), digest("reserve")))
                .isInstanceOfSatisfying(AllocatorProtocolException.class, error -> assertThat(error.code())
                        .isEqualTo(AllocatorProtocolException.Code.HEAD_IDENTITY));

        ManagedLedgerAllocatorHeadV1 future = ManagedLedgerAllocatorHeadV1.initial(
                incarnation(), 10, cell.value().nextSliceLedgerId() + 1);
        VersionedManagedLedgerAllocatorHeadV1 futureHead = new VersionedManagedLedgerAllocatorHeadV1(
                namespace(),
                assignment().sliceAssignmentId(),
                InMemoryExactStore.HEAD_KEY,
                future,
                head.metadataVersion());
        assertThatThrownBy(() -> allocator.reserve(cell, futureHead, activeView(), digest("reserve")))
                .isInstanceOfSatisfying(AllocatorProtocolException.class, error -> assertThat(error.code())
                        .isEqualTo(AllocatorProtocolException.Code.HEAD_GEOMETRY));
    }

    @Test
    void everyNonCellCasMutationRereadsAndRejectsAStaleVersionedCell() {
        VersionedAllocatorCellStateV1 reserved = exact(allocator.reserve(cell, head, activeView(), digest("reserve")));
        VersionedManagedLedgerAllocatorHeadV1 installed =
                exact(allocator.installRangeReservedGrant(reserved, head, activeView()));
        VersionedVirtualLedgerCandidateNodeV1 node =
                exactCreate(allocator.createCandidate(reserved, installed, activeView(), digest("node")));
        cell = exact(allocator.clearReservation(reserved, installed));
        VersionedManagedLedgerAllocatorHeadV1 takenOver = new VersionedManagedLedgerAllocatorHeadV1(
                installed.ledgerIdCompatibilityNamespaceId(),
                installed.sliceAssignmentId(),
                installed.authorityKey(),
                AllocatorProtocolV1.takeover(installed.value(), 11),
                installed.metadataVersion());

        assertFutureCode(
                () -> allocator.createHead(reserved, activeView(), incarnation(), 12),
                AllocatorProtocolException.Code.CELL_STATE_DRIFT);
        assertFutureCode(
                () -> allocator.installRangeReservedGrant(reserved, installed, activeView()),
                AllocatorProtocolException.Code.CELL_STATE_DRIFT);
        assertFutureCode(
                () -> allocator.createCandidate(reserved, installed, activeView(), digest("other")),
                AllocatorProtocolException.Code.CELL_STATE_DRIFT);
        assertFutureCode(
                () -> allocator.publishCandidate(reserved, installed, node, activeView()),
                AllocatorProtocolException.Code.CELL_STATE_DRIFT);
        assertFutureCode(
                () -> allocator.burnStaleCandidate(reserved, takenOver, node, activeView()),
                AllocatorProtocolException.Code.CELL_STATE_DRIFT);
        assertFutureCode(
                () -> allocator.takeover(reserved, installed, 11), AllocatorProtocolException.Code.CELL_STATE_DRIFT);
        assertThat(store.head).isEqualTo(installed);
    }

    private static void assertSliceInactive(Runnable invocation) {
        assertThatThrownBy(invocation::run)
                .isInstanceOfSatisfying(AllocatorProtocolException.class, error -> assertThat(error.code())
                        .isEqualTo(AllocatorProtocolException.Code.SLICE_NOT_ACTIVE));
    }

    private static void assertFutureCode(
            java.util.function.Supplier<? extends CompletionStage<?>> invocation,
            AllocatorProtocolException.Code expected) {
        assertThatThrownBy(() -> invocation.get().toCompletableFuture().join())
                .hasRootCauseInstanceOf(AllocatorProtocolException.class)
                .rootCause()
                .extracting(value -> ((AllocatorProtocolException) value).code())
                .isEqualTo(expected);
    }

    private static <T> T exact(CompletionStage<ConditionalCasResult<T>> stage) {
        return stage.toCompletableFuture().join().exactSnapshot().orElseThrow();
    }

    private static <T> T exactCreate(CompletionStage<CreateMutationResult<T>> stage) {
        return stage.toCompletableFuture().join().exactSnapshot().orElseThrow();
    }

    private static VersionedVirtualLedgerSliceViewV1 activeView() {
        return view(assignment());
    }

    private static VersionedVirtualLedgerSliceViewV1 retiringView() {
        return view(assignment().withLifecycle(VirtualLedgerSliceLifecycleV1.RETIRING));
    }

    private static VersionedVirtualLedgerSliceViewV1 view(VirtualLedgerSliceAssignmentV1 assignment) {
        return new VersionedVirtualLedgerSliceViewV1(
                new VirtualLedgerSliceViewV1(namespace(), 1, assignment), version(1), digest("registry"));
    }

    private static VirtualLedgerSliceAssignmentV1 assignment() {
        return VirtualLedgerSliceAssignmentV1.create(
                new DeploymentId(new Id128(1, 2)),
                new ReservationDomainId(new Id128(3, 4)),
                new PulsarCellId(new Id128(5, 6)),
                namespace(),
                VirtualLedgerSliceAssignmentV1.RESERVED_START_INCLUSIVE,
                VirtualLedgerSliceLifecycleV1.ACTIVE);
    }

    private static ManagedLedgerIncarnationIdV1 incarnation() {
        return new ManagedLedgerIncarnationIdV1(digest("incarnation"));
    }

    private static Sha256Digest namespace() {
        return digest("namespace");
    }

    private static Sha256Digest digest(String value) {
        return Sha256Digest.hash(CanonicalBytes.copyOf(value.getBytes(StandardCharsets.UTF_8)));
    }

    private static MetadataVersion version(long value) {
        return new MetadataVersion(CanonicalBytes.copyOf(
                java.nio.ByteBuffer.allocate(8).putLong(value).array()));
    }

    private static final class InMemoryExactStore implements PulsarVirtualLedgerAllocatorStore {
        private static final String CELL_KEY = "/allocator/cell";
        private static final String HEAD_KEY = "/allocator/head";
        private VersionedAllocatorCellStateV1 cell;
        private VersionedManagedLedgerAllocatorHeadV1 head;
        private final Map<Long, VersionedVirtualLedgerCandidateNodeV1> nodes = new HashMap<>();
        private long nextVersion = 1;

        private static String nodeKey(long ledgerId) {
            return "/allocator/nodes/" + ledgerId;
        }

        @Override
        public CompletionStage<Optional<VersionedAllocatorCellStateV1>> readCell(
                Sha256Digest namespaceId, Sha256Digest sliceAssignmentId) {
            return CompletableFuture.completedFuture(Optional.ofNullable(cell));
        }

        @Override
        public CompletionStage<CreateMutationResult<VersionedAllocatorCellStateV1>> createCell(
                VirtualLedgerCellAllocatorStateV1 candidate) {
            cell = new VersionedAllocatorCellStateV1(candidate, version(nextVersion++));
            return CompletableFuture.completedFuture(CreateMutationResult.created(cell));
        }

        @Override
        public CompletionStage<ConditionalCasResult<VersionedAllocatorCellStateV1>> compareAndSetCell(
                VersionedAllocatorCellStateV1 predecessor, VirtualLedgerCellAllocatorStateV1 candidate) {
            if (!Objects.equals(cell, predecessor)) {
                return CompletableFuture.completedFuture(ConditionalCasResult.definitiveConflict());
            }
            cell = new VersionedAllocatorCellStateV1(candidate, version(nextVersion++));
            return CompletableFuture.completedFuture(ConditionalCasResult.appliedExact(cell));
        }

        @Override
        public CompletionStage<Optional<VersionedManagedLedgerAllocatorHeadV1>> readHead(
                Sha256Digest namespaceId,
                Sha256Digest sliceAssignmentId,
                ManagedLedgerIncarnationIdV1 managedLedgerIncarnation) {
            return CompletableFuture.completedFuture(Optional.ofNullable(head));
        }

        @Override
        public CompletionStage<CreateMutationResult<VersionedManagedLedgerAllocatorHeadV1>> createHead(
                Sha256Digest namespaceId, Sha256Digest sliceAssignmentId, ManagedLedgerAllocatorHeadV1 candidate) {
            head = new VersionedManagedLedgerAllocatorHeadV1(
                    namespaceId, sliceAssignmentId, HEAD_KEY, candidate, version(nextVersion++));
            return CompletableFuture.completedFuture(CreateMutationResult.created(head));
        }

        @Override
        public CompletionStage<ConditionalCasResult<VersionedManagedLedgerAllocatorHeadV1>> compareAndSetHead(
                Sha256Digest namespaceId,
                Sha256Digest sliceAssignmentId,
                VersionedManagedLedgerAllocatorHeadV1 predecessor,
                ManagedLedgerAllocatorHeadV1 candidate) {
            if (!Objects.equals(head, predecessor)) {
                return CompletableFuture.completedFuture(ConditionalCasResult.definitiveConflict());
            }
            head = new VersionedManagedLedgerAllocatorHeadV1(
                    namespaceId, sliceAssignmentId, HEAD_KEY, candidate, version(nextVersion++));
            return CompletableFuture.completedFuture(ConditionalCasResult.appliedExact(head));
        }

        @Override
        public CompletionStage<Optional<VersionedVirtualLedgerCandidateNodeV1>> readNode(
                Sha256Digest namespaceId,
                Sha256Digest sliceAssignmentId,
                ManagedLedgerIncarnationIdV1 managedLedgerIncarnation,
                long ledgerId) {
            return CompletableFuture.completedFuture(Optional.ofNullable(nodes.get(ledgerId)));
        }

        @Override
        public CompletionStage<CreateMutationResult<VersionedVirtualLedgerCandidateNodeV1>> createNode(
                Sha256Digest namespaceId, Sha256Digest sliceAssignmentId, VirtualLedgerCandidateNodeV1 candidate) {
            VersionedVirtualLedgerCandidateNodeV1 snapshot = new VersionedVirtualLedgerCandidateNodeV1(
                    namespaceId, sliceAssignmentId, nodeKey(candidate.ledgerId()), candidate, version(nextVersion++));
            VersionedVirtualLedgerCandidateNodeV1 previous = nodes.putIfAbsent(candidate.ledgerId(), snapshot);
            return CompletableFuture.completedFuture(
                    previous == null
                            ? CreateMutationResult.created(snapshot)
                            : CreateMutationResult.existingExact(previous));
        }
    }
}
