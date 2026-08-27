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
import com.nereusstream.domain.registry.allocator.AllocatorModeV1;
import com.nereusstream.domain.registry.allocator.AllocatorProtocolException;
import com.nereusstream.domain.registry.allocator.AllocatorProtocolV1;
import com.nereusstream.domain.registry.allocator.ManagedLedgerAllocatorHeadV1;
import com.nereusstream.domain.registry.allocator.ManagedLedgerIncarnationIdV1;
import com.nereusstream.domain.registry.allocator.VirtualLedgerCandidateNodeV1;
import com.nereusstream.domain.registry.allocator.VirtualLedgerCellAllocatorStateV1;
import com.nereusstream.metadata.spi.allocator.BoundedVirtualLedgerAllocatorWorkflowV2.Request;
import com.nereusstream.metadata.spi.allocator.BoundedVirtualLedgerAllocatorWorkflowV2.Result;
import com.nereusstream.metadata.spi.allocator.BoundedVirtualLedgerAllocatorWorkflowV2.RetryReason;
import com.nereusstream.metadata.spi.model.ConditionalCasResult;
import com.nereusstream.metadata.spi.model.CreateMutationResult;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import com.nereusstream.metadata.spi.model.VersionedVirtualLedgerSliceViewV1;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.locks.Lock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BoundedVirtualLedgerAllocatorWorkflowV2Test {
    private ReconcileStore store;
    private ProductionVirtualLedgerAllocator strictAllocator;
    private VersionedAllocatorCellStateV1 cell;
    private VersionedManagedLedgerAllocatorHeadV1 head;

    @BeforeEach
    void setUp() {
        store = new ReconcileStore();
        strictAllocator =
                ProductionVirtualLedgerAllocator.forEvidenceCandidate(AllocatorEvidenceCandidateV1.strict(), store);
        cell = createCell(AllocatorModeV1.STRICT_SERIALIZED);
        head = createHead(cell, incarnation("ledger"));
    }

    @Test
    void strictWorkflowPublishesOneCandidateAndClearsItsReservation() {
        Result result = strictAllocator
                .boundedWorkflow(8, BoundedVirtualLedgerAllocatorWorkflowV2.RetryScheduler.immediate())
                .allocate(request(head, "strict"))
                .toCompletableFuture()
                .join();

        assertThat(result.reconcileRetries()).isZero();
        assertThat(result.exactHead().value().visibleChainHead())
                .isEqualTo(result.exactNode().value().pointer());
        assertThat(result.exactCell().value().reservation()).isEmpty();
        assertThat(result.exactCell().value().nextSliceLedgerId())
                .isEqualTo(cell.value().nextSliceLedgerId() + 1);
        assertThat(store.nodes).hasSize(1);
    }

    @Test
    void rangeWorkflowInstallsOneGrantAndPublishesWithoutGlobalLock() {
        store = new ReconcileStore();
        ProductionVirtualLedgerAllocator range =
                ProductionVirtualLedgerAllocator.forEvidenceCandidate(AllocatorEvidenceCandidateV1.range(16), store);
        cell = createCell(AllocatorModeV1.RANGE_LEASED);
        head = createHead(cell, incarnation("range"));

        Result result = range.boundedWorkflow(8, BoundedVirtualLedgerAllocatorWorkflowV2.RetryScheduler.immediate())
                .allocate(request(head, "range"))
                .toCompletableFuture()
                .join();

        assertThat(result.exactCell().value().nextSliceLedgerId())
                .isEqualTo(cell.value().nextSliceLedgerId() + 16);
        assertThat(result.exactCell().value().reservation()).isEmpty();
        assertThat(result.exactHead().value().grantId()).isPositive();
        assertThat(result.exactHead().value().nextLedgerId())
                .isEqualTo(result.exactNode().value().ledgerId() + 1);
        assertThat(List.of(BoundedVirtualLedgerAllocatorWorkflowV2.class.getDeclaredFields()))
                .extracting(Field::getType)
                .noneMatch(Lock.class::isAssignableFrom);
    }

    @Test
    void rangeReserveInstallClearCreateAndPublishAllReconcileResponseLoss() {
        store = new ReconcileStore();
        ProductionVirtualLedgerAllocator range =
                ProductionVirtualLedgerAllocator.forEvidenceCandidate(AllocatorEvidenceCandidateV1.range(16), store);
        cell = createCell(AllocatorModeV1.RANGE_LEASED);
        head = createHead(cell, incarnation("range-loss"));
        store.loseCellCasResponses = 2;
        store.loseHeadCasResponses = 2;
        store.loseNodeCreateResponses = 1;

        Result result = range.boundedWorkflow(8, BoundedVirtualLedgerAllocatorWorkflowV2.RetryScheduler.immediate())
                .allocate(request(head, "range-loss"))
                .toCompletableFuture()
                .join();

        assertThat(result.reconcileRetries()).isEqualTo(5);
        assertThat(new HashSet<>(store.reservationRequestIds)).containsExactly(digest("request-range-loss"));
        assertThat(new HashSet<>(store.candidateValues))
                .containsExactly(result.exactNode().value());
        assertThat(result.exactCell().value().nextSliceLedgerId())
                .isEqualTo(cell.value().nextSliceLedgerId() + 16);
        assertThat(result.exactHead().value().nextLedgerId())
                .isEqualTo(result.exactNode().value().ledgerId() + 1);
    }

    @Test
    void responseLossRetriesKeepRequestAndCandidateIdentityAndConsumeOneLedgerId() {
        store.loseCellCasResponses = 2;
        store.loseHeadCasResponses = 1;
        store.loseNodeCreateResponses = 1;

        Result result = strictAllocator
                .boundedWorkflow(8, BoundedVirtualLedgerAllocatorWorkflowV2.RetryScheduler.immediate())
                .allocate(request(head, "loss"))
                .toCompletableFuture()
                .join();

        assertThat(result.reconcileRetries()).isEqualTo(4);
        assertThat(new HashSet<>(store.reservationRequestIds)).containsExactly(digest("request-loss"));
        assertThat(new HashSet<>(store.candidateValues))
                .containsExactly(result.exactNode().value());
        assertThat(store.cell.value().nextSliceLedgerId())
                .isEqualTo(cell.value().nextSliceLedgerId() + 1);
        assertThat(store.nodes).hasSize(1);
    }

    @Test
    void independentCoordinatorsRebaseAConcurrentHeadBeforeEitherRequestHasCandidateState() {
        store.barrierCellCasCount = 2;
        var firstWorkflow =
                strictAllocator.boundedWorkflow(8, BoundedVirtualLedgerAllocatorWorkflowV2.RetryScheduler.immediate());
        var secondWorkflow =
                strictAllocator.boundedWorkflow(8, BoundedVirtualLedgerAllocatorWorkflowV2.RetryScheduler.immediate());

        CompletableFuture<Result> first =
                firstWorkflow.allocate(request(head, "actor-one")).toCompletableFuture();
        CompletableFuture<Result> second =
                secondWorkflow.allocate(request(head, "actor-two")).toCompletableFuture();
        Result firstResult = first.join();
        Result secondResult = second.join();

        assertThat(firstResult.exactNode()).isNotEqualTo(secondResult.exactNode());
        assertThat(List.of(firstResult.reconcileRetries(), secondResult.reconcileRetries()))
                .anyMatch(retries -> retries > 0);
        assertThat(new HashSet<>(store.reservationRequestIds))
                .containsExactlyInAnyOrder(digest("request-actor-one"), digest("request-actor-two"));
        assertThat(store.cell.value().nextSliceLedgerId())
                .isEqualTo(cell.value().nextSliceLedgerId() + 2);
        assertThat(store.nodes).hasSize(2);
    }

    @Test
    void rangeCoordinatorRebasesOnlyAfterCompetingNodeIsExactlyPublished() {
        store = new ReconcileStore();
        ProductionVirtualLedgerAllocator range =
                ProductionVirtualLedgerAllocator.forEvidenceCandidate(AllocatorEvidenceCandidateV1.range(16), store);
        cell = createCell(AllocatorModeV1.RANGE_LEASED);
        head = createHead(cell, incarnation("range-concurrent"));
        cell = exact(range.reserve(cell, head, activeView(), digest("range-concurrent-grant")));
        head = exact(range.installRangeReservedGrant(cell, head, activeView()));
        cell = exact(range.clearReservation(cell, head));
        long firstLedgerId = head.value().nextLedgerId();
        store.barrierNodeCreateCount = 2;

        CompletableFuture<Result> first = range.boundedWorkflow(
                        8, BoundedVirtualLedgerAllocatorWorkflowV2.RetryScheduler.immediate())
                .allocate(request(head, "range-actor-one"))
                .toCompletableFuture();
        CompletableFuture<Result> second = range.boundedWorkflow(
                        8, BoundedVirtualLedgerAllocatorWorkflowV2.RetryScheduler.immediate())
                .allocate(request(head, "range-actor-two"))
                .toCompletableFuture();
        Result firstResult = first.join();
        Result secondResult = second.join();

        assertThat(firstResult.exactNode().value().ledgerDescriptorDigest())
                .isEqualTo(digest("descriptor-range-actor-one"));
        assertThat(secondResult.exactNode().value().ledgerDescriptorDigest())
                .isEqualTo(digest("descriptor-range-actor-two"));
        assertThat(firstResult.exactNode().value().ledgerId())
                .isNotEqualTo(secondResult.exactNode().value().ledgerId());
        assertThat(secondResult.reconcileRetries()).isGreaterThanOrEqualTo(2);
        assertThat(store.head.value().nextLedgerId()).isEqualTo(firstLedgerId + 2);
        assertThat(store.nodes).hasSize(2);
    }

    @Test
    void rangeCoordinatorRebasesWhenExactHeadDriftsBeforeNodeCreateDispatch() {
        store = new ReconcileStore();
        ProductionVirtualLedgerAllocator range =
                ProductionVirtualLedgerAllocator.forEvidenceCandidate(AllocatorEvidenceCandidateV1.range(64), store);
        cell = createCell(AllocatorModeV1.RANGE_LEASED);
        head = createHead(cell, incarnation("range-predispatch"));
        cell = exact(range.reserve(cell, head, activeView(), digest("range-predispatch-grant")));
        head = exact(range.installRangeReservedGrant(cell, head, activeView()));
        cell = exact(range.clearReservation(cell, head));
        long firstLedgerId = head.value().nextLedgerId();
        store.delayReadHeadCall = 2;

        CompletableFuture<Result> delayed = range.boundedWorkflow(
                        8, BoundedVirtualLedgerAllocatorWorkflowV2.RetryScheduler.immediate())
                .allocate(request(head, "range-delayed"))
                .toCompletableFuture();
        Result winner = range.boundedWorkflow(8, BoundedVirtualLedgerAllocatorWorkflowV2.RetryScheduler.immediate())
                .allocate(request(head, "range-winner"))
                .toCompletableFuture()
                .join();
        assertThat(delayed).isNotDone();

        store.delayedReadHead.complete(Optional.of(store.head));
        Result rebased = delayed.join();

        assertThat(winner.exactNode().value().ledgerDescriptorDigest()).isEqualTo(digest("descriptor-range-winner"));
        assertThat(rebased.exactNode().value().ledgerDescriptorDigest()).isEqualTo(digest("descriptor-range-delayed"));
        assertThat(winner.exactNode().value().ledgerId()).isEqualTo(firstLedgerId);
        assertThat(rebased.exactNode().value().ledgerId()).isEqualTo(firstLedgerId + 1);
        assertThat(rebased.reconcileRetries()).isGreaterThanOrEqualTo(1);
        assertThat(store.nodes).hasSize(2);
    }

    @Test
    void staleOwnerAndSliceContextDriftFailClosedBeforeAllocation() {
        VersionedManagedLedgerAllocatorHeadV1 original = head;
        store.head = versionedHead(AllocatorProtocolV1.takeover(head.value(), 11));
        assertCode(
                strictAllocator
                        .boundedWorkflow(2, BoundedVirtualLedgerAllocatorWorkflowV2.RetryScheduler.immediate())
                        .allocate(request(original, "stale")),
                AllocatorProtocolException.Code.OWNER_FENCED);

        store.head = original;
        VirtualLedgerSliceAssignmentV1 other = assignment(digest("other-namespace"));
        store.cell = new VersionedAllocatorCellStateV1(
                VirtualLedgerCellAllocatorStateV1.initial(AllocatorModeV1.STRICT_SERIALIZED, other), version(99));
        assertCode(
                strictAllocator
                        .boundedWorkflow(2, BoundedVirtualLedgerAllocatorWorkflowV2.RetryScheduler.immediate())
                        .allocate(request(original, "slice-drift")),
                AllocatorProtocolException.Code.SLICE_IDENTITY_DRIFT);
        assertThat(store.nodes).isEmpty();
    }

    @Test
    void existingNodeWithAnotherDescriptorFailsClosedWithoutPublishingOrReallocating() {
        store = new ReconcileStore();
        ProductionVirtualLedgerAllocator range =
                ProductionVirtualLedgerAllocator.forEvidenceCandidate(AllocatorEvidenceCandidateV1.range(16), store);
        cell = createCell(AllocatorModeV1.RANGE_LEASED);
        head = createHead(cell, incarnation("descriptor"));
        cell = exact(range.reserve(cell, head, activeView(), digest("grant")));
        head = exact(range.installRangeReservedGrant(cell, head, activeView()));
        cell = exact(range.clearReservation(cell, head));
        VirtualLedgerCandidateNodeV1 conflicting = AllocatorProtocolV1.candidate(head.value(), digest("other"));
        exactCreate(store.createNode(namespace(), assignment().sliceAssignmentId(), conflicting));
        long nextLedgerId = head.value().nextLedgerId();

        assertCode(
                range.boundedWorkflow(2, BoundedVirtualLedgerAllocatorWorkflowV2.RetryScheduler.immediate())
                        .allocate(request(head, "descriptor")),
                AllocatorProtocolException.Code.DESCRIPTOR_MISMATCH);
        assertThat(store.head.value().nextLedgerId()).isEqualTo(nextLedgerId);
        assertThat(store.cell.value().nextSliceLedgerId())
                .isEqualTo(cell.value().nextSliceLedgerId());
        assertThat(store.nodes).hasSize(1);
    }

    @Test
    void retryBudgetExhaustionIsTypedAndCannotCreateADispositionOrConsumeAnId() {
        store.cellCasPredecessorUnchanged = true;
        List<RetryReason> reasons = new ArrayList<>();
        List<Sha256Digest> retryRequestIds = new ArrayList<>();
        var scheduler = (BoundedVirtualLedgerAllocatorWorkflowV2.RetryScheduler) (requestId, number, reason) -> {
            retryRequestIds.add(requestId);
            reasons.add(reason);
            return CompletableFuture.completedFuture(null);
        };

        assertCode(
                strictAllocator.boundedWorkflow(2, scheduler).allocate(request(head, "exhaust")),
                AllocatorProtocolException.Code.RECONCILE_RETRY_EXHAUSTED);
        assertThat(new HashSet<>(retryRequestIds)).containsExactly(digest("request-exhaust"));
        assertThat(reasons).containsExactly(RetryReason.CELL_CAS_UNRESOLVED, RetryReason.CELL_CAS_UNRESOLVED);
        assertThat(store.cell).isEqualTo(cell);
        assertThat(store.nodes).isEmpty();
    }

    @Test
    void sourceGovernedElapsedAndBackoffBoundsFailClosedWithTypedOutcomes() {
        store.cellCasPredecessorUnchanged = true;
        var boundedBackoff =
                new BoundedVirtualLedgerAllocatorWorkflowV2.Bounds(2, Duration.ofMillis(100), Duration.ofMillis(10));
        assertCode(
                strictAllocator
                        .boundedWorkflow(boundedBackoff, (requestId, number, reason) -> new CompletableFuture<>())
                        .allocate(request(head, "backoff-timeout")),
                AllocatorProtocolException.Code.RETRY_BACKOFF_EXCEEDED);

        store.cellCasPredecessorUnchanged = false;
        store.readCellNeverCompletes = true;
        var elapsed =
                new BoundedVirtualLedgerAllocatorWorkflowV2.Bounds(2, Duration.ofMillis(30), Duration.ofMillis(10));
        assertCode(
                strictAllocator
                        .boundedWorkflow(elapsed, BoundedVirtualLedgerAllocatorWorkflowV2.RetryScheduler.immediate())
                        .allocate(request(head, "elapsed-timeout")),
                AllocatorProtocolException.Code.WORKFLOW_DEADLINE_EXCEEDED);
        assertThat(store.nodes).isEmpty();
        assertThat(BoundedVirtualLedgerAllocatorWorkflowV2.Bounds.formal().totalElapsedDeadline())
                .isLessThan(Duration.ofSeconds(5));
    }

    @Test
    void lateStoreCompletionAfterDeadlineCannotDispatchTheNextAuthorizedOperation() {
        store.delayReadCellCall = 2;
        var elapsed =
                new BoundedVirtualLedgerAllocatorWorkflowV2.Bounds(2, Duration.ofMillis(30), Duration.ofMillis(10));

        assertCode(
                strictAllocator
                        .boundedWorkflow(elapsed, BoundedVirtualLedgerAllocatorWorkflowV2.RetryScheduler.immediate())
                        .allocate(request(head, "late-store-completion")),
                AllocatorProtocolException.Code.WORKFLOW_DEADLINE_EXCEEDED);
        assertThat(store.delayedReadCell).isNotNull();
        assertThat(store.cell.value().reservation()).isPresent();
        int readsBeforeLateCompletion = store.readHeadCalls;

        store.delayedReadCell.complete(Optional.of(store.cell));

        assertThat(store.readHeadCalls).isEqualTo(readsBeforeLateCompletion);
        assertThat(store.createNodeCalls).isZero();
        assertThat(store.nodes).isEmpty();
    }

    private VersionedAllocatorCellStateV1 createCell(AllocatorModeV1 mode) {
        return exactCreate(store.createCell(VirtualLedgerCellAllocatorStateV1.initial(mode, assignment())));
    }

    private VersionedManagedLedgerAllocatorHeadV1 createHead(
            VersionedAllocatorCellStateV1 exactCell, ManagedLedgerIncarnationIdV1 incarnation) {
        return exactCreate(store.createHead(
                namespace(),
                exactCell.value().sliceAssignmentId(),
                ManagedLedgerAllocatorHeadV1.initial(
                        incarnation, 10, exactCell.value().nextSliceLedgerId())));
    }

    private static Request request(VersionedManagedLedgerAllocatorHeadV1 exactHead, String identity) {
        return new Request(digest("request-" + identity), digest("descriptor-" + identity), activeView(), exactHead);
    }

    private VersionedManagedLedgerAllocatorHeadV1 versionedHead(ManagedLedgerAllocatorHeadV1 value) {
        return new VersionedManagedLedgerAllocatorHeadV1(
                namespace(), assignment().sliceAssignmentId(), ReconcileStore.HEAD_KEY, value, store.nextVersion());
    }

    private static void assertCode(CompletionStage<?> stage, AllocatorProtocolException.Code expected) {
        assertThatThrownBy(() -> stage.toCompletableFuture().join())
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
        return new VersionedVirtualLedgerSliceViewV1(
                new VirtualLedgerSliceViewV1(namespace(), 1, assignment()), version(1), digest("registry"));
    }

    private static VirtualLedgerSliceAssignmentV1 assignment() {
        return assignment(namespace());
    }

    private static VirtualLedgerSliceAssignmentV1 assignment(Sha256Digest namespace) {
        return VirtualLedgerSliceAssignmentV1.create(
                new DeploymentId(new Id128(1, 2)),
                new ReservationDomainId(new Id128(3, 4)),
                new PulsarCellId(new Id128(5, 6)),
                namespace,
                VirtualLedgerSliceAssignmentV1.RESERVED_START_INCLUSIVE,
                VirtualLedgerSliceLifecycleV1.ACTIVE);
    }

    private static ManagedLedgerIncarnationIdV1 incarnation(String identity) {
        return new ManagedLedgerIncarnationIdV1(digest(identity));
    }

    private static Sha256Digest namespace() {
        return digest("namespace");
    }

    private static Sha256Digest digest(String value) {
        return Sha256Digest.hash(CanonicalBytes.copyOf(value.getBytes(StandardCharsets.UTF_8)));
    }

    private static MetadataVersion version(long value) {
        return new MetadataVersion(
                CanonicalBytes.copyOf(ByteBuffer.allocate(8).putLong(value).array()));
    }

    private static final class ReconcileStore implements PulsarVirtualLedgerAllocatorStore {
        private static final String CELL_KEY = "/allocator/cell";
        private static final String HEAD_KEY = "/allocator/head";
        private VersionedAllocatorCellStateV1 cell;
        private VersionedManagedLedgerAllocatorHeadV1 head;
        private final Map<Long, VersionedVirtualLedgerCandidateNodeV1> nodes = new HashMap<>();
        private final List<Sha256Digest> reservationRequestIds = new ArrayList<>();
        private final List<VirtualLedgerCandidateNodeV1> candidateValues = new ArrayList<>();
        private final List<PendingCellCas> pendingCellCas = new ArrayList<>();
        private final List<PendingNodeCreate> pendingNodeCreates = new ArrayList<>();
        private long version = 1;
        private int loseCellCasResponses;
        private int loseHeadCasResponses;
        private int loseNodeCreateResponses;
        private int barrierCellCasCount;
        private int barrierNodeCreateCount;
        private boolean cellCasPredecessorUnchanged;
        private boolean readCellNeverCompletes;
        private int delayReadCellCall;
        private int delayReadHeadCall;
        private int readCellCalls;
        private int readHeadCalls;
        private int createNodeCalls;
        private CompletableFuture<Optional<VersionedAllocatorCellStateV1>> delayedReadCell;
        private CompletableFuture<Optional<VersionedManagedLedgerAllocatorHeadV1>> delayedReadHead;

        private MetadataVersion nextVersion() {
            return version(version++);
        }

        @Override
        public synchronized CompletionStage<Optional<VersionedAllocatorCellStateV1>> readCell(
                Sha256Digest namespaceId, Sha256Digest sliceAssignmentId) {
            readCellCalls++;
            if (readCellNeverCompletes) {
                return new CompletableFuture<>();
            }
            if (readCellCalls == delayReadCellCall) {
                delayedReadCell = new CompletableFuture<>();
                return delayedReadCell;
            }
            return CompletableFuture.completedFuture(Optional.ofNullable(cell));
        }

        @Override
        public synchronized CompletionStage<CreateMutationResult<VersionedAllocatorCellStateV1>> createCell(
                VirtualLedgerCellAllocatorStateV1 candidate) {
            if (cell != null) {
                return CompletableFuture.completedFuture(
                        cell.value().equals(candidate)
                                ? CreateMutationResult.existingExact(cell)
                                : CreateMutationResult.definitiveConflict());
            }
            cell = new VersionedAllocatorCellStateV1(candidate, nextVersion());
            return CompletableFuture.completedFuture(CreateMutationResult.created(cell));
        }

        @Override
        public synchronized CompletionStage<ConditionalCasResult<VersionedAllocatorCellStateV1>> compareAndSetCell(
                VersionedAllocatorCellStateV1 predecessor, VirtualLedgerCellAllocatorStateV1 candidate) {
            candidate.reservation().ifPresent(value -> reservationRequestIds.add(value.requestId()));
            if (cell != null && cell.value().equals(candidate)) {
                return CompletableFuture.completedFuture(ConditionalCasResult.appliedExact(cell));
            }
            if (barrierCellCasCount > 0 && pendingCellCas.size() < barrierCellCasCount) {
                CompletableFuture<ConditionalCasResult<VersionedAllocatorCellStateV1>> future =
                        new CompletableFuture<>();
                pendingCellCas.add(new PendingCellCas(predecessor, candidate, future));
                if (pendingCellCas.size() == barrierCellCasCount) {
                    PendingCellCas winner = pendingCellCas.get(0);
                    cell = new VersionedAllocatorCellStateV1(winner.candidate(), nextVersion());
                    winner.future().complete(ConditionalCasResult.appliedExact(cell));
                    for (int index = 1; index < pendingCellCas.size(); index++) {
                        pendingCellCas.get(index).future().complete(ConditionalCasResult.definitiveConflict());
                    }
                    barrierCellCasCount = 0;
                }
                return future;
            }
            if (!Objects.equals(cell, predecessor)) {
                return CompletableFuture.completedFuture(ConditionalCasResult.definitiveConflict());
            }
            if (cellCasPredecessorUnchanged) {
                return CompletableFuture.completedFuture(ConditionalCasResult.predecessorUnchanged(cell));
            }
            cell = new VersionedAllocatorCellStateV1(candidate, nextVersion());
            if (loseCellCasResponses > 0) {
                loseCellCasResponses--;
                return CompletableFuture.completedFuture(ConditionalCasResult.indeterminate());
            }
            return CompletableFuture.completedFuture(ConditionalCasResult.appliedExact(cell));
        }

        @Override
        public synchronized CompletionStage<Optional<VersionedManagedLedgerAllocatorHeadV1>> readHead(
                Sha256Digest namespaceId,
                Sha256Digest sliceAssignmentId,
                ManagedLedgerIncarnationIdV1 managedLedgerIncarnation) {
            readHeadCalls++;
            if (readHeadCalls == delayReadHeadCall) {
                delayedReadHead = new CompletableFuture<>();
                return delayedReadHead;
            }
            return CompletableFuture.completedFuture(Optional.ofNullable(head));
        }

        @Override
        public synchronized CompletionStage<CreateMutationResult<VersionedManagedLedgerAllocatorHeadV1>> createHead(
                Sha256Digest namespaceId, Sha256Digest sliceAssignmentId, ManagedLedgerAllocatorHeadV1 candidate) {
            if (head != null) {
                return CompletableFuture.completedFuture(
                        head.value().equals(candidate)
                                ? CreateMutationResult.existingExact(head)
                                : CreateMutationResult.definitiveConflict());
            }
            head = new VersionedManagedLedgerAllocatorHeadV1(
                    namespaceId, sliceAssignmentId, HEAD_KEY, candidate, nextVersion());
            return CompletableFuture.completedFuture(CreateMutationResult.created(head));
        }

        @Override
        public synchronized CompletionStage<ConditionalCasResult<VersionedManagedLedgerAllocatorHeadV1>>
                compareAndSetHead(
                        Sha256Digest namespaceId,
                        Sha256Digest sliceAssignmentId,
                        VersionedManagedLedgerAllocatorHeadV1 predecessor,
                        ManagedLedgerAllocatorHeadV1 candidate) {
            if (head != null && head.value().equals(candidate)) {
                return CompletableFuture.completedFuture(ConditionalCasResult.appliedExact(head));
            }
            if (!Objects.equals(head, predecessor)) {
                return CompletableFuture.completedFuture(ConditionalCasResult.definitiveConflict());
            }
            head = new VersionedManagedLedgerAllocatorHeadV1(
                    namespaceId, sliceAssignmentId, HEAD_KEY, candidate, nextVersion());
            if (loseHeadCasResponses > 0) {
                loseHeadCasResponses--;
                return CompletableFuture.completedFuture(ConditionalCasResult.indeterminate());
            }
            return CompletableFuture.completedFuture(ConditionalCasResult.appliedExact(head));
        }

        @Override
        public synchronized CompletionStage<Optional<VersionedVirtualLedgerCandidateNodeV1>> readNode(
                Sha256Digest namespaceId,
                Sha256Digest sliceAssignmentId,
                ManagedLedgerIncarnationIdV1 managedLedgerIncarnation,
                long ledgerId) {
            return CompletableFuture.completedFuture(Optional.ofNullable(nodes.get(ledgerId)));
        }

        @Override
        public synchronized CompletionStage<CreateMutationResult<VersionedVirtualLedgerCandidateNodeV1>> createNode(
                Sha256Digest namespaceId, Sha256Digest sliceAssignmentId, VirtualLedgerCandidateNodeV1 candidate) {
            createNodeCalls++;
            candidateValues.add(candidate);
            VersionedVirtualLedgerCandidateNodeV1 existing = nodes.get(candidate.ledgerId());
            if (existing != null) {
                return CompletableFuture.completedFuture(
                        existing.value().equals(candidate)
                                ? CreateMutationResult.existingExact(existing)
                                : CreateMutationResult.definitiveConflict());
            }
            if (barrierNodeCreateCount > 0 && pendingNodeCreates.size() < barrierNodeCreateCount) {
                CompletableFuture<CreateMutationResult<VersionedVirtualLedgerCandidateNodeV1>> future =
                        new CompletableFuture<>();
                pendingNodeCreates.add(new PendingNodeCreate(namespaceId, sliceAssignmentId, candidate, future));
                if (pendingNodeCreates.size() == barrierNodeCreateCount) {
                    PendingNodeCreate winner = pendingNodeCreates.get(0);
                    VersionedVirtualLedgerCandidateNodeV1 exact = new VersionedVirtualLedgerCandidateNodeV1(
                            winner.namespaceId(),
                            winner.sliceAssignmentId(),
                            "/allocator/nodes/" + winner.candidate().ledgerId(),
                            winner.candidate(),
                            nextVersion());
                    nodes.put(winner.candidate().ledgerId(), exact);
                    winner.future().complete(CreateMutationResult.created(exact));
                    for (int index = 1; index < pendingNodeCreates.size(); index++) {
                        pendingNodeCreates.get(index).future().complete(CreateMutationResult.definitiveConflict());
                    }
                    barrierNodeCreateCount = 0;
                }
                return future;
            }
            VersionedVirtualLedgerCandidateNodeV1 exact = new VersionedVirtualLedgerCandidateNodeV1(
                    namespaceId,
                    sliceAssignmentId,
                    "/allocator/nodes/" + candidate.ledgerId(),
                    candidate,
                    nextVersion());
            nodes.put(candidate.ledgerId(), exact);
            if (loseNodeCreateResponses > 0) {
                loseNodeCreateResponses--;
                return CompletableFuture.completedFuture(CreateMutationResult.indeterminate());
            }
            return CompletableFuture.completedFuture(CreateMutationResult.created(exact));
        }

        private record PendingCellCas(
                VersionedAllocatorCellStateV1 predecessor,
                VirtualLedgerCellAllocatorStateV1 candidate,
                CompletableFuture<ConditionalCasResult<VersionedAllocatorCellStateV1>> future) {}

        private record PendingNodeCreate(
                Sha256Digest namespaceId,
                Sha256Digest sliceAssignmentId,
                VirtualLedgerCandidateNodeV1 candidate,
                CompletableFuture<CreateMutationResult<VersionedVirtualLedgerCandidateNodeV1>> future) {}
    }
}
