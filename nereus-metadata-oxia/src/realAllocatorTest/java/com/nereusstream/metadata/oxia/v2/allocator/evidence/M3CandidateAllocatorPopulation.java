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

package com.nereusstream.metadata.oxia.v2.allocator.evidence;

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
import com.nereusstream.domain.registry.allocator.AllocatorRawEvidenceEventV1.OxiaOperationKind;
import com.nereusstream.domain.registry.allocator.ManagedLedgerIncarnationIdV1;
import com.nereusstream.metadata.spi.allocator.ProductionVirtualLedgerAllocator;
import com.nereusstream.metadata.spi.allocator.VersionedAllocatorCellStateV1;
import com.nereusstream.metadata.spi.allocator.VersionedManagedLedgerAllocatorHeadV1;
import com.nereusstream.metadata.spi.allocator.VersionedVirtualLedgerCandidateNodeV1;
import com.nereusstream.metadata.spi.model.ConditionalCasOutcome;
import com.nereusstream.metadata.spi.model.ConditionalCasResult;
import com.nereusstream.metadata.spi.model.CreateMutationOutcome;
import com.nereusstream.metadata.spi.model.CreateMutationResult;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import com.nereusstream.metadata.spi.model.VersionedVirtualLedgerSliceViewV1;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionService;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.ReentrantLock;

/** One full real-Oxia candidate population driven only through the production allocator SPI. */
final class M3CandidateAllocatorPopulation {
    private static final long INITIAL_OWNER_EPOCH = 1;
    private static final long OPERATION_TIMEOUT_SECONDS = 120;
    private static final long POPULATION_DRAIN_TIMEOUT_SECONDS = 600;

    private final AllocatorEvidenceCandidateV1 candidate;
    private final M3RealOxiaActors actors;
    private final ExecutorService workers;
    private final VersionedVirtualLedgerSliceViewV1 currentView;
    private final M3EvidenceAllocatorStore.TraceRegistry traces = new M3EvidenceAllocatorStore.TraceRegistry();
    private final List<ProductionVirtualLedgerAllocator> allocators;
    private final AtomicReference<VersionedAllocatorCellStateV1> cell = new AtomicReference<>();
    private final AtomicReferenceArray<VersionedManagedLedgerAllocatorHeadV1> heads =
            new AtomicReferenceArray<>(100_000);
    private final ReentrantLock[] headLocks = new ReentrantLock[100_000];
    private final ReentrantLock cellLock = new ReentrantLock();
    private final AtomicInteger activePopulation = new AtomicInteger();

    M3CandidateAllocatorPopulation(
            AllocatorEvidenceCandidateV1 candidate,
            int candidateIndex,
            String executionDiscriminator,
            M3RealOxiaActors actors,
            ExecutorService workers) {
        this.candidate = Objects.requireNonNull(candidate, "candidate");
        this.actors = Objects.requireNonNull(actors, "actors");
        this.workers = Objects.requireNonNull(workers, "workers");
        if (candidateIndex < 0 || candidateIndex >= 5) {
            throw new IllegalArgumentException("allocator candidate index is outside the closed inventory");
        }
        for (int index = 0; index < headLocks.length; index++) {
            headLocks[index] = new ReentrantLock();
        }
        VirtualLedgerSliceAssignmentV1 assignment = assignment(candidateIndex, executionDiscriminator);
        currentView = new VersionedVirtualLedgerSliceViewV1(
                new VirtualLedgerSliceViewV1(assignment.ledgerIdCompatibilityNamespaceId(), 1, assignment),
                new MetadataVersion(CanonicalBytes.copyOf(new byte[] {1})),
                digest("registry:" + executionDiscriminator + ":" + candidateIndex));
        String root = "/nereus/v2/m3/allocator/formal/" + executionDiscriminator + "/candidate-" + candidateIndex;
        List<ProductionVirtualLedgerAllocator> exactAllocators = new ArrayList<>(4);
        for (M3RealOxiaActors.Actor actor : actors.actors()) {
            M3EvidenceAllocatorStore store = new M3EvidenceAllocatorStore(root, actor.client(), traces);
            ProductionVirtualLedgerAllocator allocator =
                    ProductionVirtualLedgerAllocator.forEvidenceCandidate(candidate, store);
            if (allocator.runtimeActivated()) {
                throw new IllegalStateException("formal evidence candidate accidentally acquired runtime activation");
            }
            exactAllocators.add(allocator);
        }
        allocators = List.copyOf(exactAllocators);
        cell.set(exactCreate(allocators.get(0).createCell(currentView)));
    }

    AllocatorEvidenceCandidateV1 candidate() {
        return candidate;
    }

    long ensurePopulation(int requestedPopulation) throws Exception {
        if (!M3AllocatorWorkloadPlan.ACTIVE_POPULATIONS.contains(requestedPopulation)) {
            throw new IllegalArgumentException("allocator population differs from ADR 0094");
        }
        int from = activePopulation.get();
        if (requestedPopulation < from) {
            return 0;
        }
        long started = System.nanoTime();
        parallel(from, requestedPopulation, this::createHead);
        if (!activePopulation.compareAndSet(from, requestedPopulation)) {
            throw new IllegalStateException("allocator population construction raced");
        }
        return TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - started);
    }

    Allocation rollover(
            M3AllocatorRequestTelemetry.RequestTrace trace,
            int ledgerIndex,
            ResponseLossAt responseLossAt) {
        requireActive(ledgerIndex);
        ReentrantLock headLock = headLocks[ledgerIndex];
        headLock.lock();
        try {
            VersionedManagedLedgerAllocatorHeadV1 head = requireHead(ledgerIndex);
            ManagedLedgerIncarnationIdV1 incarnation = head.value().managedLedgerIncarnation();
            traces.bindHead(incarnation, trace, OxiaOperationKind.HEAD_PUBLISH_CAS);
            try {
                trace.setOwnerEpoch(head.value().ownerEpoch());
                trace.admitted();
                trace.appendAdmissionStart();
                Allocation allocation = candidate.mode() == AllocatorModeV1.STRICT_SERIALIZED
                        ? strictRollover(trace, ledgerIndex, head, responseLossAt)
                        : rangeRollover(trace, ledgerIndex, head, responseLossAt);
                trace.allocatedLedgerId(allocation.ledgerId());
                trace.grantUse(allocation.grantId(), allocation.ledgerId());
                trace.appendAdmissionRelease();
                return allocation;
            } finally {
                traces.unbindHead(incarnation);
            }
        } finally {
            headLock.unlock();
        }
    }

    VersionedManagedLedgerAllocatorHeadV1 takeover(
            M3AllocatorRequestTelemetry.RequestTrace trace, int ledgerIndex, long newOwnerEpoch) {
        requireActive(ledgerIndex);
        ReentrantLock lock = headLocks[ledgerIndex];
        lock.lock();
        try {
            VersionedManagedLedgerAllocatorHeadV1 predecessor = requireHead(ledgerIndex);
            ManagedLedgerIncarnationIdV1 incarnation = predecessor.value().managedLedgerIncarnation();
            traces.bindHead(incarnation, trace, OxiaOperationKind.HEAD_TAKEOVER_CAS);
            try {
                traces.setHeadMutationKind(incarnation, OxiaOperationKind.HEAD_TAKEOVER_CAS);
                traces.bindCellRead(trace);
                VersionedManagedLedgerAllocatorHeadV1 successor;
                try {
                    successor = exact(
                            allocator(trace.actorId()).takeover(cell.get(), predecessor, newOwnerEpoch));
                } finally {
                    traces.unbindCellRead(trace);
                }
                heads.set(ledgerIndex, successor);
                trace.setOwnerEpoch(successor.value().ownerEpoch());
                return successor;
            } finally {
                traces.unbindHead(incarnation);
            }
        } finally {
            lock.unlock();
        }
    }

    void admitAppendUnderFreshOwner(
            M3AllocatorRequestTelemetry.RequestTrace trace, int ledgerIndex, long expectedOwnerEpoch) {
        requireActive(ledgerIndex);
        if (expectedOwnerEpoch <= INITIAL_OWNER_EPOCH) {
            throw new IllegalArgumentException("fresh-owner append admission requires a successor owner epoch");
        }
        ReentrantLock lock = headLocks[ledgerIndex];
        lock.lock();
        try {
            VersionedManagedLedgerAllocatorHeadV1 exactHead = requireHead(ledgerIndex);
            if (exactHead.value().ownerEpoch() != expectedOwnerEpoch) {
                throw new IllegalStateException("fresh-owner append admission did not observe the exact takeover Head");
            }
            trace.setOwnerEpoch(expectedOwnerEpoch);
            trace.admitted();
            trace.appendAdmissionStart();
            trace.appendAdmissionRelease();
        } finally {
            lock.unlock();
        }
    }

    Allocation rolloverWithSingleOwnerTakeover(
            M3AllocatorRequestTelemetry.RequestTrace trace, int ledgerIndex) {
        requireActive(ledgerIndex);
        ReentrantLock lock = headLocks[ledgerIndex];
        lock.lock();
        try {
            VersionedManagedLedgerAllocatorHeadV1 head = requireHead(ledgerIndex);
            ManagedLedgerIncarnationIdV1 incarnation = head.value().managedLedgerIncarnation();
            traces.bindHead(incarnation, trace, OxiaOperationKind.HEAD_TAKEOVER_CAS);
            try {
                trace.setOwnerEpoch(head.value().ownerEpoch());
                trace.admitted();
                trace.appendAdmissionStart();
                traces.setHeadMutationKind(incarnation, OxiaOperationKind.HEAD_TAKEOVER_CAS);
                head = exact(allocator(trace.actorId())
                        .takeover(cell.get(), head, Math.addExact(head.value().ownerEpoch(), 1)));
                heads.set(ledgerIndex, head);
                trace.setOwnerEpoch(head.value().ownerEpoch());
                traces.setHeadMutationKind(incarnation, OxiaOperationKind.HEAD_PUBLISH_CAS);
                Allocation allocation = candidate.mode() == AllocatorModeV1.STRICT_SERIALIZED
                        ? strictRollover(trace, ledgerIndex, head, ResponseLossAt.NONE)
                        : rangeRollover(trace, ledgerIndex, head, ResponseLossAt.NONE);
                trace.allocatedLedgerId(allocation.ledgerId());
                trace.grantUse(allocation.grantId(), allocation.ledgerId());
                trace.appendAdmissionRelease();
                return allocation;
            } finally {
                traces.unbindHead(incarnation);
            }
        } finally {
            lock.unlock();
        }
    }

    void lateOldOwnerWriteAndRecover(
            M3AllocatorRequestTelemetry.RequestTrace trace, int ledgerIndex) {
        requireActive(ledgerIndex);
        ReentrantLock lock = headLocks[ledgerIndex];
        lock.lock();
        try {
            VersionedManagedLedgerAllocatorHeadV1 head = requireHead(ledgerIndex);
            ManagedLedgerIncarnationIdV1 incarnation = head.value().managedLedgerIncarnation();
            traces.bindHead(incarnation, trace, OxiaOperationKind.HEAD_PUBLISH_CAS);
            VersionedAllocatorCellStateV1 allocationCell = cell.get();
            boolean cellBound = false;
            try {
                trace.setOwnerEpoch(head.value().ownerEpoch());
                trace.admitted();
                trace.appendAdmissionStart();
                ProductionVirtualLedgerAllocator allocator = allocator(trace.actorId());
                if (candidate.mode() == AllocatorModeV1.STRICT_SERIALIZED) {
                    cellLock.lock();
                    traces.bindCell(trace, OxiaOperationKind.CELL_RESERVE_CAS);
                    cellBound = true;
                    allocationCell = exact(allocator.reserve(
                            cell.get(), head, currentView, digest("late-owner-reserve:" + trace.identity())));
                    cell.set(allocationCell);
                } else if (head.value().grantId() == 0
                        || head.value().nextLedgerId() >= head.value().rangeEndExclusive()) {
                    cellLock.lock();
                    traces.bindCell(trace, OxiaOperationKind.CELL_RESERVE_CAS);
                    cellBound = true;
                    allocationCell = exact(allocator.reserve(
                            cell.get(), head, currentView, digest("late-owner-reserve:" + trace.identity())));
                    cell.set(allocationCell);
                    traces.setHeadMutationKind(incarnation, OxiaOperationKind.RANGE_GRANT_INSTALL_CAS);
                    head = exact(allocator.installRangeReservedGrant(allocationCell, head, currentView));
                    heads.set(ledgerIndex, head);
                    traces.setCellMutationKind(OxiaOperationKind.CELL_CLEAR_CAS);
                    cell.set(exact(allocator.clearReservation(allocationCell, head)));
                    allocationCell = cell.get();
                    traces.unbindCell(trace);
                    cellBound = false;
                    cellLock.unlock();
                }

                VersionedManagedLedgerAllocatorHeadV1 staleOwnerHead = head;
                VersionedVirtualLedgerCandidateNodeV1 staleNode = exactCreate(allocator.createCandidate(
                        allocationCell,
                        staleOwnerHead,
                        currentView,
                        digest("late-owner-node:" + trace.identity())));

                traces.setHeadMutationKind(incarnation, OxiaOperationKind.HEAD_TAKEOVER_CAS);
                VersionedManagedLedgerAllocatorHeadV1 currentOwnerHead = exact(allocator.takeover(
                        allocationCell,
                        staleOwnerHead,
                        Math.addExact(staleOwnerHead.value().ownerEpoch(), 1)));
                heads.set(ledgerIndex, currentOwnerHead);

                traces.setHeadMutationKind(incarnation, OxiaOperationKind.HEAD_PUBLISH_CAS);
                trace.setOwnerEpoch(staleOwnerHead.value().ownerEpoch());
                ConditionalCasResult<VersionedManagedLedgerAllocatorHeadV1> late = awaitStage(
                        allocator.publishCandidate(allocationCell, staleOwnerHead, staleNode, currentView),
                        "late old-owner publish");
                if (late.outcome() != ConditionalCasOutcome.DEFINITIVE_CONFLICT
                        || late.exactSnapshot().isPresent()) {
                    throw new AssertionError("late old-owner publish did not end in exact definitive conflict");
                }

                traces.setHeadMutationKind(incarnation, OxiaOperationKind.HEAD_STALE_BURN_CAS);
                trace.setOwnerEpoch(currentOwnerHead.value().ownerEpoch());
                VersionedManagedLedgerAllocatorHeadV1 burned =
                        exact(allocator.burnStaleCandidate(allocationCell, currentOwnerHead, staleNode, currentView));
                heads.set(ledgerIndex, burned);
                trace.staleCandidateBurn(staleNode.value().grantId(), staleNode.value().ledgerId());
                if (candidate.mode() == AllocatorModeV1.STRICT_SERIALIZED) {
                    traces.setCellMutationKind(OxiaOperationKind.CELL_CLEAR_CAS);
                    cell.set(exact(allocator.clearReservation(allocationCell, burned)));
                }
                trace.appendAdmissionRelease();
            } finally {
                if (cellBound) {
                    traces.unbindCell(trace);
                    cellLock.unlock();
                }
                traces.unbindHead(incarnation);
            }
        } finally {
            lock.unlock();
        }
    }

    List<Integer> ledgersOwnedByActor(int actorId, int population) {
        if (actorId < 0 || actorId >= 4 || population > activePopulation.get()) {
            throw new IllegalArgumentException("allocator ownership inventory is outside the active population");
        }
        List<Integer> owned = new ArrayList<>(population / 4);
        for (int index = actorId; index < population; index += 4) {
            owned.add(index);
        }
        return List.copyOf(owned);
    }

    long nextOwnerEpoch(int ledgerIndex) {
        return Math.addExact(requireHead(ledgerIndex).value().ownerEpoch(), 1);
    }

    private Allocation strictRollover(
            M3AllocatorRequestTelemetry.RequestTrace trace,
            int ledgerIndex,
            VersionedManagedLedgerAllocatorHeadV1 head,
            ResponseLossAt responseLossAt) {
        cellLock.lock();
        traces.bindCell(trace, OxiaOperationKind.CELL_RESERVE_CAS);
        try {
            ProductionVirtualLedgerAllocator allocator = allocator(trace.actorId());
            maybeLoseResponse(trace.actorId(), responseLossAt, ResponseLossAt.RESERVE);
            VersionedAllocatorCellStateV1 reserved = exact(allocator.reserve(
                    cell.get(), head, currentView, digest("reserve:" + trace.identity())));
            cell.set(reserved);

            maybeLoseResponse(trace.actorId(), responseLossAt, ResponseLossAt.NODE_CREATE);
            VersionedVirtualLedgerCandidateNodeV1 node = exactCreate(allocator.createCandidate(
                    reserved, head, currentView, digest("descriptor:" + trace.identity())));

            traces.setHeadMutationKind(head.value().managedLedgerIncarnation(), OxiaOperationKind.HEAD_PUBLISH_CAS);
            maybeLoseResponse(trace.actorId(), responseLossAt, ResponseLossAt.HEAD_PUBLISH);
            VersionedManagedLedgerAllocatorHeadV1 published =
                    exact(allocator.publishCandidate(reserved, head, node, currentView));
            heads.set(ledgerIndex, published);

            traces.setCellMutationKind(OxiaOperationKind.CELL_CLEAR_CAS);
            maybeLoseResponse(trace.actorId(), responseLossAt, ResponseLossAt.CELL_CLEAR);
            cell.set(exact(allocator.clearReservation(reserved, published)));
            return new Allocation(node.value().ledgerId(), node.value().grantId());
        } finally {
            traces.unbindCell(trace);
            cellLock.unlock();
        }
    }

    private Allocation rangeRollover(
            M3AllocatorRequestTelemetry.RequestTrace trace,
            int ledgerIndex,
            VersionedManagedLedgerAllocatorHeadV1 initialHead,
            ResponseLossAt responseLossAt) {
        VersionedManagedLedgerAllocatorHeadV1 head = initialHead;
        ProductionVirtualLedgerAllocator allocator = allocator(trace.actorId());
        if (head.value().grantId() == 0 || head.value().nextLedgerId() >= head.value().rangeEndExclusive()) {
            cellLock.lock();
            traces.bindCell(trace, OxiaOperationKind.CELL_RESERVE_CAS);
            try {
                maybeLoseResponse(trace.actorId(), responseLossAt, ResponseLossAt.RESERVE);
                VersionedAllocatorCellStateV1 reserved = exact(allocator.reserve(
                        cell.get(), head, currentView, digest("reserve:" + trace.identity())));
                cell.set(reserved);

                traces.setHeadMutationKind(
                        head.value().managedLedgerIncarnation(), OxiaOperationKind.RANGE_GRANT_INSTALL_CAS);
                maybeLoseResponse(trace.actorId(), responseLossAt, ResponseLossAt.RANGE_GRANT_INSTALL);
                head = exact(allocator.installRangeReservedGrant(reserved, head, currentView));
                heads.set(ledgerIndex, head);

                traces.setCellMutationKind(OxiaOperationKind.CELL_CLEAR_CAS);
                maybeLoseResponse(trace.actorId(), responseLossAt, ResponseLossAt.CELL_CLEAR);
                cell.set(exact(allocator.clearReservation(reserved, head)));
            } finally {
                traces.unbindCell(trace);
                cellLock.unlock();
            }
        }

        maybeLoseResponse(trace.actorId(), responseLossAt, ResponseLossAt.NODE_CREATE);
        VersionedVirtualLedgerCandidateNodeV1 node = exactCreate(
                allocator.createCandidate(cell.get(), head, currentView, digest("descriptor:" + trace.identity())));
        traces.setHeadMutationKind(head.value().managedLedgerIncarnation(), OxiaOperationKind.HEAD_PUBLISH_CAS);
        maybeLoseResponse(trace.actorId(), responseLossAt, ResponseLossAt.HEAD_PUBLISH);
        VersionedManagedLedgerAllocatorHeadV1 published =
                exact(allocator.publishCandidate(cell.get(), head, node, currentView));
        heads.set(ledgerIndex, published);
        return new Allocation(node.value().ledgerId(), node.value().grantId());
    }

    private void createHead(int index) {
        int actorId = index & 3;
        VersionedAllocatorCellStateV1 exactCell = cell.get();
        VersionedManagedLedgerAllocatorHeadV1 head = exactCreate(allocator(actorId)
                .createHead(exactCell, currentView, incarnation(index), INITIAL_OWNER_EPOCH));
        if (candidate.mode() == AllocatorModeV1.RANGE_LEASED && !reservedForFaultCuts(index)) {
            cellLock.lock();
            try {
                VersionedAllocatorCellStateV1 currentCell = cell.get();
                VersionedAllocatorCellStateV1 reserved = exact(allocator(actorId)
                        .reserve(currentCell, head, currentView, digest("population-grant:" + index)));
                cell.set(reserved);
                head = exact(allocator(actorId).installRangeReservedGrant(reserved, head, currentView));
                cell.set(exact(allocator(actorId).clearReservation(reserved, head)));
            } finally {
                cellLock.unlock();
            }
        }
        heads.set(index, head);
    }

    private static boolean reservedForFaultCuts(int index) {
        int within10k = index % 10_000;
        return within10k >= 9_936;
    }

    private ProductionVirtualLedgerAllocator allocator(int actorId) {
        return allocators.get(actorId);
    }

    private void maybeLoseResponse(int actorId, ResponseLossAt requested, ResponseLossAt current) {
        if (requested == current) {
            actors.actor(actorId).client().loseNextMutationResponse();
        }
    }

    private void requireActive(int ledgerIndex) {
        if (ledgerIndex < 0 || ledgerIndex >= activePopulation.get()) {
            throw new IllegalArgumentException("allocator request selected a non-active ManagedLedger");
        }
    }

    private VersionedManagedLedgerAllocatorHeadV1 requireHead(int ledgerIndex) {
        return Objects.requireNonNull(heads.get(ledgerIndex), "allocator Head population is incomplete");
    }

    private void parallel(int fromInclusive, int toExclusive, IndexedOperation operation) throws Exception {
        CompletionService<Void> completions = new ExecutorCompletionService<>(workers);
        for (int index = fromInclusive; index < toExclusive; index++) {
            int exactIndex = index;
            completions.submit(() -> {
                operation.run(exactIndex);
                return null;
            });
        }
        M3BoundedCompletionDrain.await(
                completions,
                toExclusive - fromInclusive,
                POPULATION_DRAIN_TIMEOUT_SECONDS,
                "allocator population construction");
    }

    private static VirtualLedgerSliceAssignmentV1 assignment(int candidateIndex, String discriminator) {
        Sha256Digest namespace = digest("namespace:" + discriminator);
        long start = Math.addExact(
                VirtualLedgerSliceAssignmentV1.RESERVED_START_INCLUSIVE,
                Math.multiplyExact(candidateIndex, VirtualLedgerSliceAssignmentV1.SLICE_SIZE));
        return VirtualLedgerSliceAssignmentV1.create(
                new DeploymentId(new Id128(1, 2)),
                new ReservationDomainId(new Id128(3, 4)),
                new PulsarCellId(new Id128(5, 6)),
                namespace,
                start,
                VirtualLedgerSliceLifecycleV1.ACTIVE);
    }

    private static ManagedLedgerIncarnationIdV1 incarnation(int index) {
        return new ManagedLedgerIncarnationIdV1(digest("managed-ledger:" + index));
    }

    private static Sha256Digest digest(String value) {
        return Sha256Digest.hash(CanonicalBytes.copyOf(value.getBytes(StandardCharsets.UTF_8)));
    }

    private static <T> T exact(CompletionStage<ConditionalCasResult<T>> stage) {
        return exactResult(awaitStage(stage, "allocator conditional CAS"));
    }

    private static <T> T exactResult(ConditionalCasResult<T> result) {
        if (result.outcome() != ConditionalCasOutcome.APPLIED_EXACT
                && result.outcome() != ConditionalCasOutcome.PREDECESSOR_UNCHANGED) {
            throw new IllegalStateException("allocator production CAS did not converge exactly: " + result.outcome());
        }
        return result.exactSnapshot().orElseThrow();
    }

    private static <T> T exactCreate(CompletionStage<CreateMutationResult<T>> stage) {
        CreateMutationResult<T> result = awaitStage(stage, "allocator create mutation");
        if (result.outcome() != CreateMutationOutcome.CREATED
                && result.outcome() != CreateMutationOutcome.EXISTING_EXACT) {
            throw new IllegalStateException(
                    "allocator production create did not converge exactly: " + result.outcome());
        }
        return result.exactSnapshot().orElseThrow();
    }

    static <T> T awaitStage(CompletionStage<T> stage, String label) {
        Objects.requireNonNull(stage, "stage");
        Objects.requireNonNull(label, "label");
        try {
            return stage.toCompletableFuture().get(OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(label + " was interrupted during bounded cleanup", failure);
        } catch (TimeoutException failure) {
            throw new IllegalStateException(
                    label + " exceeded the " + OPERATION_TIMEOUT_SECONDS + " second operation cap", failure);
        } catch (ExecutionException failure) {
            Throwable cause = failure.getCause();
            if (cause instanceof RuntimeException exception) {
                throw exception;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw new IllegalStateException(label + " failed", cause);
        }
    }

    enum ResponseLossAt {
        NONE,
        RESERVE,
        RANGE_GRANT_INSTALL,
        NODE_CREATE,
        HEAD_PUBLISH,
        CELL_CLEAR
    }

    record Allocation(long ledgerId, long grantId) {}

    @FunctionalInterface
    private interface IndexedOperation {
        void run(int index) throws Exception;
    }

}
