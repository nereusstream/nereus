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

package com.nereusstream.kafka.bookkeeper.pipeline;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.kafka.bookkeeper.adapter.KafkaNbke2AssignedAppendGroupV1;
import com.nereusstream.kafka.bookkeeper.run.KafkaBookKeeperEntryReservationV1;
import com.nereusstream.kafka.bookkeeper.run.KafkaBookKeeperRunLifecycleV1;
import com.nereusstream.kafka.bookkeeper.run.KafkaBookKeeperRunStateV1;
import com.nereusstream.storage.api.bookkeeper.AppendQuorumProofV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCellSession;
import com.nereusstream.storage.api.bookkeeper.ProviderMutationOutcomeV1;
import com.nereusstream.storage.api.bookkeeper.ProviderMutationResultV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerAppendRequestV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.bookkeeper.ImmutableRetainedStoragePayload;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Capacity-first, bounded overlapping BookKeeper DATA pipeline with strictly ordered durable completion. */
public final class KafkaBookKeeperOrderedPipelineV1 {
    private enum PipelineState {
        ACTIVE,
        FENCED
    }

    private enum SlotState {
        PENDING,
        DURABLE,
        DEFINITIVELY_FAILED,
        OUTCOME_UNKNOWN
    }

    private enum MemberOutcome {
        APPLIED_EXACT,
        DEFINITIVELY_NOT_APPLIED,
        OUTCOME_UNKNOWN
    }

    private final BookKeeperCellSession session;
    private final KafkaBookKeeperRunLifecycleV1 lifecycle;
    private final KafkaAppendCapacityControllerV1 partitionCapacity;
    private final KafkaAppendCapacityControllerV1 globalCapacity;
    private final KafkaOrderedDurableCommitObserver commitObserver;
    private final ArrayDeque<Slot> orderedSlots = new ArrayDeque<>();

    private PipelineState state = PipelineState.ACTIVE;
    private long speculativeEndOffset;
    private long committedEndOffset;

    public KafkaBookKeeperOrderedPipelineV1(
            BookKeeperCellSession session,
            KafkaBookKeeperRunLifecycleV1 lifecycle,
            KafkaAppendCapacityControllerV1 partitionCapacity,
            KafkaAppendCapacityControllerV1 globalCapacity,
            KafkaOrderedDurableCommitObserver commitObserver) {
        this.session = Objects.requireNonNull(session, "session");
        this.lifecycle = Objects.requireNonNull(lifecycle, "lifecycle");
        this.partitionCapacity = Objects.requireNonNull(partitionCapacity, "partitionCapacity");
        this.globalCapacity = Objects.requireNonNull(globalCapacity, "globalCapacity");
        this.commitObserver = Objects.requireNonNull(commitObserver, "commitObserver");
        var run = lifecycle.snapshot();
        if (run.state() != KafkaBookKeeperRunStateV1.ACTIVE
                || !session.providerScopeId().equals(run.runBinding().providerScopeId())
                || !session.providerScopeId().equals(run.handle().providerScopeId())) {
            throw new IllegalArgumentException("pipeline session and lifecycle are not the same active Provider Scope");
        }
        speculativeEndOffset = run.root().kafkaStartOffset();
        committedEndOffset = speculativeEndOffset;
    }

    public CompletionStage<KafkaOrderedAppendResultV1> submit(
            KafkaAppendAdmissionRequestV1 request, KafkaOffsetAssignmentV1 offsetAssignment) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(offsetAssignment, "offsetAssignment");
        Optional<CapacityPair> capacity = reserveCapacity(request);
        if (capacity.isEmpty()) {
            return CompletableFuture.completedFuture(
                    KafkaOrderedAppendResultV1.beforeAssignment(KafkaOrderedAppendOutcomeV1.CAPACITY_REJECTED));
        }
        KafkaOffsetAssignedAppendV1 assigned;
        KafkaBookKeeperEntryReservationV1 reservation;
        KafkaNbke2AssignedAppendGroupV1 physicalGroup;
        List<CanonicalBytes> encodedEntries;
        List<CompletionStage<MemberOutcome>> memberStages = new ArrayList<>();
        Slot slot;
        synchronized (this) {
            if (state != PipelineState.ACTIVE) {
                capacity.orElseThrow().close();
                return CompletableFuture.completedFuture(
                        KafkaOrderedAppendResultV1.beforeAssignment(KafkaOrderedAppendOutcomeV1.FENCED_BY_PREDECESSOR));
            }
            try {
                assigned = Objects.requireNonNull(offsetAssignment.assign(), "assigned append");
            } catch (RuntimeException failure) {
                capacity.orElseThrow().close();
                return CompletableFuture.completedFuture(KafkaOrderedAppendResultV1.beforeAssignment(
                        KafkaOrderedAppendOutcomeV1.OFFSET_ASSIGNMENT_FAILED));
            }
            if (assigned.startOffset() != speculativeEndOffset) {
                capacity.orElseThrow().close();
                fencePending();
                return CompletableFuture.completedFuture(KafkaOrderedAppendResultV1.assigned(
                        KafkaOrderedAppendOutcomeV1.INVALID_ASSIGNMENT,
                        assigned.startOffset(),
                        assigned.endOffsetExclusive()));
            }
            speculativeEndOffset = assigned.endOffsetExclusive();
            try {
                reservation = lifecycle.reserveDataGroup(request.memberCount());
            } catch (RuntimeException failure) {
                capacity.orElseThrow().close();
                fencePending();
                return CompletableFuture.completedFuture(KafkaOrderedAppendResultV1.assigned(
                        KafkaOrderedAppendOutcomeV1.INVALID_ASSIGNMENT,
                        assigned.startOffset(),
                        assigned.endOffsetExclusive()));
            }
            try {
                physicalGroup = Objects.requireNonNull(
                        assigned.physicalGroupFactory().apply(reservation.firstEntryId()), "physical append group");
                encodedEntries = physicalGroup.encode(
                        lifecycle.snapshot().handle().ledgerIdentity().ledgerId());
                validatePhysicalGroup(request, assigned, reservation, physicalGroup, encodedEntries);
            } catch (RuntimeException failure) {
                lifecycle.completeDataGroup(reservation);
                capacity.orElseThrow().close();
                fencePending();
                return CompletableFuture.completedFuture(KafkaOrderedAppendResultV1.assigned(
                        KafkaOrderedAppendOutcomeV1.INVALID_ASSIGNMENT,
                        assigned.startOffset(),
                        assigned.endOffsetExclusive()));
            }
            slot = new Slot(assigned.startOffset(), assigned.endOffsetExclusive(), capacity.orElseThrow());
            orderedSlots.addLast(slot);
            try {
                for (int index = 0; index < encodedEntries.size(); index++) {
                    memberStages.add(submitMember(
                            lifecycle.snapshot().handle(), reservation.entryId(index), encodedEntries.get(index)));
                }
            } catch (RuntimeException failure) {
                memberStages.add(CompletableFuture.completedFuture(MemberOutcome.OUTCOME_UNKNOWN));
            } finally {
                lifecycle.completeDataGroup(reservation);
            }
        }

        CompletableFuture<?>[] futures =
                memberStages.stream().map(CompletionStage::toCompletableFuture).toArray(CompletableFuture[]::new);
        CompletableFuture.allOf(futures).whenComplete((ignored, failure) -> {
            MemberOutcome aggregate = failure == null ? aggregate(memberStages) : MemberOutcome.OUTCOME_UNKNOWN;
            finishSlot(slot, aggregate);
        });
        return slot.result;
    }

    public synchronized long speculativeEndOffset() {
        return speculativeEndOffset;
    }

    public synchronized long committedEndOffset() {
        return committedEndOffset;
    }

    public synchronized boolean fenced() {
        return state == PipelineState.FENCED;
    }

    private Optional<CapacityPair> reserveCapacity(KafkaAppendAdmissionRequestV1 request) {
        Optional<KafkaAppendCapacityControllerV1.Lease> partition =
                partitionCapacity.tryReserve(request.memberCount(), request.encodedDataBytes());
        if (partition.isEmpty()) {
            return Optional.empty();
        }
        Optional<KafkaAppendCapacityControllerV1.Lease> global =
                globalCapacity.tryReserve(request.memberCount(), request.encodedDataBytes());
        if (global.isEmpty()) {
            partition.orElseThrow().close();
            return Optional.empty();
        }
        return Optional.of(new CapacityPair(partition.orElseThrow(), global.orElseThrow()));
    }

    private CompletionStage<MemberOutcome> submitMember(
            RunLedgerHandleV1 handle, long entryId, CanonicalBytes encodedEntry) {
        ImmutableRetainedStoragePayload payload = ImmutableRetainedStoragePayload.copyOf(encodedEntry.toByteArray());
        CompletionStage<ProviderMutationResultV1<AppendQuorumProofV1>> accepted;
        try {
            accepted = session.appendExplicitEntry(new RunLedgerAppendRequestV1(handle, entryId, payload));
        } catch (RuntimeException failure) {
            payload.release();
            throw failure;
        }
        if (accepted == null) {
            payload.release();
            throw new IllegalStateException("provider returned a null DATA append stage");
        }
        return accepted.handle((result, failure) -> {
            try {
                if (failure != null || result == null) {
                    return MemberOutcome.OUTCOME_UNKNOWN;
                }
                if (result.outcome() == ProviderMutationOutcomeV1.DEFINITIVELY_NOT_APPLIED) {
                    return MemberOutcome.DEFINITIVELY_NOT_APPLIED;
                }
                if (result.outcome() != ProviderMutationOutcomeV1.APPLIED_EXACT) {
                    return MemberOutcome.OUTCOME_UNKNOWN;
                }
                AppendQuorumProofV1 proof = result.exactProof().orElseThrow();
                if (!proof.handle().equals(handle)
                        || proof.entryId() != entryId
                        || proof.payloadBytes() != payload.readableBytes()
                        || !proof.payloadSha256().equals(payload.sha256())
                        || proof.acknowledgedBookies()
                                < session.capabilitySnapshot().ackQuorumSize()) {
                    return MemberOutcome.OUTCOME_UNKNOWN;
                }
                return MemberOutcome.APPLIED_EXACT;
            } finally {
                payload.release();
            }
        });
    }

    private static void validatePhysicalGroup(
            KafkaAppendAdmissionRequestV1 request,
            KafkaOffsetAssignedAppendV1 assigned,
            KafkaBookKeeperEntryReservationV1 reservation,
            KafkaNbke2AssignedAppendGroupV1 physicalGroup,
            List<CanonicalBytes> encodedEntries) {
        if (physicalGroup.firstDataEntryId() != reservation.firstEntryId()
                || physicalGroup.dataFrames().size() != request.memberCount()
                || encodedEntries.size() != request.memberCount()
                || physicalGroup.dataFrames().get(0).baseOffset() != assigned.startOffset()
                || physicalGroup
                                .dataFrames()
                                .get(physicalGroup.dataFrames().size() - 1)
                                .endOffsetExclusive()
                        != assigned.endOffsetExclusive()) {
            throw new IllegalArgumentException("physical append group differs from its admission/assignment");
        }
        long encodedBytes = 0;
        for (CanonicalBytes encodedEntry : encodedEntries) {
            encodedBytes = Math.addExact(encodedBytes, encodedEntry.length());
        }
        if (encodedBytes != request.encodedDataBytes()) {
            throw new IllegalArgumentException("encoded DATA bytes differ from the pre-offset reservation");
        }
    }

    private static MemberOutcome aggregate(List<CompletionStage<MemberOutcome>> stages) {
        MemberOutcome aggregate = MemberOutcome.APPLIED_EXACT;
        for (CompletionStage<MemberOutcome> stage : stages) {
            MemberOutcome current = stage.toCompletableFuture().join();
            if (current == MemberOutcome.OUTCOME_UNKNOWN) {
                return current;
            }
            if (current == MemberOutcome.DEFINITIVELY_NOT_APPLIED) {
                aggregate = current;
            }
        }
        return aggregate;
    }

    private synchronized void finishSlot(Slot slot, MemberOutcome outcome) {
        slot.state = switch (outcome) {
            case APPLIED_EXACT -> SlotState.DURABLE;
            case DEFINITIVELY_NOT_APPLIED -> SlotState.DEFINITIVELY_FAILED;
            case OUTCOME_UNKNOWN -> SlotState.OUTCOME_UNKNOWN;
        };
        if (slot.fencedByPredecessor) {
            slot.result.complete(KafkaOrderedAppendResultV1.assigned(
                    KafkaOrderedAppendOutcomeV1.FENCED_BY_PREDECESSOR, slot.startOffset, slot.endOffsetExclusive));
            slot.capacity.close();
            return;
        }
        drainOrderedSlots();
    }

    private void drainOrderedSlots() {
        while (!orderedSlots.isEmpty()) {
            Slot head = orderedSlots.peekFirst();
            if (head.state == SlotState.PENDING) {
                return;
            }
            if (head.state == SlotState.DURABLE) {
                try {
                    commitObserver.onOrderedDurable(head.startOffset, head.endOffsetExclusive);
                } catch (RuntimeException failure) {
                    head.state = SlotState.OUTCOME_UNKNOWN;
                    failHeadAndFenceSuccessors(head);
                    return;
                }
                orderedSlots.removeFirst();
                committedEndOffset = head.endOffsetExclusive;
                head.result.complete(KafkaOrderedAppendResultV1.assigned(
                        KafkaOrderedAppendOutcomeV1.COMMITTED_ORDERED, head.startOffset, head.endOffsetExclusive));
                head.capacity.close();
                continue;
            }
            failHeadAndFenceSuccessors(head);
            return;
        }
    }

    private void failHeadAndFenceSuccessors(Slot head) {
        orderedSlots.removeFirst();
        KafkaOrderedAppendOutcomeV1 outcome = head.state == SlotState.DEFINITIVELY_FAILED
                ? KafkaOrderedAppendOutcomeV1.DEFINITIVELY_FAILED
                : KafkaOrderedAppendOutcomeV1.OUTCOME_UNKNOWN;
        head.result.complete(KafkaOrderedAppendResultV1.assigned(outcome, head.startOffset, head.endOffsetExclusive));
        head.capacity.close();
        state = PipelineState.FENCED;
        while (!orderedSlots.isEmpty()) {
            Slot successor = orderedSlots.removeFirst();
            fenceSuccessor(successor);
        }
    }

    private void fencePending() {
        state = PipelineState.FENCED;
        while (!orderedSlots.isEmpty()) {
            Slot slot = orderedSlots.removeFirst();
            fenceSuccessor(slot);
        }
    }

    private static void fenceSuccessor(Slot slot) {
        slot.fencedByPredecessor = true;
        if (slot.state != SlotState.PENDING) {
            slot.result.complete(KafkaOrderedAppendResultV1.assigned(
                    KafkaOrderedAppendOutcomeV1.FENCED_BY_PREDECESSOR, slot.startOffset, slot.endOffsetExclusive));
            slot.capacity.close();
        }
    }

    private static final class CapacityPair implements AutoCloseable {
        private final KafkaAppendCapacityControllerV1.Lease partition;
        private final KafkaAppendCapacityControllerV1.Lease global;

        private CapacityPair(
                KafkaAppendCapacityControllerV1.Lease partition, KafkaAppendCapacityControllerV1.Lease global) {
            this.partition = partition;
            this.global = global;
        }

        @Override
        public void close() {
            partition.close();
            global.close();
        }
    }

    private static final class Slot {
        private final long startOffset;
        private final long endOffsetExclusive;
        private final CapacityPair capacity;
        private final CompletableFuture<KafkaOrderedAppendResultV1> result = new CompletableFuture<>();
        private SlotState state = SlotState.PENDING;
        private boolean fencedByPredecessor;

        private Slot(long startOffset, long endOffsetExclusive, CapacityPair capacity) {
            this.startOffset = startOffset;
            this.endOffsetExclusive = endOffsetExclusive;
            this.capacity = capacity;
        }
    }
}
