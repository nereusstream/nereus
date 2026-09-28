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
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2AppendGroupDescriptorV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2DataV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1;
import com.nereusstream.kafka.bookkeeper.run.KafkaBookKeeperEntryReservationV1;
import com.nereusstream.kafka.bookkeeper.run.KafkaBookKeeperRunLifecycleV1;
import com.nereusstream.kafka.bookkeeper.run.KafkaBookKeeperRunSnapshotV1;
import com.nereusstream.kafka.bookkeeper.run.KafkaBookKeeperRunStateV1;
import com.nereusstream.storage.api.bookkeeper.AppendQuorumProofV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCellSession;
import com.nereusstream.storage.api.bookkeeper.ProviderMutationOutcomeV1;
import com.nereusstream.storage.api.bookkeeper.ProviderMutationResultV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerAppendRequestV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.bookkeeper.ImmutableRetainedStoragePayload;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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
        return submit(request, offsetAssignment, KafkaAppendProtocolHooksV1.none());
    }

    public CompletionStage<KafkaOrderedAppendResultV1> submit(
            KafkaAppendAdmissionRequestV1 request,
            KafkaOffsetAssignmentV1 offsetAssignment,
            KafkaAppendProtocolHooksV1 protocolHooks) {
        return submit(request, offsetAssignment, protocolHooks, null);
    }

    /** Reserve the same pipeline permits before the native Kafka log assigns offsets. */
    public Optional<AdmissionLease> reserveAdmission(KafkaAppendAdmissionRequestV1 request) {
        Objects.requireNonNull(request, "request");
        synchronized (this) {
            if (state != PipelineState.ACTIVE) {
                return Optional.empty();
            }
            return reserveCapacity(request).map(capacity -> new AdmissionLease(request, capacity));
        }
    }

    public CompletionStage<KafkaOrderedAppendResultV1> submit(
            KafkaAppendAdmissionRequestV1 request,
            KafkaOffsetAssignmentV1 offsetAssignment,
            KafkaAppendProtocolHooksV1 protocolHooks,
            AdmissionLease admission) {
        if (admission != null && admission.owner != this) {
            throw new IllegalArgumentException("native admission belongs to another pipeline");
        }
        List<Notification> notifications = new ArrayList<>();
        try {
            return submitAdmitted(request, offsetAssignment, protocolHooks, admission, notifications);
        } finally {
            if (admission != null) {
                admission.close();
            }
            notifyWaiters(notifications);
        }
    }

    private CompletionStage<KafkaOrderedAppendResultV1> submitAdmitted(
            KafkaAppendAdmissionRequestV1 request,
            KafkaOffsetAssignmentV1 offsetAssignment,
            KafkaAppendProtocolHooksV1 protocolHooks,
            AdmissionLease admission,
            List<Notification> notifications) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(offsetAssignment, "offsetAssignment");
        Objects.requireNonNull(protocolHooks, "protocolHooks");
        synchronized (this) {
            var duplicate = protocolHooks.findDuplicateBeforeOffsetAssignment();
            if (duplicate.isPresent()) {
                return duplicate.orElseThrow().thenApply(value -> value);
            }
        }
        Optional<CapacityPair> capacity =
                admission == null ? reserveCapacity(request) : Optional.of(admission.consume(this, request));
        if (capacity.isEmpty()) {
            return CompletableFuture.completedFuture(
                    KafkaOrderedAppendResultV1.beforeAssignment(KafkaOrderedAppendOutcomeV1.CAPACITY_REJECTED));
        }
        KafkaOffsetAssignedAppendV1 assigned;
        KafkaBookKeeperEntryReservationV1 reservation;
        KafkaBookKeeperRunSnapshotV1 runSnapshot;
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
            var duplicate = protocolHooks.findDuplicateBeforeOffsetAssignment();
            if (duplicate.isPresent()) {
                capacity.orElseThrow().close();
                return duplicate.orElseThrow().thenApply(value -> value);
            }
            try {
                protocolHooks.validateBeforeOffsetAssignment();
            } catch (RuntimeException failure) {
                capacity.orElseThrow().close();
                return CompletableFuture.completedFuture(KafkaOrderedAppendResultV1.beforeAssignment(
                        KafkaOrderedAppendOutcomeV1.PROTOCOL_VALIDATION_FAILED));
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
                fencePending(notifications);
                return CompletableFuture.completedFuture(KafkaOrderedAppendResultV1.assigned(
                        KafkaOrderedAppendOutcomeV1.INVALID_ASSIGNMENT,
                        assigned.startOffset(),
                        assigned.endOffsetExclusive()));
            }
            speculativeEndOffset = assigned.endOffsetExclusive();
            try {
                protocolHooks.prepareAfterOffsetAssignment(assigned);
            } catch (RuntimeException failure) {
                capacity.orElseThrow().close();
                fencePending(notifications);
                return CompletableFuture.completedFuture(KafkaOrderedAppendResultV1.assigned(
                        KafkaOrderedAppendOutcomeV1.PROTOCOL_PREPARATION_FAILED,
                        assigned.startOffset(),
                        assigned.endOffsetExclusive()));
            }
            try {
                runSnapshot = lifecycle.snapshot();
                reservation = lifecycle.reserveDataGroup(request.memberCount());
            } catch (RuntimeException failure) {
                capacity.orElseThrow().close();
                fencePending(notifications);
                return CompletableFuture.completedFuture(KafkaOrderedAppendResultV1.assigned(
                        KafkaOrderedAppendOutcomeV1.INVALID_ASSIGNMENT,
                        assigned.startOffset(),
                        assigned.endOffsetExclusive()));
            }
            try {
                physicalGroup = Objects.requireNonNull(
                        assigned.physicalGroupFactory().apply(reservation.firstEntryId()), "physical append group");
                encodedEntries = physicalGroup.encode(
                        runSnapshot.handle().ledgerIdentity().ledgerId());
                validatePhysicalGroup(
                        request, assigned, reservation, runSnapshot.runBinding(), physicalGroup, encodedEntries);
            } catch (RuntimeException failure) {
                lifecycle.completeDataGroup(reservation);
                capacity.orElseThrow().close();
                fencePending(notifications);
                return CompletableFuture.completedFuture(KafkaOrderedAppendResultV1.assigned(
                        KafkaOrderedAppendOutcomeV1.INVALID_ASSIGNMENT,
                        assigned.startOffset(),
                        assigned.endOffsetExclusive()));
            }
            var descriptor = physicalGroup
                    .dataFrames()
                    .get(physicalGroup.dataFrames().size() - 1)
                    .terminalDescriptor()
                    .orElseThrow();
            List<KafkaOrderedDurableDataMemberV1> durableMembers = new ArrayList<>(request.memberCount());
            for (int index = 0; index < physicalGroup.dataFrames().size(); index++) {
                Nbke2DataV1 frame = physicalGroup.dataFrames().get(index);
                durableMembers.add(new KafkaOrderedDurableDataMemberV1(
                        frame.baseOffset(),
                        frame.endOffsetExclusive(),
                        reservation.entryId(index),
                        index,
                        frame.rawAssignedRecordBatch().length()));
            }
            KafkaOrderedDurableCommitV1 durableCommit = new KafkaOrderedDurableCommitV1(
                    assigned.startOffset(),
                    assigned.endOffsetExclusive(),
                    runSnapshot.runBinding(),
                    runSnapshot.handle(),
                    descriptor.firstDataEntryId(),
                    descriptor.lastDataEntryId(),
                    request.memberCount(),
                    request.encodedDataBytes(),
                    descriptor.aggregateAssignedPayloadSha256(),
                    physicalGroup.dataFrames().get(0).appendGroupId(),
                    physicalGroup.dataFrames().get(0).storageAttemptId(),
                    durableMembers);
            slot = new Slot(durableCommit, capacity.orElseThrow());
            orderedSlots.addLast(slot);
            try {
                protocolHooks.registerAssignedResult(slot.startOffset, slot.endOffsetExclusive, slot.result);
                for (int index = 0; index < encodedEntries.size(); index++) {
                    memberStages.add(
                            submitMember(runSnapshot.handle(), reservation.entryId(index), encodedEntries.get(index)));
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
        return slot.result.thenApply(value -> value);
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

    public void fence() {
        List<Notification> notifications = new ArrayList<>();
        synchronized (this) {
            fencePending(notifications);
        }
        notifyWaiters(notifications);
    }

    /** One-use operation-owned capacity; a native duplicate or validation failure releases unused permits. */
    public final class AdmissionLease implements AutoCloseable {
        private final KafkaBookKeeperOrderedPipelineV1 owner = KafkaBookKeeperOrderedPipelineV1.this;
        private final KafkaAppendAdmissionRequestV1 request;
        private CapacityPair capacity;

        private AdmissionLease(KafkaAppendAdmissionRequestV1 request, CapacityPair capacity) {
            this.request = request;
            this.capacity = capacity;
        }

        private synchronized CapacityPair consume(
                KafkaBookKeeperOrderedPipelineV1 submitting, KafkaAppendAdmissionRequestV1 submitted) {
            if (owner != submitting
                    || (submitted.memberCount() > request.memberCount()
                            || submitted.encodedDataBytes() > request.encodedDataBytes())
                    || capacity == null) {
                throw new IllegalArgumentException("native admission does not match the pipeline append");
            }
            var value = capacity;
            capacity = null;
            return value;
        }

        @Override
        public synchronized void close() {
            if (capacity != null) {
                capacity.close();
                capacity = null;
            }
        }
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
            Nbke2RunBindingV1 expectedRunBinding,
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
        long nextOffset = assigned.startOffset();
        MessageDigest aggregate = sha256();
        Nbke2DataV1 first = physicalGroup.dataFrames().get(0);
        for (int index = 0; index < physicalGroup.dataFrames().size(); index++) {
            Nbke2DataV1 frame = physicalGroup.dataFrames().get(index);
            if (!frame.runBinding().equals(expectedRunBinding)
                    || frame.memberOrdinal() != index
                    || frame.memberCount() != request.memberCount()
                    || !frame.appendGroupId().equals(first.appendGroupId())
                    || !frame.storageAttemptId().equals(first.storageAttemptId())
                    || frame.baseOffset() != nextOffset) {
                throw new IllegalArgumentException("physical DATA members change run/group identity or offset order");
            }
            aggregate.update(frame.rawAssignedRecordBatch().toByteArray());
            nextOffset = frame.endOffsetExclusive();
        }
        Nbke2AppendGroupDescriptorV1 descriptor = physicalGroup
                .dataFrames()
                .get(physicalGroup.dataFrames().size() - 1)
                .terminalDescriptor()
                .orElseThrow();
        if (nextOffset != assigned.endOffsetExclusive()
                || descriptor.groupStartOffset() != assigned.startOffset()
                || descriptor.groupEndOffsetExclusive() != assigned.endOffsetExclusive()
                || descriptor.firstDataEntryId() != reservation.firstEntryId()
                || descriptor.lastDataEntryId() != reservation.lastEntryId()
                || !descriptor
                        .aggregateAssignedPayloadSha256()
                        .equals(com.nereusstream.domain.bytes.Sha256Digest.copyOf(aggregate.digest()))) {
            throw new IllegalArgumentException("terminal descriptor differs from the exact assigned DATA group");
        }
        long encodedBytes = 0;
        for (CanonicalBytes encodedEntry : encodedEntries) {
            encodedBytes = Math.addExact(encodedBytes, encodedEntry.length());
        }
        if (encodedBytes != request.encodedDataBytes()) {
            throw new IllegalArgumentException("encoded DATA bytes differ from the pre-offset reservation");
        }
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("JDK has no SHA-256 provider", failure);
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

    private void finishSlot(Slot slot, MemberOutcome outcome) {
        List<Notification> notifications = new ArrayList<>();
        synchronized (this) {
            slot.state = switch (outcome) {
                case APPLIED_EXACT -> SlotState.DURABLE;
                case DEFINITIVELY_NOT_APPLIED -> SlotState.DEFINITIVELY_FAILED;
                case OUTCOME_UNKNOWN -> SlotState.OUTCOME_UNKNOWN;
            };
            if (slot.fencedByPredecessor) {
                completedSlot(slot, KafkaOrderedAppendOutcomeV1.FENCED_BY_PREDECESSOR, notifications);
            } else {
                drainOrderedSlots(notifications);
            }
        }
        notifyWaiters(notifications);
    }

    private void drainOrderedSlots(List<Notification> notifications) {
        while (!orderedSlots.isEmpty()) {
            Slot head = orderedSlots.peekFirst();
            if (head.state == SlotState.PENDING) {
                return;
            }
            if (head.state == SlotState.DURABLE) {
                try {
                    commitObserver.onOrderedDurable(head.durableCommit);
                } catch (RuntimeException failure) {
                    head.state = SlotState.OUTCOME_UNKNOWN;
                    failHeadAndFenceSuccessors(head, notifications);
                    return;
                }
                orderedSlots.removeFirst();
                committedEndOffset = head.endOffsetExclusive;
                completedSlot(head, KafkaOrderedAppendOutcomeV1.COMMITTED_ORDERED, notifications);
                continue;
            }
            failHeadAndFenceSuccessors(head, notifications);
            return;
        }
    }

    private void failHeadAndFenceSuccessors(Slot head, List<Notification> notifications) {
        orderedSlots.removeFirst();
        KafkaOrderedAppendOutcomeV1 outcome = head.state == SlotState.DEFINITIVELY_FAILED
                ? KafkaOrderedAppendOutcomeV1.DEFINITIVELY_FAILED
                : KafkaOrderedAppendOutcomeV1.OUTCOME_UNKNOWN;
        completedSlot(head, outcome, notifications);
        fencePending(notifications);
    }

    private void fencePending(List<Notification> notifications) {
        state = PipelineState.FENCED;
        while (!orderedSlots.isEmpty()) {
            fenceSuccessor(orderedSlots.removeFirst(), notifications);
        }
    }

    private static void fenceSuccessor(Slot slot, List<Notification> notifications) {
        slot.fencedByPredecessor = true;
        if (slot.state != SlotState.PENDING) {
            completedSlot(slot, KafkaOrderedAppendOutcomeV1.FENCED_BY_PREDECESSOR, notifications);
        }
    }

    private static void completedSlot(
            Slot slot, KafkaOrderedAppendOutcomeV1 outcome, List<Notification> notifications) {
        // The actual provider terminal owns release; pending fenced successors keep their I/O capacity.
        slot.capacity.close();
        notifications.add(new Notification(
                slot.result, KafkaOrderedAppendResultV1.assigned(outcome, slot.startOffset, slot.endOffsetExclusive)));
    }

    private static void notifyWaiters(List<Notification> notifications) {
        for (Notification notification : notifications) {
            notification.future().complete(notification.result());
        }
    }

    private record Notification(
            CompletableFuture<KafkaOrderedAppendResultV1> future, KafkaOrderedAppendResultV1 result) {}

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
        private final KafkaOrderedDurableCommitV1 durableCommit;
        private final CapacityPair capacity;
        private final CompletableFuture<KafkaOrderedAppendResultV1> result = new CompletableFuture<>();
        private SlotState state = SlotState.PENDING;
        private boolean fencedByPredecessor;

        private Slot(KafkaOrderedDurableCommitV1 durableCommit, CapacityPair capacity) {
            this.durableCommit = durableCommit;
            this.startOffset = durableCommit.startOffset();
            this.endOffsetExclusive = durableCommit.endOffsetExclusive();
            this.capacity = capacity;
        }
    }
}
