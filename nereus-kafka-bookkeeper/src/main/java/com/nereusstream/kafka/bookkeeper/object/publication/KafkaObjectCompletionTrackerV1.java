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

package com.nereusstream.kafka.bookkeeper.object.publication;

import com.nereusstream.kafka.bookkeeper.commit.KafkaSpeculativeCommitV1;
import com.nereusstream.kafka.bookkeeper.object.nwkcp1.KafkaObjectRecoveredTailV1;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Bounded owner-local combined tracker/locator reservation ring with full-ticket ABA fencing. */
public final class KafkaObjectCompletionTrackerV1 {
    public enum LifecycleStage {
        POSITION_ASSIGNED,
        SEQUENCED,
        PROVIDER_DISPATCHED,
        PROVIDER_RESOLVED,
        BINDING_REJECTED,
        ROOT_PUBLISHED
    }

    public record Reservation(long ownerEpoch, long reservationId, int slot, int locatorBytes) {
        public Reservation {
            if (ownerEpoch <= 0 || reservationId <= 0 || slot < 0 || locatorBytes <= 0) {
                throw new IllegalArgumentException("Kafka Object reservation is outside its domain");
            }
        }
    }

    public record AssignedTicket(long ownerEpoch, long ticket, int slot) {
        public AssignedTicket {
            if (ownerEpoch <= 0 || ticket < 0 || slot < 0) {
                throw new IllegalArgumentException("Kafka Object completion ticket is outside its domain");
            }
        }
    }

    /** Unforgeable owner-local claim that freezes rollback while one no-effect validated plan enters sequencing. */
    public static final class SequenceClaim {
        private final KafkaObjectCompletionTrackerV1 owner;
        private final AssignedTicket assigned;

        private SequenceClaim(KafkaObjectCompletionTrackerV1 owner, AssignedTicket assigned) {
            this.owner = owner;
            this.assigned = assigned;
        }
    }

    public record ReadyCompletion(
            AssignedTicket assigned,
            KafkaSpeculativeCommitV1 commitSet,
            KafkaObjectExtentLocatorV1 locator,
            KafkaObjectNativeStateV1 nativeState) {
        public ReadyCompletion {
            Objects.requireNonNull(assigned, "assigned");
            Objects.requireNonNull(commitSet, "commitSet");
            Objects.requireNonNull(locator, "locator");
            Objects.requireNonNull(nativeState, "nativeState");
        }
    }

    private final Slot[] slots;
    private final long maxLocatorBytes;
    private long ownerEpoch;
    private long nextReservationId = 1;
    private long nextTicket;
    private boolean ticketExhausted;
    private long pendingLocatorBytes;
    private long retainedLocatorBytes;
    private final Map<KafkaObjectExtentLocatorV1, Integer> retainedLocators = new LinkedHashMap<>();
    private final Map<RollbackPlan, Boolean> issuedRollbackPlans = new IdentityHashMap<>();

    public KafkaObjectCompletionTrackerV1(int capacity, long maxLocatorBytes, long ownerEpoch) {
        this(capacity, maxLocatorBytes, ownerEpoch, 0);
    }

    /** Restores the next fresh owner-local ticket after bounded takeover reconstruction. */
    public KafkaObjectCompletionTrackerV1(int capacity, long maxLocatorBytes, long ownerEpoch, long initialTicket) {
        if (capacity <= 0 || maxLocatorBytes <= 0 || ownerEpoch <= 0 || initialTicket < 0) {
            throw new IllegalArgumentException("Kafka Object tracker capacity/fence is outside its domain");
        }
        this.slots = new Slot[capacity];
        this.maxLocatorBytes = maxLocatorBytes;
        this.ownerEpoch = ownerEpoch;
        this.nextTicket = initialTicket;
    }

    /** Reconstructs retained Root-selected locator charges only from authenticated NWG1 recovery output. */
    public KafkaObjectCompletionTrackerV1(
            int capacity,
            long maxLocatorBytes,
            long ownerEpoch,
            long initialTicket,
            KafkaObjectRecoveredTailV1 recoveredTail) {
        this(capacity, maxLocatorBytes, ownerEpoch, initialTicket);
        Objects.requireNonNull(recoveredTail, "recoveredTail");
        for (KafkaObjectExtentLocatorV1 locator : recoveredTail.activeTail().locators()) {
            if (!locator.extent().walRunRootSha().equals(recoveredTail.walRunRootSha())) {
                throw new IllegalArgumentException("recovered retained locator differs from its authenticated Root");
            }
            int charge = KafkaObjectStateCodecV1.locator(locator).length();
            if (retainedLocators.putIfAbsent(locator, charge) != null) {
                throw new IllegalArgumentException("authenticated recovery repeats a retained locator");
            }
            retainedLocatorBytes = Math.addExact(retainedLocatorBytes, charge);
        }
        if (retainedLocatorBytes > maxLocatorBytes) {
            throw new IllegalArgumentException("authenticated recovered active tail exceeds locator capacity");
        }
    }

    /** Atomically reserves one tracker slot and the exact fixed locator wire charge before offset assignment. */
    public synchronized Reservation reserveBeforePosition() {
        int locatorBytes = KafkaObjectStateCodecV1.exactLocatorBytes();
        if (Math.addExact(Math.addExact(pendingLocatorBytes, retainedLocatorBytes), locatorBytes) > maxLocatorBytes) {
            throw new IllegalStateException("combined tracker/locator pre-position capacity is exhausted");
        }
        int slot = -1;
        for (int index = 0; index < slots.length; index++) {
            if (slots[index] == null) {
                slot = index;
                break;
            }
        }
        if (slot < 0 || nextReservationId == Long.MAX_VALUE) {
            throw new IllegalStateException("combined tracker/locator reservation ring is exhausted");
        }
        Reservation reservation = new Reservation(ownerEpoch, nextReservationId++, slot, locatorBytes);
        slots[slot] = Slot.reserved(reservation);
        pendingLocatorBytes = Math.addExact(pendingLocatorBytes, locatorBytes);
        return reservation;
    }

    public synchronized void cancelBeforePosition(Reservation reservation) {
        Slot slot = requireReservation(reservation);
        if (slot.commitSet != null) {
            throw new IllegalStateException("assigned Kafka position cannot use pre-position cancellation");
        }
        releasePendingSlot(reservation.slot());
    }

    /** Assigns exactly one full 64-bit ticket after one complete commit set has exact Kafka offsets. */
    public synchronized AssignedTicket assignPosition(
            Reservation reservation, KafkaSpeculativeCommitV1 completeCommitSet) {
        Slot slot = requireReservation(reservation);
        Objects.requireNonNull(completeCommitSet, "completeCommitSet");
        if (slot.commitSet != null || ticketExhausted) {
            throw new IllegalStateException(
                    "Kafka Object reservation is already assigned or ticket space is exhausted");
        }
        long ticket = nextTicket;
        if (ticket == Long.MAX_VALUE) {
            ticketExhausted = true;
        } else {
            nextTicket = Math.addExact(nextTicket, 1);
        }
        AssignedTicket assigned = new AssignedTicket(ownerEpoch, ticket, reservation.slot());
        slot.assigned = assigned;
        slot.commitSet = completeCommitSet;
        slot.stage = LifecycleStage.POSITION_ASSIGNED;
        return assigned;
    }

    public synchronized void sequenceStarted(AssignedTicket assigned) {
        completeSequenceAfterEffect(claimSequence(assigned));
    }

    /** Freezes one exact PRE_SEQUENCE ticket only after plan validation has proved there is no physical effect. */
    public synchronized SequenceClaim claimSequence(AssignedTicket assigned) {
        Slot slot = requireAssigned(assigned);
        if (slot.stage != LifecycleStage.POSITION_ASSIGNED || slot.rollbackPlan != null || slot.sequenceClaim != null) {
            throw new IllegalStateException(
                    "Kafka Object sequence cut is out of order, rollback-fenced, or already claimed");
        }
        SequenceClaim claim = new SequenceClaim(this, assigned);
        slot.sequenceClaim = claim;
        return claim;
    }

    /** Atomically freezes every member of one already validated shared plan before its sole lane-sequence effect. */
    public synchronized List<SequenceClaim> claimSequences(List<AssignedTicket> assignedTickets) {
        Objects.requireNonNull(assignedTickets, "assignedTickets");
        if (assignedTickets.isEmpty()) {
            throw new IllegalArgumentException("shared sequence claim is empty");
        }
        ArrayList<Slot> selected = new ArrayList<>(assignedTickets.size());
        java.util.HashSet<AssignedTicket> unique = new java.util.HashSet<>();
        for (AssignedTicket assigned : assignedTickets) {
            if (!unique.add(assigned)) {
                throw new IllegalArgumentException("shared sequence claim repeats a ticket");
            }
            Slot slot = requireAssigned(assigned);
            if (slot.stage != LifecycleStage.POSITION_ASSIGNED
                    || slot.rollbackPlan != null
                    || slot.sequenceClaim != null) {
                throw new IllegalStateException("Kafka Object ticket cannot claim the shared sequence cut");
            }
            selected.add(slot);
        }
        ArrayList<SequenceClaim> claims = new ArrayList<>(selected.size());
        for (int index = 0; index < selected.size(); index++) {
            SequenceClaim claim = new SequenceClaim(this, assignedTickets.get(index));
            selected.get(index).sequenceClaim = claim;
            claims.add(claim);
        }
        return List.copyOf(claims);
    }

    /** Commits SEQUENCED iff common admission reports an exact physical sequence effect. */
    public synchronized void completeSequenceAfterEffect(SequenceClaim claim) {
        Slot slot = requireSequenceClaim(claim);
        slot.sequenceClaim = null;
        slot.stage = LifecycleStage.SEQUENCED;
    }

    public synchronized void completeSequencesAfterEffect(List<SequenceClaim> claims) {
        List<Slot> selected = requireSequenceClaims(claims);
        for (Slot slot : selected) {
            slot.sequenceClaim = null;
            slot.stage = LifecycleStage.SEQUENCED;
        }
    }

    /** Releases only a claim whose common admission failure explicitly proved there was no sequence effect. */
    public synchronized void abortSequenceBeforeEffect(SequenceClaim claim) {
        Slot slot = requireSequenceClaim(claim);
        slot.sequenceClaim = null;
    }

    public synchronized void abortSequencesBeforeEffect(List<SequenceClaim> claims) {
        List<Slot> selected = requireSequenceClaims(claims);
        for (Slot slot : selected) {
            slot.sequenceClaim = null;
        }
    }

    public synchronized void providerDispatched(AssignedTicket assigned) {
        transition(assigned, LifecycleStage.SEQUENCED, LifecycleStage.PROVIDER_DISPATCHED);
    }

    public synchronized void providerDispatched(List<AssignedTicket> assignedTickets) {
        Objects.requireNonNull(assignedTickets, "assignedTickets");
        if (assignedTickets.isEmpty()) {
            throw new IllegalArgumentException("shared Provider dispatch is empty");
        }
        ArrayList<Slot> selected = new ArrayList<>(assignedTickets.size());
        java.util.HashSet<AssignedTicket> unique = new java.util.HashSet<>();
        for (AssignedTicket assigned : assignedTickets) {
            if (!unique.add(assigned)) {
                throw new IllegalArgumentException("shared Provider dispatch repeats a ticket");
            }
            Slot slot = requireAssigned(assigned);
            if (slot.stage != LifecycleStage.SEQUENCED || slot.rollbackPlan != null) {
                throw new IllegalStateException("Kafka Object shared Provider dispatch is out of order");
            }
            selected.add(slot);
        }
        selected.forEach(slot -> slot.stage = LifecycleStage.PROVIDER_DISPATCHED);
    }

    public synchronized void providerResolved(
            AssignedTicket assigned,
            KafkaObjectPhysicalFrontiersV1 physicalFrontiers,
            KafkaVerifiedNwg1CommitV1 verifiedCommit,
            KafkaObjectNativeStateV1 nativeState) {
        Objects.requireNonNull(physicalFrontiers, "physicalFrontiers");
        Slot slot = validateProviderResolvedCandidate(assigned, verifiedCommit, nativeState);
        KafkaObjectExtentLocatorV1 locator = verifiedCommit.locator();
        if (physicalFrontiers.resolvedThrough(locator.extent().laneId())
                < locator.extent().laneSequence()) {
            throw new IllegalArgumentException("provider-resolved locator is ahead of the physical frontier");
        }
        slot.locator = locator;
        slot.nativeState = nativeState;
        slot.stage = LifecycleStage.PROVIDER_RESOLVED;
    }

    /** No-effect validation used before a shared Object makes its sole physical resolution terminal. */
    public synchronized void requireProviderResolvedCandidate(
            AssignedTicket assigned, KafkaVerifiedNwg1CommitV1 verifiedCommit, KafkaObjectNativeStateV1 nativeState) {
        validateProviderResolvedCandidate(assigned, verifiedCommit, nativeState);
    }

    private Slot validateProviderResolvedCandidate(
            AssignedTicket assigned, KafkaVerifiedNwg1CommitV1 verifiedCommit, KafkaObjectNativeStateV1 nativeState) {
        Slot slot = requireAssigned(assigned);
        Objects.requireNonNull(verifiedCommit, "verifiedCommit");
        Objects.requireNonNull(nativeState, "nativeState");
        KafkaObjectExtentLocatorV1 locator = verifiedCommit.locator();
        if (slot.stage != LifecycleStage.PROVIDER_DISPATCHED
                || slot.locator != null
                || KafkaObjectStateCodecV1.locator(locator).length() != slot.reservation.locatorBytes()
                || locator.startOffset() != slot.commitSet.startOffset()
                || locator.endOffsetExclusive() != slot.commitSet.endOffsetExclusive()
                || !locator.binding()
                        .bindingId()
                        .equals(slot.commitSet.expectedFence().bindingId())
                || !locator.binding()
                        .topicId()
                        .equals(slot.commitSet
                                .expectedFence()
                                .topicIncarnation()
                                .topicId())
                || locator.binding().partitionId()
                        != slot.commitSet.expectedFence().partitionId()
                || !locator.binding()
                        .storageEpochId()
                        .equals(slot.commitSet.expectedFence().storageEpochId())) {
            throw new IllegalArgumentException("provider-resolved locator differs from its complete Kafka commit set");
        }
        return slot;
    }

    public synchronized void requireBindingRejectionCandidate(AssignedTicket assigned) {
        Slot slot = requireAssigned(assigned);
        if (slot.stage != LifecycleStage.PROVIDER_DISPATCHED) {
            throw new IllegalStateException("binding rejection candidate is not Provider-dispatched");
        }
    }

    /**
     * Isolates one member whose shared physical Object resolved exactly but whose selected binding/unit bytes failed
     * typed verification. The ticket remains charged and fenced for owning-binding recovery; siblings may advance.
     */
    public synchronized void bindingRejectedAfterPhysicalResolution(
            AssignedTicket assigned,
            KafkaObjectPhysicalFrontiersV1 physicalFrontiers,
            KafkaObjectExtentIdentityV1 extent) {
        Slot slot = requireAssigned(assigned);
        Objects.requireNonNull(physicalFrontiers, "physicalFrontiers");
        Objects.requireNonNull(extent, "extent");
        if (slot.stage != LifecycleStage.PROVIDER_DISPATCHED
                || physicalFrontiers.resolvedThrough(extent.laneId()) < extent.laneSequence()) {
            throw new IllegalStateException("binding rejection requires an exact provider-resolved physical extent");
        }
        slot.stage = LifecycleStage.BINDING_REJECTED;
    }

    public synchronized Optional<ReadyCompletion> readyAt(long bindingDurableFrontier) {
        if (bindingDurableFrontier < 0) {
            throw new IllegalArgumentException("binding durable frontier is negative");
        }
        ReadyCompletion found = null;
        for (Slot slot : slots) {
            if (slot != null
                    && slot.stage == LifecycleStage.PROVIDER_RESOLVED
                    && slot.locator != null
                    && slot.commitSet.startOffset() == bindingDurableFrontier) {
                if (found != null) {
                    throw new IllegalStateException("multiple completion slots claim the same Kafka predecessor");
                }
                found = new ReadyCompletion(slot.assigned, slot.commitSet, slot.locator, slot.nativeState);
            }
        }
        return Optional.ofNullable(found);
    }

    /** Records the exact root selection while retaining the ticket until the protocol ACK succeeds. */
    public synchronized void rootPublished(AssignedTicket assigned, KafkaSpeculativeCommitV1 expectedCommitSet) {
        Slot slot = requireAssigned(assigned);
        if (slot.stage != LifecycleStage.PROVIDER_RESOLVED
                || !slot.commitSet.equals(Objects.requireNonNull(expectedCommitSet, "expectedCommitSet"))) {
            throw new IllegalStateException("root-selected Kafka commit set differs from its completion ticket");
        }
        slot.stage = LifecycleStage.ROOT_PUBLISHED;
    }

    /** ACK success releases the tracker slot but retains the locator charge until manifest/root retirement. */
    public synchronized void releaseAfterProtocolAck(
            AssignedTicket assigned, KafkaSpeculativeCommitV1 expectedCommitSet) {
        Slot slot = requireAssigned(assigned);
        if (slot.stage != LifecycleStage.ROOT_PUBLISHED
                || !slot.commitSet.equals(Objects.requireNonNull(expectedCommitSet, "expectedCommitSet"))) {
            throw new IllegalStateException("ACK does not bind the exact root-published Kafka commit set");
        }
        KafkaObjectExtentLocatorV1 locator = Objects.requireNonNull(slot.locator, "locator");
        if (retainedLocators.putIfAbsent(locator, slot.reservation.locatorBytes()) != null) {
            throw new IllegalStateException("root-published Kafka locator is already retained");
        }
        retainedLocatorBytes = Math.addExact(retainedLocatorBytes, slot.reservation.locatorBytes());
        releasePendingSlot(assigned.slot());
    }

    /** Derives an unforgeable exact whole suffix exclusively from live pre-sequence tracker slots. */
    public synchronized RollbackPlan prepareRollbackSuffix(long startOffset) {
        if (startOffset < 0) {
            throw new IllegalArgumentException("rollback start offset is negative");
        }
        List<Slot> assigned = new ArrayList<>();
        for (Slot slot : slots) {
            if (slot != null && slot.commitSet != null) {
                assigned.add(slot);
            }
        }
        assigned.sort(Comparator.comparingLong(slot -> slot.commitSet.startOffset()));
        int first = -1;
        for (int index = 0; index < assigned.size(); index++) {
            if (assigned.get(index).commitSet.startOffset() == startOffset) {
                first = index;
                break;
            }
        }
        if (first < 0) {
            throw new IllegalArgumentException("rollback offset is not a live assigned suffix boundary");
        }
        List<AssignedTicket> tickets = new ArrayList<>();
        List<KafkaSpeculativeCommitV1> commits = new ArrayList<>();
        long next = startOffset;
        for (int index = first; index < assigned.size(); index++) {
            Slot slot = assigned.get(index);
            if (slot.stage != LifecycleStage.POSITION_ASSIGNED
                    || slot.rollbackPlan != null
                    || slot.sequenceClaim != null
                    || slot.commitSet.startOffset() != next) {
                throw new IllegalStateException(
                        "Kafka suffix cannot roll back after sequence/provider/root effects or across a gap");
            }
            tickets.add(slot.assigned);
            commits.add(slot.commitSet);
            next = slot.commitSet.endOffsetExclusive();
        }
        RollbackPlan plan = new RollbackPlan(this, List.copyOf(tickets), List.copyOf(commits));
        for (int index = first; index < assigned.size(); index++) {
            assigned.get(index).rollbackPlan = plan;
        }
        issuedRollbackPlans.put(plan, Boolean.TRUE);
        return plan;
    }

    /** Releases only a still-live plan issued by this tracker, and only after the rollback root CAS. */
    public synchronized void completeRollbackAfterRootCas(RollbackPlan plan) {
        Objects.requireNonNull(plan, "plan");
        if (plan.owner != this || !issuedRollbackPlans.containsKey(plan)) {
            throw new IllegalStateException("rollback plan was not issued by this tracker or was already consumed");
        }
        for (int index = 0; index < plan.tickets.size(); index++) {
            AssignedTicket assigned = plan.tickets.get(index);
            Slot slot = requireAssigned(assigned);
            if (slot.stage != LifecycleStage.POSITION_ASSIGNED
                    || slot.rollbackPlan != plan
                    || !slot.commitSet.equals(plan.commits.get(index))) {
                throw new IllegalStateException("rollback plan no longer matches the exact pre-sequence suffix");
            }
        }
        issuedRollbackPlans.remove(plan);
        for (AssignedTicket assigned : plan.tickets) {
            releasePendingSlot(assigned.slot());
        }
    }

    /** Releases retained locator budget only from an exact root-selected retirement authority. */
    public synchronized void releaseRetainedAfterRootRetirement(KafkaObjectTailRetirementAuthorityV1 authority) {
        Objects.requireNonNull(authority, "authority");
        for (KafkaObjectExtentLocatorV1 locator : authority.retiredLocators()) {
            if (!retainedLocators.containsKey(locator)) {
                throw new IllegalStateException("retirement authority names a non-retained Kafka locator");
            }
        }
        for (KafkaObjectExtentLocatorV1 locator : authority.retiredLocators()) {
            Integer charge = retainedLocators.remove(locator);
            retainedLocatorBytes = Math.subtractExact(retainedLocatorBytes, charge);
        }
    }

    /**
     * Called only after the old owner is fenced. Only reservations that never received a Kafka position may be
     * discarded. Assigned positions require exact recovery or whole-suffix rollback; Root-selected locators remain
     * charged until a manifest/pin/root retirement authority releases them.
     */
    public synchronized void discardOnTakeover(long newOwnerEpoch) {
        if (newOwnerEpoch <= ownerEpoch) {
            throw new IllegalArgumentException("Kafka Object tracker takeover epoch does not advance");
        }
        for (Slot slot : slots) {
            if (slot != null && slot.commitSet != null) {
                throw new IllegalStateException(
                        "Kafka Object takeover cannot discard an assigned position; recover or roll back it exactly");
            }
        }
        Arrays.fill(slots, null);
        pendingLocatorBytes = 0;
        issuedRollbackPlans.clear();
        ownerEpoch = newOwnerEpoch;
        nextReservationId = 1;
        nextTicket = 0;
        ticketExhausted = false;
    }

    public synchronized long reservedLocatorBytes() {
        return Math.addExact(pendingLocatorBytes, retainedLocatorBytes);
    }

    public synchronized long retainedLocatorBytes() {
        return retainedLocatorBytes;
    }

    public synchronized int pendingUnits() {
        int count = 0;
        for (Slot slot : slots) {
            if (slot != null) {
                count++;
            }
        }
        return count;
    }

    private Slot requireReservation(Reservation reservation) {
        Objects.requireNonNull(reservation, "reservation");
        if (reservation.ownerEpoch() != ownerEpoch || reservation.slot() >= slots.length) {
            throw new IllegalStateException("Kafka Object reservation is stale");
        }
        Slot slot = slots[reservation.slot()];
        if (slot == null || !slot.reservation.equals(reservation)) {
            throw new IllegalStateException("Kafka Object reservation failed its full-value ABA fence");
        }
        return slot;
    }

    private Slot requireAssigned(AssignedTicket assigned) {
        Objects.requireNonNull(assigned, "assigned");
        if (assigned.ownerEpoch() != ownerEpoch || assigned.slot() >= slots.length) {
            throw new IllegalStateException("Kafka Object completion ticket is stale");
        }
        Slot slot = slots[assigned.slot()];
        if (slot == null || !assigned.equals(slot.assigned)) {
            throw new IllegalStateException("Kafka Object completion ticket failed its full-value ABA fence");
        }
        return slot;
    }

    private Slot requireSequenceClaim(SequenceClaim claim) {
        Objects.requireNonNull(claim, "claim");
        if (claim.owner != this) {
            throw new IllegalStateException("Kafka Object sequence claim belongs to another tracker");
        }
        Slot slot = requireAssigned(claim.assigned);
        if (slot.sequenceClaim != claim || slot.stage != LifecycleStage.POSITION_ASSIGNED) {
            throw new IllegalStateException("Kafka Object sequence claim is stale or already consumed");
        }
        return slot;
    }

    private List<Slot> requireSequenceClaims(List<SequenceClaim> claims) {
        Objects.requireNonNull(claims, "claims");
        if (claims.isEmpty()) {
            throw new IllegalArgumentException("shared sequence claims are empty");
        }
        ArrayList<Slot> selected = new ArrayList<>(claims.size());
        java.util.HashSet<SequenceClaim> unique = new java.util.HashSet<>();
        for (SequenceClaim claim : claims) {
            if (!unique.add(claim)) {
                throw new IllegalArgumentException("shared sequence claims repeat a claim");
            }
            selected.add(requireSequenceClaim(claim));
        }
        return List.copyOf(selected);
    }

    private void releasePendingSlot(int index) {
        Slot slot = slots[index];
        pendingLocatorBytes = Math.subtractExact(pendingLocatorBytes, slot.reservation.locatorBytes());
        slots[index] = null;
    }

    private void transition(AssignedTicket assigned, LifecycleStage expected, LifecycleStage replacement) {
        Slot slot = requireAssigned(assigned);
        if (slot.stage != expected || slot.rollbackPlan != null) {
            throw new IllegalStateException("Kafka Object lifecycle transition is out of order");
        }
        slot.stage = replacement;
    }

    public static final class RollbackPlan {
        private final KafkaObjectCompletionTrackerV1 owner;
        private final List<AssignedTicket> tickets;
        private final List<KafkaSpeculativeCommitV1> commits;

        private RollbackPlan(
                KafkaObjectCompletionTrackerV1 owner,
                List<AssignedTicket> tickets,
                List<KafkaSpeculativeCommitV1> commits) {
            this.owner = owner;
            this.tickets = tickets;
            this.commits = commits;
        }

        public List<KafkaSpeculativeCommitV1> commits() {
            return commits;
        }
    }

    private static final class Slot {
        private final Reservation reservation;
        private AssignedTicket assigned;
        private KafkaSpeculativeCommitV1 commitSet;
        private KafkaObjectExtentLocatorV1 locator;
        private KafkaObjectNativeStateV1 nativeState;
        private RollbackPlan rollbackPlan;
        private SequenceClaim sequenceClaim;
        private LifecycleStage stage;

        private Slot(Reservation reservation) {
            this.reservation = reservation;
        }

        private static Slot reserved(Reservation reservation) {
            return new Slot(reservation);
        }
    }
}
