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

package com.nereusstream.storage.object.control;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Synchronized WalRun admission kernel with three lazy lanes, aggregate accounting, and lane-local physical barriers.
 * It owns no metadata or provider transport and therefore performs zero control-plane I/O on normal admission.
 */
public final class WalRunRuntime {
    public enum State {
        ADMITTING,
        STOPPING,
        SEALED
    }

    public enum StopReason {
        OWNER_REQUEST,
        RUN_LIMIT,
        RUN_AGE,
        PROVIDER_ABSENT_GAP,
        PROVIDER_CONFLICT,
        SEQUENCE_EXHAUSTED,
        RECOVERY_ENVELOPE
    }

    private final WalRunRootRecord root;
    private final long openedAtMillis;
    private final long sequenceMaximum;
    private final LaneRuntime[] lanes = new LaneRuntime[3];
    private State state = State.ADMITTING;
    private StopReason stopReason;
    private long reservedExtentCount;
    private long reservedCanonicalBodyBytes;
    private long resolvedExtentCount;
    private long resolvedCanonicalBodyBytes;

    public WalRunRuntime(WalRunRootRecord root) {
        this(root, Long.MAX_VALUE);
    }

    /** Package-visible small maximum injection exercises the exact production exhaustion state machine. */
    WalRunRuntime(WalRunRootRecord root, long sequenceMaximum) {
        this.root = Objects.requireNonNull(root, "root");
        this.openedAtMillis = root.openedAtMillis();
        if (sequenceMaximum < 0) {
            throw new IllegalArgumentException("lane sequence maximum must be non-negative");
        }
        this.sequenceMaximum = sequenceMaximum;
    }

    private WalRunRuntime(WalRunRootRecord root, RecoveredState recovered) {
        this.root = Objects.requireNonNull(root, "root");
        this.openedAtMillis = root.openedAtMillis();
        this.sequenceMaximum = Long.MAX_VALUE;
        Objects.requireNonNull(recovered, "recovered");
        this.state = recovered.state();
        this.stopReason = recovered.stopReason().orElse(null);
        this.reservedExtentCount = recovered.reservedExtentCount();
        this.reservedCanonicalBodyBytes = recovered.reservedCanonicalBodyBytes();
        this.resolvedExtentCount = recovered.resolvedExtentCount();
        this.resolvedCanonicalBodyBytes = recovered.resolvedCanonicalBodyBytes();
        long computedResolved = 0;
        long pendingCount = 0;
        long burnedCount = 0;
        Set<WalLaneId> identities = new HashSet<>();
        for (RecoveredLane lane : recovered.lanes()) {
            if (!identities.add(lane.laneId())) {
                throw new IllegalArgumentException("recovered WalRun contains a duplicate lane");
            }
            if (lane.resolvedThrough() == Long.MAX_VALUE) {
                throw new IllegalArgumentException(
                        "a terminal lane cut cannot fit the signed aggregate extent counter; successor required");
            }
            if (lane.resolvedThrough() >= 0) {
                computedResolved = Math.addExact(computedResolved, Math.incrementExact(lane.resolvedThrough()));
            }
            if (lane.pendingReservation().isPresent()) {
                pendingCount = Math.incrementExact(pendingCount);
            }
            if (lane.burnedOrExhaustedBeforeSuccessor()) {
                burnedCount = Math.incrementExact(burnedCount);
            }
            lanes[lane.laneId().code()] = new LaneRuntime(lane);
        }
        if (computedResolved != resolvedExtentCount
                || Math.addExact(Math.addExact(resolvedExtentCount, pendingCount), burnedCount) != reservedExtentCount
                || resolvedCanonicalBodyBytes > reservedCanonicalBodyBytes
                || reservedExtentCount > root.bounds().maxExtentCount()
                || reservedCanonicalBodyBytes > root.bounds().maxCanonicalBodyBytes()) {
            throw new IllegalArgumentException("recovered WalRun counters differ from its exact lane cut/bounds");
        }
        if (state == State.ADMITTING && (stopReason != null || burnedCount != 0)) {
            throw new IllegalArgumentException("an admitting recovered WalRun cannot carry a stop/burn disposition");
        }
        if (state != State.ADMITTING && stopReason == null) {
            throw new IllegalArgumentException("a stopped/sealed recovered WalRun requires its exact stop reason");
        }
        if (state == State.SEALED && pendingCount != 0) {
            throw new IllegalArgumentException("a sealed recovered WalRun cannot contain a pending candidate");
        }
    }

    public static WalRunRuntime restore(WalRunRootRecord root, RecoveredState recovered) {
        return new WalRunRuntime(root, recovered);
    }

    /**
     * Allocates only after receiving an already immutable plan and only while the lane has no unresolved predecessor.
     */
    public synchronized LaneSequenceReservation admitSealedPlan(ImmutableExtentPlan plan, long nowMillis) {
        Objects.requireNonNull(plan, "plan");
        requireAdmitting(nowMillis);
        long nextExtentCount = Math.addExact(reservedExtentCount, 1);
        long nextBodyBytes = Math.addExact(reservedCanonicalBodyBytes, plan.maximumCanonicalBodyBytes());
        if (nextExtentCount > root.bounds().maxExtentCount()
                || nextBodyBytes > root.bounds().maxCanonicalBodyBytes()) {
            stop(StopReason.RUN_LIMIT);
            throw new IllegalStateException("WalRun aggregate admission bound reached");
        }
        LaneRuntime lane = lanes[plan.laneId().code()];
        if (lane == null) {
            lane = new LaneRuntime(plan.laneId(), sequenceMaximum);
            lanes[plan.laneId().code()] = lane;
        }
        LaneSequenceReservation reservation;
        try {
            reservation = lane.allocate(plan);
        } catch (SequenceExhaustedException failure) {
            stop(StopReason.SEQUENCE_EXHAUSTED);
            throw failure;
        }
        reservedExtentCount = nextExtentCount;
        reservedCanonicalBodyBytes = nextBodyBytes;
        return reservation;
    }

    /**
     * Production session entry: an authenticated recovered/same-candidate plan reuses the pending sequence; only a
     * lane without pending work can allocate. This keeps retry available after admission has begun draining.
     */
    synchronized LaneSequenceReservation admitOrRetrySealedPlan(ImmutableExtentPlan plan, long nowMillis) {
        Objects.requireNonNull(plan, "plan");
        LaneRuntime lane = lanes[plan.laneId().code()];
        if (lane != null && lane.allocator.pendingReservation().isPresent()) {
            LaneSequenceReservation pending =
                    lane.allocator.pendingReservation().orElseThrow();
            if (!pending.canonicalPlanSha256().equals(plan.canonicalPlanSha256())
                    || pending.maximumCanonicalBodyBytes() != plan.maximumCanonicalBodyBytes()) {
                throw new IllegalArgumentException("sealed plan differs from the unresolved lane candidate");
            }
            return pending;
        }
        return admitSealedPlan(plan, nowMillis);
    }

    /** Same-candidate retry returns the extant reservation and never consumes a second sequence. */
    public synchronized LaneSequenceReservation retryReservation(
            WalLaneId laneId, long sequence, com.nereusstream.domain.bytes.Sha256Digest planSha256) {
        LaneRuntime lane = requireLane(laneId);
        LaneSequenceReservation pending = lane.allocator
                .pendingReservation()
                .orElseThrow(() -> new IllegalStateException("lane has no unresolved candidate"));
        if (pending.laneSequence() != sequence || !pending.canonicalPlanSha256().equals(planSha256)) {
            throw new IllegalArgumentException("retry identity differs from the unresolved lane candidate");
        }
        return pending;
    }

    /** Provider-resolved is the lane barrier release; typed binding ACK state is intentionally absent from this API. */
    public synchronized void providerResolved(LaneSequenceReservation reservation, long actualCanonicalBodyBytes) {
        if (actualCanonicalBodyBytes <= 0 || actualCanonicalBodyBytes > reservation.maximumCanonicalBodyBytes()) {
            throw new IllegalArgumentException("actual body length is outside the sealed-plan reservation");
        }
        LaneRuntime lane = requireLane(reservation.laneId());
        lane.resolve(reservation);
        resolvedExtentCount = Math.addExact(resolvedExtentCount, 1);
        resolvedCanonicalBodyBytes = Math.addExact(resolvedCanonicalBodyBytes, actualCanonicalBodyBytes);
        if (lane.allocator.exhausted()) {
            stop(StopReason.SEQUENCE_EXHAUSTED);
        }
    }

    /**
     * A definitive absent result burns the already allocated identity and stops the whole run before that candidate.
     */
    public synchronized void providerAbsent(LaneSequenceReservation reservation) {
        requireLane(reservation.laneId()).burn(reservation);
        stop(StopReason.PROVIDER_ABSENT_GAP);
    }

    public synchronized void providerConflict(LaneSequenceReservation reservation) {
        requireLane(reservation.laneId()).burn(reservation);
        stop(StopReason.PROVIDER_CONFLICT);
    }

    public synchronized void stopAdmission(StopReason reason) {
        stop(Objects.requireNonNull(reason, "reason"));
    }

    public synchronized LaneSequenceVector seal() {
        if (state == State.ADMITTING) {
            throw new IllegalStateException("admission must stop before sealing");
        }
        for (LaneRuntime lane : lanes) {
            if (lane != null && lane.allocator.pendingReservation().isPresent()) {
                throw new IllegalStateException("cannot seal with an unresolved lane candidate");
            }
        }
        state = State.SEALED;
        return resolvedVector();
    }

    public synchronized LaneSequenceVector resolvedVector() {
        LaneSequenceVector vector = LaneSequenceVector.empty();
        for (LaneRuntime lane : lanes) {
            if (lane != null && lane.allocator.resolvedThrough() >= 0) {
                vector = vector.with(lane.laneId, lane.allocator.resolvedThrough());
            }
        }
        return vector;
    }

    public synchronized int instantiatedLaneCount() {
        int count = 0;
        for (LaneRuntime lane : lanes) {
            if (lane != null) {
                count++;
            }
        }
        return count;
    }

    public synchronized State state() {
        return state;
    }

    public synchronized Optional<StopReason> stopReason() {
        return Optional.ofNullable(stopReason);
    }

    public synchronized long resolvedExtentCount() {
        return resolvedExtentCount;
    }

    public synchronized long resolvedCanonicalBodyBytes() {
        return resolvedCanonicalBodyBytes;
    }

    WalRunRootRecord rootRecord() {
        return root;
    }

    public synchronized RecoveredState recoveryState() {
        java.util.ArrayList<RecoveredLane> recoveredLanes = new java.util.ArrayList<>();
        for (LaneRuntime lane : lanes) {
            if (lane != null) {
                recoveredLanes.add(new RecoveredLane(
                        lane.laneId,
                        lane.allocator.resolvedThrough(),
                        lane.allocator.pendingReservation(),
                        lane.allocator.burned()));
            }
        }
        return new RecoveredState(
                state,
                Optional.ofNullable(stopReason),
                recoveredLanes,
                reservedExtentCount,
                reservedCanonicalBodyBytes,
                resolvedExtentCount,
                resolvedCanonicalBodyBytes);
    }

    private void requireAdmitting(long nowMillis) {
        if (nowMillis < openedAtMillis) {
            throw new IllegalArgumentException("clock regressed before WalRun open time");
        }
        if (state != State.ADMITTING) {
            throw new IllegalStateException("WalRun no longer admits plans: " + state);
        }
        if (nowMillis - openedAtMillis >= root.bounds().maxRunAgeMillis()) {
            stop(StopReason.RUN_AGE);
            throw new IllegalStateException("WalRun age bound reached");
        }
    }

    private LaneRuntime requireLane(WalLaneId laneId) {
        Objects.requireNonNull(laneId, "laneId");
        LaneRuntime lane = lanes[laneId.code()];
        if (lane == null) {
            throw new IllegalArgumentException("lane has not been instantiated");
        }
        return lane;
    }

    private void stop(StopReason reason) {
        if (state == State.ADMITTING) {
            state = State.STOPPING;
            stopReason = reason;
        }
    }

    private static final class LaneRuntime {
        private final WalLaneId laneId;
        private final LaneSequenceAllocator allocator;

        private LaneRuntime(WalLaneId laneId, long sequenceMaximum) {
            this.laneId = laneId;
            this.allocator = new LaneSequenceAllocator(laneId, -1, Optional.empty(), false, sequenceMaximum);
        }

        private LaneRuntime(RecoveredLane recovered) {
            this.laneId = recovered.laneId();
            this.allocator = new LaneSequenceAllocator(
                    recovered.laneId(),
                    recovered.resolvedThrough(),
                    recovered.pendingReservation(),
                    recovered.burnedOrExhaustedBeforeSuccessor());
        }

        private LaneSequenceReservation allocate(ImmutableExtentPlan plan) {
            return allocator.allocate(plan);
        }

        private void resolve(LaneSequenceReservation reservation) {
            allocator.resolve(reservation);
        }

        private void burn(LaneSequenceReservation reservation) {
            allocator.burn(reservation);
        }
    }

    /** Explicit terminal exhaustion signal; a successor WalRun is required and wrapping is impossible. */
    public static final class SequenceExhaustedException extends IllegalStateException {
        SequenceExhaustedException() {
            super("lane sequence space is exhausted; successor WalRun required");
        }
    }

    public record RecoveredLane(
            WalLaneId laneId,
            long resolvedThrough,
            Optional<LaneSequenceReservation> pendingReservation,
            boolean burnedOrExhaustedBeforeSuccessor) {
        public RecoveredLane {
            Objects.requireNonNull(laneId, "laneId");
            Objects.requireNonNull(pendingReservation, "pendingReservation");
            if (resolvedThrough < -1) {
                throw new IllegalArgumentException("recovered lane component must be -1 or non-negative");
            }
        }
    }

    public record RecoveredState(
            State state,
            Optional<StopReason> stopReason,
            List<RecoveredLane> lanes,
            long reservedExtentCount,
            long reservedCanonicalBodyBytes,
            long resolvedExtentCount,
            long resolvedCanonicalBodyBytes) {
        public RecoveredState {
            Objects.requireNonNull(state, "state");
            Objects.requireNonNull(stopReason, "stopReason");
            Objects.requireNonNull(lanes, "lanes");
            lanes = List.copyOf(lanes);
            if (lanes.size() > 3
                    || reservedExtentCount < 0
                    || reservedCanonicalBodyBytes < 0
                    || resolvedExtentCount < 0
                    || resolvedCanonicalBodyBytes < 0) {
                throw new IllegalArgumentException("recovered WalRun state is outside its closed bounds");
            }
        }
    }
}
