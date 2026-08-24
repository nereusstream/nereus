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

import com.nereusstream.domain.bytes.Sha256Digest;
import java.util.Objects;
import java.util.Optional;

/** Lane-local post-plan-seal allocator with same-candidate retry, no reuse, and exact Long.MAX_VALUE exhaustion. */
public final class LaneSequenceAllocator {
    private final WalLaneId laneId;
    private final long sequenceMaximum;
    private long nextSequence;
    private long resolvedThrough;
    private boolean exhausted;
    private boolean burned;
    private Optional<LaneSequenceReservation> pending = Optional.empty();

    public LaneSequenceAllocator(WalLaneId laneId) {
        this(laneId, -1, Optional.empty(), false, Long.MAX_VALUE);
    }

    /** Restores from a validated provider-resolved checkpoint component; -1 means an uninstantiated/empty lane. */
    public LaneSequenceAllocator(WalLaneId laneId, long recoveredResolvedThrough) {
        this(laneId, recoveredResolvedThrough, Optional.empty(), false, Long.MAX_VALUE);
    }

    /** Restores the exact lane cut after authenticated checkpoint/tail reconciliation. */
    public LaneSequenceAllocator(
            WalLaneId laneId,
            long recoveredResolvedThrough,
            Optional<LaneSequenceReservation> recoveredPending,
            boolean recoveredBurnedOrExhausted) {
        this(laneId, recoveredResolvedThrough, recoveredPending, recoveredBurnedOrExhausted, Long.MAX_VALUE);
    }

    LaneSequenceAllocator(
            WalLaneId laneId,
            long recoveredResolvedThrough,
            Optional<LaneSequenceReservation> recoveredPending,
            boolean recoveredBurnedOrExhausted,
            long sequenceMaximum) {
        this.laneId = Objects.requireNonNull(laneId, "laneId");
        Objects.requireNonNull(recoveredPending, "recoveredPending");
        if (sequenceMaximum < 0 || recoveredResolvedThrough < -1 || recoveredResolvedThrough > sequenceMaximum) {
            throw new IllegalArgumentException("lane sequence maximum/recovered cut is outside the closed domain");
        }
        this.sequenceMaximum = sequenceMaximum;
        this.resolvedThrough = recoveredResolvedThrough;
        recoveredPending.ifPresent(value -> {
            if (value.laneId() != laneId
                    || recoveredResolvedThrough == sequenceMaximum
                    || value.laneSequence() != Math.incrementExact(recoveredResolvedThrough)) {
                throw new IllegalArgumentException("recovered pending reservation is not the exact lane successor");
            }
        });
        if (recoveredPending.isPresent() && recoveredBurnedOrExhausted) {
            throw new IllegalArgumentException("a recovered lane cannot be pending and burned/exhausted");
        }
        this.pending = recoveredPending;
        this.burned = recoveredBurnedOrExhausted;
        if (recoveredBurnedOrExhausted || recoveredResolvedThrough == sequenceMaximum) {
            nextSequence = sequenceMaximum;
            exhausted = true;
        } else if (recoveredPending.isPresent()) {
            long pendingSequence = recoveredPending.orElseThrow().laneSequence();
            if (pendingSequence > sequenceMaximum) {
                throw new IllegalArgumentException("recovered pending reservation exceeds the lane sequence maximum");
            }
            if (pendingSequence == sequenceMaximum) {
                nextSequence = sequenceMaximum;
                exhausted = true;
            } else {
                nextSequence = Math.incrementExact(pendingSequence);
            }
        } else {
            nextSequence = Math.incrementExact(recoveredResolvedThrough);
        }
    }

    public synchronized LaneSequenceReservation allocate(ImmutableExtentPlan plan) {
        Objects.requireNonNull(plan, "plan");
        if (plan.laneId() != laneId) {
            throw new IllegalArgumentException("sealed plan belongs to a different lane");
        }
        if (pending.isPresent()) {
            throw new IllegalStateException("lane physical outcome barrier is unresolved");
        }
        if (exhausted) {
            throw new WalRunRuntime.SequenceExhaustedException();
        }
        long sequence = nextSequence;
        if (sequence == sequenceMaximum) {
            exhausted = true;
        } else {
            nextSequence = Math.incrementExact(sequence);
        }
        LaneSequenceReservation reservation = new LaneSequenceReservation(
                laneId, sequence, plan.canonicalPlanSha256(), plan.maximumCanonicalBodyBytes());
        pending = Optional.of(reservation);
        return reservation;
    }

    public synchronized LaneSequenceReservation retry(long sequence, Sha256Digest planSha256) {
        LaneSequenceReservation candidate =
                pending.orElseThrow(() -> new IllegalStateException("lane has no unresolved candidate"));
        if (candidate.laneSequence() != sequence
                || !candidate.canonicalPlanSha256().equals(planSha256)) {
            throw new IllegalArgumentException("retry identity differs from the unresolved lane candidate");
        }
        return candidate;
    }

    public synchronized void resolve(LaneSequenceReservation reservation) {
        requirePending(reservation);
        if (resolvedThrough == sequenceMaximum || reservation.laneSequence() != Math.incrementExact(resolvedThrough)) {
            throw new IllegalStateException("provider resolution would create a lane gap");
        }
        resolvedThrough = reservation.laneSequence();
        pending = Optional.empty();
    }

    public synchronized void burn(LaneSequenceReservation reservation) {
        requirePending(reservation);
        pending = Optional.empty();
        exhausted = true;
        burned = true;
    }

    public synchronized Optional<LaneSequenceReservation> pendingReservation() {
        return pending;
    }

    public synchronized long resolvedThrough() {
        return resolvedThrough;
    }

    public synchronized boolean exhausted() {
        return exhausted;
    }

    public synchronized boolean burned() {
        return burned;
    }

    private void requirePending(LaneSequenceReservation reservation) {
        if (pending.isEmpty() || !pending.orElseThrow().equals(reservation)) {
            throw new IllegalArgumentException("reservation is not the lane's unresolved candidate");
        }
    }
}
