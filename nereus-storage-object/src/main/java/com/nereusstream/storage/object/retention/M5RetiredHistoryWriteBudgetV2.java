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

package com.nereusstream.storage.object.retention;

import com.nereusstream.domain.bytes.Sha256Digest;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Cell-owned bounded prewrite reservations. Native namespace admission supplies and restores durable headroom. */
public final class M5RetiredHistoryWriteBudgetV2 {
    public record Snapshot(
            int unresolvedFolds,
            long reservedWriteBytes,
            long chargedDurableBytes,
            long remainingDurableHeadroomBytes) {}

    public final class Reservation {
        private final Sha256Digest attempt;
        private final M5RetiredBatchHistoryV2.Insertion insertion;
        private final Set<Sha256Digest> dispatched = new HashSet<>();
        private boolean executing;
        private boolean terminalObserved;

        private Reservation(Sha256Digest attempt, M5RetiredBatchHistoryV2.Insertion insertion) {
            this.attempt = attempt;
            this.insertion = insertion;
        }

        public boolean tryBegin() {
            synchronized (M5RetiredHistoryWriteBudgetV2.this) {
                if (active.get(attempt) != this || executing) {
                    return false;
                }
                executing = true;
                return true;
            }
        }

        public void endAttempt() {
            synchronized (M5RetiredHistoryWriteBudgetV2.this) {
                executing = false;
                if (terminalObserved) {
                    release(this);
                }
            }
        }

        /** Charge before dispatch, including unknown outcomes; repeated reconciliation never charges twice. */
        public void beforeNodeDispatch(M5RetiredBatchHistoryV2.NodeWrite node) {
            synchronized (M5RetiredHistoryWriteBudgetV2.this) {
                if (active.get(attempt) != this
                        || terminalObserved
                        || !insertion.nodes().contains(node)) {
                    throw new IllegalStateException("history prewrite has no exact active reservation");
                }
                if (dispatched.add(node.root().sha256())) {
                    chargedDurableBytes =
                            Math.addExact(chargedDurableBytes, node.bytes().length());
                }
            }
        }
    }

    private final int maximumUnresolvedFolds;
    private final long maximumReservedBytes;
    private final Map<Sha256Digest, Reservation> active = new HashMap<>();
    private long reservedBytes;
    private long chargedDurableBytes;
    private long admittedDurableBytes;

    public M5RetiredHistoryWriteBudgetV2(
            int maximumUnresolvedFolds, long maximumReservedBytes, long admittedDurableHeadroomBytes) {
        if (maximumUnresolvedFolds <= 0
                || maximumUnresolvedFolds > 1_024
                || maximumReservedBytes <= 0
                || admittedDurableHeadroomBytes < 0) {
            throw new IllegalArgumentException("retired history prewrite admission bounds differ");
        }
        this.maximumUnresolvedFolds = maximumUnresolvedFolds;
        this.maximumReservedBytes = maximumReservedBytes;
        this.admittedDurableBytes = admittedDurableHeadroomBytes;
    }

    public synchronized Optional<Reservation> reserve(
            Sha256Digest attempt, M5RetiredBatchHistoryV2.Insertion insertion) {
        Objects.requireNonNull(attempt, "attempt");
        Objects.requireNonNull(insertion, "insertion");
        Reservation existing = active.get(attempt);
        if (existing != null) {
            if (!existing.insertion.equals(insertion)) {
                throw new IllegalStateException("history reservation attempt changed its exact insertion");
            }
            return Optional.of(existing);
        }
        long bytes = insertion.encodedBytes();
        if (active.size() >= maximumUnresolvedFolds
                || bytes > maximumReservedBytes - reservedBytes
                || bytes > admittedDurableBytes - chargedDurableBytes - unchargedReservedBytes()) {
            return Optional.empty();
        }
        var reservation = new Reservation(attempt, insertion);
        active.put(attempt, reservation);
        reservedBytes = Math.addExact(reservedBytes, bytes);
        return Optional.of(reservation);
    }

    /** A concurrent terminal observation cannot release a reservation until its native attempt has drained. */
    public synchronized void reconciled(Sha256Digest attempt) {
        Reservation reservation = active.get(attempt);
        if (reservation != null) {
            reservation.terminalObserved = true;
            if (!reservation.executing) {
                release(reservation);
            }
        }
    }

    private void release(Reservation reservation) {
        if (active.get(reservation.attempt) == reservation) {
            active.remove(reservation.attempt);
            reservedBytes -= reservation.insertion.encodedBytes();
        }
    }

    /** Operator/backend admission can expand durable capacity; it cannot roll back charged usage. */
    public synchronized void addAdmittedHeadroom(long additionalBytes) {
        if (additionalBytes <= 0) {
            throw new IllegalArgumentException("additional history headroom must be positive");
        }
        admittedDurableBytes = Math.addExact(admittedDurableBytes, additionalBytes);
    }

    public synchronized Snapshot snapshot() {
        return new Snapshot(
                active.size(),
                reservedBytes,
                chargedDurableBytes,
                admittedDurableBytes - chargedDurableBytes - unchargedReservedBytes());
    }

    private long unchargedReservedBytes() {
        long result = 0;
        for (Reservation reservation : active.values()) {
            for (var node : reservation.insertion.nodes()) {
                if (!reservation.dispatched.contains(node.root().sha256())) {
                    result = Math.addExact(result, node.bytes().length());
                }
            }
        }
        return result;
    }
}
