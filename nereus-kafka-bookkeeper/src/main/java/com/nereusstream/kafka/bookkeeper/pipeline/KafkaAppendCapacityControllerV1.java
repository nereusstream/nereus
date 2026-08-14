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

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/** Checked groups+entries+bytes reservation controller shared at partition or Cell/host scope. */
public final class KafkaAppendCapacityControllerV1 {
    private final KafkaAppendCapacityBudgetV1 budget;
    private long groups;
    private long entries;
    private long bytes;

    public KafkaAppendCapacityControllerV1(KafkaAppendCapacityBudgetV1 budget) {
        this.budget = Objects.requireNonNull(budget, "budget");
    }

    public synchronized Optional<Lease> tryReserve(long requestedEntries, long requestedBytes) {
        if (requestedEntries <= 0 || requestedBytes <= 0) {
            throw new IllegalArgumentException("requested entries and bytes must be positive");
        }
        long candidateGroups;
        long candidateEntries;
        long candidateBytes;
        try {
            candidateGroups = Math.addExact(groups, 1L);
            candidateEntries = Math.addExact(entries, requestedEntries);
            candidateBytes = Math.addExact(bytes, requestedBytes);
        } catch (ArithmeticException failure) {
            return Optional.empty();
        }
        if (candidateGroups > budget.maximumGroups()
                || candidateEntries > budget.maximumEntries()
                || candidateBytes > budget.maximumBytes()) {
            return Optional.empty();
        }
        groups = candidateGroups;
        entries = candidateEntries;
        bytes = candidateBytes;
        return Optional.of(new Lease(this, requestedEntries, requestedBytes));
    }

    public synchronized KafkaAppendCapacitySnapshotV1 snapshot() {
        return new KafkaAppendCapacitySnapshotV1(groups, entries, bytes);
    }

    private synchronized void release(long releasedEntries, long releasedBytes) {
        groups = Math.subtractExact(groups, 1L);
        entries = Math.subtractExact(entries, releasedEntries);
        bytes = Math.subtractExact(bytes, releasedBytes);
    }

    /** One idempotently releasable exact reservation. */
    public static final class Lease implements AutoCloseable {
        private final KafkaAppendCapacityControllerV1 owner;
        private final long entries;
        private final long bytes;
        private final AtomicBoolean released = new AtomicBoolean();

        private Lease(KafkaAppendCapacityControllerV1 owner, long entries, long bytes) {
            this.owner = owner;
            this.entries = entries;
            this.bytes = bytes;
        }

        @Override
        public void close() {
            if (released.compareAndSet(false, true)) {
                owner.release(entries, bytes);
            }
        }
    }
}
