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

package com.nereusstream.kafka.bookkeeper.compaction;

import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.api.kafka.KafkaRunRootRecordV2;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * One admitted process-local Cell owner's fixed Binding shares for raw/selected BK read lifetimes. No borrowing,
 * waiting queue or timeout release. Counts cover configured live-read allowances, not retained result caches,
 * backend-internal buffers or durable process-drain authority. The Cell owner must retain this instance for its life.
 */
public final class KafkaBookKeeperReadCellBudgetV2 {
    public record Usage(long owners, long readSlots, long encodedBytes, long decodedBytes) {
        public Usage {
            if (owners < 0 || readSlots < 0 || encodedBytes < 0 || decodedBytes < 0) {
                throw new IllegalArgumentException("negative BK read Cell usage");
            }
        }

        public static Usage forOwner(KafkaBookKeeperSelectedSourceV2.Bounds bounds, int capacity) {
            Objects.requireNonNull(bounds, "bounds");
            if (capacity < 1 || capacity > 64) {
                throw new IllegalArgumentException("BK read owner capacity is outside bounds");
            }
            return new Usage(
                    1,
                    capacity,
                    Math.multiplyExact((long) capacity, bounds.encodedBytes()),
                    Math.multiplyExact((long) capacity, bounds.decodedBytes()));
        }

        /** Reserve both bounded root snapshots as well as native frames and retained canonical payload. */
        public static Usage forRunSource(KafkaBookKeeperRunSourceV2.Bounds bounds) {
            Objects.requireNonNull(bounds, "bounds");
            long roots = Math.multiplyExact(2L, KafkaRunRootRecordV2.MAX_BYTES);
            return new Usage(
                    1,
                    1,
                    Math.addExact(Math.addExact(bounds.nativeBytes(), bounds.payloadBytes()), roots),
                    bounds.decodedBytes());
        }

        Usage plus(Usage other) {
            return new Usage(
                    Math.addExact(owners, other.owners),
                    Math.addExact(readSlots, other.readSlots),
                    Math.addExact(encodedBytes, other.encodedBytes),
                    Math.addExact(decodedBytes, other.decodedBytes));
        }

        Usage minus(Usage other) {
            return new Usage(
                    Math.subtractExact(owners, other.owners),
                    Math.subtractExact(readSlots, other.readSlots),
                    Math.subtractExact(encodedBytes, other.encodedBytes),
                    Math.subtractExact(decodedBytes, other.decodedBytes));
        }

        boolean within(Usage limit) {
            return owners <= limit.owners
                    && readSlots <= limit.readSlots
                    && encodedBytes <= limit.encodedBytes
                    && decodedBytes <= limit.decodedBytes;
        }
    }

    private static final Usage ZERO = new Usage(0, 0, 0, 0);
    private final CellProviderScopeId cell;
    private final Usage limit;
    private final Map<BindingIdentity, Usage> shares;
    private final Map<BindingIdentity, Usage> used;
    private Usage total = ZERO;

    public KafkaBookKeeperReadCellBudgetV2(CellProviderScopeId cell, Usage limit, Map<BindingIdentity, Usage> shares) {
        this.cell = Objects.requireNonNull(cell, "cell");
        this.limit = Objects.requireNonNull(limit, "limit");
        this.shares = Map.copyOf(shares);
        if (this.shares.isEmpty()
                || this.shares.size() > 1024
                || limit.owners() < 1
                || limit.readSlots() < 1
                || limit.owners() > 65536
                || limit.readSlots() > 65536) {
            throw new IllegalArgumentException("BK read Cell profile exceeds bounded owner/Binding capacity");
        }
        var reserved = ZERO;
        used = new HashMap<>();
        for (var entry : this.shares.entrySet()) {
            var share = entry.getValue();
            if (share.owners() < 1 || share.readSlots() < 1 || share.encodedBytes() < 1) {
                throw new IllegalArgumentException("BK read Binding share must reserve positive capacity");
            }
            reserved = reserved.plus(share);
            used.put(entry.getKey(), ZERO);
        }
        if (!reserved.within(limit)) {
            throw new IllegalArgumentException("BK read Binding shares exceed the Cell hard limit");
        }
    }

    /** Observation only; there is no public release/reset/import path. */
    public synchronized Usage usage() {
        return total;
    }

    public synchronized Map<BindingIdentity, Usage> bindingUsage() {
        return Map.copyOf(used);
    }

    synchronized Reservation reserve(
            KafkaSealedBookKeeperDescriptorV2 descriptor, KafkaBookKeeperSelectedSourceV2.Bounds bounds, int capacity) {
        return reserveRequest(
                descriptor.task().capability().providerScopeId(),
                descriptor.sourceCut().identity().binding(),
                Usage.forOwner(bounds, capacity));
    }

    synchronized Reservation reserveRaw(
            CellProviderScopeId expectedCell, BindingIdentity binding, KafkaBookKeeperRunSourceV2.Bounds bounds) {
        return reserveRequest(expectedCell, binding, Usage.forRunSource(bounds));
    }

    /** Keeps one selected result charged while the same plan verifies subsequent raw sources. */
    synchronized Reservation reserveRetained(
            CellProviderScopeId expectedCell, BindingIdentity binding, Usage retained) {
        if (retained.owners() != 0 || retained.readSlots() != 0 || retained.encodedBytes() < 1) {
            throw new IllegalArgumentException("retained BK read result requires a bounded byte allowance");
        }
        return reserveRequest(expectedCell, binding, retained);
    }

    private Reservation reserveRequest(CellProviderScopeId expectedCell, BindingIdentity binding, Usage request) {
        if (!cell.equals(expectedCell)) {
            throw new IllegalArgumentException("BK read budget belongs to another Cell");
        }
        var share = shares.get(binding);
        if (share == null) {
            throw new IllegalStateException("BK read Binding has no admitted Cell share");
        }
        var nextBinding = used.get(binding).plus(request);
        var nextTotal = total.plus(request);
        if (!nextBinding.within(share) || !nextTotal.within(limit)) {
            throw new IllegalStateException("BK read Binding or Cell capacity exhausted");
        }
        used.put(binding, nextBinding);
        total = nextTotal;
        return new Reservation(binding, request);
    }

    final class Reservation {
        private final BindingIdentity binding;
        private final Usage charge;
        private boolean sessionStarted;
        private boolean released;

        private Reservation(BindingIdentity binding, Usage charge) {
            this.binding = binding;
            this.charge = charge;
        }

        void sessionStarting() {
            synchronized (KafkaBookKeeperReadCellBudgetV2.this) {
                if (released) {
                    throw new IllegalStateException("BK read Cell reservation is already released");
                }
                sessionStarted = true;
            }
        }

        void releaseBeforeSession() {
            synchronized (KafkaBookKeeperReadCellBudgetV2.this) {
                if (!sessionStarted) {
                    release();
                }
            }
        }

        void releaseAfterDrain() {
            synchronized (KafkaBookKeeperReadCellBudgetV2.this) {
                release();
            }
        }

        private void release() {
            if (!released) {
                used.put(binding, used.get(binding).minus(charge));
                total = total.minus(charge);
                released = true;
            }
        }
    }
}
