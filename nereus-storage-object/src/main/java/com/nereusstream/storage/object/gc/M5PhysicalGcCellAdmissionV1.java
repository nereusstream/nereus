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

package com.nereusstream.storage.object.gc;

import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import java.util.Objects;
import java.util.Optional;

/** Pure hard per-Cell reservation and usage admission for M5 physical GC. */
public final class M5PhysicalGcCellAdmissionV1 {
    private M5PhysicalGcCellAdmissionV1() {}

    public static AdmissionResult admit(
            CellBudgetEnvelopeV1 envelope, CellBudgetStateV1 current, CellBudgetStateV1 requested) {
        Objects.requireNonNull(envelope, "envelope");
        Objects.requireNonNull(current, "current");
        Objects.requireNonNull(requested, "requested");
        if (!envelope.cellProviderScopeId().equals(current.cellProviderScopeId())
                || !envelope.cellProviderScopeId().equals(requested.cellProviderScopeId())) {
            return AdmissionResult.cellIdentityMismatch();
        }
        Optional<ResourceVectorV1> candidate = current.usage().plus(requested.usage());
        if (candidate.isEmpty() || !candidate.orElseThrow().within(envelope.hardLimit())) {
            return AdmissionResult.cellHardLimitReached();
        }
        return AdmissionResult.admitted(new CellBudgetStateV1(envelope.cellProviderScopeId(), candidate.orElseThrow()));
    }

    public enum AdmissionOutcome {
        ADMITTED,
        CELL_HARD_LIMIT_REACHED,
        CELL_IDENTITY_MISMATCH
    }

    public record CellBudgetEnvelopeV1(
            CellProviderScopeId cellProviderScopeId, ResourceVectorV1 reservedMinimum, ResourceVectorV1 hardLimit) {
        public CellBudgetEnvelopeV1 {
            Objects.requireNonNull(cellProviderScopeId, "cellProviderScopeId");
            Objects.requireNonNull(reservedMinimum, "reservedMinimum");
            Objects.requireNonNull(hardLimit, "hardLimit");
            if (!reservedMinimum.within(hardLimit)) {
                throw new IllegalArgumentException("per-Cell reserved minimum exceeds its hard limit");
            }
        }
    }

    public record CellBudgetStateV1(CellProviderScopeId cellProviderScopeId, ResourceVectorV1 usage) {
        public CellBudgetStateV1 {
            Objects.requireNonNull(cellProviderScopeId, "cellProviderScopeId");
            Objects.requireNonNull(usage, "usage");
        }
    }

    public record AdmissionResult(AdmissionOutcome outcome, Optional<CellBudgetStateV1> updatedState) {
        public AdmissionResult {
            Objects.requireNonNull(outcome, "outcome");
            Objects.requireNonNull(updatedState, "updatedState");
            if ((outcome == AdmissionOutcome.ADMITTED) != updatedState.isPresent()) {
                throw new IllegalArgumentException("only ADMITTED carries updated per-Cell usage");
            }
        }

        static AdmissionResult admitted(CellBudgetStateV1 state) {
            return new AdmissionResult(AdmissionOutcome.ADMITTED, Optional.of(state));
        }

        static AdmissionResult cellHardLimitReached() {
            return new AdmissionResult(AdmissionOutcome.CELL_HARD_LIMIT_REACHED, Optional.empty());
        }

        static AdmissionResult cellIdentityMismatch() {
            return new AdmissionResult(AdmissionOutcome.CELL_IDENTITY_MISMATCH, Optional.empty());
        }
    }

    public record ResourceVectorV1(
            InventoryUsageV1 inventory,
            QueueUsageV1 queues,
            ConcurrencyUsageV1 concurrency,
            RateUsageV1 rates,
            MemoryUsageV1 memory,
            ScannerUsageV1 scanner,
            QuarantineUsageV1 quarantine) {
        public ResourceVectorV1 {
            Objects.requireNonNull(inventory, "inventory");
            Objects.requireNonNull(queues, "queues");
            Objects.requireNonNull(concurrency, "concurrency");
            Objects.requireNonNull(rates, "rates");
            Objects.requireNonNull(memory, "memory");
            Objects.requireNonNull(scanner, "scanner");
            Objects.requireNonNull(quarantine, "quarantine");
        }

        public static ResourceVectorV1 zero() {
            return uniform(0);
        }

        public static ResourceVectorV1 uniform(long value) {
            return new ResourceVectorV1(
                    InventoryUsageV1.uniform(value),
                    QueueUsageV1.uniform(value),
                    ConcurrencyUsageV1.uniform(value),
                    RateUsageV1.uniform(value),
                    MemoryUsageV1.uniform(value),
                    ScannerUsageV1.uniform(value),
                    QuarantineUsageV1.uniform(value));
        }

        Optional<ResourceVectorV1> plus(ResourceVectorV1 other) {
            try {
                return Optional.of(new ResourceVectorV1(
                        inventory.plus(other.inventory),
                        queues.plus(other.queues),
                        concurrency.plus(other.concurrency),
                        rates.plus(other.rates),
                        memory.plus(other.memory),
                        scanner.plus(other.scanner),
                        quarantine.plus(other.quarantine)));
            } catch (ArithmeticException overflow) {
                return Optional.empty();
            }
        }

        boolean within(ResourceVectorV1 limit) {
            return inventory.within(limit.inventory)
                    && queues.within(limit.queues)
                    && concurrency.within(limit.concurrency)
                    && rates.within(limit.rates)
                    && memory.within(limit.memory)
                    && scanner.within(limit.scanner)
                    && quarantine.within(limit.quarantine);
        }
    }

    public record InventoryUsageV1(
            long candidateCount,
            long candidateBytes,
            long intentCount,
            long intentBytes,
            long doneCount,
            long doneBytes) {
        public InventoryUsageV1 {
            requireNonNegative(candidateCount, candidateBytes, intentCount, intentBytes, doneCount, doneBytes);
        }

        static InventoryUsageV1 uniform(long value) {
            return new InventoryUsageV1(value, value, value, value, value, value);
        }

        InventoryUsageV1 plus(InventoryUsageV1 other) {
            return new InventoryUsageV1(
                    add(candidateCount, other.candidateCount),
                    add(candidateBytes, other.candidateBytes),
                    add(intentCount, other.intentCount),
                    add(intentBytes, other.intentBytes),
                    add(doneCount, other.doneCount),
                    add(doneBytes, other.doneBytes));
        }

        boolean within(InventoryUsageV1 limit) {
            return candidateCount <= limit.candidateCount
                    && candidateBytes <= limit.candidateBytes
                    && intentCount <= limit.intentCount
                    && intentBytes <= limit.intentBytes
                    && doneCount <= limit.doneCount
                    && doneBytes <= limit.doneBytes;
        }
    }

    public record QueueUsageV1(
            long deleteQueue,
            long reconciliationQueue,
            long responseUnknownSlots,
            long oldestPendingAgeMillis,
            long orphanGraceBacklog) {
        public QueueUsageV1 {
            requireNonNegative(
                    deleteQueue, reconciliationQueue, responseUnknownSlots, oldestPendingAgeMillis, orphanGraceBacklog);
        }

        static QueueUsageV1 uniform(long value) {
            return new QueueUsageV1(value, value, value, value, value);
        }

        QueueUsageV1 plus(QueueUsageV1 other) {
            return new QueueUsageV1(
                    add(deleteQueue, other.deleteQueue),
                    add(reconciliationQueue, other.reconciliationQueue),
                    add(responseUnknownSlots, other.responseUnknownSlots),
                    Math.max(oldestPendingAgeMillis, other.oldestPendingAgeMillis),
                    add(orphanGraceBacklog, other.orphanGraceBacklog));
        }

        boolean within(QueueUsageV1 limit) {
            return deleteQueue <= limit.deleteQueue
                    && reconciliationQueue <= limit.reconciliationQueue
                    && responseUnknownSlots <= limit.responseUnknownSlots
                    && oldestPendingAgeMillis <= limit.oldestPendingAgeMillis
                    && orphanGraceBacklog <= limit.orphanGraceBacklog;
        }
    }

    public record ConcurrencyUsageV1(
            long objectGet,
            long objectList,
            long objectDelete,
            long multipart,
            long bookKeeper,
            long oxia,
            long kms,
            long network) {
        public ConcurrencyUsageV1 {
            requireNonNegative(objectGet, objectList, objectDelete, multipart, bookKeeper, oxia, kms, network);
        }

        static ConcurrencyUsageV1 uniform(long value) {
            return new ConcurrencyUsageV1(value, value, value, value, value, value, value, value);
        }

        ConcurrencyUsageV1 plus(ConcurrencyUsageV1 other) {
            return new ConcurrencyUsageV1(
                    add(objectGet, other.objectGet),
                    add(objectList, other.objectList),
                    add(objectDelete, other.objectDelete),
                    add(multipart, other.multipart),
                    add(bookKeeper, other.bookKeeper),
                    add(oxia, other.oxia),
                    add(kms, other.kms),
                    add(network, other.network));
        }

        boolean within(ConcurrencyUsageV1 limit) {
            return objectGet <= limit.objectGet
                    && objectList <= limit.objectList
                    && objectDelete <= limit.objectDelete
                    && multipart <= limit.multipart
                    && bookKeeper <= limit.bookKeeper
                    && oxia <= limit.oxia
                    && kms <= limit.kms
                    && network <= limit.network;
        }
    }

    public record RateUsageV1(long requests, long bytes, long ioOperations, long retries) {
        public RateUsageV1 {
            requireNonNegative(requests, bytes, ioOperations, retries);
        }

        static RateUsageV1 uniform(long value) {
            return new RateUsageV1(value, value, value, value);
        }

        RateUsageV1 plus(RateUsageV1 other) {
            return new RateUsageV1(
                    add(requests, other.requests),
                    add(bytes, other.bytes),
                    add(ioOperations, other.ioOperations),
                    add(retries, other.retries));
        }

        boolean within(RateUsageV1 limit) {
            return requests <= limit.requests
                    && bytes <= limit.bytes
                    && ioOperations <= limit.ioOperations
                    && retries <= limit.retries;
        }
    }

    public record MemoryUsageV1(long cacheBytes, long cacheEntries, long perTargetBufferBytes) {
        public MemoryUsageV1 {
            requireNonNegative(cacheBytes, cacheEntries, perTargetBufferBytes);
        }

        static MemoryUsageV1 uniform(long value) {
            return new MemoryUsageV1(value, value, value);
        }

        MemoryUsageV1 plus(MemoryUsageV1 other) {
            return new MemoryUsageV1(
                    add(cacheBytes, other.cacheBytes),
                    add(cacheEntries, other.cacheEntries),
                    Math.max(perTargetBufferBytes, other.perTargetBufferBytes));
        }

        boolean within(MemoryUsageV1 limit) {
            return cacheBytes <= limit.cacheBytes
                    && cacheEntries <= limit.cacheEntries
                    && perTargetBufferBytes <= limit.perTargetBufferBytes;
        }
    }

    public record ScannerUsageV1(long pages, long keys, long bytes) {
        public ScannerUsageV1 {
            requireNonNegative(pages, keys, bytes);
        }

        static ScannerUsageV1 uniform(long value) {
            return new ScannerUsageV1(value, value, value);
        }

        ScannerUsageV1 plus(ScannerUsageV1 other) {
            return new ScannerUsageV1(add(pages, other.pages), add(keys, other.keys), add(bytes, other.bytes));
        }

        boolean within(ScannerUsageV1 limit) {
            return pages <= limit.pages && keys <= limit.keys && bytes <= limit.bytes;
        }
    }

    public record QuarantineUsageV1(long count, long bytes, long oldestAgeMillis) {
        public QuarantineUsageV1 {
            requireNonNegative(count, bytes, oldestAgeMillis);
        }

        static QuarantineUsageV1 uniform(long value) {
            return new QuarantineUsageV1(value, value, value);
        }

        QuarantineUsageV1 plus(QuarantineUsageV1 other) {
            return new QuarantineUsageV1(
                    add(count, other.count), add(bytes, other.bytes), Math.max(oldestAgeMillis, other.oldestAgeMillis));
        }

        boolean within(QuarantineUsageV1 limit) {
            return count <= limit.count && bytes <= limit.bytes && oldestAgeMillis <= limit.oldestAgeMillis;
        }
    }

    private static long add(long left, long right) {
        return Math.addExact(left, right);
    }

    private static void requireNonNegative(long... values) {
        for (long value : values) {
            if (value < 0) {
                throw new IllegalArgumentException("per-Cell resource values must be non-negative");
            }
        }
    }
}
