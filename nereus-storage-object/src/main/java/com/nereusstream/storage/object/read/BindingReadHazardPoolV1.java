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

package com.nereusstream.storage.object.read;

import com.nereusstream.domain.identity.TopicBindingId;
import java.lang.invoke.VarHandle;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

/** Bounded cross-Binding hazard slots with full-fence capture and exact-lease terminal clear. */
public final class BindingReadHazardPoolV1 {
    public enum CaptureOutcome {
        CAPTURED,
        ADMISSION_CLOSED,
        UNSTABLE,
        EXHAUSTED
    }

    public enum ScanOutcome {
        CLEAN,
        PINNED,
        INCONCLUSIVE
    }

    private final AtomicLongArray leaseWords;
    private final AtomicLongArray nextLeaseWords;
    private final AtomicLongArray payloadLeaseWords;
    private final AtomicLongArray sourceGenerations;
    private final AtomicReferenceArray<TopicBindingId> bindingIds;
    private final AtomicIntegerArray retired;
    private final int maximumCaptureRetries;
    private final Runnable afterLeaseClaimBeforePayload;
    private final AtomicBoolean admissionClosed = new AtomicBoolean();

    public BindingReadHazardPoolV1(int capacity, int maximumCaptureRetries) {
        this(capacity, maximumCaptureRetries, 1, null);
    }

    BindingReadHazardPoolV1(
            int capacity, int maximumCaptureRetries, long initialLeaseWord, Runnable afterLeaseClaimBeforePayload) {
        if (capacity <= 0 || capacity > 65_536 || maximumCaptureRetries <= 0 || maximumCaptureRetries > 64) {
            throw new IllegalArgumentException("hazard-pool capacity/retry cap is outside the admitted bound");
        }
        if (initialLeaseWord <= 0) {
            throw new IllegalArgumentException("initial lease word must be positive");
        }
        leaseWords = new AtomicLongArray(capacity);
        nextLeaseWords = new AtomicLongArray(capacity);
        payloadLeaseWords = new AtomicLongArray(capacity);
        sourceGenerations = new AtomicLongArray(capacity);
        bindingIds = new AtomicReferenceArray<>(capacity);
        retired = new AtomicIntegerArray(capacity);
        this.maximumCaptureRetries = maximumCaptureRetries;
        this.afterLeaseClaimBeforePayload = afterLeaseClaimBeforePayload;
        for (int index = 0; index < capacity; index++) {
            nextLeaseWords.set(index, initialLeaseWord);
        }
    }

    public int capacity() {
        return leaseWords.length();
    }

    /** Stops new capture without clearing, reassigning, or otherwise weakening any live lease. */
    public void closeAdmission() {
        admissionClosed.set(true);
    }

    public CaptureOutcome tryCapture(
            AtomicReference<BindingReadAuthorityV1> currentAuthority, BindingReadBatchContextV1 target) {
        Objects.requireNonNull(currentAuthority, "currentAuthority");
        Objects.requireNonNull(target, "target");
        if (target.active()) {
            throw new IllegalArgumentException("capture target already owns a lease");
        }
        for (int retry = 0; retry < maximumCaptureRetries; retry++) {
            if (admissionClosed.get()) {
                return CaptureOutcome.ADMISSION_CLOSED;
            }
            BindingReadAuthorityV1 authority = currentAuthority.get();
            if (authority == null || !authority.admitting()) {
                return CaptureOutcome.ADMISSION_CLOSED;
            }
            int slot = claimFreeSlot(authority.bindingId(), authority.sourceGeneration(), retry);
            if (slot < 0) {
                return CaptureOutcome.EXHAUSTED;
            }
            long lease = leaseWords.get(slot);
            target.accept(this, slot, lease, authority);

            VarHandle.fullFence();
            BindingReadAuthorityV1 revalidated = currentAuthority.get();
            BindingReadPublicationCellV1 cell = authority.publicationCell();
            if (revalidated == authority && cell.sourceGeneration() == authority.sourceGeneration()) {
                return CaptureOutcome.CAPTURED;
            }
            target.abortCapture();
        }
        return CaptureOutcome.UNSTABLE;
    }

    /** All-or-release reservation; caller supplies reusable arrays and no provider I/O may precede success. */
    public static CaptureOutcome tryCaptureAll(
            AtomicReference<BindingReadAuthorityV1>[] authorities, BindingReadBatchContextV1[] targets, int count) {
        Objects.requireNonNull(authorities, "authorities");
        Objects.requireNonNull(targets, "targets");
        if (count <= 0 || count > authorities.length || count > targets.length) {
            throw new IllegalArgumentException("multi-Binding reservation count is outside supplied storage");
        }
        for (int index = 0; index < count; index++) {
            CaptureOutcome outcome = Objects.requireNonNull(targets[index], "target")
                    .captureFrom(Objects.requireNonNull(authorities[index], "authority"));
            if (outcome != CaptureOutcome.CAPTURED) {
                releaseUnused(targets, index);
                return outcome;
            }
        }
        return CaptureOutcome.CAPTURED;
    }

    public ScanOutcome scan(TopicBindingId bindingId, long sourceGeneration) {
        Objects.requireNonNull(bindingId, "bindingId");
        if (sourceGeneration <= 0) {
            throw new IllegalArgumentException("source generation must be positive");
        }
        for (int index = 0; index < leaseWords.length(); index++) {
            if (retired.get(index) != 0) {
                continue;
            }
            long firstLease = leaseWords.get(index);
            if (firstLease == 0) {
                continue;
            }
            long payloadLease = payloadLeaseWords.get(index);
            TopicBindingId observedBinding = bindingIds.get(index);
            long observedGeneration = sourceGenerations.get(index);
            long secondLease = leaseWords.get(index);
            if (firstLease != secondLease
                    || payloadLease != firstLease
                    || observedBinding == null
                    || observedGeneration <= 0) {
                return ScanOutcome.INCONCLUSIVE;
            }
            if (bindingId.equals(observedBinding) && observedGeneration == sourceGeneration) {
                return ScanOutcome.PINNED;
            }
        }
        return ScanOutcome.CLEAN;
    }

    boolean leaseEquals(int slotIndex, long exactLease) {
        return exactLease != 0 && leaseWords.get(slotIndex) == exactLease;
    }

    boolean terminalClear(int slotIndex, long exactLease) {
        if (!leaseEquals(slotIndex, exactLease)) {
            return false;
        }
        if (exactLease == Long.MAX_VALUE) {
            bindingIds.set(slotIndex, null);
            sourceGenerations.set(slotIndex, 0);
            retired.set(slotIndex, 1);
            return true;
        }
        nextLeaseWords.set(slotIndex, exactLease + 1);
        return leaseWords.compareAndSet(slotIndex, exactLease, 0);
    }

    private int claimFreeSlot(TopicBindingId bindingId, long sourceGeneration, int retry) {
        int capacity = leaseWords.length();
        int start = Math.floorMod(System.identityHashCode(bindingId) + retry, capacity);
        for (int step = 0; step < capacity; step++) {
            int index = (start + step) % capacity;
            if (retired.get(index) != 0 || leaseWords.get(index) != 0) {
                continue;
            }
            long candidate = nextLeaseWords.get(index);
            if (candidate <= 0 || !leaseWords.compareAndSet(index, 0, candidate)) {
                continue;
            }
            if (afterLeaseClaimBeforePayload != null) {
                afterLeaseClaimBeforePayload.run();
            }
            bindingIds.set(index, bindingId);
            sourceGenerations.set(index, sourceGeneration);
            payloadLeaseWords.set(index, candidate);
            return index;
        }
        return -1;
    }

    private static void releaseUnused(BindingReadBatchContextV1[] targets, int capturedCount) {
        for (int index = capturedCount - 1; index >= 0; index--) {
            BindingReadBatchContextV1 target = targets[index];
            target.closeNewSourceUse();
            if (!target.terminalClearExactLease()) {
                throw new IllegalStateException("failed to release partial multi-Binding reservation");
            }
        }
    }
}
