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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/** Caller-owned owner/event-loop-serialized lifecycle fields for one bounded read batch. */
public final class BindingReadBatchContextV1 {
    private final BindingReadHazardPoolV1 configuredPool;
    private BindingReadHazardPoolV1 pool;
    private BindingReadAuthorityV1 authority;
    private int slotIndex = -1;
    private long leaseWord;
    private long ownerThreadId;
    private long attemptIdentity;
    private int outstandingSourceUses;
    private boolean gateOpen;
    private boolean currentPurityUnitObservable;
    private boolean quarantined;

    public BindingReadBatchContextV1() {
        configuredPool = null;
    }

    public BindingReadBatchContextV1(BindingReadHazardPoolV1 configuredPool) {
        this.configuredPool = Objects.requireNonNull(configuredPool, "configuredPool");
    }

    public BindingReadHazardPoolV1.CaptureOutcome captureFrom(
            AtomicReference<BindingReadAuthorityV1> currentAuthority) {
        if (configuredPool == null) {
            throw new IllegalStateException("multi-Binding target has no configured hazard pool");
        }
        return configuredPool.tryCapture(currentAuthority, this);
    }

    void accept(
            BindingReadHazardPoolV1 valuePool,
            int valueSlotIndex,
            long valueLeaseWord,
            BindingReadAuthorityV1 valueAuthority) {
        if (active()) {
            throw new IllegalStateException("read batch context already owns a hazard lease");
        }
        pool = valuePool;
        slotIndex = valueSlotIndex;
        leaseWord = valueLeaseWord;
        authority = valueAuthority;
        ownerThreadId = Thread.currentThread().getId();
        attemptIdentity = 0;
        outstandingSourceUses = 0;
        gateOpen = true;
        currentPurityUnitObservable = false;
        quarantined = false;
    }

    public boolean active() {
        return leaseWord != 0;
    }

    public BindingReadAuthorityV1 authority() {
        requireActive();
        return authority;
    }

    public long leaseWord() {
        requireActive();
        return leaseWord;
    }

    public int slotIndex() {
        requireActive();
        return slotIndex;
    }

    public boolean beginAttempt(long exactAttemptIdentity) {
        requireOwner();
        if (!gateOpen
                || outstandingSourceUses != 0
                || exactAttemptIdentity <= attemptIdentity
                || !pool.leaseEquals(slotIndex, leaseWord)) {
            return false;
        }
        attemptIdentity = exactAttemptIdentity;
        outstandingSourceUses = 1;
        currentPurityUnitObservable = false;
        return true;
    }

    /** Signals real provider/decode/source-backed-buffer termination for the exact active attempt. */
    public boolean endAttempt(long exactAttemptIdentity) {
        requireOwner();
        if (attemptIdentity != exactAttemptIdentity
                || outstandingSourceUses != 1
                || !pool.leaseEquals(slotIndex, leaseWord)) {
            return false;
        }
        outstandingSourceUses = 0;
        return true;
    }

    public void markObservable() {
        requireOwner();
        if (outstandingSourceUses != 1) {
            throw new IllegalStateException("only an active source-purity unit can become observable");
        }
        currentPurityUnitObservable = true;
    }

    public boolean observable() {
        requireActive();
        return currentPurityUnitObservable;
    }

    public void closeNewSourceUse() {
        requireOwner();
        gateOpen = false;
    }

    public boolean newSourceUseOpen() {
        requireActive();
        return gateOpen;
    }

    public void quarantine() {
        requireOwner();
        gateOpen = false;
        quarantined = true;
    }

    public boolean quarantined() {
        requireActive();
        return quarantined;
    }

    public boolean terminalClearExactLease() {
        requireOwner();
        if (gateOpen || outstandingSourceUses != 0 || quarantined) {
            return false;
        }
        if (!pool.terminalClear(slotIndex, leaseWord)) {
            return false;
        }
        resetAfterClear();
        return true;
    }

    void abortCapture() {
        requireOwner();
        gateOpen = false;
        if (!pool.terminalClear(slotIndex, leaseWord)) {
            throw new IllegalStateException("failed to clear an unused unstable capture");
        }
        resetAfterClear();
    }

    private void resetAfterClear() {
        pool = null;
        authority = null;
        slotIndex = -1;
        leaseWord = 0;
        ownerThreadId = 0;
        attemptIdentity = 0;
        outstandingSourceUses = 0;
        gateOpen = false;
        currentPurityUnitObservable = false;
        quarantined = false;
    }

    private void requireActive() {
        if (!active()) {
            throw new IllegalStateException("read batch context has no active hazard lease");
        }
    }

    private void requireOwner() {
        requireActive();
        if (ownerThreadId != Thread.currentThread().getId()) {
            throw new IllegalStateException("read batch lifecycle must remain owner/event-loop serialized");
        }
    }
}
