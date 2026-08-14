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

package com.nereusstream.storage.bookkeeper;

import com.nereusstream.storage.api.bookkeeper.RetainedStoragePayload;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/** Cell-local admission and ownership kernel shared by real provider operations and exact reconciliation. */
public final class CellSessionOperationRegistry {
    public enum State {
        OPEN,
        DRAINING,
        CLOSED
    }

    private State state = State.OPEN;
    private boolean closeRequested;
    private long acceptedOperations;
    private long retainedPayloadBytes;
    private final CompletableFuture<Void> drained = new CompletableFuture<>();
    private final CompletableFuture<Void> closed = new CompletableFuture<>();

    public synchronized OperationLease acceptOperation() {
        ensureOpen();
        acceptedOperations = Math.incrementExact(acceptedOperations);
        return new OperationLease(this, null, 0);
    }

    public synchronized OperationLease acceptAppend(RetainedStoragePayload callerPayload) {
        Objects.requireNonNull(callerPayload, "callerPayload");
        ensureOpen();
        int payloadBytes = callerPayload.readableBytes();
        if (payloadBytes <= 0) {
            throw new IllegalArgumentException("accepted append payload must be non-empty");
        }
        RetainedStoragePayload sessionReference = callerPayload.retain();
        try {
            long nextAcceptedOperations = Math.incrementExact(acceptedOperations);
            long nextRetainedPayloadBytes = Math.addExact(retainedPayloadBytes, payloadBytes);
            acceptedOperations = nextAcceptedOperations;
            retainedPayloadBytes = nextRetainedPayloadBytes;
        } catch (RuntimeException failure) {
            sessionReference.release();
            throw failure;
        }
        return new OperationLease(this, sessionReference, payloadBytes);
    }

    public synchronized CompletionStage<Void> drain() {
        if (state == State.OPEN) {
            state = State.DRAINING;
        }
        completeLifecycleIfIdle();
        return drained;
    }

    public synchronized CompletionStage<Void> closeAsync() {
        closeRequested = true;
        if (state == State.OPEN) {
            state = State.DRAINING;
        }
        completeLifecycleIfIdle();
        return closed;
    }

    public synchronized State state() {
        return state;
    }

    public synchronized long acceptedOperations() {
        return acceptedOperations;
    }

    public synchronized long retainedPayloadBytes() {
        return retainedPayloadBytes;
    }

    private void ensureOpen() {
        if (state != State.OPEN) {
            throw new RejectedExecutionException("Cell session no longer accepts operations: " + state);
        }
    }

    private synchronized void terminal(OperationLease lease) {
        try {
            if (lease.sessionPayload != null) {
                lease.sessionPayload.release();
            }
        } finally {
            acceptedOperations = Math.decrementExact(acceptedOperations);
            retainedPayloadBytes = Math.subtractExact(retainedPayloadBytes, lease.payloadBytes);
            completeLifecycleIfIdle();
        }
    }

    private void completeLifecycleIfIdle() {
        if (state != State.OPEN && acceptedOperations == 0) {
            drained.complete(null);
            if (closeRequested) {
                state = State.CLOSED;
                closed.complete(null);
            }
        }
    }

    /** One accepted operation. It remains owned until the exact resolver declares a terminal outcome. */
    public static final class OperationLease {
        private final CellSessionOperationRegistry owner;
        private final RetainedStoragePayload sessionPayload;
        private final int payloadBytes;
        private final AtomicBoolean terminal = new AtomicBoolean();

        private OperationLease(
                CellSessionOperationRegistry owner, RetainedStoragePayload sessionPayload, int payloadBytes) {
            this.owner = owner;
            this.sessionPayload = sessionPayload;
            this.payloadBytes = payloadBytes;
        }

        /** Returns true exactly once; observer cancellation and response loss must not invoke this method. */
        public boolean resolveTerminal() {
            if (!terminal.compareAndSet(false, true)) {
                return false;
            }
            owner.terminal(this);
            return true;
        }

        public boolean isTerminal() {
            return terminal.get();
        }
    }
}
