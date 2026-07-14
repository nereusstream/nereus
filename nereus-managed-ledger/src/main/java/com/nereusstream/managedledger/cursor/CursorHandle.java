/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.mledger.ManagedLedgerException;

/** Local owner-scoped handle over one authoritative durable cursor generation. */
public final class CursorHandle {
    private final CursorIdentity identity;
    private final CursorOwnerSession owner;
    private final AtomicReference<CursorState> state;
    private final CursorMutationLane mutationLane;
    private final AtomicBoolean closed = new AtomicBoolean();

    CursorHandle(
            CursorState initialState,
            CursorOwnerSession owner,
            int maximumPendingMutations,
            Executor laneExecutor) {
        CursorState exactState = Objects.requireNonNull(initialState, "initialState");
        this.owner = Objects.requireNonNull(owner, "owner");
        if (!exactState.identity().ledger().equals(owner.ledger())
                || !exactState.ownerSessionId().equals(owner.ownerSessionId())
                || exactState.lifecycle() != CursorLifecycle.ACTIVE) {
            throw new IllegalArgumentException("cursor handle state does not match its active owner");
        }
        identity = exactState.identity();
        state = new AtomicReference<>(exactState);
        mutationLane = new CursorMutationLane(maximumPendingMutations, laneExecutor);
    }

    public CursorIdentity identity() {
        return identity;
    }

    public CursorOwnerSession owner() {
        return owner;
    }

    public CursorState state() {
        return state.get();
    }

    public CompletableFuture<Void> closeAsync() {
        if (closed.compareAndSet(false, true)) {
            mutationLane.close(new ManagedLedgerException.CursorAlreadyClosedException(
                    "durable cursor handle is closed"));
        }
        return mutationLane.whenDrained();
    }

    boolean isClosed() {
        return closed.get();
    }

    CursorMutationLane mutationLane() {
        return mutationLane;
    }

    void publish(CursorState candidate) {
        Objects.requireNonNull(candidate, "candidate");
        if (!candidate.identity().equals(identity)
                || !candidate.ownerSessionId().equals(owner.ownerSessionId())) {
            throw new IllegalArgumentException("cursor completion does not match handle identity or owner");
        }
        state.getAndUpdate(current -> candidate.mutationSequence() >= current.mutationSequence()
                ? candidate
                : current);
        if (candidate.lifecycle() == CursorLifecycle.DELETED) {
            closeAsync();
        }
    }
}
