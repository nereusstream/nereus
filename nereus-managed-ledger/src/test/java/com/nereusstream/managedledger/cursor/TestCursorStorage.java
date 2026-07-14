/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import com.nereusstream.metadata.oxia.CursorNames;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/** Lightweight owner-aware cursor storage used by managed-ledger facade unit tests. */
public final class TestCursorStorage implements CursorStorage {
    private final Map<CursorKey, CursorHandle> handles = new ConcurrentHashMap<>();
    private final AtomicLong ids = new AtomicLong();
    private volatile boolean closed;

    @Override
    public CompletableFuture<CursorHandle> open(
            CursorOwnerSession owner,
            String cursorName,
            CursorOpenRequest request) {
        if (closed) {
            return closedFuture();
        }
        try {
            String exactName = CursorNames.requireCursorName(cursorName);
            CursorKey key = new CursorKey(owner.ledger(), exactName);
            CursorHandle handle = handles.compute(key, (ignored, current) -> {
                if (current == null) {
                    return newHandle(new CursorState(
                            new CursorIdentity(
                                    owner.ledger(),
                                    exactName,
                                    CursorNames.cursorNameHash(exactName),
                                    1),
                            owner.ownerSessionId(),
                            CursorLifecycle.ACTIVE,
                            1,
                            1,
                            nextId(),
                            CursorAckState.empty(request.initialMarkDeleteOffset()),
                            request.initialPositionProperties(),
                            request.initialCursorProperties(),
                            Optional.empty(),
                            0,
                            0,
                            0), owner);
                }
                return claim(current, owner);
            });
            return CompletableFuture.completedFuture(handle);
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
    }

    @Override
    public CompletableFuture<List<CursorHandle>> claimAndLoadActiveCursors(CursorOwnerSession owner) {
        if (closed) {
            return closedFuture();
        }
        List<CursorHandle> claimed = new ArrayList<>();
        handles.forEach((key, current) -> {
            if (!key.ledger().equals(owner.ledger())) {
                return;
            }
            handles.computeIfPresent(key, (ignored, latest) -> {
                CursorHandle replacement = claim(latest, owner);
                claimed.add(replacement);
                return replacement;
            });
        });
        return CompletableFuture.completedFuture(List.copyOf(claimed));
    }

    @Override
    public CompletableFuture<CursorMutationResult> cumulativeAck(
            CursorHandle handle,
            CursorAckRequest request) {
        return unsupported("cumulativeAck");
    }

    @Override
    public CompletableFuture<CursorMutationResult> individualAck(
            CursorHandle handle,
            List<CursorAckRequest> requests) {
        return unsupported("individualAck");
    }

    @Override
    public CompletableFuture<CursorMutationResult> reset(
            CursorHandle handle,
            CursorResetRequest request) {
        return unsupported("reset");
    }

    @Override
    public CompletableFuture<CursorMutationResult> clearBacklog(
            CursorHandle handle,
            long observedCommittedEndOffset) {
        return unsupported("clearBacklog");
    }

    @Override
    public CompletableFuture<CursorMutationResult> mutateCursorProperties(
            CursorHandle handle,
            CursorPropertyMutation mutation) {
        return unsupported("mutateCursorProperties");
    }

    @Override
    public CompletableFuture<CursorMutationResult> flushPositionProperties(
            CursorHandle handle,
            Map<String, Long> stagedProperties) {
        return unsupported("flushPositionProperties");
    }

    @Override
    public CompletableFuture<Void> delete(CursorOwnerSession owner, String cursorName) {
        handles.remove(new CursorKey(owner.ledger(), cursorName));
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<CursorRetentionView> retentionView(CursorOwnerSession owner) {
        return CompletableFuture.completedFuture(new CursorRetentionView(
                owner.ledger(),
                owner.ownerSessionId(),
                CursorRetentionView.Lifecycle.ACTIVE,
                1,
                0,
                0,
                0,
                Optional.empty(),
                Optional.empty()));
    }

    @Override
    public void close() {
        closed = true;
        handles.values().forEach(CursorHandle::closeAsync);
        handles.clear();
    }

    private CursorHandle claim(CursorHandle current, CursorOwnerSession owner) {
        CursorState state = current.state();
        if (current.owner().equals(owner) && !current.isClosed()) {
            return current;
        }
        CursorState claimed = new CursorState(
                state.identity(),
                owner.ownerSessionId(),
                state.lifecycle(),
                Math.addExact(state.mutationSequence(), 1),
                state.ackStateEpoch(),
                state.lastProtectionAttemptId(),
                state.acknowledgements(),
                state.positionProperties(),
                state.cursorProperties(),
                state.snapshotReference(),
                state.createdAtMillis(),
                Math.addExact(state.updatedAtMillis(), 1),
                Math.addExact(state.metadataVersion(), 1));
        return newHandle(claimed, owner);
    }

    private static CursorHandle newHandle(CursorState state, CursorOwnerSession owner) {
        return new CursorHandle(state, owner, 1_024, Runnable::run);
    }

    private String nextId() {
        return String.format("%032x", ids.incrementAndGet());
    }

    private static <T> CompletableFuture<T> unsupported(String operation) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException(operation));
    }

    private static <T> CompletableFuture<T> closedFuture() {
        return CompletableFuture.failedFuture(new IllegalStateException("test cursor storage is closed"));
    }

    private record CursorKey(CursorLedgerIdentity ledger, String cursorName) {
    }
}
