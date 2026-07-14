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
    private final CursorStateMachine stateMachine =
            new CursorStateMachine(CursorStorageConfig.defaults());
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
        return mutate(handle, state -> stateMachine.cumulativeAck(
                state,
                request,
                0,
                mutationEnd(state, List.of(request)),
                nextNow(state)));
    }

    @Override
    public CompletableFuture<CursorMutationResult> individualAck(
            CursorHandle handle,
            List<CursorAckRequest> requests) {
        final List<CursorAckRequest> copied;
        try {
            copied = List.copyOf(requests);
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
        return mutate(handle, state -> stateMachine.individualAck(
                state,
                copied,
                0,
                mutationEnd(state, copied),
                nextNow(state)));
    }

    @Override
    public CompletableFuture<CursorMutationResult> reset(
            CursorHandle handle,
            CursorResetRequest request) {
        return mutate(handle, state -> stateMachine.reset(
                state, request, nextId(), nextNow(state)));
    }

    @Override
    public CompletableFuture<CursorMutationResult> clearBacklog(
            CursorHandle handle,
            long observedCommittedEndOffset) {
        return mutate(handle, state -> stateMachine.clearBacklog(
                state, observedCommittedEndOffset, nextNow(state)));
    }

    @Override
    public CompletableFuture<CursorMutationResult> mutateCursorProperties(
            CursorHandle handle,
            CursorPropertyMutation mutation) {
        return mutate(handle, state -> stateMachine.mutateCursorProperties(
                state, mutation, nextNow(state)));
    }

    @Override
    public CompletableFuture<CursorMutationResult> flushPositionProperties(
            CursorHandle handle,
            Map<String, Long> stagedProperties) {
        final Map<String, Long> copied;
        try {
            copied = Map.copyOf(stagedProperties);
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
        return mutate(handle, state -> stateMachine.flushPositionProperties(
                state, copied, nextNow(state)));
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

    private CompletableFuture<CursorMutationResult> mutate(
            CursorHandle handle,
            Mutation mutation) {
        if (closed) {
            return closedFuture();
        }
        if (handle.isClosed()) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("test cursor handle is closed"));
        }
        return handle.mutationLane().submit(() -> {
            try {
                CursorMutationResult result = mutation.apply(handle.state());
                handle.publish(result.state());
                return CompletableFuture.completedFuture(result);
            } catch (Throwable error) {
                return CompletableFuture.failedFuture(error);
            }
        });
    }

    private static long mutationEnd(
            CursorState state,
            List<CursorAckRequest> requests) {
        long end = state.acknowledgements().markDeleteOffset();
        for (OffsetRange range : state.acknowledgements().wholeAckRanges()) {
            end = Math.max(end, range.endOffset());
        }
        for (long offset : state.acknowledgements().partialBatchAcks().keySet()) {
            end = Math.max(end, Math.addExact(offset, 1));
        }
        for (CursorAckRequest request : requests) {
            end = Math.max(end, Math.addExact(request.entryOffset(), 1));
        }
        return end;
    }

    private static long nextNow(CursorState state) {
        return Math.addExact(state.updatedAtMillis(), 1);
    }

    private static <T> CompletableFuture<T> closedFuture() {
        return CompletableFuture.failedFuture(new IllegalStateException("test cursor storage is closed"));
    }

    private record CursorKey(CursorLedgerIdentity ledger, String cursorName) {
    }

    @FunctionalInterface
    private interface Mutation {
        CursorMutationResult apply(CursorState state) throws Exception;
    }
}
