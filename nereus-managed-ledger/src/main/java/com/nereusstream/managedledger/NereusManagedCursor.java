/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger;

import com.google.common.collect.Range;
import com.nereusstream.api.StreamMetadata;
import com.nereusstream.api.StreamState;
import com.nereusstream.managedledger.cursor.BatchAckState;
import com.nereusstream.managedledger.cursor.CursorAckRequest;
import com.nereusstream.managedledger.cursor.CursorAckState;
import com.nereusstream.managedledger.cursor.CursorHandle;
import com.nereusstream.managedledger.cursor.CursorIdentity;
import com.nereusstream.managedledger.cursor.CursorLifecycle;
import com.nereusstream.managedledger.cursor.CursorMutationResult;
import com.nereusstream.managedledger.cursor.CursorPropertyMutation;
import com.nereusstream.managedledger.cursor.CursorResetRequest;
import com.nereusstream.managedledger.cursor.CursorState;
import com.nereusstream.managedledger.cursor.CursorStateMachine;
import com.nereusstream.managedledger.cursor.OffsetRange;
import com.nereusstream.managedledger.errors.ManagedLedgerErrorMapper;
import com.nereusstream.managedledger.errors.OperationContext;
import com.nereusstream.managedledger.snapshot.PendingReadWaiter;
import com.nereusstream.metadata.oxia.CursorNames;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedCursorAttributes;
import org.apache.bookkeeper.mledger.ManagedCursorMXBean;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.ScanOutcome;
import org.apache.bookkeeper.mledger.impl.AckSetStateUtil;
import org.apache.bookkeeper.mledger.util.ManagedLedgerUtils;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats;

/** F3 dual-mode cursor facade with durable ack truth and broker-local dispatch position. */
public final class NereusManagedCursor implements ManagedCursor {
    private enum Mode {
        DURABLE,
        NON_DURABLE
    }

    private enum LocalLifecycle {
        OPEN,
        CLOSING,
        CLOSED,
        CLOSE_FAILED
    }

    @FunctionalInterface
    private interface LocalMutation {
        CursorMutationResult apply(CursorState state) throws Exception;
    }

    private record PropertyCapture(long revision, Map<String, Long> values) {
    }

    private record PreparedAck(CursorAckRequest request, StreamMetadata metadata) {
    }

    private record ReadBatch(List<Entry> entries, long nextOffset) {
    }

    private static final SecureRandom RANDOM = new SecureRandom();

    private final NereusManagedLedger ledger;
    private final String name;
    private final Mode mode;
    private final CursorHandle durableHandle;
    private final AtomicReference<CursorState> localState;
    private final CursorStateMachine stateMachine;
    private final ManagedLedgerErrorMapper errors = new ManagedLedgerErrorMapper();
    private final NereusManagedCursorStats stats;
    private final AtomicReference<LocalLifecycle> lifecycle =
            new AtomicReference<>(LocalLifecycle.OPEN);
    private final AtomicReference<Pending> pendingRead = new AtomicReference<>();
    private final AtomicLong localReadOffset;
    private final AtomicLong readEpoch = new AtomicLong();
    private final AtomicLong lastActive = new AtomicLong(System.currentTimeMillis());
    private final AtomicLong readEntriesCount = new AtomicLong();
    private final AtomicLong readEntriesSize = new AtomicLong();
    private final AtomicLong priorReadPositionSample = new AtomicLong(Long.MIN_VALUE);
    private final Object readLaneLock = new Object();
    private final Object localMutationLock = new Object();
    private final Object propertyStageLock = new Object();
    private final Object closeLock = new Object();

    private CompletableFuture<Void> readTail = CompletableFuture.completedFuture(null);
    private CompletableFuture<Void> localMutationTail = CompletableFuture.completedFuture(null);
    private CompletableFuture<Void> closeFuture;
    private int localPendingMutations;
    private boolean localMutationLaneClosed;
    private Map<String, Long> stagedPositionProperties;
    private long positionPropertyStageRevision;
    private volatile boolean active = true;
    private volatile boolean alwaysInactive;
    private volatile double throttleMarkDelete;
    private volatile ManagedCursorAttributes attributes;

    NereusManagedCursor(NereusManagedLedger ledger, CursorHandle handle) {
        this.ledger = Objects.requireNonNull(ledger, "ledger");
        this.durableHandle = Objects.requireNonNull(handle, "handle");
        this.name = handle.identity().cursorName();
        this.mode = Mode.DURABLE;
        this.localState = null;
        this.stateMachine = new CursorStateMachine(ledger.runtime().cursorStorageConfig());
        this.localReadOffset = new AtomicLong(firstUnackedOffset(handle.state().acknowledgements()));
        this.stats = new NereusManagedCursorStats(name, ledger.getName());
    }

    NereusManagedCursor(
            NereusManagedLedger ledger,
            String name,
            CursorAckState acknowledgements,
            long initialReadOffset,
            Map<String, Long> positionProperties,
            Map<String, String> cursorProperties) {
        this.ledger = Objects.requireNonNull(ledger, "ledger");
        this.name = CursorNames.requireCursorName(name);
        this.mode = Mode.NON_DURABLE;
        this.durableHandle = null;
        this.stateMachine = new CursorStateMachine(ledger.runtime().cursorStorageConfig());
        long now = nonNegativeNow();
        CursorState initial = new CursorState(
                new CursorIdentity(
                        ledger.cursorOwnerSession().ledger(),
                        this.name,
                        CursorNames.cursorNameHash(this.name),
                        1),
                ledger.cursorOwnerSession().ownerSessionId(),
                CursorLifecycle.ACTIVE,
                1,
                1,
                randomId(),
                Objects.requireNonNull(acknowledgements, "acknowledgements"),
                positionProperties == null ? Map.of() : positionProperties,
                cursorProperties == null ? Map.of() : cursorProperties,
                Optional.empty(),
                now,
                now,
                0);
        this.localState = new AtomicReference<>(initial);
        this.localReadOffset = new AtomicLong(initialReadOffset);
        this.stats = new NereusManagedCursorStats(this.name, ledger.getName());
    }

    CursorHandle durableHandle() {
        return durableHandle;
    }

    CursorAckState acknowledgementState() {
        return state().acknowledgements();
    }

    long localReadOffset() {
        return localReadOffset.get();
    }

    void closeDetached() {
        lifecycle.set(LocalLifecycle.CLOSED);
        cancelPendingRead(new ManagedLedgerException.ManagedLedgerAlreadyClosedException(
                "detached cursor facade is closed"));
    }

    CompletableFuture<Void> closeAfterDelete() {
        synchronized (closeLock) {
            if (closeFuture != null) {
                return closeFuture;
            }
            lifecycle.set(LocalLifecycle.CLOSING);
            cancelPendingRead(new ManagedLedgerException.CursorAlreadyClosedException(
                    "cursor was deleted"));
            closeFuture = finishClose(CompletableFuture.completedFuture(null), false);
            return closeFuture;
        }
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public long getLastActive() {
        return lastActive.get();
    }

    @Override
    public void updateLastActive() {
        lastActive.set(System.currentTimeMillis());
    }

    @Override
    public Map<String, Long> getProperties() {
        synchronized (propertyStageLock) {
            return stagedPositionProperties == null
                    ? Map.copyOf(state().positionProperties())
                    : Map.copyOf(stagedPositionProperties);
        }
    }

    @Override
    public Map<String, String> getCursorProperties() {
        return Map.copyOf(state().cursorProperties());
    }

    @Override
    public CompletableFuture<Void> putCursorProperty(String key, String value) {
        final CursorPropertyMutation mutation;
        try {
            mutation = new CursorPropertyMutation.Put(key, value);
        } catch (Throwable error) {
            return mappedFailure(error, "putCursorProperty");
        }
        return mutateCursorProperties(mutation).thenApply(ignored -> null);
    }

    @Override
    public CompletableFuture<Void> setCursorProperties(Map<String, String> values) {
        final CursorPropertyMutation mutation;
        try {
            mutation = new CursorPropertyMutation.ReplaceExternal(values == null ? Map.of() : values);
        } catch (Throwable error) {
            return mappedFailure(error, "setCursorProperties");
        }
        return mutateCursorProperties(mutation).thenApply(ignored -> null);
    }

    @Override
    public CompletableFuture<Void> removeCursorProperty(String key) {
        final CursorPropertyMutation mutation;
        try {
            mutation = new CursorPropertyMutation.Remove(key);
        } catch (Throwable error) {
            return mappedFailure(error, "removeCursorProperty");
        }
        return mutateCursorProperties(mutation).thenApply(ignored -> null);
    }

    @Override
    public boolean putProperty(String key, Long value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        synchronized (propertyStageLock) {
            if (lifecycle.get() != LocalLifecycle.OPEN) {
                return false;
            }
            Map<String, Long> visible = visiblePositionPropertiesLocked();
            LinkedHashMap<String, Long> candidate = new LinkedHashMap<>(visible);
            candidate.put(key, value);
            if (!candidate.equals(visible)) {
                positionPropertyStageRevision = Math.addExact(positionPropertyStageRevision, 1);
            }
            stagedPositionProperties = Collections.unmodifiableMap(candidate);
            return true;
        }
    }

    @Override
    public boolean removeProperty(String key) {
        Objects.requireNonNull(key, "key");
        synchronized (propertyStageLock) {
            if (lifecycle.get() != LocalLifecycle.OPEN) {
                return false;
            }
            Map<String, Long> visible = visiblePositionPropertiesLocked();
            LinkedHashMap<String, Long> candidate = new LinkedHashMap<>(visible);
            candidate.remove(key);
            if (!candidate.equals(visible)) {
                positionPropertyStageRevision = Math.addExact(positionPropertyStageRevision, 1);
            }
            stagedPositionProperties = Collections.unmodifiableMap(candidate);
            return true;
        }
    }

    @Override
    public List<Entry> readEntries(int count) throws InterruptedException, ManagedLedgerException {
        return await(readEntriesFuture(count, ManagedLedgerUtils.NO_MAX_SIZE_LIMIT, null, null));
    }

    @Override
    public void asyncReadEntries(
            int count,
            AsyncCallbacks.ReadEntriesCallback callback,
            Object ctx,
            Position maxPosition) {
        asyncReadEntries(count, ManagedLedgerUtils.NO_MAX_SIZE_LIMIT, callback, ctx, maxPosition);
    }

    @Override
    public void asyncReadEntries(
            int count,
            long maxSizeBytes,
            AsyncCallbacks.ReadEntriesCallback callback,
            Object ctx,
            Position maxPosition) {
        asyncRead(count, maxSizeBytes, callback, ctx, maxPosition, null, false);
    }

    @Override
    public void asyncReadEntriesWithSkip(
            int count,
            long maxSizeBytes,
            AsyncCallbacks.ReadEntriesCallback callback,
            Object ctx,
            Position maxPosition,
            Predicate<Position> skipCondition) {
        asyncRead(count, maxSizeBytes, callback, ctx, maxPosition, skipCondition, false);
    }

    @Override
    public Entry getNthEntry(int n, IndividualDeletedEntries deletedEntries)
            throws InterruptedException, ManagedLedgerException {
        return await(nthEntryFuture(n, deletedEntries));
    }

    @Override
    public void asyncGetNthEntry(
            int n,
            IndividualDeletedEntries deletedEntries,
            AsyncCallbacks.ReadEntryCallback callback,
            Object ctx) {
        Objects.requireNonNull(callback, "callback");
        nthEntryFuture(n, deletedEntries).whenComplete((entry, error) ->
                executeCallback(() -> {
                    if (error == null) {
                        try {
                            callback.readEntryComplete(entry, ctx);
                        } catch (Throwable callbackError) {
                            if (entry != null) {
                                entry.release();
                            }
                        }
                    } else {
                        callback.readEntryFailed(map(error, "getNthEntry"), ctx);
                    }
                }));
    }

    @Override
    public List<Entry> readEntriesOrWait(int count)
            throws InterruptedException, ManagedLedgerException {
        return readEntriesOrWait(count, ManagedLedgerUtils.NO_MAX_SIZE_LIMIT);
    }

    @Override
    public List<Entry> readEntriesOrWait(int count, long maxSizeBytes)
            throws InterruptedException, ManagedLedgerException {
        CompletableFuture<List<Entry>> future = new CompletableFuture<>();
        asyncReadEntriesOrWait(count, maxSizeBytes, new AsyncCallbacks.ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                future.complete(entries);
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                future.completeExceptionally(exception);
            }
        }, null, null);
        return await(future);
    }

    @Override
    public void asyncReadEntriesOrWait(
            int count,
            AsyncCallbacks.ReadEntriesCallback callback,
            Object ctx,
            Position maxPosition) {
        asyncReadEntriesOrWait(
                count, ManagedLedgerUtils.NO_MAX_SIZE_LIMIT, callback, ctx, maxPosition);
    }

    @Override
    public void asyncReadEntriesOrWait(
            int count,
            long maxSizeBytes,
            AsyncCallbacks.ReadEntriesCallback callback,
            Object ctx,
            Position maxPosition) {
        asyncRead(count, maxSizeBytes, callback, ctx, maxPosition, null, true);
    }

    @Override
    public void asyncReadEntriesWithSkipOrWait(
            int count,
            AsyncCallbacks.ReadEntriesCallback callback,
            Object ctx,
            Position maxPosition,
            Predicate<Position> skipCondition) {
        asyncRead(
                count,
                ManagedLedgerUtils.NO_MAX_SIZE_LIMIT,
                callback,
                ctx,
                maxPosition,
                skipCondition,
                true);
    }

    @Override
    public void asyncReadEntriesWithSkipOrWait(
            int count,
            long maxSizeBytes,
            AsyncCallbacks.ReadEntriesCallback callback,
            Object ctx,
            Position maxPosition,
            Predicate<Position> skipCondition) {
        asyncRead(count, maxSizeBytes, callback, ctx, maxPosition, skipCondition, true);
    }

    @Override
    public boolean cancelPendingReadRequest() {
        Pending waiter = pendingRead.get();
        return waiter != null && waiter.cancel();
    }

    @Override
    public boolean hasMoreEntries() {
        if (lifecycle.get() != LocalLifecycle.OPEN) {
            return false;
        }
        StreamMetadata metadata = ledger.currentMetadata();
        return nextDispatchableOffset(
                        Math.max(localReadOffset.get(), metadata.trimOffset()),
                        state().acknowledgements())
                < metadata.committedEndOffset();
    }

    @Override
    public long getNumberOfEntries() {
        StreamMetadata metadata = ledger.currentMetadata();
        long start = Math.max(localReadOffset.get(), metadata.trimOffset());
        return outstandingEntries(state().acknowledgements(), start, metadata.committedEndOffset());
    }

    @Override
    public long getNumberOfEntriesInBacklog(boolean precise) {
        StreamMetadata metadata = ledger.currentMetadata();
        CursorAckState ack = state().acknowledgements();
        long firstRetained = Math.max(ack.markDeleteOffset(), metadata.trimOffset());
        return outstandingEntries(ack, firstRetained, metadata.committedEndOffset());
    }

    @Override
    public void markDelete(Position position) throws InterruptedException, ManagedLedgerException {
        await(markDeleteFuture(position, null));
    }

    @Override
    public void markDelete(Position position, Map<String, Long> properties)
            throws InterruptedException, ManagedLedgerException {
        await(markDeleteFuture(position, properties));
    }

    @Override
    public void asyncMarkDelete(
            Position position,
            AsyncCallbacks.MarkDeleteCallback callback,
            Object ctx) {
        asyncMarkDelete(position, null, callback, ctx);
    }

    @Override
    public void asyncMarkDelete(
            Position position,
            Map<String, Long> properties,
            AsyncCallbacks.MarkDeleteCallback callback,
            Object ctx) {
        Objects.requireNonNull(callback, "callback");
        markDeleteFuture(position, properties).whenComplete((ignored, error) ->
                executeCallback(() -> {
                    if (error == null) {
                        callback.markDeleteComplete(ctx);
                    } else {
                        callback.markDeleteFailed(map(error, "markDelete"), ctx);
                    }
                }));
    }

    @Override
    public void delete(Position position) throws InterruptedException, ManagedLedgerException {
        await(individualAckFuture(List.of(copyPosition(position))));
    }

    @Override
    public void asyncDelete(
            Position position,
            AsyncCallbacks.DeleteCallback callback,
            Object ctx) {
        asyncDelete(List.of(copyPosition(position)), callback, ctx);
    }

    @Override
    public void delete(Iterable<Position> positions)
            throws InterruptedException, ManagedLedgerException {
        await(individualAckFuture(copyPositions(positions)));
    }

    @Override
    public void asyncDelete(
            Iterable<Position> positions,
            AsyncCallbacks.DeleteCallback callback,
            Object ctx) {
        Objects.requireNonNull(callback, "callback");
        final List<Position> copied;
        try {
            copied = copyPositions(positions);
        } catch (Throwable error) {
            executeCallback(() -> callback.deleteFailed(map(error, "individualAck"), ctx));
            return;
        }
        individualAckFuture(copied).whenComplete((ignored, error) ->
                executeCallback(() -> {
                    if (error == null) {
                        callback.deleteComplete(ctx);
                    } else {
                        callback.deleteFailed(map(error, "individualAck"), ctx);
                    }
                }));
    }

    @Override
    public Position getReadPosition() {
        return PositionFactory.create(ledger.projection().virtualLedgerId(), localReadOffset.get());
    }

    @Override
    public Position getMarkDeletedPosition() {
        return ledger.positionBeforeOffset(state().acknowledgements().markDeleteOffset());
    }

    @Override
    public Position getPersistentMarkDeletedPosition() {
        return mode == Mode.DURABLE ? getMarkDeletedPosition() : null;
    }

    @Override
    public void rewind() {
        moveReadOffset(firstUnackedOffset(state().acknowledgements()));
    }

    @Override
    public void rewind(boolean readCompacted) {
        if (readCompacted) {
            throw unsupportedRuntime("rewind(readCompacted)");
        }
        rewind();
    }

    @Override
    public void seek(Position position, boolean force) {
        StreamMetadata metadata = ledger.currentMetadata();
        long target = ledger.requireCursorReadOffset(position, metadata);
        if (!force) {
            target = Math.max(target, firstUnackedOffset(state().acknowledgements()));
        }
        moveReadOffset(target);
    }

    @Override
    public void clearBacklog() throws InterruptedException, ManagedLedgerException {
        await(clearBacklogFuture());
    }

    @Override
    public void asyncClearBacklog(
            AsyncCallbacks.ClearBacklogCallback callback,
            Object ctx) {
        Objects.requireNonNull(callback, "callback");
        clearBacklogFuture().whenComplete((ignored, error) ->
                executeCallback(() -> {
                    if (error == null) {
                        callback.clearBacklogComplete(ctx);
                    } else {
                        callback.clearBacklogFailed(map(error, "clearBacklog"), ctx);
                    }
                }));
    }

    @Override
    public void skipEntries(int count, IndividualDeletedEntries deletedEntries)
            throws InterruptedException, ManagedLedgerException {
        await(skipEntriesFuture(count, deletedEntries));
    }

    @Override
    public void asyncSkipEntries(
            int count,
            IndividualDeletedEntries deletedEntries,
            AsyncCallbacks.SkipEntriesCallback callback,
            Object ctx) {
        Objects.requireNonNull(callback, "callback");
        skipEntriesFuture(count, deletedEntries).whenComplete((ignored, error) ->
                executeCallback(() -> {
                    if (error == null) {
                        callback.skipEntriesComplete(ctx);
                    } else {
                        callback.skipEntriesFailed(map(error, "skipEntries"), ctx);
                    }
                }));
    }

    @Override
    public Position findNewestMatching(Predicate<Entry> condition)
            throws InterruptedException, ManagedLedgerException {
        return findNewestMatching(FindPositionConstraint.SearchActiveEntries, condition);
    }

    @Override
    public CompletableFuture<ScanOutcome> scan(
            Optional<Position> startingPosition,
            Predicate<Entry> condition,
            int batchSize,
            long maxEntries,
            long timeOutMs) {
        Objects.requireNonNull(startingPosition, "startingPosition");
        Objects.requireNonNull(condition, "condition");
        if (batchSize <= 0 || maxEntries <= 0 || timeOutMs <= 0) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("scan bounds must be positive"));
        }
        return ledger.refreshMetadata().thenCompose(metadata -> {
            long start = startingPosition
                    .map(position -> ledger.requireCursorReadOffset(position, metadata))
                    .orElseGet(() -> firstUnackedOffset(state().acknowledgements()));
            long deadline;
            try {
                deadline = Math.addExact(System.nanoTime(), Math.multiplyExact(timeOutMs, 1_000_000L));
            } catch (ArithmeticException ignored) {
                deadline = Long.MAX_VALUE;
            }
            return scanNext(
                    Math.max(start, metadata.trimOffset()),
                    metadata,
                    condition,
                    maxEntries,
                    deadline,
                    0);
        });
    }

    @Override
    public Position findNewestMatching(
            FindPositionConstraint constraint,
            Predicate<Entry> condition) throws InterruptedException, ManagedLedgerException {
        return await(findFuture(constraint, condition, Optional.empty(), Optional.empty()));
    }

    @Override
    public void asyncFindNewestMatching(
            FindPositionConstraint constraint,
            Predicate<Entry> condition,
            AsyncCallbacks.FindEntryCallback callback,
            Object ctx) {
        asyncFindNewestMatching(constraint, condition, callback, ctx, true);
    }

    @Override
    public void asyncFindNewestMatching(
            FindPositionConstraint constraint,
            Predicate<Entry> condition,
            AsyncCallbacks.FindEntryCallback callback,
            Object ctx,
            boolean isFindFromLedger) {
        asyncFind(constraint, condition, Optional.empty(), Optional.empty(), callback, ctx);
    }

    @Override
    public void asyncFindNewestMatching(
            FindPositionConstraint constraint,
            Predicate<Entry> condition,
            Position startPosition,
            Position endPosition,
            AsyncCallbacks.FindEntryCallback callback,
            Object ctx,
            boolean isFindFromLedger) {
        asyncFind(
                constraint,
                condition,
                Optional.ofNullable(startPosition),
                Optional.ofNullable(endPosition),
                callback,
                ctx);
    }

    @Override
    public void resetCursor(Position position)
            throws InterruptedException, ManagedLedgerException {
        await(resetFuture(position, false));
    }

    @Override
    public void asyncResetCursor(
            Position position,
            boolean force,
            AsyncCallbacks.ResetCursorCallback callback) {
        Objects.requireNonNull(callback, "callback");
        final Position copied;
        try {
            copied = copyPosition(position);
        } catch (Throwable error) {
            executeCallback(() -> callback.resetFailed(map(error, "resetCursor"), position));
            return;
        }
        resetFuture(copied, force).whenComplete((canonicalPosition, error) ->
                executeCallback(() -> {
                    if (error == null) {
                        callback.resetComplete(canonicalPosition);
                    } else {
                        callback.resetFailed(map(error, "resetCursor"), canonicalPosition);
                    }
                }));
    }

    @Override
    public List<Entry> replayEntries(Set<? extends Position> positions)
            throws InterruptedException, ManagedLedgerException {
        CompletableFuture<List<Entry>> future = new CompletableFuture<>();
        asyncReplayEntries(positions, new AsyncCallbacks.ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                future.complete(entries);
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                future.completeExceptionally(exception);
            }
        }, null, true);
        return await(future);
    }

    @Override
    public Set<? extends Position> asyncReplayEntries(
            Set<? extends Position> positions,
            AsyncCallbacks.ReadEntriesCallback callback,
            Object ctx) {
        return asyncReplayEntries(positions, callback, ctx, false);
    }

    @Override
    public Set<? extends Position> asyncReplayEntries(
            Set<? extends Position> positions,
            AsyncCallbacks.ReadEntriesCallback callback,
            Object ctx,
            boolean sortEntries) {
        Objects.requireNonNull(callback, "callback");
        final List<Position> copied;
        try {
            Objects.requireNonNull(positions, "positions");
            copied = positions.stream().map(NereusManagedCursor::copyPosition).toList();
        } catch (Throwable error) {
            executeCallback(() -> callback.readEntriesFailed(map(error, "replayEntries"), ctx));
            return Set.copyOf(positions == null ? Set.of() : positions);
        }
        StreamMetadata metadata = ledger.currentMetadata();
        CursorAckState ack = state().acknowledgements();
        Set<Position> skipped = new HashSet<>();
        List<Long> offsets = new ArrayList<>();
        for (Position position : copied) {
            try {
                long offset = ledger.requireCursorEntryOffset(position, metadata);
                if (ack.isWholeEntryAcknowledged(offset)) {
                    skipped.add(position);
                } else {
                    offsets.add(offset);
                }
            } catch (Throwable ignored) {
                skipped.add(position);
            }
        }
        if (sortEntries) {
            offsets.sort(Long::compareTo);
        }
        replayOffsets(offsets, metadata, 0, new ArrayList<>()).whenComplete((entries, error) ->
                completeReadCallback(callback, ctx, entries, error, "replayEntries"));
        return Set.copyOf(skipped);
    }

    @Override
    public void close() throws ManagedLedgerException {
        awaitUnchecked(closeFuture());
    }

    @Override
    public void asyncClose(AsyncCallbacks.CloseCallback callback, Object ctx) {
        Objects.requireNonNull(callback, "callback");
        closeFuture().whenComplete((ignored, error) ->
                executeCallback(() -> {
                    if (error == null) {
                        callback.closeComplete(ctx);
                    } else {
                        callback.closeFailed(map(error, "closeCursor"), ctx);
                    }
                }));
    }

    @Override
    public Position getFirstPosition() {
        return ledger.getFirstPosition();
    }

    @Override
    public void setActive() {
        if (!alwaysInactive) {
            active = true;
        }
    }

    @Override
    public void setInactive() {
        active = false;
    }

    @Override
    public void setAlwaysInactive() {
        alwaysInactive = true;
        active = false;
    }

    @Override
    public boolean isActive() {
        return active && lifecycle.get() == LocalLifecycle.OPEN;
    }

    @Override
    public boolean isDurable() {
        return mode == Mode.DURABLE;
    }

    @Override
    public long getNumberOfEntriesSinceFirstNotAckedMessage() {
        StreamMetadata metadata = ledger.currentMetadata();
        long first = firstUnackedOffset(state().acknowledgements());
        return Math.max(0, Math.min(localReadOffset.get(), metadata.committedEndOffset()) - first);
    }

    @Override
    public int getTotalNonContiguousDeletedMessagesRange() {
        return state().acknowledgements().wholeAckRanges().size();
    }

    @Override
    public int getNonContiguousDeletedMessagesRangeSerializedSize() {
        return Math.toIntExact(Math.multiplyExact(
                (long) getTotalNonContiguousDeletedMessagesRange(), 2L * Long.BYTES));
    }

    @Override
    public long getEstimatedSizeSinceMarkDeletePosition() {
        StreamMetadata metadata = ledger.currentMetadata();
        long backlog = getNumberOfEntriesInBacklog(true);
        if (backlog == 0 || metadata.committedEndOffset() == 0) {
            return 0;
        }
        return Math.max(0, Math.round(
                (double) metadata.cumulativeSize() * backlog / metadata.committedEndOffset()));
    }

    @Override
    public void skipNonRecoverableLedger(long ledgerId) {
        // Missing committed bytes are corruption; F3 admission rejects auto-skip.
    }

    @Override
    public double getThrottleMarkDelete() {
        return throttleMarkDelete;
    }

    @Override
    public void setThrottleMarkDelete(double value) {
        if (!Double.isFinite(value) || value < 0) {
            throw new IllegalArgumentException("throttleMarkDelete must be finite and non-negative");
        }
        throttleMarkDelete = value;
    }

    @Override
    public ManagedLedger getManagedLedger() {
        return ledger;
    }

    @Override
    public Range<Position> getLastIndividualDeletedRange() {
        List<OffsetRange> ranges = state().acknowledgements().wholeAckRanges();
        if (ranges.isEmpty()) {
            return null;
        }
        OffsetRange range = ranges.get(ranges.size() - 1);
        return Range.openClosed(
                ledger.positionBeforeOffset(range.startOffset()),
                ledger.positionBeforeOffset(range.endOffset()));
    }

    @Override
    public void trimDeletedEntries(List<Entry> entries) {
        Objects.requireNonNull(entries, "entries");
        CursorAckState ack = state().acknowledgements();
        for (int index = entries.size() - 1; index >= 0; index--) {
            Entry entry = entries.get(index);
            if (ack.isWholeEntryAcknowledged(entry.getEntryId())) {
                entries.remove(index);
                entry.release();
            }
        }
    }

    @Override
    public long[] getDeletedBatchIndexesAsLongArray(Position position) {
        return batchAckWords(position);
    }

    @Override
    public ManagedCursorMXBean getStats() {
        return stats;
    }

    @Override
    public boolean checkAndUpdateReadPositionChanged() {
        long current = localReadOffset.get();
        long previous = priorReadPositionSample.getAndSet(current);
        return previous != current || current >= ledger.currentMetadata().committedEndOffset();
    }

    @Override
    public boolean isClosed() {
        return lifecycle.get() != LocalLifecycle.OPEN;
    }

    @Override
    public boolean isCursorDataFullyPersistable() {
        return true;
    }

    @Override
    public boolean periodicRollover() {
        return false;
    }

    @Override
    public ManagedCursorAttributes getManagedCursorAttributes() {
        ManagedCursorAttributes current = attributes;
        if (current != null) {
            return current;
        }
        synchronized (this) {
            if (attributes == null) {
                attributes = new ManagedCursorAttributes(this);
            }
            return attributes;
        }
    }

    @Override
    public ManagedLedgerInternalStats.CursorStats getCursorStats() {
        CursorState current = state();
        ManagedLedgerInternalStats.CursorStats result = new ManagedLedgerInternalStats.CursorStats();
        result.markDeletePosition = getMarkDeletedPosition().toString();
        result.readPosition = getReadPosition().toString();
        result.cursorLedger = -1;
        result.cursorLedgerLastEntry = -1;
        result.individuallyDeletedMessages = current.acknowledgements().wholeAckRanges().toString();
        result.state = lifecycle.get() == LocalLifecycle.OPEN ? "Open" : "Closed";
        result.active = isActive();
        result.properties = new HashMap<>(getProperties());
        return result;
    }

    @Override
    public boolean isMessageDeleted(Position position) {
        try {
            StreamMetadata metadata = ledger.currentMetadata();
            long offset = ledger.requireCursorEntryOffset(position, metadata);
            return state().acknowledgements().isWholeEntryAcknowledged(offset);
        } catch (Throwable ignored) {
            return false;
        }
    }

    @Override
    public ManagedCursor duplicateNonDurableCursor(String duplicateName)
            throws ManagedLedgerException {
        CursorState current = state();
        return ledger.createLocalCursor(
                duplicateName,
                current.acknowledgements(),
                firstUnackedOffset(current.acknowledgements()),
                getProperties(),
                current.cursorProperties());
    }

    @Override
    public long[] getBatchPositionAckSet(Position position) {
        return batchAckWords(position);
    }

    @Override
    public int applyMaxSizeCap(int maxEntries, long maxSizeBytes) {
        if (maxEntries < 0
                || (maxSizeBytes < 0 && maxSizeBytes != ManagedLedgerUtils.NO_MAX_SIZE_LIMIT)) {
            throw new IllegalArgumentException("read caps cannot be negative");
        }
        if (maxEntries == 0 || maxSizeBytes == 0) {
            return 0;
        }
        int cappedEntries = Math.min(maxEntries, ledger.runtime().config().maxReadEntries());
        if (maxSizeBytes == ManagedLedgerUtils.NO_MAX_SIZE_LIMIT) {
            return cappedEntries;
        }
        long count = readEntriesCount.get();
        long size = readEntriesSize.get();
        long average = count == 0 ? streamAverageEntrySize() : Math.max(1, size / count);
        long byteCap = Math.max(1, maxSizeBytes / Math.max(1, average));
        return (int) Math.min(cappedEntries, Math.min(Integer.MAX_VALUE, byteCap));
    }

    @Override
    public void updateReadStats(int count, long size) {
        if (count < 0 || size < 0) {
            throw new IllegalArgumentException("read stats cannot be negative");
        }
        readEntriesCount.addAndGet(count);
        readEntriesSize.addAndGet(size);
        updateLastActive();
    }

    private CompletableFuture<Void> markDeleteFuture(
            Position position,
            Map<String, Long> explicitProperties) {
        final Position copied;
        final PropertyCapture capture;
        try {
            copied = copyPosition(position);
            capture = capturePositionProperties(explicitProperties, false);
        } catch (Throwable error) {
            return mappedFailure(error, "markDelete");
        }
        return prepareAck(copied, capture.values())
                .thenCompose(prepared -> cumulativeAck(prepared.request(), prepared.metadata()))
                .thenAccept(result -> completePropertyCapture(capture, result.state()));
    }

    private CompletableFuture<Void> individualAckFuture(List<Position> copied) {
        if (copied.isEmpty()) {
            return mappedFailure(
                    new IllegalArgumentException("individual ack positions cannot be empty"),
                    "individualAck");
        }
        return prepareAcks(copied, 0, new ArrayList<>(), null)
                .thenCompose(prepared -> individualAck(
                        prepared,
                        ledger.currentMetadata()))
                .thenApply(ignored -> null);
    }

    private CompletableFuture<Void> clearBacklogFuture() {
        final PropertyCapture capture;
        try {
            requireOpen();
            capture = capturePositionProperties(Map.of(), true);
        } catch (Throwable error) {
            return mappedFailure(error, "clearBacklog");
        }
        long committedEnd = ledger.currentMetadata().committedEndOffset();
        CompletableFuture<CursorMutationResult> mutation = mode == Mode.DURABLE
                ? ledger.runtime().cursorStorage().clearBacklog(durableHandle, committedEnd)
                : submitLocalMutation(state -> stateMachine.clearBacklog(
                        state, committedEnd, nonNegativeNow()), false);
        return mutation.thenAccept(result -> {
            completePropertyCapture(capture, result.state());
            moveReadOffset(firstUnackedOffset(result.state().acknowledgements()));
        });
    }

    private CompletableFuture<Void> skipEntriesFuture(
            int count,
            IndividualDeletedEntries deletedEntries) {
        if (count < 0) {
            return mappedFailure(
                    new IllegalArgumentException("skip count cannot be negative"),
                    "skipEntries");
        }
        if (count == 0) {
            return CompletableFuture.completedFuture(null);
        }
        Objects.requireNonNull(deletedEntries, "deletedEntries");
        StreamMetadata metadata = ledger.currentMetadata();
        CursorAckState ack = state().acknowledgements();
        OptionalLong target = nthEligibleOffset(
                ack,
                ack.markDeleteOffset(),
                metadata.committedEndOffset(),
                count,
                deletedEntries == IndividualDeletedEntries.Exclude);
        long markThrough = target.orElse(metadata.committedEndOffset() - 1);
        if (markThrough < metadata.trimOffset()) {
            return CompletableFuture.completedFuture(null);
        }
        return markDeleteFuture(
                PositionFactory.create(ledger.projection().virtualLedgerId(), markThrough), null);
    }

    private CompletableFuture<Position> resetFuture(Position position, boolean force) {
        final Position copied;
        final PropertyCapture capture;
        try {
            copied = copyPosition(position);
            capture = capturePositionProperties(Map.of(), true);
        } catch (Throwable error) {
            return mappedFailure(error, "resetCursor");
        }
        return ledger.refreshMetadata().thenCompose(metadata -> {
            final long target;
            try {
                target = ledger.normalizeCursorResetReadOffset(copied, force, metadata);
            } catch (Throwable error) {
                return CompletableFuture.failedFuture(invalidPosition(error));
            }
            long[] words = AckSetStateUtil.getAckSetArrayOrNull(copied);
            CompletableFuture<Optional<BatchAckState>> batch = words == null
                    ? CompletableFuture.completedFuture(Optional.empty())
                    : batchState(target, words, metadata).thenApply(Optional::of);
            return batch.thenCompose(targetBatch -> {
                final CursorResetRequest request;
                try {
                    request = new CursorResetRequest(
                            target,
                            targetBatch,
                            force,
                            metadata.trimOffset(),
                            metadata.committedEndOffset());
                } catch (Throwable error) {
                    return CompletableFuture.failedFuture(invalidPosition(error));
                }
                CompletableFuture<CursorMutationResult> mutation = mode == Mode.DURABLE
                        ? ledger.runtime().cursorStorage().reset(durableHandle, request)
                        : submitLocalMutation(state -> stateMachine.reset(
                                state, request, randomId(), nonNegativeNow()), false);
                Position canonical = ledger.readPosition(target, metadata);
                return mutation.thenApply(result -> {
                    completePropertyCapture(capture, result.state());
                    moveReadOffset(firstUnackedOffset(result.state().acknowledgements()));
                    return canonical;
                });
            });
        });
    }

    private CompletableFuture<CursorMutationResult> cumulativeAck(
            CursorAckRequest request,
            StreamMetadata metadata) {
        requireOpen();
        return mode == Mode.DURABLE
                ? ledger.runtime().cursorStorage().cumulativeAck(durableHandle, request)
                : submitLocalMutation(state -> stateMachine.cumulativeAck(
                        state,
                        request,
                        metadata.trimOffset(),
                        metadata.committedEndOffset(),
                        nonNegativeNow()), false);
    }

    private CompletableFuture<CursorMutationResult> individualAck(
            List<CursorAckRequest> requests,
            StreamMetadata metadata) {
        requireOpen();
        return mode == Mode.DURABLE
                ? ledger.runtime().cursorStorage().individualAck(durableHandle, requests)
                : submitLocalMutation(state -> stateMachine.individualAck(
                        state,
                        requests,
                        metadata.trimOffset(),
                        metadata.committedEndOffset(),
                        nonNegativeNow()), false);
    }

    private CompletableFuture<CursorMutationResult> mutateCursorProperties(
            CursorPropertyMutation mutation) {
        requireOpen();
        return mode == Mode.DURABLE
                ? ledger.runtime().cursorStorage().mutateCursorProperties(durableHandle, mutation)
                : submitLocalMutation(state -> stateMachine.mutateCursorProperties(
                        state, mutation, nonNegativeNow()), false);
    }

    private CompletableFuture<PreparedAck> prepareAck(
            Position position,
            Map<String, Long> properties) {
        return ledger.refreshMetadata().thenCompose(metadata -> {
            final long offset;
            try {
                offset = ledger.requireCursorEntryOffset(position, metadata);
            } catch (Throwable error) {
                return CompletableFuture.failedFuture(invalidPosition(error));
            }
            long[] words = AckSetStateUtil.getAckSetArrayOrNull(position);
            if (words == null) {
                return CompletableFuture.completedFuture(new PreparedAck(
                        new CursorAckRequest(offset, Optional.empty(), properties), metadata));
            }
            return batchState(offset, words, metadata).thenApply(batch -> new PreparedAck(
                    new CursorAckRequest(offset, Optional.of(batch), properties), metadata));
        });
    }

    private CompletableFuture<List<CursorAckRequest>> prepareAcks(
            List<Position> positions,
            int index,
            List<CursorAckRequest> requests,
            StreamMetadata capturedMetadata) {
        if (index >= positions.size()) {
            return CompletableFuture.completedFuture(List.copyOf(requests));
        }
        return prepareAck(positions.get(index), Map.of()).thenCompose(prepared -> {
            if (capturedMetadata != null
                    && !capturedMetadata.streamId().equals(prepared.metadata().streamId())) {
                return CompletableFuture.failedFuture(new ManagedLedgerException(
                        "individual ack metadata snapshot changed stream identity"));
            }
            requests.add(prepared.request());
            return prepareAcks(
                    positions,
                    index + 1,
                    requests,
                    capturedMetadata == null ? prepared.metadata() : capturedMetadata);
        });
    }

    private CompletableFuture<BatchAckState> batchState(
            long offset,
            long[] remainingWords,
            StreamMetadata metadata) {
        if (offset >= metadata.committedEndOffset()) {
            return CompletableFuture.failedFuture(new ManagedLedgerException.InvalidCursorPositionException(
                    "batch ack target is one-past-tail"));
        }
        return ledger.readAt(offset, metadata).thenCompose(entry -> {
            try {
                MessageMetadata messageMetadata = entry.getMessageMetadata();
                if (messageMetadata == null
                        || !messageMetadata.hasNumMessagesInBatch()
                        || messageMetadata.getNumMessagesInBatch() <= 0) {
                    return CompletableFuture.failedFuture(
                            new ManagedLedgerException.InvalidCursorPositionException(
                                    "batch ack target does not carry valid batch metadata"));
                }
                return CompletableFuture.completedFuture(new BatchAckState(
                        messageMetadata.getNumMessagesInBatch(), remainingWords.clone()));
            } catch (Throwable error) {
                return CompletableFuture.failedFuture(invalidPosition(error));
            } finally {
                entry.release();
            }
        });
    }

    private void asyncRead(
            int count,
            long maxSizeBytes,
            AsyncCallbacks.ReadEntriesCallback callback,
            Object ctx,
            Position maxPosition,
            Predicate<Position> skipCondition,
            boolean wait) {
        Objects.requireNonNull(callback, "callback");
        final Position copiedMax;
        try {
            validateReadLimits(count, maxSizeBytes);
            copiedMax = maxPosition == null ? null : copyPosition(maxPosition);
        } catch (Throwable error) {
            executeCallback(() -> callback.readEntriesFailed(map(error, "readEntries"), ctx));
            return;
        }
        readEntriesFuture(count, maxSizeBytes, copiedMax, skipCondition)
                .whenComplete((entries, error) -> {
                    if (error != null) {
                        completeReadCallback(callback, ctx, null, error, "readEntries");
                        return;
                    }
                    if (!wait || !entries.isEmpty()) {
                        completeReadCallback(callback, ctx, entries, null, "readEntries");
                        return;
                    }
                    registerWaiter(
                            count,
                            maxSizeBytes,
                            copiedMax,
                            skipCondition,
                            callback,
                            ctx);
                });
    }

    private CompletableFuture<List<Entry>> readEntriesFuture(
            int count,
            long maxSizeBytes,
            Position maxPosition,
            Predicate<Position> skipCondition) {
        try {
            validateReadLimits(count, maxSizeBytes);
        } catch (Throwable error) {
            return mappedFailure(error, "readEntries");
        }
        int capped = Math.min(count, ledger.runtime().config().maxReadEntries());
        long byteCap = maxSizeBytes == ManagedLedgerUtils.NO_MAX_SIZE_LIMIT
                ? Long.MAX_VALUE
                : maxSizeBytes;
        return enqueueRead(() -> readBatch(capped, byteCap, maxPosition, skipCondition));
    }

    private CompletableFuture<List<Entry>> readBatch(
            int count,
            long maxSizeBytes,
            Position maxPosition,
            Predicate<Position> skipCondition) {
        return ledger.refreshMetadata().thenCompose(metadata -> {
            final Position inclusiveMax;
            try {
                inclusiveMax = ledger.normalizeInclusiveMax(maxPosition, metadata);
            } catch (Throwable error) {
                return CompletableFuture.failedFuture(invalidPosition(error));
            }
            long epoch = readEpoch.get();
            long start = Math.max(localReadOffset.get(), metadata.trimOffset());
            CursorAckState ack = state().acknowledgements();
            start = nextDispatchableOffset(start, ack);
            long endExclusive = Math.min(
                    metadata.committedEndOffset(), Math.addExact(inclusiveMax.getEntryId(), 1));
            List<Entry> entries = new ArrayList<>();
            return readNext(
                    start,
                    endExclusive,
                    count,
                    maxSizeBytes,
                    0,
                    0,
                    metadata,
                    ack,
                    skipCondition,
                    entries)
                    .thenApply(next -> {
                        if (readEpoch.get() == epoch) {
                            localReadOffset.set(next);
                        }
                        long size = entries.stream().mapToLong(Entry::getLength).sum();
                        updateReadStats(entries.size(), size);
                        return List.copyOf(entries);
                    })
                    .exceptionallyCompose(error -> {
                        entries.forEach(Entry::release);
                        entries.clear();
                        return CompletableFuture.failedFuture(unwrap(error));
                    });
        });
    }

    private CompletableFuture<Long> readNext(
            long offset,
            long endExclusive,
            int remaining,
            long maxBytes,
            long usedBytes,
            int examined,
            StreamMetadata metadata,
            CursorAckState ack,
            Predicate<Position> skipCondition,
            List<Entry> entries) {
        long candidate = nextDispatchableOffset(offset, ack);
        if (remaining == 0
                || candidate >= endExclusive
                || examined >= ledger.runtime().config().maxScanEntries()) {
            return CompletableFuture.completedFuture(candidate);
        }
        Position position = PositionFactory.create(ledger.projection().virtualLedgerId(), candidate);
        if (skipCondition != null && skipCondition.test(position)) {
            return readNext(
                    Math.addExact(candidate, 1),
                    endExclusive,
                    remaining,
                    maxBytes,
                    usedBytes,
                    examined + 1,
                    metadata,
                    ack,
                    skipCondition,
                    entries);
        }
        return ledger.readAt(candidate, metadata).thenCompose(entry -> {
            long nextBytes;
            try {
                nextBytes = Math.addExact(usedBytes, entry.getLength());
            } catch (ArithmeticException overflow) {
                entry.release();
                return CompletableFuture.failedFuture(overflow);
            }
            if (!entries.isEmpty() && nextBytes > maxBytes) {
                entry.release();
                return CompletableFuture.completedFuture(candidate);
            }
            entries.add(entry);
            return readNext(
                    Math.addExact(candidate, 1),
                    endExclusive,
                    remaining - 1,
                    maxBytes,
                    nextBytes,
                    examined + 1,
                    metadata,
                    ack,
                    skipCondition,
                    entries);
        });
    }

    private void registerWaiter(
            int count,
            long maxSizeBytes,
            Position maxPosition,
            Predicate<Position> skipCondition,
            AsyncCallbacks.ReadEntriesCallback callback,
            Object ctx) {
        if (lifecycle.get() != LocalLifecycle.OPEN) {
            completeReadCallback(
                    callback,
                    ctx,
                    null,
                    new ManagedLedgerException.CursorAlreadyClosedException("cursor is closing"),
                    "readEntriesOrWait");
            return;
        }
        StreamMetadata metadata = ledger.currentMetadata();
        long next = nextDispatchableOffset(
                Math.max(localReadOffset.get(), metadata.trimOffset()),
                state().acknowledgements());
        OptionalLong maxOffset = maxPosition == null
                ? OptionalLong.empty()
                : OptionalLong.of(maxPosition.getEntryId());
        if (maxOffset.isPresent() && maxOffset.getAsLong() < next) {
            completeReadCallback(callback, ctx, List.of(), null, "readEntriesOrWait");
            return;
        }
        if (metadata.state() == StreamState.SEALED && next >= metadata.committedEndOffset()) {
            completeReadCallback(
                    callback,
                    ctx,
                    null,
                    new ManagedLedgerException.ManagedLedgerTerminatedException(
                            "sealed cursor is at tail"),
                    "readEntriesOrWait");
            return;
        }
        Pending waiter = new Pending(
                next,
                maxOffset,
                count,
                maxSizeBytes,
                maxPosition,
                skipCondition,
                callback,
                ctx);
        if (!pendingRead.compareAndSet(null, waiter)) {
            completeReadCallback(
                    callback,
                    ctx,
                    null,
                    new ManagedLedgerException.ConcurrentWaitCallbackException(),
                    "readEntriesOrWait");
            return;
        }
        ledger.tailPoll().register(waiter);
    }

    private CompletableFuture<Entry> nthEntryFuture(
            int n,
            IndividualDeletedEntries deletedEntries) {
        if (n <= 0) {
            return mappedFailure(new IllegalArgumentException("n must be positive"), "getNthEntry");
        }
        Objects.requireNonNull(deletedEntries, "deletedEntries");
        return ledger.refreshMetadata().thenCompose(metadata -> {
            CursorAckState ack = state().acknowledgements();
            OptionalLong offset = nthEligibleOffset(
                    ack,
                    ack.markDeleteOffset(),
                    metadata.committedEndOffset(),
                    n,
                    deletedEntries == IndividualDeletedEntries.Exclude);
            return offset.isEmpty()
                    ? CompletableFuture.completedFuture(null)
                    : ledger.readAt(offset.getAsLong(), metadata);
        });
    }

    private CompletableFuture<Position> findFuture(
            FindPositionConstraint constraint,
            Predicate<Entry> condition,
            Optional<Position> start,
            Optional<Position> end) {
        Objects.requireNonNull(constraint, "constraint");
        Objects.requireNonNull(condition, "condition");
        return ledger.refreshMetadata().thenCompose(metadata -> {
            long lower = constraint == FindPositionConstraint.SearchActiveEntries
                    ? firstUnackedOffset(state().acknowledgements())
                    : metadata.trimOffset();
            long upper = metadata.committedEndOffset() - 1;
            try {
                if (start.isPresent()) {
                    lower = Math.max(lower, ledger.requireCursorReadOffset(start.orElseThrow(), metadata));
                }
                if (end.isPresent()) {
                    upper = Math.min(
                            upper,
                            ledger.requireCursorEntryOffset(end.orElseThrow(), metadata));
                }
            } catch (Throwable error) {
                return CompletableFuture.failedFuture(invalidPosition(error));
            }
            if (upper < lower) {
                return CompletableFuture.completedFuture(null);
            }
            return findBackward(
                    upper,
                    lower,
                    metadata,
                    constraint == FindPositionConstraint.SearchActiveEntries,
                    condition,
                    ledger.runtime().config().maxScanEntries());
        });
    }

    private CompletableFuture<Position> findBackward(
            long offset,
            long lower,
            StreamMetadata metadata,
            boolean skipDeleted,
            Predicate<Entry> condition,
            int remaining) {
        if (offset < lower) {
            return CompletableFuture.completedFuture(null);
        }
        if (remaining == 0) {
            return CompletableFuture.failedFuture(new ManagedLedgerException(
                    "cursor find scan budget is exhausted"));
        }
        if (skipDeleted && state().acknowledgements().isWholeEntryAcknowledged(offset)) {
            return findBackward(offset - 1, lower, metadata, true, condition, remaining - 1);
        }
        return ledger.readAt(offset, metadata).thenCompose(entry -> {
            boolean matches;
            try {
                matches = condition.test(entry);
            } finally {
                entry.release();
            }
            return matches
                    ? CompletableFuture.completedFuture(
                            PositionFactory.create(ledger.projection().virtualLedgerId(), offset))
                    : findBackward(
                            offset - 1,
                            lower,
                            metadata,
                            skipDeleted,
                            condition,
                            remaining - 1);
        });
    }

    private void asyncFind(
            FindPositionConstraint constraint,
            Predicate<Entry> condition,
            Optional<Position> start,
            Optional<Position> end,
            AsyncCallbacks.FindEntryCallback callback,
            Object ctx) {
        Objects.requireNonNull(callback, "callback");
        findFuture(constraint, condition, start, end).whenComplete((position, error) ->
                executeCallback(() -> {
                    if (error == null) {
                        callback.findEntryComplete(position, ctx);
                    } else {
                        callback.findEntryFailed(map(error, "findNewestMatching"), Optional.empty(), ctx);
                    }
                }));
    }

    private CompletableFuture<ScanOutcome> scanNext(
            long offset,
            StreamMetadata metadata,
            Predicate<Entry> condition,
            long maxEntries,
            long deadlineNanos,
            long examined) {
        if (offset >= metadata.committedEndOffset()) {
            return CompletableFuture.completedFuture(ScanOutcome.COMPLETED);
        }
        if (examined >= maxEntries || System.nanoTime() >= deadlineNanos) {
            return CompletableFuture.completedFuture(ScanOutcome.ABORTED);
        }
        return ledger.readAt(offset, metadata).thenCompose(entry -> {
            boolean proceed;
            try {
                proceed = condition.test(entry);
            } finally {
                entry.release();
            }
            return proceed
                    ? scanNext(
                            Math.addExact(offset, 1),
                            metadata,
                            condition,
                            maxEntries,
                            deadlineNanos,
                            examined + 1)
                    : CompletableFuture.completedFuture(ScanOutcome.USER_INTERRUPTED);
        });
    }

    private CompletableFuture<List<Entry>> replayOffsets(
            List<Long> offsets,
            StreamMetadata metadata,
            int index,
            List<Entry> entries) {
        if (index >= offsets.size()) {
            return CompletableFuture.completedFuture(List.copyOf(entries));
        }
        return ledger.readAt(offsets.get(index), metadata)
                .thenCompose(entry -> {
                    entries.add(entry);
                    return replayOffsets(offsets, metadata, index + 1, entries);
                })
                .exceptionallyCompose(error -> {
                    entries.forEach(Entry::release);
                    entries.clear();
                    return CompletableFuture.failedFuture(unwrap(error));
                });
    }

    private CompletableFuture<Void> closeFuture() {
        synchronized (closeLock) {
            if (closeFuture != null) {
                return closeFuture;
            }
            if (!lifecycle.compareAndSet(LocalLifecycle.OPEN, LocalLifecycle.CLOSING)) {
                return closeFuture == null
                        ? CompletableFuture.failedFuture(closedException())
                        : closeFuture;
            }
            cancelPendingRead(new ManagedLedgerException.CursorAlreadyClosedException(
                    "cursor is closing"));
            PropertyCapture capture = captureVisibleProperties();
            CompletableFuture<CursorMutationResult> flush = mode == Mode.DURABLE
                    ? ledger.runtime().cursorStorage().flushPositionProperties(
                            durableHandle, capture.values())
                    : submitLocalMutation(state -> stateMachine.flushPositionProperties(
                            state, capture.values(), nonNegativeNow()), true);
            CompletableFuture<Void> flushed = flush.thenAccept(result ->
                    completePropertyCapture(capture, result.state()));
            closeFuture = finishClose(flushed, true);
            return closeFuture;
        }
    }

    private CompletableFuture<Void> finishClose(
            CompletableFuture<Void> flush,
            boolean removeFromLedger) {
        CompletableFuture<Void> reads;
        synchronized (readLaneLock) {
            reads = readTail;
        }
        return flush.handle((ignored, flushError) -> flushError)
                .thenCompose(flushError -> {
                    CompletableFuture<Void> handleClose = durableHandle == null
                            ? CompletableFuture.completedFuture(null)
                            : durableHandle.closeAsync();
                    return CompletableFuture.allOf(reads, handleClose)
                            .handle((ignored, closeError) -> {
                                Throwable failure = flushError != null
                                        ? unwrap(flushError)
                                        : closeError == null ? null : unwrap(closeError);
                                synchronized (localMutationLock) {
                                    localMutationLaneClosed = true;
                                }
                                lifecycle.set(failure == null
                                        ? LocalLifecycle.CLOSED
                                        : LocalLifecycle.CLOSE_FAILED);
                                if (removeFromLedger) {
                                    ledger.removeCursor(this);
                                }
                                if (failure != null) {
                                    throw new CompletionException(failure);
                                }
                                return null;
                            });
                });
    }

    private CompletableFuture<CursorMutationResult> submitLocalMutation(
            LocalMutation operation,
            boolean allowClosing) {
        Objects.requireNonNull(operation, "operation");
        synchronized (localMutationLock) {
            LocalLifecycle current = lifecycle.get();
            if ((!allowClosing && current != LocalLifecycle.OPEN)
                    || (allowClosing && current != LocalLifecycle.CLOSING)
                    || localMutationLaneClosed) {
                return CompletableFuture.failedFuture(closedException());
            }
            if (localPendingMutations >= ledger.runtime().cursorStorageConfig().cursorMutationQueueMax()) {
                return CompletableFuture.failedFuture(
                        new ManagedLedgerException.TooManyRequestsException(
                                "non-durable cursor mutation queue is full"));
            }
            localPendingMutations++;
            CompletableFuture<CursorMutationResult> result = localMutationTail
                    .handle((ignored, error) -> null)
                    .thenApplyAsync(ignored -> {
                        try {
                            CursorMutationResult mutation = operation.apply(localState.get());
                            localState.set(mutation.state());
                            return mutation;
                        } catch (Throwable error) {
                            throw new CompletionException(error);
                        }
                    }, ledger.runtime().scheduler());
            localMutationTail = result.handle((ignored, error) -> null);
            result.whenComplete((ignored, error) -> {
                synchronized (localMutationLock) {
                    localPendingMutations--;
                }
            });
            return result;
        }
    }

    private <T> CompletableFuture<T> enqueueRead(
            java.util.function.Supplier<CompletableFuture<T>> operation) {
        synchronized (readLaneLock) {
            if (lifecycle.get() != LocalLifecycle.OPEN) {
                return CompletableFuture.failedFuture(closedException());
            }
            CompletableFuture<T> result = readTail
                    .handle((ignored, error) -> null)
                    .thenComposeAsync(ignored -> operation.get(), ledger.runtime().scheduler());
            readTail = result.handle((ignored, error) -> null);
            return result;
        }
    }

    private PropertyCapture capturePositionProperties(
            Map<String, Long> explicitProperties,
            boolean clear) {
        synchronized (propertyStageLock) {
            requireOpen();
            Map<String, Long> visible = visiblePositionPropertiesLocked();
            Map<String, Long> candidate = clear
                    ? Map.of()
                    : explicitProperties == null
                            ? visible
                            : immutableLongMap(explicitProperties);
            if (clear || explicitProperties != null) {
                if (!candidate.equals(visible)) {
                    positionPropertyStageRevision = Math.addExact(
                            positionPropertyStageRevision, 1);
                }
                stagedPositionProperties = candidate;
            }
            return new PropertyCapture(positionPropertyStageRevision, Map.copyOf(candidate));
        }
    }

    private PropertyCapture captureVisibleProperties() {
        synchronized (propertyStageLock) {
            return new PropertyCapture(
                    positionPropertyStageRevision,
                    Map.copyOf(visiblePositionPropertiesLocked()));
        }
    }

    private void completePropertyCapture(PropertyCapture capture, CursorState persisted) {
        synchronized (propertyStageLock) {
            if (positionPropertyStageRevision == capture.revision()
                    && persisted.positionProperties().equals(capture.values())) {
                stagedPositionProperties = null;
            }
        }
    }

    private Map<String, Long> visiblePositionPropertiesLocked() {
        return stagedPositionProperties == null
                ? state().positionProperties()
                : stagedPositionProperties;
    }

    private CursorState state() {
        return mode == Mode.DURABLE ? durableHandle.state() : localState.get();
    }

    private long[] batchAckWords(Position position) {
        try {
            long offset = ledger.requireCursorEntryOffset(position, ledger.currentMetadata());
            BatchAckState partial = state().acknowledgements().partialBatchAcks().get(offset);
            return partial == null ? null : partial.remainingWords();
        } catch (Throwable ignored) {
            return null;
        }
    }

    private long streamAverageEntrySize() {
        StreamMetadata metadata = ledger.currentMetadata();
        return metadata.committedEndOffset() == 0
                ? 1
                : Math.max(1, metadata.cumulativeSize() / metadata.committedEndOffset());
    }

    private void moveReadOffset(long offset) {
        localReadOffset.set(offset);
        readEpoch.incrementAndGet();
    }

    private void cancelPendingRead(ManagedLedgerException failure) {
        Pending waiter = pendingRead.getAndSet(null);
        if (waiter != null) {
            ledger.tailPoll().remove(waiter);
            waiter.tryFail(failure);
        }
    }

    private void completeReadCallback(
            AsyncCallbacks.ReadEntriesCallback callback,
            Object ctx,
            List<Entry> entries,
            Throwable error,
            String operation) {
        executeCallback(() -> {
            if (error != null) {
                callback.readEntriesFailed(map(error, operation), ctx);
                return;
            }
            List<Entry> exactEntries = entries == null ? List.of() : entries;
            try {
                callback.readEntriesComplete(exactEntries, ctx);
            } catch (Throwable callbackError) {
                exactEntries.forEach(Entry::release);
            }
        });
    }

    private void executeCallback(Runnable callback) {
        ledger.runtime().callbackExecutor().execute(callback);
    }

    private void requireOpen() {
        if (lifecycle.get() != LocalLifecycle.OPEN) {
            throw new CompletionException(closedException());
        }
    }

    private ManagedLedgerException.CursorAlreadyClosedException closedException() {
        return new ManagedLedgerException.CursorAlreadyClosedException(
                "cursor " + name + " is not open");
    }

    private ManagedLedgerException map(Throwable error, String operation) {
        return errors.map(unwrap(error), OperationContext.ledger("cursor/" + operation));
    }

    private <T> CompletableFuture<T> mappedFailure(Throwable error, String operation) {
        return CompletableFuture.failedFuture(map(error, operation));
    }

    private ManagedLedgerException.InvalidCursorPositionException invalidPosition(Throwable error) {
        ManagedLedgerException.InvalidCursorPositionException invalid =
                new ManagedLedgerException.InvalidCursorPositionException(
                        error.getMessage() == null ? "invalid cursor position" : error.getMessage());
        invalid.initCause(error);
        return invalid;
    }

    private ManagedLedgerException unsupported(String operation) {
        return errors.unsupported(operation);
    }

    private UnsupportedOperationException unsupportedRuntime(String operation) {
        return errors.unsupportedRuntime(operation);
    }

    private static void validateReadLimits(int count, long maxSizeBytes) {
        if (count <= 0
                || (maxSizeBytes <= 0 && maxSizeBytes != ManagedLedgerUtils.NO_MAX_SIZE_LIMIT)) {
            throw new IllegalArgumentException("read limits must be positive or use NO_MAX_SIZE_LIMIT");
        }
    }

    private static Map<String, Long> immutableLongMap(Map<String, Long> values) {
        Objects.requireNonNull(values, "values");
        LinkedHashMap<String, Long> copy = new LinkedHashMap<>();
        values.forEach((key, value) -> copy.put(
                Objects.requireNonNull(key, "position properties contain null key"),
                Objects.requireNonNull(value, "position properties contain null value")));
        return Collections.unmodifiableMap(copy);
    }

    private static Position copyPosition(Position position) {
        Objects.requireNonNull(position, "position");
        long[] ackSet = AckSetStateUtil.getAckSetArrayOrNull(position);
        return ackSet == null
                ? PositionFactory.create(position)
                : AckSetStateUtil.createPositionWithAckSet(
                        position.getLedgerId(), position.getEntryId(), ackSet.clone());
    }

    private static List<Position> copyPositions(Iterable<Position> positions) {
        Objects.requireNonNull(positions, "positions");
        List<Position> copy = new ArrayList<>();
        positions.forEach(position -> copy.add(copyPosition(position)));
        return List.copyOf(copy);
    }

    private static long nextDispatchableOffset(long offset, CursorAckState ack) {
        long candidate = Math.max(offset, ack.markDeleteOffset());
        for (OffsetRange range : ack.wholeAckRanges()) {
            if (range.endOffset() <= candidate) {
                continue;
            }
            if (range.startOffset() > candidate) {
                break;
            }
            candidate = range.endOffset();
        }
        return candidate;
    }

    private static long firstUnackedOffset(CursorAckState ack) {
        return nextDispatchableOffset(ack.markDeleteOffset(), ack);
    }

    private static long outstandingEntries(CursorAckState ack, long start, long end) {
        long lower = Math.max(start, ack.markDeleteOffset());
        if (end <= lower) {
            return 0;
        }
        long deleted = 0;
        for (OffsetRange range : ack.wholeAckRanges()) {
            long intersectionStart = Math.max(lower, range.startOffset());
            long intersectionEnd = Math.min(end, range.endOffset());
            if (intersectionEnd > intersectionStart) {
                deleted = Math.addExact(deleted, intersectionEnd - intersectionStart);
            }
        }
        return Math.max(0, Math.subtractExact(end - lower, deleted));
    }

    private static OptionalLong nthEligibleOffset(
            CursorAckState ack,
            long start,
            long end,
            long n,
            boolean excludeDeleted) {
        if (n <= 0 || start >= end) {
            return OptionalLong.empty();
        }
        if (!excludeDeleted) {
            long target;
            try {
                target = Math.addExact(start, n - 1);
            } catch (ArithmeticException ignored) {
                return OptionalLong.empty();
            }
            return target < end ? OptionalLong.of(target) : OptionalLong.empty();
        }
        long candidate = Math.max(start, ack.markDeleteOffset());
        long remaining = n;
        for (OffsetRange range : ack.wholeAckRanges()) {
            if (range.endOffset() <= candidate) {
                continue;
            }
            if (range.startOffset() > candidate) {
                long available = Math.min(end, range.startOffset()) - candidate;
                if (remaining <= available) {
                    return OptionalLong.of(candidate + remaining - 1);
                }
                remaining -= available;
            }
            candidate = Math.max(candidate, range.endOffset());
            if (candidate >= end) {
                return OptionalLong.empty();
            }
        }
        long available = end - candidate;
        return remaining <= available
                ? OptionalLong.of(candidate + remaining - 1)
                : OptionalLong.empty();
    }

    private static long nonNegativeNow() {
        return Math.max(0, System.currentTimeMillis());
    }

    private static String randomId() {
        byte[] bytes = new byte[16];
        synchronized (RANDOM) {
            RANDOM.nextBytes(bytes);
        }
        return HexFormat.of().formatHex(bytes);
    }

    private static Throwable unwrap(Throwable error) {
        Throwable current = Objects.requireNonNull(error, "error");
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static <T> T await(CompletableFuture<T> future)
            throws InterruptedException, ManagedLedgerException {
        try {
            return future.get();
        } catch (ExecutionException error) {
            Throwable cause = unwrap(error);
            if (cause instanceof ManagedLedgerException managedLedger) {
                throw managedLedger;
            }
            throw new ManagedLedgerException(cause);
        }
    }

    private static <T> T awaitUnchecked(CompletableFuture<T> future)
            throws ManagedLedgerException {
        try {
            return future.get();
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new ManagedLedgerException(error);
        } catch (ExecutionException error) {
            Throwable cause = unwrap(error);
            if (cause instanceof ManagedLedgerException managedLedger) {
                throw managedLedger;
            }
            throw new ManagedLedgerException(cause);
        }
    }

    private final class Pending implements PendingReadWaiter {
        private final long nextOffset;
        private final OptionalLong maxOffset;
        private final int count;
        private final long maxSizeBytes;
        private final Position maxPosition;
        private final Predicate<Position> skipCondition;
        private final AsyncCallbacks.ReadEntriesCallback callback;
        private final Object ctx;
        private final AtomicBoolean terminal = new AtomicBoolean();

        private Pending(
                long nextOffset,
                OptionalLong maxOffset,
                int count,
                long maxSizeBytes,
                Position maxPosition,
                Predicate<Position> skipCondition,
                AsyncCallbacks.ReadEntriesCallback callback,
                Object ctx) {
            this.nextOffset = nextOffset;
            this.maxOffset = maxOffset;
            this.count = count;
            this.maxSizeBytes = maxSizeBytes;
            this.maxPosition = maxPosition;
            this.skipCondition = skipCondition;
            this.callback = callback;
            this.ctx = ctx;
        }

        @Override
        public long nextOffset() {
            return nextOffset;
        }

        @Override
        public OptionalLong inclusiveMaxOffset() {
            return maxOffset;
        }

        @Override
        public boolean trySignal(StreamMetadata snapshot) {
            long candidate = nextDispatchableOffset(
                    Math.max(nextOffset, snapshot.trimOffset()),
                    state().acknowledgements());
            boolean readable = candidate < snapshot.committedEndOffset()
                    && (maxOffset.isEmpty() || candidate <= maxOffset.getAsLong());
            if (!readable && snapshot.state() != StreamState.SEALED) {
                return false;
            }
            if (!terminal.compareAndSet(false, true)) {
                return true;
            }
            pendingRead.compareAndSet(this, null);
            if (!readable) {
                completeReadCallback(
                        callback,
                        ctx,
                        null,
                        new ManagedLedgerException.ManagedLedgerTerminatedException(
                                "sealed cursor is at tail"),
                        "readEntriesOrWait");
            } else {
                asyncRead(
                        count,
                        maxSizeBytes,
                        callback,
                        ctx,
                        maxPosition,
                        skipCondition,
                        true);
            }
            return true;
        }

        @Override
        public boolean tryFail(ManagedLedgerException error) {
            if (!terminal.compareAndSet(false, true)) {
                return true;
            }
            pendingRead.compareAndSet(this, null);
            completeReadCallback(callback, ctx, null, error, "readEntriesOrWait");
            return true;
        }

        private boolean cancel() {
            if (!terminal.compareAndSet(false, true)) {
                return false;
            }
            pendingRead.compareAndSet(this, null);
            return ledger.tailPoll().remove(this);
        }
    }
}
