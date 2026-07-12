/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.nereusstream.api.AppendAttemptId;
import com.nereusstream.api.AppendOutcome;
import com.nereusstream.api.AppendResult;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StreamMetadata;
import com.nereusstream.api.StreamState;
import com.nereusstream.managedledger.callbacks.SerialCallbackLane;
import com.nereusstream.managedledger.config.ManagedLedgerConfigValidator;
import com.nereusstream.managedledger.entry.EncodedAppend;
import com.nereusstream.managedledger.entry.PulsarEntryCodec;
import com.nereusstream.managedledger.errors.OperationContext;
import com.nereusstream.managedledger.projection.F2L0RequestFactory;
import com.nereusstream.managedledger.projection.PositionProjection;
import com.nereusstream.managedledger.projection.ProjectionValidationException;
import com.nereusstream.managedledger.projection.StreamPositionBounds;
import com.nereusstream.managedledger.projection.VirtualLedgerProjection;
import com.nereusstream.managedledger.snapshot.StreamSnapshotTracker;
import com.nereusstream.managedledger.snapshot.TailPollCoordinator;
import com.nereusstream.managedledger.stats.NereusManagedLedgerStats;
import com.nereusstream.metadata.oxia.ManagedLedgerFacadeState;
import com.nereusstream.metadata.oxia.records.TopicProjectionRecord;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.CloseCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteLedgerCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.TerminateCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.UpdatePropertiesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerMXBean;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionBound;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo.LedgerInfo;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats;
import org.apache.pulsar.common.util.DateFormatter;

/** One writable Pulsar managed-ledger facade over one immutable F2 virtual-ledger projection. */
public final class NereusManagedLedger extends AbstractNereusManagedLedger
        implements NereusWriteFenceView, NereusCursorLedgerView {
    private static final Duration TERMINAL_OBSERVER_TIMEOUT = Duration.ofNanos(Long.MAX_VALUE);

    private enum LocalState {
        OPEN,
        TERMINATING,
        SEALED,
        DELETING,
        DELETED,
        PERMANENTLY_FENCED,
        CLOSED
    }

    private record WriteFence(
            long generation,
            AppendAttemptId attemptId,
            CompletableFuture<NereusWriteFenceResolution> terminal) {
    }

    private final NereusManagedLedgerRuntime runtime;
    private final VirtualLedgerProjection projection;
    private final StreamSnapshotTracker snapshots;
    private final PulsarEntryCodec entryCodec;
    private final PositionProjection positions = new PositionProjection();
    private final F2L0RequestFactory requests = new F2L0RequestFactory();
    private final SerialCallbackLane callbacks;
    private final AtomicReference<TopicProjectionRecord> topicProjection;
    private final ConcurrentHashMap<String, NereusManagedCursor> cursors = new ConcurrentHashMap<>();
    private final AtomicInteger pendingAdds = new AtomicInteger();
    private final AtomicLong lastAddEntryTime = new AtomicLong();
    private final NereusManagedLedgerStats stats;
    private final TailPollCoordinator tailPoll;
    private final AtomicBoolean closeNotified = new AtomicBoolean();
    private final Runnable onClose;
    private volatile ManagedLedgerConfig config;
    private LocalState state;
    private long fenceGeneration;
    private WriteFence currentFence;
    private WriteFence lastFence;

    public NereusManagedLedger(
            NereusManagedLedgerRuntime runtime,
            NereusLedgerOpenResult opened,
            ManagedLedgerConfig config,
            Runnable onClose) {
        this.runtime = Objects.requireNonNull(runtime, "runtime");
        NereusLedgerOpenResult result = Objects.requireNonNull(opened, "opened");
        this.config = Objects.requireNonNull(config, "config");
        ManagedLedgerConfigValidator.captureForOperation(config);
        this.onClose = Objects.requireNonNull(onClose, "onClose");
        this.projection = result.projection();
        this.topicProjection = new AtomicReference<>(result.topicProjection());
        this.snapshots = new StreamSnapshotTracker(result.streamMetadata(), 0);
        this.entryCodec = new PulsarEntryCodec(runtime.config().maxEntryBytes());
        this.callbacks = new SerialCallbackLane(runtime.callbackExecutor(), runtime.config().maxPendingCallbacks());
        this.stats = new NereusManagedLedgerStats(
                projection.managedLedgerName(), () -> snapshots.current().metadata().cumulativeSize());
        this.tailPoll = new TailPollCoordinator(
                runtime.scheduler(), runtime.config().tailPollInterval(),
                this::refreshMetadata, this::currentMetadata);
        this.state = switch (result.streamMetadata().state()) {
            case ACTIVE -> LocalState.OPEN;
            case SEALED -> LocalState.SEALED;
            case DELETING -> LocalState.DELETING;
            case DELETED -> LocalState.DELETED;
            case CREATING -> throw new IllegalArgumentException("published ledger cannot be CREATING");
        };
    }

    public VirtualLedgerProjection projection() {
        return projection;
    }

    @Override
    public NereusManagedLedgerRuntime runtime() {
        return runtime;
    }

    @Override
    public StreamMetadata currentMetadata() {
        return snapshots.current().metadata();
    }

    @Override
    public CompletableFuture<StreamMetadata> refreshMetadata() {
        return runtime.streamStorage().getStreamMetadata(projection.streamId())
                .thenApply(metadata -> snapshots.updateFromMetadata(metadata).metadata());
    }

    @Override
    public CompletableFuture<Entry> readAt(long offset, StreamMetadata metadata) {
        Position position = positions.entryPosition(projection, offset);
        positions.requireReadableEntryOffset(projection, position, metadata);
        return runtime.streamStorage().read(
                        projection.streamId(),
                        offset,
                        requests.singleEntryReadOptions(
                                runtime.config().maxEntryBytes(), runtime.config().readTimeout()))
                .thenApply(result -> entryCodec.decode(position, result));
    }

    @Override
    public Position readPosition(long offset, StreamMetadata metadata) {
        return positions.readPosition(projection, offset, metadata);
    }

    @Override
    public Position normalizeInclusiveMax(Position position, StreamMetadata metadata) {
        return positions.normalizeInclusiveMaxPosition(projection, position, metadata);
    }

    @Override
    public String getName() {
        return projection.managedLedgerName();
    }

    @Override
    public ManagedLedgerConfig getConfig() {
        return config;
    }

    @Override
    public void setConfig(ManagedLedgerConfig newConfig) {
        ManagedLedgerConfigValidator.captureForOperation(newConfig);
        config = newConfig;
    }

    @Override
    public ManagedCursor openCursor(String name) throws ManagedLedgerException {
        return openCursor(name, InitialPosition.Earliest);
    }

    @Override
    public ManagedCursor openCursor(String name, InitialPosition initialPosition) throws ManagedLedgerException {
        return openCursor(name, initialPosition, Map.of(), Map.of());
    }

    @Override
    public ManagedCursor openCursor(
            String name,
            InitialPosition initialPosition,
            Map<String, Long> properties,
            Map<String, String> cursorProperties) throws ManagedLedgerException {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(initialPosition, "initialPosition");
        try {
            return cursors.computeIfAbsent(name, ignored -> initialCursor(
                    name, true, initialPosition, properties, cursorProperties));
        } catch (CompletionException e) {
            Throwable cause = unwrap(e);
            if (cause instanceof ManagedLedgerException managedLedger) throw managedLedger;
            throw new ManagedLedgerException(cause);
        }
    }

    @Override
    public void asyncOpenCursor(String name, OpenCursorCallback callback, Object ctx) {
        asyncOpenCursor(name, InitialPosition.Earliest, callback, ctx);
    }

    @Override
    public void asyncOpenCursor(
            String name, InitialPosition initialPosition, OpenCursorCallback callback, Object ctx) {
        asyncOpenCursor(name, initialPosition, Map.of(), Map.of(), callback, ctx);
    }

    @Override
    public void asyncOpenCursor(
            String name,
            InitialPosition initialPosition,
            Map<String, Long> properties,
            Map<String, String> cursorProperties,
            OpenCursorCallback callback,
            Object ctx) {
        try {
            callback.openCursorComplete(openCursor(
                    name, initialPosition, properties, cursorProperties), ctx);
        } catch (ManagedLedgerException e) {
            callback.openCursorFailed(e, ctx);
        }
    }

    @Override
    public ManagedCursor newNonDurableCursor(Position startCursorPosition) throws ManagedLedgerException {
        return newNonDurableCursor(startCursorPosition, "non-durable-" + System.identityHashCode(startCursorPosition));
    }

    @Override
    public ManagedCursor newNonDurableCursor(Position startPosition, String subscriptionName)
            throws ManagedLedgerException {
        return newNonDurableCursor(startPosition, subscriptionName, InitialPosition.Earliest, false);
    }

    @Override
    public ManagedCursor newNonDurableCursor(
            Position startPosition,
            String subscriptionName,
            InitialPosition initialPosition,
            boolean isReadCompacted) throws ManagedLedgerException {
        if (isReadCompacted) {
            throw unsupported("newNonDurableCursor(readCompacted)");
        }
        StreamMetadata metadata = currentMetadata();
        long offset = normalizeCursorReadOffset(startPosition, initialPosition, metadata);
        Position read = readPosition(offset, metadata);
        Position markDelete = PositionFactory.create(projection.virtualLedgerId(), offset - 1);
        return createLocalCursor(subscriptionName, false, markDelete, read, Map.of(), Map.of());
    }

    @Override
    public void deleteCursor(String name) throws ManagedLedgerException {
        NereusManagedCursor cursor = cursors.remove(name);
        if (cursor == null) {
            throw new ManagedLedgerException.CursorNotFoundException("cursor does not exist");
        }
        cursor.close();
    }

    @Override
    public void asyncDeleteCursor(String name, DeleteCursorCallback callback, Object ctx) {
        try { deleteCursor(name); callback.deleteCursorComplete(ctx); }
        catch (ManagedLedgerException e) { callback.deleteCursorFailed(e, ctx); }
    }

    @Override public Iterable<ManagedCursor> getCursors() { return List.copyOf(cursors.values()); }
    @Override public Iterable<ManagedCursor> getActiveCursors() { return cursors.values().stream().filter(ManagedCursor::isActive).map(value -> (ManagedCursor) value).toList(); }
    @Override public void removeWaitingCursor(ManagedCursor cursor) { }

    @Override
    public Position addEntry(byte[] data) throws ManagedLedgerException {
        return addEntry(data, 1);
    }

    @Override
    public Position addEntry(byte[] data, int numberOfMessages) throws ManagedLedgerException {
        Objects.requireNonNull(data, "data");
        return addEntry(data, numberOfMessages, 0, data.length);
    }

    @Override
    public Position addEntry(byte[] data, int offset, int length) throws ManagedLedgerException {
        return addEntry(data, 1, offset, length);
    }

    @Override
    public Position addEntry(byte[] data, int numberOfMessages, int offset, int length)
            throws ManagedLedgerException {
        CompletableFuture<Position> result = new CompletableFuture<>();
        asyncAddEntry(data, numberOfMessages, offset, length, new AddEntryCallback() {
            @Override
            public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                result.complete(position);
            }

            @Override
            public void addFailed(ManagedLedgerException exception, Object ctx) {
                result.completeExceptionally(exception);
            }
        }, null);
        return await(result, runtime.config().closeTimeout());
    }

    @Override
    public void asyncAddEntry(byte[] data, AddEntryCallback callback, Object ctx) {
        Objects.requireNonNull(data, "data");
        asyncAddEntry(data, 1, 0, data.length, callback, ctx);
    }

    @Override
    public void asyncAddEntry(
            byte[] data, int offset, int length, AddEntryCallback callback, Object ctx) {
        asyncAddEntry(data, 1, offset, length, callback, ctx);
    }

    @Override
    public void asyncAddEntry(
            byte[] data,
            int numberOfMessages,
            int offset,
            int length,
            AddEntryCallback callback,
            Object ctx) {
        Objects.requireNonNull(data, "data");
        Objects.checkFromIndexSize(offset, length, data.length);
        asyncAddEntry(Unpooled.wrappedBuffer(data, offset, length), numberOfMessages, callback, ctx);
    }

    @Override
    public void asyncAddEntry(ByteBuf buffer, AddEntryCallback callback, Object ctx) {
        asyncAddEntry(buffer, 1, callback, ctx);
    }

    @Override
    public void asyncAddEntry(
            ByteBuf buffer, int numberOfMessages, AddEntryCallback callback, Object ctx) {
        Objects.requireNonNull(buffer, "buffer");
        Objects.requireNonNull(callback, "callback");
        Admission admission = admit("append", callback::addFailed, ctx);
        if (admission == null) {
            return;
        }
        EncodedAppend encoded;
        try {
            requireAppendable();
            encoded = entryCodec.encode(buffer, numberOfMessages);
        } catch (Throwable error) {
            fail(admission, callback::addFailed, ctx, error, "append", false);
            return;
        }
        pendingAdds.incrementAndGet();
        long startedNanos = System.nanoTime();
        runtime.streamStorage().append(
                        projection.streamId(),
                        encoded.appendBatch(),
                        requests.appendOptions(runtime.config().appendTimeout()))
                .whenComplete((result, error) -> {
                    if (error == null) {
                        completeAppend(admission, encoded, result, callback, ctx, Optional.empty(), startedNanos);
                    } else {
                        recoverOrFailAppend(admission, encoded, unwrap(error), callback, ctx, startedNanos);
                    }
                });
    }

    @Override
    public void asyncReadEntry(Position position, ReadEntryCallback callback, Object ctx) {
        Objects.requireNonNull(callback, "callback");
        Admission admission = admit("readEntry", callback::readEntryFailed, ctx);
        if (admission == null) {
            return;
        }
        long offset;
        try {
            requireReadable();
            offset = positions.requireReadableEntryOffset(
                    projection, position, snapshots.current().metadata());
        } catch (Throwable error) {
            stats.recordReadFailure();
            fail(admission, callback::readEntryFailed, ctx, error, "readEntry", true);
            return;
        }
        runtime.streamStorage().read(
                        projection.streamId(),
                        offset,
                        requests.singleEntryReadOptions(
                                runtime.config().maxEntryBytes(), runtime.config().readTimeout()))
                .whenComplete((result, error) -> {
                    if (error != null) {
                        stats.recordReadFailure();
                        fail(admission, callback::readEntryFailed, ctx, unwrap(error), "readEntry", true);
                        return;
                    }
                    Entry entry;
                    try {
                        entry = entryCodec.decode(position, result);
                    } catch (Throwable invalid) {
                        stats.recordReadFailure();
                        fail(admission, callback::readEntryFailed, ctx, invariant(invalid), "readEntry", true);
                        return;
                    }
                    stats.recordReadSuccess(entry.getLength());
                    finish(admission, () -> callback.readEntryComplete(entry, ctx));
                });
    }

    @Override
    public synchronized Optional<NereusWriteFenceSnapshot> currentWriteFence() {
        return currentFence == null
                ? Optional.empty()
                : Optional.of(new NereusWriteFenceSnapshot(currentFence.generation(), currentFence.attemptId()));
    }

    @Override
    public synchronized CompletableFuture<NereusWriteFenceResolution> awaitWriteFence(long generation) {
        if (currentFence != null && currentFence.generation() == generation) {
            return currentFence.terminal();
        }
        if (lastFence != null && lastFence.generation() == generation) {
            return lastFence.terminal();
        }
        return CompletableFuture.failedFuture(new NereusException(
                ErrorCode.METADATA_INVARIANT_VIOLATION,
                false,
                "unknown or stale managed-ledger write-fence generation"));
    }

    @Override
    public void asyncTerminate(TerminateCallback callback, Object ctx) {
        Objects.requireNonNull(callback, "callback");
        Admission admission = admit("terminate", callback::terminateFailed, ctx);
        if (admission == null) {
            return;
        }
        synchronized (this) {
            if (state == LocalState.SEALED) {
                finish(admission, () -> callback.terminateComplete(lastConfirmed(), ctx));
                return;
            }
            if (state != LocalState.OPEN || currentFence != null) {
                fail(admission, callback::terminateFailed, ctx, fencedOrClosed("terminate"), "terminate", false);
                return;
            }
            state = LocalState.TERMINATING;
        }
        runtime.streamStorage().seal(
                        projection.streamId(), requests.sealOptions(runtime.config().metadataTimeout()))
                .thenCompose(metadata -> {
                    snapshots.updateFromMetadata(metadata);
                    return mirrorState(ManagedLedgerFacadeState.SEALED);
                })
                .whenComplete((ignored, error) -> {
                    if (error == null) {
                        synchronized (this) {
                            state = LocalState.SEALED;
                        }
                        finish(admission, () -> callback.terminateComplete(lastConfirmed(), ctx));
                    } else {
                        synchronized (this) {
                            if (state == LocalState.TERMINATING) {
                                state = LocalState.OPEN;
                            }
                        }
                        fail(admission, callback::terminateFailed, ctx, unwrap(error), "terminate", false);
                    }
                });
    }

    @Override
    public Position terminate() throws ManagedLedgerException {
        CompletableFuture<Position> result = new CompletableFuture<>();
        asyncTerminate(new TerminateCallback() {
            @Override
            public void terminateComplete(Position lastCommittedPosition, Object ctx) {
                result.complete(lastCommittedPosition);
            }

            @Override
            public void terminateFailed(ManagedLedgerException exception, Object ctx) {
                result.completeExceptionally(exception);
            }
        }, null);
        return await(result, runtime.config().closeTimeout());
    }

    @Override
    public void asyncDelete(DeleteLedgerCallback callback, Object ctx) {
        Objects.requireNonNull(callback, "callback");
        Admission admission = admit("delete", callback::deleteLedgerFailed, ctx);
        if (admission == null) {
            return;
        }
        synchronized (this) {
            if (state == LocalState.DELETED) {
                finish(admission, () -> callback.deleteLedgerComplete(ctx));
                return;
            }
            if ((state != LocalState.OPEN && state != LocalState.SEALED) || currentFence != null) {
                fail(admission, callback::deleteLedgerFailed, ctx, fencedOrClosed("delete"), "delete", false);
                return;
            }
            state = LocalState.DELETING;
        }
        runtime.streamStorage().delete(
                        projection.streamId(), requests.deleteOptions(runtime.config().metadataTimeout()))
                .thenCompose(metadata -> {
                    snapshots.updateFromMetadata(metadata);
                    return mirrorDeleteState();
                })
                .whenComplete((ignored, error) -> {
                    if (error == null) {
                        synchronized (this) {
                            state = LocalState.DELETED;
                        }
                        finish(admission, () -> callback.deleteLedgerComplete(ctx));
                    } else {
                        fail(admission, callback::deleteLedgerFailed, ctx, unwrap(error), "delete", false);
                    }
                });
    }

    @Override
    public void delete() throws ManagedLedgerException {
        CompletableFuture<Void> result = new CompletableFuture<>();
        asyncDelete(new DeleteLedgerCallback() {
            @Override
            public void deleteLedgerComplete(Object ctx) {
                result.complete(null);
            }

            @Override
            public void deleteLedgerFailed(ManagedLedgerException exception, Object ctx) {
                result.completeExceptionally(exception);
            }
        }, null);
        await(result, runtime.config().closeTimeout());
    }

    @Override
    public void asyncClose(CloseCallback callback, Object ctx) {
        Objects.requireNonNull(callback, "callback");
        long sequence;
        synchronized (this) {
            if (state == LocalState.CLOSED) {
                runtime.callbackExecutor().execute(() -> callback.closeComplete(ctx));
                return;
            }
            state = LocalState.CLOSED;
            failFenceOnClose();
        }
        try {
            sequence = callbacks.admit();
        } catch (Throwable error) {
            notifyClosed();
            runtime.callbackExecutor().execute(() -> callback.closeFailed(
                    errorMapper.map(error, OperationContext.ledger("close")), ctx));
            return;
        }
        callbacks.complete(sequence, () -> {
            tailPoll.close();
            List.copyOf(cursors.values()).forEach(NereusManagedCursor::close);
            notifyClosed();
            callback.closeComplete(ctx);
        });
        callbacks.closeAfterDrain();
    }

    @Override
    public void close() throws ManagedLedgerException {
        CompletableFuture<Void> result = new CompletableFuture<>();
        asyncClose(new CloseCallback() {
            @Override
            public void closeComplete(Object ctx) {
                result.complete(null);
            }

            @Override
            public void closeFailed(ManagedLedgerException exception, Object ctx) {
                result.completeExceptionally(exception);
            }
        }, null);
        await(result, runtime.config().closeTimeout());
    }

    @Override
    public long getNumberOfEntries() {
        StreamMetadata metadata = snapshots.current().metadata();
        return metadata.committedEndOffset() - metadata.trimOffset();
    }

    @Override
    public long getNumberOfEntries(Range<Position> range) {
        Objects.requireNonNull(range, "range");
        StreamMetadata metadata = snapshots.current().metadata();
        long lower = metadata.trimOffset();
        long upper = metadata.committedEndOffset();
        if (range.hasLowerBound()) {
            long entry = requireVirtualLedgerPosition(range.lowerEndpoint());
            lower = range.lowerBoundType() == BoundType.CLOSED ? entry : incrementSaturated(entry);
        }
        if (range.hasUpperBound()) {
            long entry = requireVirtualLedgerPosition(range.upperEndpoint());
            upper = range.upperBoundType() == BoundType.CLOSED ? incrementSaturated(entry) : entry;
        }
        lower = Math.max(lower, metadata.trimOffset());
        upper = Math.min(upper, metadata.committedEndOffset());
        return Math.max(0, upper - lower);
    }

    @Override
    public long getNumberOfActiveEntries() {
        ManagedCursor slowest = getSlowestConsumer();
        return slowest == null ? 0 : slowest.getNumberOfEntriesInBacklog(true);
    }

    @Override
    public long getTotalSize() {
        return snapshots.current().metadata().cumulativeSize();
    }

    @Override
    public long getEstimatedBacklogSize() {
        ManagedCursor slowest = getSlowestConsumer();
        return slowest == null ? 0 : getEstimatedBacklogSize(slowest.getReadPosition());
    }

    @Override
    public long getEstimatedBacklogSize(Position position) {
        StreamMetadata metadata = snapshots.current().metadata();
        long offset = requireVirtualLedgerPosition(position);
        offset = Math.max(metadata.trimOffset(), Math.min(offset, metadata.committedEndOffset()));
        long entries = metadata.committedEndOffset() - offset;
        if (entries == 0 || metadata.committedEndOffset() == 0) {
            return 0;
        }
        return Math.round((double) metadata.cumulativeSize() * entries / metadata.committedEndOffset());
    }

    @Override
    public Position getLastConfirmedEntry() {
        return lastConfirmed();
    }

    @Override
    public Position getFirstPosition() {
        return positions.bounds(projection, snapshots.current().metadata()).beforeFirstAvailable();
    }

    @Override
    public Position getNextValidPosition(Position position) {
        StreamMetadata metadata = snapshots.current().metadata();
        long offset = position.getEntryId() == metadata.trimOffset() - 1
                && position.getLedgerId() == projection.virtualLedgerId()
                ? metadata.trimOffset()
                : Math.addExact(positions.requireReadableEntryOffset(projection, position, metadata), 1);
        return positions.readPosition(projection, offset, metadata);
    }

    @Override
    public Position getPreviousPosition(Position position) {
        StreamMetadata metadata = snapshots.current().metadata();
        long offset = positions.requireReadPositionOffset(projection, position, metadata);
        long previous = Math.max(metadata.trimOffset() - 1, offset - 1);
        return PositionFactory.create(projection.virtualLedgerId(), previous);
    }

    @Override
    public Position getPositionAfterN(Position startPosition, long n, PositionBound bound) {
        if (n < 0) {
            throw new IllegalArgumentException("n cannot be negative");
        }
        StreamMetadata metadata = snapshots.current().metadata();
        long start = positions.requireReadPositionOffset(projection, startPosition, metadata);
        if (bound == PositionBound.startExcluded && start < metadata.committedEndOffset()) {
            start++;
        }
        long requested;
        try {
            requested = Math.addExact(start, n);
        } catch (ArithmeticException ignored) {
            requested = Long.MAX_VALUE;
        }
        long result = Math.min(requested, metadata.committedEndOffset());
        return PositionFactory.create(projection.virtualLedgerId(), result);
    }

    @Override
    public boolean isTerminated() {
        synchronized (this) {
            return state == LocalState.SEALED;
        }
    }

    @Override
    public ManagedCursor getSlowestConsumer() {
        return cursors.values().stream()
                .filter(NereusManagedCursor::isDurable)
                .min(java.util.Comparator.comparing(NereusManagedCursor::getMarkDeletedPosition))
                .orElse(null);
    }

    @Override
    public int getPendingAddEntriesCount() {
        return pendingAdds.get();
    }

    @Override
    public ManagedLedgerMXBean getStats() {
        return stats;
    }

    @Override
    public CompletableFuture<Long> getEarliestMessagePublishTimeInBacklog() {
        return CompletableFuture.completedFuture(0L);
    }

    @Override
    public CompletableFuture<ManagedLedgerInternalStats> getManagedLedgerInternalStats(
            boolean includeLedgerMetadata) {
        StreamMetadata metadata = snapshots.current().metadata();
        ManagedLedgerInternalStats internal = new ManagedLedgerInternalStats();
        internal.entriesAddedCounter = stats.getAddEntrySucceedTotal();
        internal.numberOfEntries = metadata.committedEndOffset() - metadata.trimOffset();
        internal.totalSize = metadata.cumulativeSize();
        internal.currentLedgerEntries = metadata.committedEndOffset();
        internal.currentLedgerSize = metadata.cumulativeSize();
        internal.lastLedgerCreatedTimestamp = DateFormatter.format(projection.createdAtMillis());
        internal.waitingCursorsCount = 0;
        internal.pendingAddEntriesCount = pendingAdds.get();
        internal.lastConfirmedEntry = lastConfirmed().toString();
        synchronized (this) {
            internal.state = switch (state) {
                case OPEN -> "LedgerOpened";
                case SEALED -> "Terminated";
                case TERMINATING, DELETING -> "Closing";
                case DELETED, CLOSED -> "Closed";
                case PERMANENTLY_FENCED -> "Fenced";
            };
        }
        internal.properties = new java.util.HashMap<>(getProperties());
        ManagedLedgerInternalStats.LedgerInfo ledger = new ManagedLedgerInternalStats.LedgerInfo();
        ledger.ledgerId = projection.virtualLedgerId();
        ledger.entries = metadata.committedEndOffset();
        ledger.size = metadata.cumulativeSize();
        ledger.offloaded = false;
        ledger.underReplicated = false;
        ledger.properties = Map.of();
        ledger.metadata = includeLedgerMetadata
                ? "nereus{streamId=" + projection.streamId().value()
                        + ",incarnation=" + projection.incarnation()
                        + ",profile=" + metadata.profile().name() + "}"
                : null;
        internal.ledgers = List.of(ledger);
        java.util.TreeMap<String, ManagedLedgerInternalStats.CursorStats> cursorStats = new java.util.TreeMap<>();
        cursors.forEach((name, cursor) -> cursorStats.put(name, cursor.getCursorStats()));
        internal.cursors = Map.copyOf(cursorStats);
        return CompletableFuture.completedFuture(internal);
    }

    @Override
    public CompletableFuture<Position> asyncFindPosition(Predicate<Entry> predicate) {
        Objects.requireNonNull(predicate, "predicate");
        StreamMetadata metadata = snapshots.current().metadata();
        long last = metadata.committedEndOffset() - 1;
        return searchBackward(predicate, last, metadata, runtime.config().maxScanEntries())
                .thenApply(found -> found
                        .map(this::getNextValidPosition)
                        .orElseGet(() -> positions.bounds(projection, metadata).firstAvailable()));
    }

    @Override
    public CompletableFuture<Position> getLastDispatchablePosition(
            Predicate<Entry> predicate, Position startPosition) {
        Objects.requireNonNull(predicate, "predicate");
        StreamMetadata metadata = snapshots.current().metadata();
        Position normalized = positions.normalizeInclusiveMaxPosition(
                projection, startPosition, metadata);
        return searchBackward(
                        predicate,
                        normalized.getEntryId(),
                        metadata,
                        runtime.config().maxScanEntries())
                .thenApply(found -> found.orElseGet(
                        () -> positions.bounds(projection, metadata).beforeFirstAvailable()));
    }

    private CompletableFuture<Optional<Position>> searchBackward(
            Predicate<Entry> predicate,
            long offset,
            StreamMetadata metadata,
            int remaining) {
        if (offset < metadata.trimOffset()) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        if (remaining == 0) {
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.READ_LIMIT_TOO_SMALL, false, "managed-ledger scan budget is exhausted"));
        }
        Position position = positions.entryPosition(projection, offset);
        return runtime.streamStorage().read(
                        projection.streamId(),
                        offset,
                        requests.singleEntryReadOptions(
                                runtime.config().maxEntryBytes(), runtime.config().readTimeout()))
                .thenApply(result -> entryCodec.decode(position, result))
                .thenCompose(entry -> {
                    boolean matches;
                    try {
                        matches = predicate.test(entry);
                    } finally {
                        entry.release();
                    }
                    return matches
                            ? CompletableFuture.completedFuture(Optional.of(position))
                            : searchBackward(predicate, offset - 1, metadata, remaining - 1);
                });
    }

    @Override
    public long getCacheSize() {
        return 0;
    }

    @Override
    public Map<String, String> getProperties() {
        return topicProjection.get().properties();
    }

    @Override
    public void setProperty(String key, String value) throws ManagedLedgerException {
        updateProperties(current -> {
            Map<String, String> updated = new java.util.HashMap<>(current);
            updated.put(key, value);
            return updated;
        });
    }

    @Override
    public void deleteProperty(String key) throws ManagedLedgerException {
        updateProperties(current -> {
            Map<String, String> updated = new java.util.HashMap<>(current);
            updated.remove(key);
            return updated;
        });
    }

    @Override
    public void setProperties(Map<String, String> properties) throws ManagedLedgerException {
        updateProperties(ignored -> properties);
    }

    @Override
    public void asyncSetProperty(
            String key, String value, UpdatePropertiesCallback callback, Object ctx) {
        asyncUpdateProperties(current -> {
            Map<String, String> updated = new java.util.HashMap<>(current);
            updated.put(key, value);
            return updated;
        }, callback, ctx);
    }

    @Override
    public void asyncDeleteProperty(String key, UpdatePropertiesCallback callback, Object ctx) {
        asyncUpdateProperties(current -> {
            Map<String, String> updated = new java.util.HashMap<>(current);
            updated.remove(key);
            return updated;
        }, callback, ctx);
    }

    @Override
    public void asyncSetProperties(
            Map<String, String> properties, UpdatePropertiesCallback callback, Object ctx) {
        asyncUpdateProperties(ignored -> properties, callback, ctx);
    }

    @Override
    public CompletableFuture<LedgerInfo> getLedgerInfo(long ledgerId) {
        if (ledgerId != projection.virtualLedgerId()) {
            return CompletableFuture.completedFuture(null);
        }
        return CompletableFuture.completedFuture(syntheticLedgerInfo());
    }

    @Override
    public Optional<LedgerInfo> getOptionalLedgerInfo(long ledgerId) {
        return ledgerId == projection.virtualLedgerId()
                ? Optional.of(syntheticLedgerInfo())
                : Optional.empty();
    }

    @Override
    public NavigableMap<Long, LedgerInfo> getLedgersInfo() {
        TreeMap<Long, LedgerInfo> result = new TreeMap<>();
        result.put(projection.virtualLedgerId(), syntheticLedgerInfo());
        return Collections.unmodifiableNavigableMap(result);
    }

    private void recoverOrFailAppend(
            Admission admission,
            EncodedAppend encoded,
            Throwable error,
            AddEntryCallback callback,
            Object ctx,
            long startedNanos) {
        if (!(error instanceof NereusException nereus)) {
            failAppend(admission, callback, ctx, error);
            return;
        }
        AppendOutcome outcome = nereus.appendOutcome().orElse(AppendOutcome.KNOWN_NOT_COMMITTED);
        if (outcome == AppendOutcome.KNOWN_NOT_COMMITTED) {
            failAppend(admission, callback, ctx, nereus);
            return;
        }
        AppendAttemptId attemptId = nereus.appendAttemptId().orElse(null);
        if (attemptId == null) {
            synchronized (this) {
                state = LocalState.PERMANENTLY_FENCED;
            }
            failAppend(admission, callback, ctx, invariant(nereus));
            return;
        }
        WriteFence fence;
        try {
            fence = beginFence(attemptId);
        } catch (Throwable invariant) {
            failAppend(admission, callback, ctx, invariant);
            return;
        }
        runtime.streamStorage().recoverAppend(
                        projection.streamId(),
                        attemptId,
                        requests.recoveryOptions(runtime.config().appendRecoveryTimeout()))
                .whenComplete((result, recoveryError) -> {
                    if (recoveryError == null) {
                        completeAppend(admission, encoded, result, callback, ctx, Optional.of(fence), startedNanos);
                        return;
                    }
                    Throwable cause = unwrap(recoveryError);
                    if (isProvenNotCommitted(cause)) {
                        resolveFence(fence, NereusWriteFenceResolution.PROVEN_NOT_COMMITTED, null);
                        failAppend(admission, callback, ctx, cause);
                        return;
                    }
                    failAppend(admission, callback, ctx, cause);
                    if (isRetriableUncertainty(cause)) {
                        observeFenceTerminal(fence);
                    } else {
                        resolveFence(fence, null, cause);
                    }
                });
    }

    private void observeFenceTerminal(WriteFence fence) {
        runtime.streamStorage().recoverAppend(
                        projection.streamId(),
                        fence.attemptId(),
                        requests.recoveryOptions(TERMINAL_OBSERVER_TIMEOUT))
                .whenComplete((result, error) -> {
                    if (error == null) {
                        try {
                            snapshots.advanceFromAppend(result);
                            resolveFence(fence, NereusWriteFenceResolution.COMMITTED, null);
                        } catch (Throwable invariant) {
                            resolveFence(fence, null, invariant);
                        }
                    } else if (isProvenNotCommitted(unwrap(error))) {
                        resolveFence(fence, NereusWriteFenceResolution.PROVEN_NOT_COMMITTED, null);
                    } else {
                        resolveFence(fence, null, unwrap(error));
                    }
                });
    }

    private void completeAppend(
            Admission admission,
            EncodedAppend encoded,
            AppendResult result,
            AddEntryCallback callback,
            Object ctx,
            Optional<WriteFence> fence,
            long startedNanos) {
        Position position;
        try {
            snapshots.advanceFromAppend(result);
            tailPoll.signalLocalAppend();
            position = positions.entryPosition(projection, result.range().startOffset());
            fence.ifPresent(value -> resolveFence(
                    value, NereusWriteFenceResolution.COMMITTED, null));
        } catch (Throwable invalid) {
            fence.ifPresent(value -> resolveFence(value, null, invalid));
            failAppend(admission, callback, ctx, invariant(invalid));
            return;
        }
        ByteBuf callbackData = Unpooled.wrappedBuffer(encoded.callbackBytes()).asReadOnly();
        stats.recordAddSuccess(result.logicalBytes(), startedNanos);
        finish(admission, () -> {
            try {
                lastAddEntryTime.set(System.currentTimeMillis());
                callback.addComplete(position, callbackData, ctx);
            } finally {
                callbackData.release();
                pendingAdds.decrementAndGet();
            }
        });
    }

    private void failAppend(Admission admission, AddEntryCallback callback, Object ctx, Throwable error) {
        stats.recordAddFailure();
        ManagedLedgerException mapped = map(error, "append", false);
        finish(admission, () -> {
            try {
                callback.addFailed(mapped, ctx);
            } finally {
                pendingAdds.decrementAndGet();
            }
        });
    }

    private synchronized WriteFence beginFence(AppendAttemptId attemptId) {
        if (currentFence != null) {
            if (currentFence.attemptId().equals(attemptId)) {
                return currentFence;
            }
            throw invariant(new IllegalStateException("multiple unresolved append attempts"));
        }
        if (fenceGeneration == Long.MAX_VALUE) {
            state = LocalState.PERMANENTLY_FENCED;
            throw invariant(new ArithmeticException("write-fence generation exhausted"));
        }
        WriteFence created = new WriteFence(++fenceGeneration, attemptId, new CompletableFuture<>());
        currentFence = created;
        lastFence = null;
        return created;
    }

    private void resolveFence(
            WriteFence fence,
            NereusWriteFenceResolution resolution,
            Throwable error) {
        synchronized (this) {
            if (currentFence != fence) {
                return;
            }
            currentFence = null;
            lastFence = fence;
            if (error != null) {
                state = LocalState.PERMANENTLY_FENCED;
            }
        }
        if (error == null) {
            fence.terminal().complete(Objects.requireNonNull(resolution, "resolution"));
        } else {
            fence.terminal().completeExceptionally(error);
        }
    }

    private synchronized void failFenceOnClose() {
        WriteFence fence = currentFence;
        currentFence = null;
        lastFence = fence == null ? lastFence : fence;
        if (fence != null) {
            fence.terminal().completeExceptionally(new NereusException(
                    ErrorCode.STORAGE_CLOSED, false, "managed ledger closed with unresolved append recovery"));
        }
    }

    private CompletableFuture<TopicProjectionRecord> mirrorState(ManagedLedgerFacadeState target) {
        return mirrorStateAttempt(target, metadataDeadlineNanos());
    }

    private CompletableFuture<TopicProjectionRecord> mirrorStateAttempt(
            ManagedLedgerFacadeState target, long deadlineNanos) {
        TopicProjectionRecord current = topicProjection.get();
        return runtime.projectionStore().mirrorFacadeState(
                        runtime.cluster(),
                        getName(),
                        current.projectionIdentity(),
                        current.metadataVersion(),
                        target)
                .handle((updated, error) -> {
                    if (error == null) {
                        topicProjection.set(updated);
                        return CompletableFuture.completedFuture(updated);
                    }
                    Throwable cause = unwrap(error);
                    if (!isMetadataConditionFailure(cause) || deadlineExpired(deadlineNanos)) {
                        return CompletableFuture.<TopicProjectionRecord>failedFuture(cause);
                    }
                    return refreshTopic(current).thenCompose(
                            ignored -> mirrorStateAttempt(target, deadlineNanos));
                }).thenCompose(value -> value);
    }

    private CompletableFuture<TopicProjectionRecord> mirrorDeleteState() {
        TopicProjectionRecord current = topicProjection.get();
        CompletableFuture<TopicProjectionRecord> deleting =
                current.parsedFacadeState() == ManagedLedgerFacadeState.DELETING
                        ? CompletableFuture.completedFuture(current)
                        : mirrorState(ManagedLedgerFacadeState.DELETING);
        return deleting.thenCompose(ignored -> mirrorState(ManagedLedgerFacadeState.DELETED));
    }

    private void updateProperties(
            java.util.function.Function<Map<String, String>, Map<String, String>> mutation)
            throws ManagedLedgerException {
        CompletableFuture<Void> result = new CompletableFuture<>();
        asyncUpdateProperties(mutation, new UpdatePropertiesCallback() {
            @Override
            public void updatePropertiesComplete(Map<String, String> properties, Object ctx) {
                result.complete(null);
            }

            @Override
            public void updatePropertiesFailed(ManagedLedgerException exception, Object ctx) {
                result.completeExceptionally(exception);
            }
        }, null);
        await(result, runtime.config().metadataTimeout());
    }

    private void asyncUpdateProperties(
            java.util.function.Function<Map<String, String>, Map<String, String>> mutation,
            UpdatePropertiesCallback callback,
            Object ctx) {
        Objects.requireNonNull(mutation, "mutation");
        Objects.requireNonNull(callback, "callback");
        Admission admission = admit("updateProperties", callback::updatePropertiesFailed, ctx);
        if (admission == null) {
            return;
        }
        updatePropertiesAttempt(mutation, metadataDeadlineNanos())
                .whenComplete((record, error) -> {
                    if (error != null) {
                        fail(admission, callback::updatePropertiesFailed, ctx, unwrap(error),
                                "updateProperties", false);
                    } else {
                        finish(admission, () -> callback.updatePropertiesComplete(record.properties(), ctx));
                    }
                });
    }

    private CompletableFuture<TopicProjectionRecord> updatePropertiesAttempt(
            java.util.function.Function<Map<String, String>, Map<String, String>> mutation,
            long deadlineNanos) {
        TopicProjectionRecord current = topicProjection.get();
        Map<String, String> updated;
        try {
            updated = Map.copyOf(mutation.apply(current.properties()));
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
        return runtime.projectionStore().updateProperties(
                        runtime.cluster(),
                        getName(),
                        current.projectionIdentity(),
                        current.metadataVersion(),
                        updated)
                .handle((record, error) -> {
                    if (error == null) {
                        topicProjection.set(record);
                        return CompletableFuture.completedFuture(record);
                    }
                    Throwable cause = unwrap(error);
                    if (!isMetadataConditionFailure(cause) || deadlineExpired(deadlineNanos)) {
                        return CompletableFuture.<TopicProjectionRecord>failedFuture(cause);
                    }
                    return refreshTopic(current).thenCompose(
                            ignored -> updatePropertiesAttempt(mutation, deadlineNanos));
                }).thenCompose(value -> value);
    }

    private CompletableFuture<TopicProjectionRecord> refreshTopic(TopicProjectionRecord expected) {
        return runtime.projectionStore().getProjection(runtime.cluster(), getName())
                .thenApply(optional -> {
                    TopicProjectionRecord refreshed = optional.orElseThrow(() -> invariant(
                            new IllegalStateException("topic projection disappeared during CAS retry")));
                    if (!refreshed.projectionIdentity().equals(expected.projectionIdentity())) {
                        throw invariant(new IllegalStateException(
                                "topic projection identity changed during CAS retry"));
                    }
                    topicProjection.getAndUpdate(current ->
                            refreshed.metadataVersion() >= current.metadataVersion() ? refreshed : current);
                    return refreshed;
                });
    }

    private Admission admit(
            String operation,
            FailureCallback failure,
            Object ctx) {
        if (!runtime.tryAcquireCallbackPermit()) {
            ManagedLedgerException rejected = new ManagedLedgerException.TooManyRequestsException(
                    "Nereus callback capacity is exhausted");
            runtime.callbackExecutor().execute(() -> failure.fail(rejected, ctx));
            return null;
        }
        try {
            return new Admission(callbacks.admit());
        } catch (Throwable error) {
            runtime.releaseCallbackPermit();
            ManagedLedgerException mapped = map(error, operation, false);
            runtime.callbackExecutor().execute(() -> failure.fail(mapped, ctx));
            return null;
        }
    }

    private void finish(Admission admission, Runnable callback) {
        callbacks.complete(admission.sequence(), () -> {
            try {
                callback.run();
            } finally {
                runtime.releaseCallbackPermit();
            }
        });
    }

    private void fail(
            Admission admission,
            FailureCallback callback,
            Object ctx,
            Throwable error,
            String operation,
            boolean directRead) {
        ManagedLedgerException mapped = map(error, operation, directRead);
        finish(admission, () -> callback.fail(mapped, ctx));
    }

    private ManagedLedgerException map(Throwable error, String operation, boolean directRead) {
        StreamState observed = snapshots.current().metadata().state();
        return errorMapper.map(error, new OperationContext(
                operation, false, directRead, Optional.of(observed)));
    }

    private synchronized void requireAppendable() throws ManagedLedgerException {
        if (state == LocalState.OPEN && currentFence == null) {
            return;
        }
        throw fencedOrClosed("append");
    }

    private synchronized void requireReadable() throws ManagedLedgerException {
        if (state == LocalState.OPEN
                || state == LocalState.SEALED
                || state == LocalState.PERMANENTLY_FENCED) {
            return;
        }
        throw fencedOrClosed("readEntry");
    }

    private ManagedLedgerException fencedOrClosed(String operation) {
        return switch (state) {
            case SEALED, TERMINATING -> new ManagedLedgerException.ManagedLedgerTerminatedException(
                    "managed ledger is sealed or terminating");
            case CLOSED, DELETING, DELETED -> new ManagedLedgerException.ManagedLedgerAlreadyClosedException(
                    "managed ledger is closed or deleted");
            case PERMANENTLY_FENCED, OPEN -> new ManagedLedgerException.ManagedLedgerFencedException(
                    new IllegalStateException("Nereus " + operation + " is write-fenced"));
        };
    }

    private Position lastConfirmed() {
        return positions.bounds(projection, snapshots.current().metadata()).lastConfirmed();
    }

    @Override
    public long getLastAddEntryTime() {
        return lastAddEntryTime.get();
    }

    @Override
    public long getMetadataCreationTimestamp() {
        return projection.createdAtMillis();
    }

    private LedgerInfo syntheticLedgerInfo() {
        StreamMetadata metadata = snapshots.current().metadata();
        return new LedgerInfo()
                .setLedgerId(projection.virtualLedgerId())
                .setEntries(metadata.committedEndOffset())
                .setSize(metadata.cumulativeSize());
    }

    private NereusManagedCursor initialCursor(
            String name,
            boolean durable,
            InitialPosition initialPosition,
            Map<String, Long> properties,
            Map<String, String> cursorProperties) {
        StreamMetadata metadata = currentMetadata();
        long offset = initialPosition == InitialPosition.Latest
                ? metadata.committedEndOffset()
                : metadata.trimOffset();
        Position read = readPosition(offset, metadata);
        Position markDelete = PositionFactory.create(projection.virtualLedgerId(), offset - 1);
        return new NereusManagedCursor(
                this, name, durable, markDelete, read, properties, cursorProperties);
    }

    NereusManagedCursor createLocalCursor(
            String name,
            boolean durable,
            Position markDelete,
            Position read,
            Map<String, Long> properties,
            Map<String, String> cursorProperties) {
        NereusManagedCursor cursor = new NereusManagedCursor(
                this, name, durable, markDelete, read, properties, cursorProperties);
        if (name != null) {
            NereusManagedCursor existing = cursors.putIfAbsent(name, cursor);
            if (existing != null) return existing;
        }
        return cursor;
    }

    void removeCursor(NereusManagedCursor cursor) {
        if (cursor.getName() != null) {
            cursors.remove(cursor.getName(), cursor);
        }
    }

    TailPollCoordinator tailPoll() {
        return tailPoll;
    }

    long positionsMarkDeleteAfter(Position position) {
        return positions.markDeleteOffsetAfter(projection, position, currentMetadata());
    }

    long requireCursorReadOffset(Position position) {
        return positions.requireReadPositionOffset(projection, position, currentMetadata());
    }

    private long normalizeCursorReadOffset(
            Position position, InitialPosition initialPosition, StreamMetadata metadata) {
        if (position == null || samePosition(position, PositionFactory.EARLIEST)) {
            return initialPosition == InitialPosition.Latest
                    ? metadata.committedEndOffset()
                    : metadata.trimOffset();
        }
        if (samePosition(position, PositionFactory.LATEST)) {
            return metadata.committedEndOffset();
        }
        return positions.requireReadPositionOffset(projection, position, metadata);
    }

    private static boolean samePosition(Position left, Position right) {
        return left.getLedgerId() == right.getLedgerId() && left.getEntryId() == right.getEntryId();
    }

    private long requireVirtualLedgerPosition(Position position) {
        Objects.requireNonNull(position, "position");
        if (position.getLedgerId() != projection.virtualLedgerId()) {
            throw new ProjectionValidationException("position belongs to a different virtual ledger");
        }
        return position.getEntryId();
    }

    private static long incrementSaturated(long value) {
        return value == Long.MAX_VALUE ? Long.MAX_VALUE : value + 1;
    }

    private void notifyClosed() {
        if (closeNotified.compareAndSet(false, true)) {
            onClose.run();
        }
    }

    private static boolean isProvenNotCommitted(Throwable error) {
        return error instanceof NereusException nereus
                && nereus.appendOutcome().orElse(null) == AppendOutcome.KNOWN_NOT_COMMITTED;
    }

    private static boolean isRetriableUncertainty(Throwable error) {
        return error instanceof NereusException nereus
                && nereus.retriable()
                && nereus.appendOutcome().orElse(AppendOutcome.MAY_HAVE_COMMITTED)
                        != AppendOutcome.KNOWN_NOT_COMMITTED;
    }

    private static boolean isMetadataConditionFailure(Throwable error) {
        return error instanceof NereusException nereus
                && nereus.code() == ErrorCode.METADATA_CONDITION_FAILED;
    }

    private long metadataDeadlineNanos() {
        try {
            return Math.addExact(System.nanoTime(), runtime.config().metadataTimeout().toNanos());
        } catch (ArithmeticException ignored) {
            return Long.MAX_VALUE;
        }
    }

    private static boolean deadlineExpired(long deadlineNanos) {
        return deadlineNanos != Long.MAX_VALUE && System.nanoTime() - deadlineNanos >= 0;
    }

    private static NereusException invariant(Throwable cause) {
        return new NereusException(
                ErrorCode.METADATA_INVARIANT_VIOLATION,
                false,
                "managed-ledger facade invariant failed",
                cause);
    }

    private static Throwable unwrap(Throwable error) {
        Throwable current = error;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static <T> T await(CompletableFuture<T> future, Duration timeout)
            throws ManagedLedgerException {
        try {
            Objects.requireNonNull(timeout, "timeout");
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ManagedLedgerException(e);
        } catch (ExecutionException e) {
            Throwable cause = unwrap(e);
            if (cause instanceof ManagedLedgerException managedLedger) {
                throw managedLedger;
            }
            throw new ManagedLedgerException(cause);
        }
    }

    private record Admission(long sequence) {
    }

    @FunctionalInterface
    private interface FailureCallback {
        void fail(ManagedLedgerException exception, Object ctx);
    }
}
