/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger;

import com.google.common.collect.Range;
import com.nereusstream.managedledger.errors.ManagedLedgerErrorMapper;
import com.nereusstream.api.StreamMetadata;
import com.nereusstream.api.StreamState;
import com.nereusstream.managedledger.snapshot.PendingReadWaiter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedCursorMXBean;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats;

/** F2 local cursor; durable instances expose a boundary but reject every persistence mutation. */
public final class NereusManagedCursor implements ManagedCursor {
    private final NereusManagedLedger ledger;
    private final NereusReadOnlyCursor reader;
    private final String name;
    private final boolean durable;
    private final ManagedLedgerErrorMapper errors = new ManagedLedgerErrorMapper();
    private final Map<String, Long> properties = new ConcurrentHashMap<>();
    private final Map<String, String> cursorProperties = new ConcurrentHashMap<>();
    private final NereusManagedCursorStats stats;
    private final AtomicReference<Pending> pending = new AtomicReference<>();
    private volatile Position markDelete;
    private volatile long lastActive = System.currentTimeMillis();
    private volatile boolean active = true;
    private volatile boolean alwaysInactive;
    private volatile boolean closed;
    private volatile double throttleMarkDelete;

    NereusManagedCursor(
            NereusManagedLedger ledger,
            String name,
            boolean durable,
            Position markDelete,
            Position readPosition,
            Map<String, Long> properties,
            Map<String, String> cursorProperties) {
        this.ledger = Objects.requireNonNull(ledger, "ledger");
        this.name = Objects.requireNonNull(name, "name");
        this.durable = durable;
        this.markDelete = Objects.requireNonNull(markDelete, "markDelete");
        this.reader = new NereusReadOnlyCursor(ledger, Objects.requireNonNull(readPosition, "readPosition"));
        this.properties.putAll(properties == null ? Map.of() : properties);
        this.cursorProperties.putAll(cursorProperties == null ? Map.of() : cursorProperties);
        this.stats = new NereusManagedCursorStats(name, ledger.getName());
    }

    @Override public String getName() { return name; }
    @Override public long getLastActive() { return lastActive; }
    @Override public void updateLastActive() { lastActive = System.currentTimeMillis(); }
    @Override public Map<String, Long> getProperties() { return Map.copyOf(properties); }
    @Override public Map<String, String> getCursorProperties() { return Map.copyOf(cursorProperties); }
    @Override public CompletableFuture<Void> putCursorProperty(String key, String value) {
        if (durable) return unsupportedFuture("putCursorProperty"); cursorProperties.put(key, value); return CompletableFuture.completedFuture(null);
    }
    @Override public CompletableFuture<Void> setCursorProperties(Map<String, String> values) {
        if (durable) return unsupportedFuture("setCursorProperties"); cursorProperties.clear(); cursorProperties.putAll(values); return CompletableFuture.completedFuture(null);
    }
    @Override public CompletableFuture<Void> removeCursorProperty(String key) {
        if (durable) return unsupportedFuture("removeCursorProperty"); cursorProperties.remove(key); return CompletableFuture.completedFuture(null);
    }
    @Override public boolean putProperty(String key, Long value) { if (durable) throw unsupportedRuntime("putProperty"); return properties.put(key, value) != null; }
    @Override public boolean removeProperty(String key) { if (durable) throw unsupportedRuntime("removeProperty"); return properties.remove(key) != null; }
    @Override public List<Entry> readEntries(int count) throws InterruptedException, ManagedLedgerException { return reader.readEntries(count); }
    @Override public void asyncReadEntries(int count, AsyncCallbacks.ReadEntriesCallback cb, Object ctx, Position max) { reader.asyncReadEntries(count, cb, ctx, max); }
    @Override public void asyncReadEntries(int count, long bytes, AsyncCallbacks.ReadEntriesCallback cb, Object ctx, Position max) { reader.asyncReadEntries(count, bytes, cb, ctx, max); }
    @Override public Entry getNthEntry(int n, IndividualDeletedEntries deleted) throws InterruptedException, ManagedLedgerException {
        if (n <= 0 || deleted == IndividualDeletedEntries.Exclude) throw unsupported("getNthEntry");
        Position saved = reader.getReadPosition();
        reader.seekTo(ledger.readPosition(Math.min(ledger.currentMetadata().committedEndOffset(), markDelete.getEntryId() + n), ledger.currentMetadata()));
        List<Entry> entries = reader.readEntries(1); reader.seekTo(saved); return entries.isEmpty() ? null : entries.getFirst();
    }
    @Override public void asyncGetNthEntry(int n, IndividualDeletedEntries deleted, AsyncCallbacks.ReadEntryCallback cb, Object ctx) {
        try { Entry e = getNthEntry(n, deleted); if (e == null) cb.readEntryFailed(new ManagedLedgerException("entry not found"), ctx); else cb.readEntryComplete(e, ctx); }
        catch (Exception e) { cb.readEntryFailed(e instanceof ManagedLedgerException m ? m : new ManagedLedgerException(e), ctx); }
    }
    @Override public List<Entry> readEntriesOrWait(int count) throws InterruptedException, ManagedLedgerException { return readEntriesOrWait(count, Long.MAX_VALUE); }
    @Override public List<Entry> readEntriesOrWait(int count, long bytes) throws InterruptedException, ManagedLedgerException {
        CompletableFuture<List<Entry>> result = new CompletableFuture<>();
        asyncReadEntriesOrWait(count, bytes, new AsyncCallbacks.ReadEntriesCallback() {
            @Override public void readEntriesComplete(List<Entry> entries, Object ctx) { result.complete(entries); }
            @Override public void readEntriesFailed(ManagedLedgerException exception, Object ctx) { result.completeExceptionally(exception); }
        }, null, null);
        try { return result.get(); } catch (ExecutionException e) { Throwable c = e.getCause(); if (c instanceof ManagedLedgerException m) throw m; throw new ManagedLedgerException(c); }
    }
    @Override public void asyncReadEntriesOrWait(int count, AsyncCallbacks.ReadEntriesCallback cb, Object ctx, Position max) { asyncReadEntriesOrWait(count, Long.MAX_VALUE, cb, ctx, max); }
    @Override public void asyncReadEntriesOrWait(int count, long bytes, AsyncCallbacks.ReadEntriesCallback cb, Object ctx, Position max) {
        reader.asyncReadEntries(count, bytes, new AsyncCallbacks.ReadEntriesCallback() {
            @Override public void readEntriesComplete(List<Entry> entries, Object ignored) {
                if (!entries.isEmpty()) { cb.readEntriesComplete(entries, ctx); return; }
                StreamMetadata metadata = ledger.currentMetadata();
                if (metadata.state() == StreamState.SEALED) { cb.readEntriesFailed(new ManagedLedgerException.ManagedLedgerTerminatedException("sealed cursor is at tail"), ctx); return; }
                OptionalLong maxOffset = max == null ? OptionalLong.empty() : OptionalLong.of(ledger.normalizeInclusiveMax(max, metadata).getEntryId());
                Pending waiter = new Pending(reader.getReadPosition().getEntryId(), maxOffset, count, bytes, max, cb, ctx);
                if (!pending.compareAndSet(null, waiter)) { cb.readEntriesFailed(new ManagedLedgerException.ConcurrentWaitCallbackException(), ctx); return; }
                ledger.tailPoll().register(waiter);
            }
            @Override public void readEntriesFailed(ManagedLedgerException exception, Object ignored) { cb.readEntriesFailed(exception, ctx); }
        }, null, max);
    }
    @Override public boolean cancelPendingReadRequest() { Pending waiter = pending.getAndSet(null); return waiter != null && ledger.tailPoll().remove(waiter); }
    @Override public boolean hasMoreEntries() { return reader.hasMoreEntries(); }
    @Override public long getNumberOfEntries() { return reader.getNumberOfEntries(); }
    @Override public long getNumberOfEntriesInBacklog(boolean precise) { return Math.max(0, ledger.currentMetadata().committedEndOffset() - markDelete.getEntryId() - 1); }
    @Override public void markDelete(Position position) throws ManagedLedgerException { markDelete(position, Map.of()); }
    @Override public void markDelete(Position position, Map<String, Long> values) throws ManagedLedgerException {
        requireLocal("markDelete"); long next = ledger.positionsMarkDeleteAfter(position); markDelete = position; reader.seekTo(ledger.readPosition(next, ledger.currentMetadata())); properties.clear(); properties.putAll(values);
    }
    @Override public void asyncMarkDelete(Position p, AsyncCallbacks.MarkDeleteCallback cb, Object ctx) { asyncMarkDelete(p, Map.of(), cb, ctx); }
    @Override public void asyncMarkDelete(Position p, Map<String, Long> values, AsyncCallbacks.MarkDeleteCallback cb, Object ctx) { try { markDelete(p, values); cb.markDeleteComplete(ctx); } catch (ManagedLedgerException e) { cb.markDeleteFailed(e, ctx); } }
    @Override public void delete(Position p) throws ManagedLedgerException { throw unsupported("individualDelete"); }
    @Override public void asyncDelete(Position p, AsyncCallbacks.DeleteCallback cb, Object ctx) { cb.deleteFailed(unsupported("individualDelete"), ctx); }
    @Override public void delete(Iterable<Position> p) throws ManagedLedgerException { throw unsupported("individualDelete"); }
    @Override public void asyncDelete(Iterable<Position> p, AsyncCallbacks.DeleteCallback cb, Object ctx) { cb.deleteFailed(unsupported("individualDelete"), ctx); }
    @Override public Position getReadPosition() { return reader.getReadPosition(); }
    @Override public Position getMarkDeletedPosition() { return markDelete; }
    @Override public Position getPersistentMarkDeletedPosition() { return markDelete; }
    @Override public void rewind() { reader.seekTo(ledger.readPosition(markDelete.getEntryId() + 1, ledger.currentMetadata())); }
    @Override public void seek(Position p, boolean force) { long offset = ledger.requireCursorReadOffset(p); reader.seekTo(ledger.readPosition(offset, ledger.currentMetadata())); }
    @Override public void clearBacklog() throws ManagedLedgerException { requireLocal("clearBacklog"); Position lac = ledger.getLastConfirmedEntry(); markDelete = lac; reader.seekTo(ledger.readPosition(lac.getEntryId() + 1, ledger.currentMetadata())); }
    @Override public void asyncClearBacklog(AsyncCallbacks.ClearBacklogCallback cb, Object ctx) { try { clearBacklog(); cb.clearBacklogComplete(ctx); } catch (ManagedLedgerException e) { cb.clearBacklogFailed(e, ctx); } }
    @Override public void skipEntries(int count, IndividualDeletedEntries deleted) throws ManagedLedgerException { if (deleted == IndividualDeletedEntries.Exclude) throw unsupported("skipDeletedHoles"); reader.skipEntries(count); }
    @Override public void asyncSkipEntries(int count, IndividualDeletedEntries deleted, AsyncCallbacks.SkipEntriesCallback cb, Object ctx) { try { skipEntries(count, deleted); cb.skipEntriesComplete(ctx); } catch (ManagedLedgerException e) { cb.skipEntriesFailed(e, ctx); } }
    @Override public Position findNewestMatching(Predicate<Entry> condition) throws InterruptedException, ManagedLedgerException { return reader.findNewestMatching(FindPositionConstraint.SearchAllAvailableEntries, condition); }
    @Override public Position findNewestMatching(FindPositionConstraint c, Predicate<Entry> condition) throws InterruptedException, ManagedLedgerException { return reader.findNewestMatching(c, condition); }
    @Override public void asyncFindNewestMatching(FindPositionConstraint c, Predicate<Entry> condition, AsyncCallbacks.FindEntryCallback cb, Object ctx) { asyncFindNewestMatching(c, condition, cb, ctx, true); }
    @Override public void asyncFindNewestMatching(FindPositionConstraint c, Predicate<Entry> condition, AsyncCallbacks.FindEntryCallback cb, Object ctx, boolean fromLedger) { try { cb.findEntryComplete(findNewestMatching(c, condition), ctx); } catch (Exception e) { cb.findEntryFailed(e instanceof ManagedLedgerException m ? m : new ManagedLedgerException(e), Optional.empty(), ctx); } }
    @Override public void resetCursor(Position p) throws ManagedLedgerException { requireLocal("resetCursor"); seek(p, true); markDelete = PositionFactory.create(p.getLedgerId(), p.getEntryId() - 1); }
    @Override public void asyncResetCursor(Position p, boolean force, AsyncCallbacks.ResetCursorCallback cb) { try { resetCursor(p); cb.resetComplete(null); } catch (ManagedLedgerException e) { cb.resetFailed(e, null); } }
    @Override public List<Entry> replayEntries(Set<? extends Position> requested) throws ManagedLedgerException {
        List<Position> ordered = requested.stream().map(value -> (Position) value).sorted().toList();
        List<Entry> result = new ArrayList<>();
        try {
            for (Position position : ordered) {
                long offset = ledger.requireCursorReadOffset(position);
                if (offset >= ledger.currentMetadata().committedEndOffset()) continue;
                result.add(ledger.readAt(offset, ledger.currentMetadata()).join());
            }
            return result;
        } catch (RuntimeException error) {
            result.forEach(Entry::release);
            throw new ManagedLedgerException("Nereus replay failed", error);
        }
    }
    @Override public Set<? extends Position> asyncReplayEntries(Set<? extends Position> p, AsyncCallbacks.ReadEntriesCallback cb, Object ctx) { return asyncReplayEntries(p, cb, ctx, false); }
    @Override public Set<? extends Position> asyncReplayEntries(Set<? extends Position> p, AsyncCallbacks.ReadEntriesCallback cb, Object ctx, boolean sort) {
        try { cb.readEntriesComplete(replayEntries(p), ctx); return Set.of(); }
        catch (ManagedLedgerException e) { cb.readEntriesFailed(e, ctx); return new HashSet<>(p); }
    }
    @Override public void close() { Pending waiter = pending.getAndSet(null); if (waiter != null) { ledger.tailPoll().remove(waiter); waiter.tryFail(new ManagedLedgerException.ManagedLedgerAlreadyClosedException("cursor closed while waiting")); } closed = true; reader.close(); ledger.removeCursor(this); }
    @Override public void asyncClose(AsyncCallbacks.CloseCallback cb, Object ctx) { close(); cb.closeComplete(ctx); }
    @Override public Position getFirstPosition() { return ledger.getFirstPosition(); }
    @Override public void setActive() { if (!alwaysInactive) active = true; }
    @Override public void setInactive() { active = false; }
    @Override public void setAlwaysInactive() { alwaysInactive = true; active = false; }
    @Override public boolean isActive() { return active; }
    @Override public boolean isDurable() { return durable; }
    @Override public long getNumberOfEntriesSinceFirstNotAckedMessage() { return getNumberOfEntriesInBacklog(true); }
    @Override public int getTotalNonContiguousDeletedMessagesRange() { return 0; }
    @Override public int getNonContiguousDeletedMessagesRangeSerializedSize() { return 0; }
    @Override public long getEstimatedSizeSinceMarkDeletePosition() { return ledger.getEstimatedBacklogSize(reader.getReadPosition()); }
    @Override public double getThrottleMarkDelete() { return throttleMarkDelete; }
    @Override public void setThrottleMarkDelete(double value) { if (!Double.isFinite(value) || value < 0) throw new IllegalArgumentException(); throttleMarkDelete = value; }
    @Override public ManagedLedger getManagedLedger() { return ledger; }
    @Override public Range<Position> getLastIndividualDeletedRange() { return null; }
    @Override public void trimDeletedEntries(List<Entry> entries) { }
    @Override public long[] getDeletedBatchIndexesAsLongArray(Position p) { return null; }
    @Override public ManagedCursorMXBean getStats() { return stats; }
    @Override public boolean checkAndUpdateReadPositionChanged() { return false; }
    @Override public boolean isClosed() { return closed; }
    @Override public boolean isCursorDataFullyPersistable() { return false; }
    @Override public ManagedLedgerInternalStats.CursorStats getCursorStats() { ManagedLedgerInternalStats.CursorStats s = new ManagedLedgerInternalStats.CursorStats(); s.markDeletePosition = markDelete.toString(); s.readPosition = reader.getReadPosition().toString(); s.cursorLedger = -1; s.cursorLedgerLastEntry = -1; s.individuallyDeletedMessages = "[]"; s.state = closed ? "Closed" : "Open"; s.active = active; s.properties = new HashMap<>(properties); return s; }
    @Override public boolean isMessageDeleted(Position p) { return p.compareTo(markDelete) <= 0; }
    @Override public ManagedCursor duplicateNonDurableCursor(String duplicateName) throws ManagedLedgerException { return ledger.createLocalCursor(duplicateName, false, markDelete, reader.getReadPosition(), properties, cursorProperties); }
    @Override public long[] getBatchPositionAckSet(Position p) { return null; }
    @Override public int applyMaxSizeCap(int maxEntries, long maxBytes) { if (maxEntries <= 0 || maxBytes <= 0) return 0; return Math.min(maxEntries, ledger.runtime().config().maxReadEntries()); }
    @Override public void updateReadStats(int count, long size) { updateLastActive(); }

    private void requireLocal(String operation) throws ManagedLedgerException { if (durable) throw unsupported(operation); }
    private ManagedLedgerException unsupported(String operation) { return errors.unsupported(operation); }
    private UnsupportedOperationException unsupportedRuntime(String operation) { return errors.unsupportedRuntime(operation); }
    private <T> CompletableFuture<T> unsupportedFuture(String operation) { return CompletableFuture.failedFuture(unsupported(operation)); }

    private final class Pending implements PendingReadWaiter {
        private final long nextOffset;
        private final OptionalLong maxOffset;
        private final int count;
        private final long bytes;
        private final Position max;
        private final AsyncCallbacks.ReadEntriesCallback callback;
        private final Object ctx;
        private final AtomicBoolean terminal = new AtomicBoolean();
        private Pending(long nextOffset, OptionalLong maxOffset, int count, long bytes, Position max, AsyncCallbacks.ReadEntriesCallback callback, Object ctx) { this.nextOffset = nextOffset; this.maxOffset = maxOffset; this.count = count; this.bytes = bytes; this.max = max; this.callback = callback; this.ctx = ctx; }
        @Override public long nextOffset() { return nextOffset; }
        @Override public OptionalLong inclusiveMaxOffset() { return maxOffset; }
        @Override public boolean trySignal(StreamMetadata metadata) {
            boolean readable = metadata.committedEndOffset() > nextOffset && (maxOffset.isEmpty() || maxOffset.getAsLong() >= nextOffset);
            if (!readable && metadata.state() != StreamState.SEALED) return false;
            if (!terminal.compareAndSet(false, true)) return true;
            pending.compareAndSet(this, null);
            if (!readable) callback.readEntriesFailed(new ManagedLedgerException.ManagedLedgerTerminatedException("sealed cursor is at tail"), ctx);
            else reader.asyncReadEntries(count, bytes, callback, ctx, max);
            return true;
        }
        @Override public boolean tryFail(ManagedLedgerException error) { if (!terminal.compareAndSet(false, true)) return true; pending.compareAndSet(this, null); callback.readEntriesFailed(error, ctx); return true; }
    }
}
