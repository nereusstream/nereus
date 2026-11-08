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

package com.nereusstream.pulsar.broker;

import static com.nereusstream.pulsar.broker.PulsarM6ObjectWalManagedLedger.asLedgerError;
import static com.nereusstream.pulsar.broker.PulsarM6ObjectWalManagedLedger.await;
import com.google.common.collect.Range;
import com.nereusstream.pulsar.broker.PulsarM6ObjectWalCursorStore.CursorValue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ClearBacklogCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.FindEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.SkipEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedCursorMXBean;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.AckSetStateUtil;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats.CursorStats;

/** Native cursor state advances only through exact durable Pulsar positions. */
final class PulsarM6ObjectWalCursor implements ManagedCursor {
    private final PulsarM6ObjectWalManagedLedger ledger;
    private final String name;
    private final boolean durable;
    private final PulsarM6ObjectWalCursorStats stats = new PulsarM6ObjectWalCursorStats(this);
    private volatile CursorValue local;
    private volatile Position read;
    private Position lastObservedRead;
    private volatile boolean active;
    private volatile boolean closed;
    private boolean alwaysInactive;
    private volatile long lastActive = System.currentTimeMillis();
    private volatile double throttleMarkDelete;
    private long readGeneration;
    private boolean reading;
    private ReadRequest waiting;

    PulsarM6ObjectWalCursor(PulsarM6ObjectWalManagedLedger ledger, String name, boolean durable, CursorValue state) {
        this.ledger = ledger;
        this.name = name;
        this.durable = durable;
        local = state;
        read = ledger.getNextValidPosition(state.mark());
        lastObservedRead = read;
    }

    private record ReadRequest(
            int count,
            long maxBytes,
            ReadEntriesCallback callback,
            Object ctx,
            Position maxPosition,
            Predicate<Position> skip) {}

    private CursorValue value() {
        return durable ? ledger.storedCursor(name) : local;
    }

    private CompletionStage<Void> change(UnaryOperator<CursorValue> update) {
        if (closed) {
            return CompletableFuture.failedFuture(new ManagedLedgerException("Pulsar Object WAL cursor is closed"));
        }
        if (durable) {
            return ledger.mutateCursor(name, update).whenComplete((unused, failure) -> {
                stats.recordMetadata(failure == null);
            });
        }
        synchronized (this) {
            local = update.apply(local);
            return CompletableFuture.completedFuture(null);
        }
    }

    private static boolean deleted(CursorValue value, Position position) {
        return position.compareTo(value.mark()) <= 0
                || value.individuallyDeleted().contains(position);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public long getLastActive() {
        return lastActive;
    }

    @Override
    public void updateLastActive() {
        lastActive = System.currentTimeMillis();
    }

    @Override
    public Map<String, Long> getProperties() {
        return value().properties();
    }

    @Override
    public Map<String, String> getCursorProperties() {
        return value().cursorProperties();
    }

    @Override
    public Position getReadPosition() {
        return read;
    }

    @Override
    public Position getMarkDeletedPosition() {
        return value().mark();
    }

    @Override
    public Position getPersistentMarkDeletedPosition() {
        return value().mark();
    }

    @Override
    public boolean isDurable() {
        return durable;
    }

    @Override
    public boolean isActive() {
        return active && !closed;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public PulsarM6ObjectWalManagedLedger getManagedLedger() {
        return ledger;
    }

    @Override
    public double getThrottleMarkDelete() {
        return throttleMarkDelete;
    }

    @Override
    public void setThrottleMarkDelete(double value) {
        throttleMarkDelete = value;
    }

    @Override
    public boolean isMessageDeleted(Position position) {
        return deleted(value(), position);
    }

    @Override
    public synchronized void setActive() {
        if (!alwaysInactive && !closed) {
            active = true;
        }
    }

    @Override
    public synchronized void setInactive() {
        active = false;
    }

    @Override
    public synchronized void setAlwaysInactive() {
        active = false;
        alwaysInactive = true;
    }

    @Override
    public CompletableFuture<Void> putCursorProperty(String key, String property) {
        return updateCursorProperties(props -> {
            props.put(key, property);
            return props;
        });
    }

    @Override
    public CompletableFuture<Void> removeCursorProperty(String key) {
        return updateCursorProperties(props -> {
            props.remove(key);
            return props;
        });
    }

    @Override
    public CompletableFuture<Void> setCursorProperties(Map<String, String> properties) {
        return updateCursorProperties(ignored -> Map.copyOf(properties));
    }

    private CompletableFuture<Void> updateCursorProperties(UnaryOperator<Map<String, String>> edit) {
        return change(value -> new CursorValue(
                        value.mark(),
                        value.individuallyDeleted(),
                        value.batchAcks(),
                        value.properties(),
                        edit.apply(new HashMap<>(value.cursorProperties()))))
                .toCompletableFuture();
    }

    @Override
    public boolean putProperty(String key, Long value) {
        return longProperty(key, value);
    }

    @Override
    public boolean removeProperty(String key) {
        return longProperty(key, null);
    }

    private boolean longProperty(String key, Long property) {
        try {
            await(change(value -> {
                var next = new HashMap<>(value.properties());
                if (property == null) {
                    next.remove(key);
                } else {
                    next.put(key, property);
                }
                return new CursorValue(
                        value.mark(), value.individuallyDeleted(), value.batchAcks(), next, value.cursorProperties());
            }));
            return true;
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            return false;
        } catch (ManagedLedgerException failure) {
            return false;
        }
    }

    @Override
    public List<Entry> readEntries(int count) throws InterruptedException, ManagedLedgerException {
        return await(readResult(count, Long.MAX_VALUE, false));
    }

    @Override
    public List<Entry> readEntriesOrWait(int count) throws InterruptedException, ManagedLedgerException {
        return readEntriesOrWait(count, Long.MAX_VALUE);
    }

    @Override
    public List<Entry> readEntriesOrWait(int count, long bytes) throws InterruptedException, ManagedLedgerException {
        return await(readResult(count, bytes, true));
    }

    private CompletableFuture<List<Entry>> readResult(int count, long bytes, boolean wait) {
        var result = new CompletableFuture<List<Entry>>();
        read(
                new ReadRequest(
                        count,
                        bytes,
                        new ReadEntriesCallback() {
                            @Override
                            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                                result.complete(entries);
                            }

                            @Override
                            public void readEntriesFailed(ManagedLedgerException error, Object ctx) {
                                result.completeExceptionally(error);
                            }
                        },
                        null,
                        PositionFactory.LATEST,
                        ignored -> false),
                wait);
        return result;
    }

    @Override
    public void asyncReadEntries(int count, ReadEntriesCallback callback, Object ctx, Position max) {
        asyncReadEntries(count, Long.MAX_VALUE, callback, ctx, max);
    }

    @Override
    public void asyncReadEntries(int count, long bytes, ReadEntriesCallback callback, Object ctx, Position max) {
        asyncReadEntriesWithSkip(count, bytes, callback, ctx, max, ignored -> false);
    }

    @Override
    public void asyncReadEntriesWithSkip(
            int count, long bytes, ReadEntriesCallback callback, Object ctx, Position max, Predicate<Position> skip) {
        read(new ReadRequest(count, bytes, callback, ctx, max, skip), false);
    }

    @Override
    public void asyncReadEntriesOrWait(int count, ReadEntriesCallback callback, Object ctx, Position max) {
        asyncReadEntriesOrWait(count, Long.MAX_VALUE, callback, ctx, max);
    }

    @Override
    public void asyncReadEntriesOrWait(int count, long bytes, ReadEntriesCallback callback, Object ctx, Position max) {
        asyncReadEntriesWithSkipOrWait(count, bytes, callback, ctx, max, ignored -> false);
    }

    @Override
    public void asyncReadEntriesWithSkipOrWait(
            int count, long bytes, ReadEntriesCallback callback, Object ctx, Position max, Predicate<Position> skip) {
        read(new ReadRequest(count, bytes, callback, ctx, max, skip), true);
    }

    private void read(ReadRequest request, boolean wait) {
        var selected = new ArrayList<Position>();
        Position next;
        long generation;
        synchronized (this) {
            if (closed || reading || waiting != null || request.count() <= 0) {
                request.callback()
                        .readEntriesFailed(
                                new ManagedLedgerException("Pulsar Object WAL cursor read rejected"), request.ctx());
                return;
            }
            var state = value();
            int count = applyMaxSizeCap(request.count(), request.maxBytes());
            long limit = request.maxBytes() > 0
                    ? Math.min(request.maxBytes(), ledger.maximumReadBytes())
                    : ledger.maximumReadBytes();
            long bytes = 0;
            next = read;
            for (var entry : ledger.dataPositions().tailMap(read, true).entrySet()) {
                Position position = entry.getKey();
                if (request.maxPosition() != null && position.compareTo(request.maxPosition()) > 0) {
                    break;
                }
                if (!deleted(state, position)
                        && (request.skip() == null || !request.skip().test(position))) {
                    if (selected.size() == count || !selected.isEmpty() && entry.getValue() > limit - bytes) {
                        break;
                    }
                    selected.add(position);
                    bytes += entry.getValue();
                }
                next = position.getNext();
            }
            if (selected.isEmpty()) {
                read = next;
                if (wait && (request.maxPosition() == null || read.compareTo(request.maxPosition()) <= 0)) {
                    waiting = request;
                    return;
                }
            }
            generation = readGeneration;
            reading = true;
        }
        Position exactNext = next;
        ledger.readBatch(selected).whenComplete((entries, failure) -> {
            synchronized (this) {
                reading = false;
                if (failure == null && !closed && generation == readGeneration) {
                    read = exactNext;
                } else if (failure == null) {
                    entries.forEach(Entry::release);
                    failure = new ManagedLedgerException("Pulsar Object WAL cursor read was fenced");
                }
            }
            if (failure == null) {
                request.callback().readEntriesComplete(entries, request.ctx());
            } else {
                request.callback().readEntriesFailed(asLedgerError(failure), request.ctx());
            }
        });
    }

    void wake() {
        ReadRequest request;
        synchronized (this) {
            request = waiting;
            waiting = null;
        }
        if (request != null) {
            read(request, true);
        }
    }

    @Override
    public synchronized boolean cancelPendingReadRequest() {
        boolean cancelled = waiting != null;
        waiting = null;
        return cancelled;
    }

    @Override
    public boolean hasMoreEntries() {
        return getNumberOfEntries() > 0;
    }

    @Override
    public long getNumberOfEntries() {
        var state = value();
        return ledger.dataPositions().tailMap(read, true).keySet().stream()
                .filter(position -> !deleted(state, position))
                .count();
    }

    @Override
    public long getNumberOfEntriesInBacklog(boolean precise) {
        var state = value();
        return ledger.dataPositions().tailMap(state.mark(), false).keySet().stream()
                .filter(position -> !deleted(state, position))
                .count();
    }

    @Override
    public long getEstimatedSizeSinceMarkDeletePosition() {
        var state = value();
        return ledger.dataPositions().tailMap(state.mark(), false).entrySet().stream()
                .filter(entry -> !deleted(state, entry.getKey()))
                .mapToLong(Map.Entry::getValue)
                .sum();
    }

    @Override
    public Entry getNthEntry(int n, IndividualDeletedEntries include)
            throws InterruptedException, ManagedLedgerException {
        return await(nth(n, include));
    }

    @Override
    public void asyncGetNthEntry(int n, IndividualDeletedEntries include, ReadEntryCallback callback, Object ctx) {
        nth(n, include).whenComplete((entry, failure) -> {
            if (failure == null) {
                callback.readEntryComplete(entry, ctx);
            } else {
                callback.readEntryFailed(asLedgerError(failure), ctx);
            }
        });
    }

    private CompletionStage<Entry> nth(int n, IndividualDeletedEntries include) {
        if (n < 1) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Nth Pulsar entry must be positive"));
        }
        var state = value();
        var position = ledger.dataPositions().tailMap(state.mark(), false).keySet().stream()
                .filter(candidate -> include == IndividualDeletedEntries.Include || !deleted(state, candidate))
                .skip(n - 1L)
                .findFirst();
        return position.isEmpty()
                ? CompletableFuture.completedFuture(null)
                : ledger.readBatch(List.of(position.orElseThrow())).thenApply(entries -> entries.get(0));
    }

    @Override
    public void markDelete(Position position) throws InterruptedException, ManagedLedgerException {
        await(mark(position, null));
    }

    @Override
    public void markDelete(Position position, Map<String, Long> props)
            throws InterruptedException, ManagedLedgerException {
        await(mark(position, props));
    }

    @Override
    public void asyncMarkDelete(Position position, MarkDeleteCallback callback, Object ctx) {
        asyncMarkDelete(position, null, callback, ctx);
    }

    @Override
    public void asyncMarkDelete(Position position, Map<String, Long> props, MarkDeleteCallback callback, Object ctx) {
        mark(position, props).whenComplete((unused, failure) -> {
            if (failure == null) {
                callback.markDeleteComplete(ctx);
            } else {
                callback.markDeleteFailed(asLedgerError(failure), ctx);
            }
        });
    }

    private CompletionStage<Void> mark(Position position, Map<String, Long> props) {
        var exact = PositionFactory.create(position);
        long[] remaining = AckSetStateUtil.getAckSetArrayOrNull(position);
        return change(state -> {
                    requireAckPosition(exact);
                    var deleted = new TreeSet<>(state.individuallyDeleted());
                    var batches = new HashMap<>(state.batchAcks());
                    Position next = state.mark();
                    if (exact.compareTo(next) > 0) {
                        if (remaining != null && remaining.length != 0) {
                            if (mergeAck(batches, exact, remaining)) {
                                next = exact;
                            } else {
                                var prior = ledger.getPreviousPosition(exact);
                                next = prior.compareTo(next) > 0 ? prior : next;
                            }
                        } else {
                            next = exact;
                        }
                    }
                    return fold(
                            next,
                            deleted,
                            batches,
                            props == null ? state.properties() : props,
                            state.cursorProperties());
                })
                .thenRun(() -> {
                    ((PulsarM6ObjectWalLedgerStats) ledger.getStats()).marked();
                    advanceReadToMark();
                });
    }

    private void requireAckPosition(Position position) {
        if (!ledger.dataPositions().containsKey(position)
                && !position.equals(PositionFactory.EARLIEST)
                && !(position.getEntryId() == -1 && ledger.getLedgersInfo().containsKey(position.getLedgerId()))) {
            throw new IllegalArgumentException("Pulsar ACK is outside its exact Object WAL chain");
        }
    }

    @Override
    public void delete(Position position) throws InterruptedException, ManagedLedgerException {
        delete(List.of(position));
    }

    @Override
    public void delete(Iterable<Position> positions) throws InterruptedException, ManagedLedgerException {
        await(deletePositions(positions));
    }

    @Override
    public void asyncDelete(Position position, DeleteCallback callback, Object ctx) {
        asyncDelete(List.of(position), callback, ctx);
    }

    @Override
    public void asyncDelete(Iterable<Position> positions, DeleteCallback callback, Object ctx) {
        deletePositions(positions).whenComplete((unused, failure) -> {
            if (failure == null) {
                callback.deleteComplete(ctx);
            } else {
                callback.deleteFailed(asLedgerError(failure), ctx);
            }
        });
    }

    private CompletionStage<Void> deletePositions(Iterable<Position> positions) {
        var exactPositions = new ArrayList<Position>();
        positions.forEach(exactPositions::add);
        return change(state -> {
                    var deleted = new TreeSet<>(state.individuallyDeleted());
                    var batches = new HashMap<>(state.batchAcks());
                    for (var item : exactPositions) {
                        var position = PositionFactory.create(item);
                        requireAckPosition(position);
                        if (!deleted(state, position)) {
                            long[] remaining = AckSetStateUtil.getAckSetArrayOrNull(item);
                            if (remaining == null || mergeAck(batches, position, remaining)) {
                                deleted.add(position);
                                batches.remove(position);
                            }
                        }
                    }
                    return fold(state.mark(), deleted, batches, state.properties(), state.cursorProperties());
                })
                .thenRun(this::advanceReadToMark);
    }

    private static boolean mergeAck(Map<Position, List<Long>> batches, Position position, long[] remaining) {
        var next = BitSet.valueOf(remaining);
        var original = batches.get(position);
        if (original != null) {
            next.and(BitSet.valueOf(original.stream().mapToLong(Long::longValue).toArray()));
        }
        if (next.isEmpty()) {
            batches.remove(position);
            return true;
        }
        batches.put(position, Arrays.stream(next.toLongArray()).boxed().toList());
        return false;
    }

    private CursorValue fold(
            Position mark,
            TreeSet<Position> deleted,
            Map<Position, List<Long>> batches,
            Map<String, Long> props,
            Map<String, String> cursorProps) {
        deleted.headSet(mark, true).clear();
        for (var position : ledger.dataPositions().tailMap(mark, false).keySet()) {
            if (!deleted.remove(position)) {
                break;
            }
            mark = position;
        }
        Position exact = mark;
        batches.keySet().removeIf(position -> position.compareTo(exact) <= 0);
        return new CursorValue(exact, deleted, batches, props, cursorProps);
    }

    private synchronized void advanceReadToMark() {
        Position next = ledger.getNextValidPosition(value().mark());
        if (next.compareTo(read) > 0) {
            read = next;
            readGeneration++;
        }
    }

    @Override
    public void clearBacklog() throws InterruptedException, ManagedLedgerException {
        markDelete(ledger.getLastConfirmedEntry());
    }

    @Override
    public void asyncClearBacklog(ClearBacklogCallback callback, Object ctx) {
        mark(ledger.getLastConfirmedEntry(), null).whenComplete((unused, failure) -> {
            if (failure == null) {
                callback.clearBacklogComplete(ctx);
            } else {
                callback.clearBacklogFailed(asLedgerError(failure), ctx);
            }
        });
    }

    @Override
    public void skipEntries(int count, IndividualDeletedEntries include)
            throws InterruptedException, ManagedLedgerException {
        await(skip(count, include));
    }

    @Override
    public void asyncSkipEntries(
            int count, IndividualDeletedEntries include, SkipEntriesCallback callback, Object ctx) {
        skip(count, include).whenComplete((unused, failure) -> {
            if (failure == null) {
                callback.skipEntriesComplete(ctx);
            } else {
                callback.skipEntriesFailed(asLedgerError(failure), ctx);
            }
        });
    }

    private CompletionStage<Void> skip(int count, IndividualDeletedEntries include) {
        if (count <= 0) {
            return CompletableFuture.completedFuture(null);
        }
        var state = value();
        var candidates = ledger.dataPositions().tailMap(state.mark(), false).keySet().stream()
                .filter(position -> include == IndividualDeletedEntries.Include || !deleted(state, position))
                .limit(count)
                .toList();
        return candidates.isEmpty()
                ? CompletableFuture.completedFuture(null)
                : mark(candidates.get(candidates.size() - 1), null);
    }

    @Override
    public synchronized void rewind() {
        read = ledger.getNextValidPosition(value().mark());
        readGeneration++;
    }

    @Override
    public synchronized void seek(Position position, boolean force) {
        if (!force && position.compareTo(value().mark()) <= 0) {
            read = ledger.getNextValidPosition(value().mark());
        } else {
            read = PositionFactory.create(position);
        }
        readGeneration++;
    }

    @Override
    public void resetCursor(Position position) throws InterruptedException, ManagedLedgerException {
        await(reset(position));
    }

    @Override
    public void asyncResetCursor(Position position, boolean forceReset, AsyncCallbacks.ResetCursorCallback callback) {
        reset(position).whenComplete((unused, failure) -> {
            if (failure == null) {
                callback.resetComplete(null);
            } else {
                callback.resetFailed(asLedgerError(failure), null);
            }
        });
    }

    private CompletionStage<Void> reset(Position position) {
        if (position.equals(PositionFactory.LATEST)) {
            position = ledger.getLastConfirmedEntry().getNext();
        }
        var exact = PositionFactory.create(position);
        var available = ledger.dataPositions();
        if (!exact.equals(PositionFactory.EARLIEST)
                && !available.containsKey(exact)
                && !exact.equals(ledger.getLastConfirmedEntry().getNext())
                && !exact.equals(ledger.getFirstPosition())) {
            return CompletableFuture.failedFuture(
                    new ManagedLedgerException("Pulsar reset is outside its ledger chain"));
        }
        Position mark =
                exact.equals(PositionFactory.EARLIEST) ? PositionFactory.EARLIEST : ledger.getPreviousPosition(exact);
        return change(state -> new CursorValue(mark, Set.of(), Map.of(), state.properties(), state.cursorProperties()))
                .thenRun(() -> seek(ledger.getNextValidPosition(mark), true));
    }

    @Override
    public Position findNewestMatching(Predicate<Entry> condition) throws InterruptedException, ManagedLedgerException {
        return findNewestMatching(FindPositionConstraint.SearchAllAvailableEntries, condition);
    }

    @Override
    public Position findNewestMatching(FindPositionConstraint constraint, Predicate<Entry> condition)
            throws InterruptedException, ManagedLedgerException {
        return await(find(constraint, condition, null, null));
    }

    @Override
    public void asyncFindNewestMatching(
            FindPositionConstraint constraint, Predicate<Entry> condition, FindEntryCallback callback, Object ctx) {
        asyncFindNewestMatching(constraint, condition, callback, ctx, false);
    }

    @Override
    public void asyncFindNewestMatching(
            FindPositionConstraint constraint,
            Predicate<Entry> condition,
            FindEntryCallback callback,
            Object ctx,
            boolean fromLedger) {
        asyncFindNewestMatching(constraint, condition, null, null, callback, ctx, fromLedger);
    }

    @Override
    public void asyncFindNewestMatching(
            FindPositionConstraint constraint,
            Predicate<Entry> condition,
            Position start,
            Position end,
            FindEntryCallback callback,
            Object ctx,
            boolean fromLedger) {
        find(constraint, condition, start, end).whenComplete((position, failure) -> {
            if (failure == null) {
                callback.findEntryComplete(position, ctx);
            } else {
                callback.findEntryFailed(asLedgerError(failure), Optional.empty(), ctx);
            }
        });
    }

    private CompletionStage<Position> find(
            FindPositionConstraint constraint, Predicate<Entry> condition, Position start, Position end) {
        var state = value();
        var entries = ledger.dataPositions().keySet().stream()
                .filter(position -> start == null || position.compareTo(start) >= 0)
                .filter(position -> end == null || position.compareTo(end) <= 0)
                .filter(position ->
                        constraint == FindPositionConstraint.SearchAllAvailableEntries || !deleted(state, position))
                .toList();
        return ledger.find(entries, condition);
    }

    @Override
    public List<Entry> replayEntries(Set<? extends Position> positions)
            throws InterruptedException, ManagedLedgerException {
        return await(ledger.readBatch(positions.stream()
                .filter(position -> !isMessageDeleted(position))
                .map(PositionFactory::create)
                .sorted()
                .toList()));
    }

    @Override
    public Set<? extends Position> asyncReplayEntries(
            Set<? extends Position> positions, ReadEntriesCallback callback, Object ctx) {
        return asyncReplayEntries(positions, callback, ctx, true);
    }

    @Override
    public Set<? extends Position> asyncReplayEntries(
            Set<? extends Position> positions, ReadEntriesCallback callback, Object ctx, boolean sort) {
        Set<Position> skipped = new TreeSet<>();
        List<Position> selected = new ArrayList<>();
        var state = value();
        for (var position : positions) {
            (deleted(state, position) ? skipped : selected).add(PositionFactory.create(position));
        }
        if (sort) {
            selected.sort(Position::compareTo);
        }
        ledger.readBatch(selected).whenComplete((entries, failure) -> {
            if (failure == null) {
                callback.readEntriesComplete(entries, ctx);
            } else {
                callback.readEntriesFailed(asLedgerError(failure), ctx);
            }
        });
        return skipped;
    }

    @Override
    public void close() {
        stop();
    }

    @Override
    public void asyncClose(AsyncCallbacks.CloseCallback callback, Object ctx) {
        stop();
        callback.closeComplete(ctx);
    }

    void stop() {
        ReadRequest pending;
        synchronized (this) {
            closed = true;
            active = false;
            readGeneration++;
            pending = waiting;
            waiting = null;
        }
        if (pending != null) {
            pending.callback()
                    .readEntriesFailed(new ManagedLedgerException("Pulsar Object WAL cursor closed"), pending.ctx());
        }
        ledger.cursorClosed(this);
    }

    @Override
    public Position getFirstPosition() {
        return ledger.getNextValidPosition(value().mark());
    }

    @Override
    public long getNumberOfEntriesSinceFirstNotAckedMessage() {
        return ledger.dataPositions().tailMap(value().mark(), false).size();
    }

    @Override
    public int getTotalNonContiguousDeletedMessagesRange() {
        return value().individuallyDeleted().size();
    }

    @Override
    public int getNonContiguousDeletedMessagesRangeSerializedSize() {
        return 2 * Long.BYTES * value().individuallyDeleted().size();
    }

    @Override
    public Range<Position> getLastIndividualDeletedRange() {
        var positions = new TreeSet<>(value().individuallyDeleted());
        if (positions.isEmpty()) {
            return null;
        }
        var last = positions.last();
        return Range.openClosed(ledger.getPreviousPosition(last), last);
    }

    @Override
    public void trimDeletedEntries(List<Entry> entries) {
        var state = value();
        entries.removeIf(entry -> {
            boolean deleted = deleted(state, entry.getPosition());
            if (deleted) {
                entry.release();
            }
            return deleted;
        });
    }

    @Override
    public long[] getDeletedBatchIndexesAsLongArray(Position position) {
        return getBatchPositionAckSet(position);
    }

    @Override
    public long[] getBatchPositionAckSet(Position position) {
        var words = value().batchAcks().get(PositionFactory.create(position));
        return words == null ? null : words.stream().mapToLong(Long::longValue).toArray();
    }

    @Override
    public synchronized boolean checkAndUpdateReadPositionChanged() {
        boolean changed = !read.equals(lastObservedRead);
        lastObservedRead = read;
        return changed;
    }

    @Override
    public CursorStats getCursorStats() {
        var result = new CursorStats();
        result.markDeletePosition = getMarkDeletedPosition().toString();
        result.readPosition = read.toString();
        result.state = closed ? "Closed" : "Open";
        result.active = isActive();
        result.numberOfEntriesSinceFirstNotAckedMessage = getNumberOfEntriesSinceFirstNotAckedMessage();
        result.totalNonContiguousDeletedMessagesRange = getTotalNonContiguousDeletedMessagesRange();
        result.properties = getProperties();
        result.cursorLedger = -1;
        result.cursorLedgerLastEntry = -1;
        return result;
    }

    @Override
    public ManagedCursor duplicateNonDurableCursor(String name) throws ManagedLedgerException {
        return ledger.newNonDurableCursor(ledger.getPreviousPosition(read), name);
    }

    @Override
    public int applyMaxSizeCap(int count, long maxSizeBytes) {
        return Math.min(count, ledger.maximumReadEntries());
    }

    @Override
    public void updateReadStats(int count, long size) {}

    @Override
    public ManagedCursorMXBean getStats() {
        return stats;
    }
}
