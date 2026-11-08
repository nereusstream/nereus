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

import com.google.common.collect.Range;
import com.nereusstream.pulsar.broker.PulsarM6ObjectWalCursorStore.CursorValue;
import com.nereusstream.pulsar.broker.PulsarM6ObjectWalCursorStore.Value;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.PulsarPosition;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.CloseCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteLedgerCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OffloadCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.TerminateCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerMXBean;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionBound;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.intercept.ManagedLedgerInterceptor;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo.LedgerInfo;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats;

/** Native ManagedLedger positions and entry bytes backed by the admitted Pulsar Object WAL. */
final class PulsarM6ObjectWalManagedLedger implements ManagedLedger {
    private final String name;
    private final PulsarM6ObjectWalTopic storage;
    private final PulsarM6ObjectWalCursorStore cursors;
    private final int maximumReadEntries;
    private final long maximumReadBytes;
    private final Map<String, PulsarM6ObjectWalCursor> opened = new HashMap<>();
    private final PulsarM6ObjectWalLedgerStats stats = new PulsarM6ObjectWalLedgerStats(this);
    private volatile ManagedLedgerConfig config;
    private volatile long lastAdd;
    private CompletableFuture<Void> closing;

    PulsarM6ObjectWalManagedLedger(
            String name,
            PulsarM6ObjectWalTopic storage,
            PulsarM6ObjectWalCursorStore cursors,
            ManagedLedgerConfig config,
            int maximumReadEntries,
            long maximumReadBytes) {
        this.name = name;
        this.storage = storage;
        this.cursors = cursors;
        this.config = config;
        this.maximumReadEntries = maximumReadEntries;
        this.maximumReadBytes = maximumReadBytes;
        cursors.snapshot().cursors().forEach((cursor, state) -> {
            requireStoredCursor(state);
            opened.put(cursor, new PulsarM6ObjectWalCursor(this, cursor, true, state));
        });
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Position addEntry(byte[] data) throws InterruptedException, ManagedLedgerException {
        return addEntry(data, 1);
    }

    @Override
    public Position addEntry(byte[] data, int messages) throws InterruptedException, ManagedLedgerException {
        return addEntry(data, messages, 0, data.length);
    }

    @Override
    public Position addEntry(byte[] data, int offset, int length) throws InterruptedException, ManagedLedgerException {
        return addEntry(data, 1, offset, length);
    }

    @Override
    public Position addEntry(byte[] data, int messages, int offset, int length)
            throws InterruptedException, ManagedLedgerException {
        var result = new CompletableFuture<Position>();
        asyncAddEntry(
                data,
                messages,
                offset,
                length,
                new AddEntryCallback() {
                    @Override
                    public void addComplete(Position position, ByteBuf entry, Object ctx) {
                        result.complete(position);
                    }

                    @Override
                    public void addFailed(ManagedLedgerException failure, Object ctx) {
                        result.completeExceptionally(failure);
                    }
                },
                null);
        return await(result);
    }

    @Override
    public void asyncAddEntry(byte[] data, AddEntryCallback callback, Object ctx) {
        asyncAddEntry(data, 1, 0, data.length, callback, ctx);
    }

    @Override
    public void asyncAddEntry(byte[] data, int offset, int length, AddEntryCallback callback, Object ctx) {
        asyncAddEntry(data, 1, offset, length, callback, ctx);
    }

    @Override
    public void asyncAddEntry(
            byte[] data, int messages, int offset, int length, AddEntryCallback callback, Object ctx) {
        var buffer = Unpooled.wrappedBuffer(data, offset, length);
        try {
            asyncAddEntry(buffer, messages, callback, ctx);
        } finally {
            buffer.release();
        }
    }

    @Override
    public void asyncAddEntry(ByteBuf buffer, AddEntryCallback callback, Object ctx) {
        asyncAddEntry(buffer, 1, callback, ctx);
    }

    @Override
    public void asyncAddEntry(ByteBuf buffer, int messages, AddEntryCallback callback, Object ctx) {
        var fence = config.getNativeWriteAuthority();
        final Object word;
        final byte[] payload;
        try {
            word = fence.capture(ctx);
            if (!fence.isCurrent(word)) {
                throw new ManagedLedgerException.ManagedLedgerFencedException();
            }
            if (messages <= 0 || buffer.readableBytes() > storage.maximumEntryBytes()) {
                throw new IllegalArgumentException("Native Pulsar append exceeds its admitted entry limits");
            }
            payload = new byte[buffer.readableBytes()];
            buffer.getBytes(buffer.readerIndex(), payload);
        } catch (Exception failure) {
            callback.addFailed(asLedgerError(failure), ctx);
            return;
        }
        long started = System.nanoTime();
        storage.append(payload, word).whenComplete((position, error) -> {
            if (error != null || !fence.isCurrent(word)) {
                stats.addFailed();
                callback.addFailed(
                        error == null
                                ? new ManagedLedgerException.ManagedLedgerFencedException()
                                : asLedgerError(error),
                        ctx);
                return;
            }
            lastAdd = System.currentTimeMillis();
            stats.added(payload.length, (System.nanoTime() - started) / 1_000);
            var entry = Unpooled.wrappedBuffer(payload).asReadOnly();
            try {
                callback.addComplete(nativePosition(position), entry, ctx);
            } finally {
                entry.release();
                wakeWaitingReaders();
            }
        });
    }

    @Override
    public ManagedCursor openCursor(String cursor) throws InterruptedException, ManagedLedgerException {
        return openCursor(cursor, InitialPosition.Latest);
    }

    @Override
    public ManagedCursor openCursor(String cursor, InitialPosition position)
            throws InterruptedException, ManagedLedgerException {
        return openCursor(cursor, position, Map.of(), Map.of());
    }

    @Override
    public ManagedCursor openCursor(
            String cursor, InitialPosition position, Map<String, Long> properties, Map<String, String> cursorProperties)
            throws InterruptedException, ManagedLedgerException {
        return await(openDurable(cursor, position, properties, cursorProperties));
    }

    private CompletionStage<ManagedCursor> openDurable(
            String cursor,
            InitialPosition position,
            Map<String, Long> properties,
            Map<String, String> cursorProperties) {
        var initial = new CursorValue(
                position == InitialPosition.Earliest ? PositionFactory.EARLIEST : getLastConfirmedEntry(),
                Set.of(),
                Map.of(),
                properties,
                cursorProperties);
        return cursors.update(current -> {
                    if (current.cursors().containsKey(cursor)) {
                        return current;
                    }
                    var values = new HashMap<>(current.cursors());
                    values.put(cursor, initial);
                    return new Value(current.ownerEpoch(), current.ledgerProperties(), values);
                })
                .thenApply(unused -> {
                    synchronized (this) {
                        var value = cursors.snapshot().cursors().get(cursor);
                        return opened.compute(
                                cursor,
                                (name, prior) -> prior == null || prior.isClosed()
                                        ? new PulsarM6ObjectWalCursor(this, name, true, value)
                                        : prior);
                    }
                });
    }

    @Override
    public void asyncOpenCursor(String name, OpenCursorCallback callback, Object ctx) {
        asyncOpenCursor(name, InitialPosition.Latest, callback, ctx);
    }

    @Override
    public void asyncOpenCursor(String name, InitialPosition position, OpenCursorCallback callback, Object ctx) {
        asyncOpenCursor(name, position, Map.of(), Map.of(), callback, ctx);
    }

    @Override
    public void asyncOpenCursor(
            String name,
            InitialPosition position,
            Map<String, Long> properties,
            Map<String, String> cursorProperties,
            OpenCursorCallback callback,
            Object ctx) {
        openDurable(name, position, properties, cursorProperties).whenComplete((cursor, failure) -> {
            if (failure == null) {
                callback.openCursorComplete(cursor, ctx);
            } else {
                callback.openCursorFailed(asLedgerError(failure), ctx);
            }
        });
    }

    @Override
    public ManagedCursor newNonDurableCursor(Position start) throws ManagedLedgerException {
        return newNonDurableCursor(start, java.util.UUID.randomUUID().toString());
    }

    @Override
    public ManagedCursor newNonDurableCursor(Position start, String name) throws ManagedLedgerException {
        return newNonDurableCursor(start, name, InitialPosition.Latest, false);
    }

    @Override
    public synchronized ManagedCursor newNonDurableCursor(
            Position start, String name, InitialPosition initialPosition, boolean readCompacted)
            throws ManagedLedgerException {
        if (readCompacted) {
            throw unsupported("compacted reads");
        }
        var mark = start == null || start.equals(PositionFactory.LATEST)
                ? initialPosition == InitialPosition.Earliest ? PositionFactory.EARLIEST : getLastConfirmedEntry()
                : start;
        var state = new CursorValue(mark, Set.of(), Map.of(), Map.of(), Map.of());
        if (closing != null || opened.containsKey(name)) {
            throw new ManagedLedgerException("Native cursor name is already open or ledger is closing");
        }
        var cursor = new PulsarM6ObjectWalCursor(this, name, false, state);
        opened.put(name, cursor);
        return cursor;
    }

    @Override
    public void asyncDeleteCursor(String name, DeleteCursorCallback callback, Object ctx) {
        deleteCursorStage(name).whenComplete((unused, failure) -> {
            if (failure == null) {
                callback.deleteCursorComplete(ctx);
            } else {
                callback.deleteCursorFailed(asLedgerError(failure), ctx);
            }
        });
    }

    @Override
    public void deleteCursor(String name) throws InterruptedException, ManagedLedgerException {
        await(deleteCursorStage(name));
    }

    private CompletionStage<Void> deleteCursorStage(String name) {
        return cursors.update(value -> {
                    var updated = new HashMap<>(value.cursors());
                    updated.remove(name);
                    return new Value(value.ownerEpoch(), value.ledgerProperties(), updated);
                })
                .thenRun(() -> {
                    PulsarM6ObjectWalCursor old;
                    synchronized (this) {
                        old = opened.remove(name);
                    }
                    if (old != null) {
                        old.stop();
                    }
                });
    }

    CursorValue storedCursor(String name) {
        var value = cursors.snapshot().cursors().get(name);
        if (value == null) {
            throw new IllegalStateException("Native Object WAL cursor no longer exists");
        }
        return value;
    }

    CompletionStage<Void> mutateCursor(String name, UnaryOperator<CursorValue> change) {
        return cursors.update(value -> {
            var current = value.cursors().get(name);
            if (current == null) {
                throw new IllegalStateException("Native Object WAL cursor no longer exists");
            }
            var updated = new HashMap<>(value.cursors());
            updated.put(name, change.apply(current));
            return new Value(value.ownerEpoch(), value.ledgerProperties(), updated);
        });
    }

    private void requireStoredCursor(CursorValue value) {
        var exact = dataPositions();
        var mark = value.mark();
        if (!mark.equals(PositionFactory.EARLIEST)
                        && !(mark.getEntryId() == -1 && getLedgersInfo().containsKey(mark.getLedgerId()))
                        && !exact.containsKey(mark)
                || !exact.keySet().containsAll(value.individuallyDeleted())
                || !exact.keySet().containsAll(value.batchAcks().keySet())) {
            throw new IllegalStateException("Durable Pulsar cursor references positions outside its verified chain");
        }
    }

    private void wakeWaitingReaders() {
        for (var cursor : openSnapshot()) {
            cursor.wake();
        }
    }

    private synchronized List<PulsarM6ObjectWalCursor> openSnapshot() {
        return List.copyOf(opened.values());
    }

    @Override
    public void removeWaitingCursor(ManagedCursor cursor) {
        cursor.cancelPendingReadRequest();
    }

    synchronized void cursorClosed(PulsarM6ObjectWalCursor cursor) {
        if (!cursor.isDurable()) {
            opened.remove(cursor.getName(), cursor);
        }
    }

    @Override
    public Iterable<ManagedCursor> getCursors() {
        return new ArrayList<>(
                openSnapshot().stream().filter(ManagedCursor::isDurable).toList());
    }

    @Override
    public Iterable<ManagedCursor> getActiveCursors() {
        return new ArrayList<>(
                openSnapshot().stream().filter(ManagedCursor::isActive).toList());
    }

    @Override
    public ManagedCursor getSlowestConsumer() {
        return openSnapshot().stream()
                .filter(ManagedCursor::isDurable)
                .min(Comparator.comparing(ManagedCursor::getMarkDeletedPosition))
                .orElse(null);
    }

    NavigableMap<Position, Integer> dataPositions() {
        NavigableMap<Position, Integer> positions = new TreeMap<>();
        for (var ledger : storage.publishedView().ledgers()) {
            for (long entry = 0; entry <= ledger.readableThrough(); entry++) {
                var key = new PulsarPosition(ledger.virtualLedgerId(), entry);
                positions.put(nativePosition(key), storage.size(key));
            }
        }
        return positions;
    }

    @Override
    public Position getLastConfirmedEntry() {
        var view = storage.publishedView().ledgers();
        var active = view.get(view.size() - 1);
        return PositionFactory.create(active.virtualLedgerId(), active.readableThrough());
    }

    @Override
    public Position getFirstPosition() {
        var first = storage.publishedView().ledgers().get(0);
        return PositionFactory.create(first.virtualLedgerId(), -1);
    }

    @Override
    public Position getNextValidPosition(Position position) {
        var next = dataPositions().higherKey(position);
        return next == null ? getLastConfirmedEntry().getNext() : next;
    }

    @Override
    public Position getPreviousPosition(Position position) {
        var previous = dataPositions().lowerKey(position);
        return previous == null ? getFirstPosition() : previous;
    }

    @Override
    public Position getPositionAfterN(Position start, long n, PositionBound bound) {
        if (n <= 0) {
            return start;
        }
        long seen = 0;
        for (var position : dataPositions()
                .tailMap(start, bound == PositionBound.startIncluded)
                .keySet()) {
            if (++seen == n) {
                return position;
            }
        }
        return getLastConfirmedEntry();
    }

    @Override
    public long getNumberOfEntries() {
        return dataPositions().size();
    }

    @Override
    public long getNumberOfEntries(Range<Position> range) {
        return dataPositions().keySet().stream().filter(range::contains).count();
    }

    @Override
    public long getNumberOfActiveEntries() {
        var slowest = getSlowestConsumer();
        return slowest == null ? getNumberOfEntries() : slowest.getNumberOfEntriesInBacklog(true);
    }

    @Override
    public long getTotalSize() {
        return dataPositions().values().stream().mapToLong(Integer::longValue).sum();
    }

    @Override
    public long getEstimatedBacklogSize() {
        var slowest = getSlowestConsumer();
        return slowest == null ? getTotalSize() : slowest.getEstimatedSizeSinceMarkDeletePosition();
    }

    @Override
    public long getEstimatedBacklogSize(Position position) {
        return dataPositions().tailMap(position, false).values().stream()
                .mapToLong(Integer::longValue)
                .sum();
    }

    @Override
    public CompletableFuture<Long> getEarliestMessagePublishTimeInBacklog() {
        var slowest = getSlowestConsumer();
        var from = slowest == null ? getFirstPosition() : slowest.getMarkDeletedPosition();
        var start = dataPositions().higherKey(from);
        if (start == null) {
            return CompletableFuture.completedFuture(-1L);
        }
        return readBatch(List.of(start)).thenApply(entries -> {
            var entry = entries.get(0);
            try {
                return org.apache.pulsar.common.protocol.Commands.peekMessageMetadata(
                                entry.getDataBuffer(), "Object WAL", -1)
                        .getPublishTime();
            } finally {
                entry.release();
            }
        });
    }

    @Override
    public void asyncReadEntry(Position position, AsyncCallbacks.ReadEntryCallback callback, Object ctx) {
        readBatch(List.of(position)).whenComplete((entries, failure) -> {
            if (failure == null) {
                callback.readEntryComplete(entries.get(0), ctx);
            } else {
                callback.readEntryFailed(asLedgerError(failure), ctx);
            }
        });
    }

    CompletableFuture<List<Entry>> readBatch(List<Position> positions) {
        if (positions.isEmpty()) {
            return CompletableFuture.completedFuture(List.of());
        }
        if (positions.size() > maximumReadEntries
                || positions.stream()
                                .mapToLong(pos -> storage.size(virtualPosition(pos)))
                                .sum()
                        > maximumReadBytes) {
            return CompletableFuture.failedFuture(
                    new ManagedLedgerException("Pulsar Object WAL read exceeds its quota"));
        }
        try {
            return storage.read(positions.stream()
                            .map(PulsarM6ObjectWalManagedLedger::virtualPosition)
                            .toList())
                    .thenApply(read -> {
                        List<Entry> entries = new ArrayList<>(read.size());
                        for (var entry : read) {
                            entries.add(EntryImpl.create(
                                    entry.position().virtualLedgerId(),
                                    entry.position().entryId(),
                                    entry.payload()));
                        }
                        stats.read(
                                entries.size(),
                                entries.stream().mapToLong(Entry::getLength).sum());
                        return List.copyOf(entries);
                    })
                    .whenComplete((entries, failure) -> {
                        if (failure != null) {
                            stats.readFailed();
                        }
                    })
                    .toCompletableFuture();
        } catch (RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    int maximumReadEntries() {
        return maximumReadEntries;
    }

    long maximumReadBytes() {
        return maximumReadBytes;
    }

    CompletableFuture<Position> find(List<Position> candidates, Predicate<Entry> predicate) {
        var reversed = new ArrayList<>(candidates);
        java.util.Collections.reverse(reversed);
        CompletionStage<Position> found = CompletableFuture.completedFuture(null);
        for (var position : reversed) {
            found = found.thenCompose(match -> match == null
                    ? readBatch(List.of(position)).thenApply(entries -> {
                        var entry = entries.get(0);
                        try {
                            return predicate.test(entry) ? position : null;
                        } finally {
                            entry.release();
                        }
                    })
                    : CompletableFuture.completedFuture(match));
        }
        return found.toCompletableFuture();
    }

    @Override
    public CompletableFuture<Position> asyncFindPosition(Predicate<Entry> predicate) {
        return find(List.copyOf(dataPositions().keySet()), predicate);
    }

    @Override
    public NavigableMap<Long, LedgerInfo> getLedgersInfo() {
        var result = new TreeMap<Long, LedgerInfo>();
        for (var ledger : storage.publishedView().ledgers()) {
            long size = 0;
            for (long index = 0; index <= ledger.readableThrough(); index++) {
                size += storage.size(new PulsarPosition(ledger.virtualLedgerId(), index));
            }
            result.put(
                    ledger.virtualLedgerId(),
                    new LedgerInfo()
                            .setLedgerId(ledger.virtualLedgerId())
                            .setEntries(ledger.readableThrough() + 1)
                            .setSize(size));
        }
        return result;
    }

    @Override
    public CompletableFuture<LedgerInfo> getLedgerInfo(long ledgerId) {
        return CompletableFuture.completedFuture(getLedgersInfo().get(ledgerId));
    }

    @Override
    public Optional<LedgerInfo> getOptionalLedgerInfo(long ledgerId) {
        return Optional.ofNullable(getLedgersInfo().get(ledgerId));
    }

    @Override
    public CompletableFuture<ManagedLedgerInternalStats> getManagedLedgerInternalStats(boolean includeLedgerMetadata) {
        var result = new ManagedLedgerInternalStats();
        result.entriesAddedCounter = getNumberOfEntries();
        result.numberOfEntries = getNumberOfEntries();
        result.totalSize = getTotalSize();
        result.lastConfirmedEntry = getLastConfirmedEntry().toString();
        result.state = closing == null ? "LedgerOpened" : "Closed";
        result.ledgers = new ArrayList<>();
        for (var item : getLedgersInfo().values()) {
            var info = new ManagedLedgerInternalStats.LedgerInfo();
            info.ledgerId = item.getLedgerId();
            info.entries = item.getEntries();
            info.size = item.getSize();
            result.ledgers.add(info);
        }
        result.cursors = new TreeMap<>();
        openSnapshot().stream()
                .filter(ManagedCursor::isDurable)
                .forEach(cursor -> result.cursors.put(cursor.getName(), cursor.getCursorStats()));
        return CompletableFuture.completedFuture(result);
    }

    synchronized CompletableFuture<Void> drain() {
        if (closing == null) {
            openSnapshot().forEach(PulsarM6ObjectWalCursor::stop);
            closing = CompletableFuture.allOf(cursors.stopAndDrain(), storage.close());
        }
        return closing.copy();
    }

    @Override
    public void close() throws InterruptedException, ManagedLedgerException {
        await(drain());
    }

    @Override
    public void asyncClose(CloseCallback callback, Object ctx) {
        drain().whenComplete((unused, failure) -> {
            if (failure == null) {
                callback.closeComplete(ctx);
            } else {
                callback.closeFailed(asLedgerError(failure), ctx);
            }
        });
    }

    @Override
    public ManagedLedgerConfig getConfig() {
        return config;
    }

    @Override
    public void setConfig(ManagedLedgerConfig updated) {
        updated.setNativeWriteAuthority(config.getNativeWriteAuthority());
        config = updated;
    }

    @Override
    public Map<String, String> getProperties() {
        return cursors.snapshot().ledgerProperties();
    }

    private CompletionStage<Void> editProperties(UnaryOperator<Map<String, String>> change) {
        return cursors.update(value ->
                new Value(value.ownerEpoch(), change.apply(new HashMap<>(value.ledgerProperties())), value.cursors()));
    }

    @Override
    public void setProperty(String key, String value) throws InterruptedException, ManagedLedgerException {
        await(editProperties(props -> {
            props.put(key, value);
            return props;
        }));
    }

    @Override
    public void asyncSetProperty(
            String key, String value, AsyncCallbacks.UpdatePropertiesCallback callback, Object ctx) {
        deliverProperties(
                editProperties(props -> {
                    props.put(key, value);
                    return props;
                }),
                callback,
                ctx);
    }

    @Override
    public void deleteProperty(String key) throws InterruptedException, ManagedLedgerException {
        await(editProperties(props -> {
            props.remove(key);
            return props;
        }));
    }

    @Override
    public void asyncDeleteProperty(String key, AsyncCallbacks.UpdatePropertiesCallback callback, Object ctx) {
        deliverProperties(
                editProperties(props -> {
                    props.remove(key);
                    return props;
                }),
                callback,
                ctx);
    }

    @Override
    public void setProperties(Map<String, String> properties) throws InterruptedException, ManagedLedgerException {
        await(editProperties(ignored -> Map.copyOf(properties)));
    }

    @Override
    public void asyncSetProperties(
            Map<String, String> properties, AsyncCallbacks.UpdatePropertiesCallback callback, Object ctx) {
        deliverProperties(editProperties(ignored -> Map.copyOf(properties)), callback, ctx);
    }

    private void deliverProperties(
            CompletionStage<Void> stage, AsyncCallbacks.UpdatePropertiesCallback callback, Object ctx) {
        stage.whenComplete((unused, failure) -> {
            if (failure == null) {
                callback.updatePropertiesComplete(getProperties(), ctx);
            } else {
                callback.updatePropertiesFailed(asLedgerError(failure), ctx);
            }
        });
    }

    @Override
    public void asyncTerminate(TerminateCallback callback, Object ctx) {
        callback.terminateFailed(unsupported("terminate"), ctx);
    }

    @Override
    public Position terminate() throws ManagedLedgerException {
        throw unsupported("terminate");
    }

    @Override
    public CompletableFuture<Position> asyncMigrate() {
        return CompletableFuture.failedFuture(unsupported("migration"));
    }

    @Override
    public void delete() throws ManagedLedgerException {
        throw unsupported("physical topic deletion");
    }

    @Override
    public void asyncDelete(DeleteLedgerCallback callback, Object ctx) {
        callback.deleteLedgerFailed(unsupported("physical topic deletion"), ctx);
    }

    @Override
    public CompletableFuture<Void> asyncTruncate() {
        return CompletableFuture.failedFuture(unsupported("truncate"));
    }

    @Override
    public Position offloadPrefix(Position position) throws ManagedLedgerException {
        throw unsupported("BookKeeper offload");
    }

    @Override
    public void asyncOffloadPrefix(Position position, OffloadCallback callback, Object ctx) {
        callback.offloadFailed(unsupported("BookKeeper offload"), ctx);
    }

    @Override
    public CompletableFuture<Void> asyncAddLedgerProperty(long ledgerId, String key, String value) {
        return CompletableFuture.failedFuture(unsupported("virtual ledger property mutation"));
    }

    @Override
    public CompletableFuture<Void> asyncRemoveLedgerProperty(long ledgerId, String key) {
        return CompletableFuture.failedFuture(unsupported("virtual ledger property mutation"));
    }

    @Override
    public CompletableFuture<String> asyncGetLedgerProperty(long ledgerId, String key) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public long getOffloadedSize() {
        return 0;
    }

    @Override
    public long getLastOffloadedLedgerId() {
        return 0;
    }

    @Override
    public long getLastOffloadedSuccessTimestamp() {
        return 0;
    }

    @Override
    public long getLastOffloadedFailureTimestamp() {
        return 0;
    }

    @Override
    public boolean isTerminated() {
        return false;
    }

    @Override
    public boolean isMigrated() {
        return false;
    }

    @Override
    public long getLastAddEntryTime() {
        return lastAdd;
    }

    @Override
    public long getCacheSize() {
        return 0;
    }

    @Override
    public int getPendingAddEntriesCount() {
        return storage.pendingAppends();
    }

    @Override
    public ManagedLedgerInterceptor getManagedLedgerInterceptor() {
        return null;
    }

    @Override
    public void readyToCreateNewLedger() {}

    @Override
    public void rollCurrentLedgerIfFull() {} // The typed bridge rolls before assigning the next position.

    @Override
    public boolean checkInactiveLedgerAndRollOver() {
        return false;
    }

    @Override
    public void checkCursorsToCacheEntries() {}

    @Override
    public void trimConsumedLedgersInBackground(CompletableFuture<?> promise) {
        if (promise != null) {
            promise.complete(null); // The Object primary retains its immutable virtual history.
        }
    }

    @Override
    public ManagedLedgerMXBean getStats() {
        return stats;
    }

    static Position nativePosition(PulsarPosition position) {
        return PositionFactory.create(position.virtualLedgerId(), position.entryId());
    }

    static PulsarPosition virtualPosition(Position position) {
        return new PulsarPosition(position.getLedgerId(), position.getEntryId());
    }

    static ManagedLedgerException unsupported(String operation) {
        return new ManagedLedgerException("Pulsar Object WAL does not support " + operation);
    }

    static ManagedLedgerException asLedgerError(Throwable error) {
        while ((error instanceof CompletionException || error instanceof ExecutionException)
                && error.getCause() != null) {
            error = error.getCause();
        }
        return ManagedLedgerException.getManagedLedgerException(error);
    }

    static <T> T await(CompletionStage<T> stage) throws InterruptedException, ManagedLedgerException {
        try {
            return stage.toCompletableFuture().get();
        } catch (ExecutionException failure) {
            throw asLedgerError(failure);
        }
    }
}
