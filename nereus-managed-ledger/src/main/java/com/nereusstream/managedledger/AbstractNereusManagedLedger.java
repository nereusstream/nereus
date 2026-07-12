/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger;

import com.google.common.collect.Range;
import com.nereusstream.managedledger.errors.ManagedLedgerErrorMapper;
import io.netty.buffer.ByteBuf;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.CloseCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteLedgerCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OffloadCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.TerminateCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.UpdatePropertiesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerMXBean;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionBound;
import org.apache.bookkeeper.mledger.intercept.ManagedLedgerInterceptor;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo.LedgerInfo;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats;

/** Explicit failure/neutral baseline so every locked ManagedLedger method has an audited implementation. */
abstract class AbstractNereusManagedLedger implements ManagedLedger {
    protected final ManagedLedgerErrorMapper errorMapper = new ManagedLedgerErrorMapper();

    protected ManagedLedgerException unsupported(String operation) {
        return errorMapper.unsupported(operation);
    }

    protected <T> CompletableFuture<T> unsupportedFuture(String operation) {
        return errorMapper.failedFuture(unsupported(operation));
    }

    @Override
    public String getName() { throw errorMapper.unsupportedRuntime("getName"); }
    @Override
    public Position addEntry(byte[] data) throws ManagedLedgerException { throw unsupported("addEntry"); }
    @Override
    public Position addEntry(byte[] data, int numberOfMessages) throws ManagedLedgerException { throw unsupported("addEntry"); }
    @Override
    public void asyncAddEntry(byte[] data, AddEntryCallback callback, Object ctx) { callback.addFailed(unsupported("asyncAddEntry"), ctx); }
    @Override
    public Position addEntry(byte[] data, int offset, int length) throws ManagedLedgerException { throw unsupported("addEntry"); }
    @Override
    public Position addEntry(byte[] data, int numberOfMessages, int offset, int length) throws ManagedLedgerException { throw unsupported("addEntry"); }
    @Override
    public void asyncAddEntry(byte[] data, int offset, int length, AddEntryCallback callback, Object ctx) { callback.addFailed(unsupported("asyncAddEntry"), ctx); }
    @Override
    public void asyncAddEntry(byte[] data, int numberOfMessages, int offset, int length, AddEntryCallback callback, Object ctx) { callback.addFailed(unsupported("asyncAddEntry"), ctx); }
    @Override
    public void asyncAddEntry(ByteBuf buffer, AddEntryCallback callback, Object ctx) { callback.addFailed(unsupported("asyncAddEntry"), ctx); }
    @Override
    public void asyncAddEntry(ByteBuf buffer, int numberOfMessages, AddEntryCallback callback, Object ctx) { callback.addFailed(unsupported("asyncAddEntry"), ctx); }
    @Override
    public ManagedCursor openCursor(String name) throws ManagedLedgerException { throw unsupported("openCursor"); }
    @Override
    public ManagedCursor openCursor(String name, InitialPosition initialPosition) throws ManagedLedgerException { throw unsupported("openCursor"); }
    @Override
    public ManagedCursor openCursor(String name, InitialPosition initialPosition, Map<String, Long> properties, Map<String, String> cursorProperties) throws ManagedLedgerException { throw unsupported("openCursor"); }
    @Override
    public ManagedCursor newNonDurableCursor(Position startCursorPosition) throws ManagedLedgerException { throw unsupported("newNonDurableCursor"); }
    @Override
    public ManagedCursor newNonDurableCursor(Position startPosition, String subscriptionName) throws ManagedLedgerException { throw unsupported("newNonDurableCursor"); }
    @Override
    public ManagedCursor newNonDurableCursor(Position startPosition, String subscriptionName, InitialPosition initialPosition, boolean isReadCompacted) throws ManagedLedgerException { throw unsupported("newNonDurableCursor"); }
    @Override
    public void asyncDeleteCursor(String name, DeleteCursorCallback callback, Object ctx) { callback.deleteCursorFailed(unsupported("deleteCursor"), ctx); }
    @Override
    public void deleteCursor(String name) throws ManagedLedgerException { throw unsupported("deleteCursor"); }
    @Override
    public void removeWaitingCursor(ManagedCursor cursor) { }
    @Override
    public void asyncOpenCursor(String name, OpenCursorCallback callback, Object ctx) { callback.openCursorFailed(unsupported("openCursor"), ctx); }
    @Override
    public void asyncOpenCursor(String name, InitialPosition initialPosition, OpenCursorCallback callback, Object ctx) { callback.openCursorFailed(unsupported("openCursor"), ctx); }
    @Override
    public void asyncOpenCursor(String name, InitialPosition initialPosition, Map<String, Long> properties, Map<String, String> cursorProperties, OpenCursorCallback callback, Object ctx) { callback.openCursorFailed(unsupported("openCursor"), ctx); }
    @Override
    public Iterable<ManagedCursor> getCursors() { return List.of(); }
    @Override
    public Iterable<ManagedCursor> getActiveCursors() { return List.of(); }
    @Override
    public long getNumberOfEntries() { return 0; }
    @Override
    public long getNumberOfEntries(Range<Position> range) { throw errorMapper.unsupportedRuntime("getNumberOfEntries(range)"); }
    @Override
    public long getNumberOfActiveEntries() { return 0; }
    @Override
    public long getTotalSize() { return 0; }
    @Override
    public long getEstimatedBacklogSize() { return 0; }
    @Override
    public CompletableFuture<Long> getEarliestMessagePublishTimeInBacklog() { return unsupportedFuture("getEarliestMessagePublishTimeInBacklog"); }
    @Override
    public long getOffloadedSize() { return 0; }
    @Override
    public long getLastOffloadedLedgerId() { return 0; }
    @Override
    public long getLastOffloadedSuccessTimestamp() { return 0; }
    @Override
    public long getLastOffloadedFailureTimestamp() { return 0; }
    @Override
    public void asyncTerminate(TerminateCallback callback, Object ctx) { callback.terminateFailed(unsupported("terminate"), ctx); }
    @Override
    public CompletableFuture<Position> asyncMigrate() { return unsupportedFuture("migrate"); }
    @Override
    public CompletableFuture<Void> asyncAddLedgerProperty(long ledgerId, String key, String value) { return unsupportedFuture("addLedgerProperty"); }
    @Override
    public CompletableFuture<Void> asyncRemoveLedgerProperty(long ledgerId, String key) { return unsupportedFuture("removeLedgerProperty"); }
    @Override
    public CompletableFuture<String> asyncGetLedgerProperty(long ledgerId, String key) { return unsupportedFuture("getLedgerProperty"); }
    @Override
    public Position terminate() throws ManagedLedgerException { throw unsupported("terminate"); }
    @Override
    public void close() throws ManagedLedgerException { throw unsupported("close"); }
    @Override
    public void asyncClose(CloseCallback callback, Object ctx) { callback.closeFailed(unsupported("close"), ctx); }
    @Override
    public ManagedLedgerMXBean getStats() { throw errorMapper.unsupportedRuntime("getStats"); }
    @Override
    public void delete() throws ManagedLedgerException { throw unsupported("delete"); }
    @Override
    public void asyncDelete(DeleteLedgerCallback callback, Object ctx) { callback.deleteLedgerFailed(unsupported("delete"), ctx); }
    @Override
    public Position offloadPrefix(Position pos) throws ManagedLedgerException { throw unsupported("offloadPrefix"); }
    @Override
    public void asyncOffloadPrefix(Position pos, OffloadCallback callback, Object ctx) { callback.offloadFailed(unsupported("offloadPrefix"), ctx); }
    @Override
    public ManagedCursor getSlowestConsumer() { return null; }
    @Override
    public boolean isTerminated() { return false; }
    @Override
    public boolean isMigrated() { return false; }
    @Override
    public ManagedLedgerConfig getConfig() { throw errorMapper.unsupportedRuntime("getConfig"); }
    @Override
    public void setConfig(ManagedLedgerConfig config) { throw errorMapper.unsupportedRuntime("setConfig"); }
    @Override
    public Position getLastConfirmedEntry() { throw errorMapper.unsupportedRuntime("getLastConfirmedEntry"); }
    @Override
    public void readyToCreateNewLedger() { }
    @Override
    public Map<String, String> getProperties() { return Map.of(); }
    @Override
    public void setProperty(String key, String value) throws ManagedLedgerException { throw unsupported("setProperty"); }
    @Override
    public void asyncSetProperty(String key, String value, UpdatePropertiesCallback callback, Object ctx) { callback.updatePropertiesFailed(unsupported("setProperty"), ctx); }
    @Override
    public void deleteProperty(String key) throws ManagedLedgerException { throw unsupported("deleteProperty"); }
    @Override
    public void asyncDeleteProperty(String key, UpdatePropertiesCallback callback, Object ctx) { callback.updatePropertiesFailed(unsupported("deleteProperty"), ctx); }
    @Override
    public void setProperties(Map<String, String> properties) throws ManagedLedgerException { throw unsupported("setProperties"); }
    @Override
    public void asyncSetProperties(Map<String, String> properties, UpdatePropertiesCallback callback, Object ctx) { callback.updatePropertiesFailed(unsupported("setProperties"), ctx); }
    @Override
    public void trimConsumedLedgersInBackground(CompletableFuture<?> promise) { promise.complete(null); }
    @Override
    public void skipNonRecoverableLedger(long ledgerId) { throw errorMapper.unsupportedRuntime("skipNonRecoverableLedger"); }
    @Override
    public void rollCurrentLedgerIfFull() { }
    @Override
    public CompletableFuture<Position> asyncFindPosition(Predicate<Entry> predicate) { return unsupportedFuture("findPosition"); }
    @Override
    public ManagedLedgerInterceptor getManagedLedgerInterceptor() { return null; }
    @Override
    public CompletableFuture<LedgerInfo> getLedgerInfo(long ledgerId) { return unsupportedFuture("getLedgerInfo"); }
    @Override
    public Optional<LedgerInfo> getOptionalLedgerInfo(long ledgerId) { return Optional.empty(); }
    @Override
    public CompletableFuture<Void> asyncTruncate() { return unsupportedFuture("truncate"); }
    @Override
    public CompletableFuture<ManagedLedgerInternalStats> getManagedLedgerInternalStats(boolean includeLedgerMetadata) { return unsupportedFuture("getManagedLedgerInternalStats"); }
    @Override
    public boolean checkInactiveLedgerAndRollOver() { return false; }
    @Override
    public void checkCursorsToCacheEntries() { }
    @Override
    public void asyncReadEntry(Position position, ReadEntryCallback callback, Object ctx) { callback.readEntryFailed(unsupported("readEntry"), ctx); }
    @Override
    public NavigableMap<Long, LedgerInfo> getLedgersInfo() { return java.util.Collections.unmodifiableNavigableMap(new TreeMap<>()); }
    @Override
    public Position getNextValidPosition(Position position) { throw errorMapper.unsupportedRuntime("getNextValidPosition"); }
    @Override
    public Position getPreviousPosition(Position position) { throw errorMapper.unsupportedRuntime("getPreviousPosition"); }
    @Override
    public long getEstimatedBacklogSize(Position position) { return 0; }
    @Override
    public Position getPositionAfterN(Position startPosition, long n, PositionBound bound) { throw errorMapper.unsupportedRuntime("getPositionAfterN"); }
    @Override
    public int getPendingAddEntriesCount() { return 0; }
    @Override
    public long getCacheSize() { return 0; }
    @Override
    public CompletableFuture<Position> getLastDispatchablePosition(Predicate<Entry> predicate, Position startPosition) { return unsupportedFuture("getLastDispatchablePosition"); }
    @Override
    public Position getFirstPosition() { throw errorMapper.unsupportedRuntime("getFirstPosition"); }
}
