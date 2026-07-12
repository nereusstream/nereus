/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cache;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.IntSupplier;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.cache.EntryCache;
import org.apache.bookkeeper.mledger.impl.cache.EntryCacheManager;

/** Non-null stock cache surface that never stores entries or falls back to BookKeeper. */
public final class NereusNoopEntryCacheManager implements EntryCacheManager {
    private static final String UNSUPPORTED = "NEREUS_UNSUPPORTED_OPERATION:bookkeeperEntryCacheRead";
    private final ConcurrentMap<String, EntryCache> caches = new ConcurrentHashMap<>();

    @Override
    public EntryCache getEntryCache(ManagedLedger ledger) {
        return caches.computeIfAbsent(ledger.getName(), NoopEntryCache::new);
    }

    @Override public void removeEntryCache(String name) { caches.remove(name); }
    @Override public long getSize() { return 0; }
    @Override public long getMaxSize() { return 0; }
    @Override public void clear() { caches.clear(); }
    @Override public void updateCacheSizeAndThreshold(long maxSize) { }
    @Override public void updateCacheEvictionWatermark(double cacheEvictionWatermark) { }
    @Override public double getCacheEvictionWatermark() { return 0; }
    @Override public void doCacheEviction() { }
    @Override public void updateCacheEvictionExtendTTLOfEntriesWithRemainingExpectedReadsMaxTimes(int times) { }
    @Override public void updateCacheEvictionExtendTTLOfRecentlyAccessed(boolean enabled) { }

    private record NoopEntryCache(String name) implements EntryCache {
        @Override public String getName() { return name; }
        @Override public boolean insert(Entry entry) { return false; }
        @Override public void invalidateEntries(Position lastPosition) { }
        @Override public void invalidateAllEntries(long ledgerId) { }
        @Override public void clear() { }
        @Override
        public void asyncReadEntry(
                ReadHandle handle,
                long firstEntry,
                long lastEntry,
                IntSupplier expectedReadCount,
                ReadEntriesCallback callback,
                Object ctx) {
            callback.readEntriesFailed(new ManagedLedgerException(UNSUPPORTED), ctx);
        }
        @Override
        public void asyncReadEntry(
                ReadHandle handle, Position position, ReadEntryCallback callback, Object ctx) {
            callback.readEntryFailed(new ManagedLedgerException(UNSUPPORTED), ctx);
        }
        @Override public long getSize() { return 0; }
    }
}
