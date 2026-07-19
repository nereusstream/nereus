/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.apache.bookkeeper.client.api.ReadHandle;

/** Bounded, non-authoritative read-handle cache; durable targets remain sufficient after restart. */
public final class BookKeeperLedgerHandleCache implements AutoCloseable {
    public record Key(String clusterAlias, long ledgerId, long ledgerRootEpoch) {
        public Key {
            Objects.requireNonNull(clusterAlias, "clusterAlias");
            if (clusterAlias.isBlank() || ledgerId < 0 || ledgerRootEpoch <= 0) {
                throw new IllegalArgumentException("invalid BookKeeper handle-cache key");
            }
        }
    }

    public final class Lease implements AutoCloseable {
        private final Entry entry;
        private final ReadHandle handle;
        private final AtomicBoolean released = new AtomicBoolean();
        private Lease(Entry entry, ReadHandle handle) { this.entry = entry; this.handle = handle; }
        public ReadHandle handle() { return handle; }
        @Override public void close() {
            if (released.compareAndSet(false, true)) release(entry);
        }
    }

    private final int maxHandles;
    private final long maxEstimatedBytes;
    private final long estimatedBytesPerHandle;
    private final long idleNanos;
    private final Map<Key, Entry> entries = new HashMap<>();
    private boolean closed;

    public BookKeeperLedgerHandleCache(
            int maxHandles, long maxEstimatedBytes, long estimatedBytesPerHandle, Duration idleTime) {
        if (maxHandles <= 0 || maxEstimatedBytes <= 0 || estimatedBytesPerHandle <= 0
                || estimatedBytesPerHandle > maxEstimatedBytes) {
            throw new IllegalArgumentException("invalid BookKeeper handle-cache bounds");
        }
        Objects.requireNonNull(idleTime, "idleTime");
        if (idleTime.isZero() || idleTime.isNegative()) throw new IllegalArgumentException("idleTime must be positive");
        this.maxHandles = maxHandles;
        this.maxEstimatedBytes = maxEstimatedBytes;
        this.estimatedBytesPerHandle = estimatedBytesPerHandle;
        this.idleNanos = idleTime.toNanos();
    }

    public CompletableFuture<Lease> borrow(Key key, Supplier<CompletableFuture<ReadHandle>> opener) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(opener, "opener");
        Entry selected;
        List<ReadHandle> evicted;
        synchronized (this) {
            if (closed) return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.STORAGE_CLOSED, false, "BookKeeper handle cache is closed"));
            long now = System.nanoTime();
            evicted = evictIdle(now);
            selected = entries.get(key);
            if (selected == null) {
                if (entries.size() >= maxHandles
                        || Math.multiplyExact((long) (entries.size() + 1), estimatedBytesPerHandle)
                                > maxEstimatedBytes) {
                    evicted.forEach(handle -> handle.closeAsync());
                    return CompletableFuture.failedFuture(new NereusException(
                            ErrorCode.BACKPRESSURE_REJECTED, true, "BookKeeper read-handle cache is full"));
                }
                CompletableFuture<ReadHandle> opening;
                try { opening = Objects.requireNonNull(opener.get(), "opener future"); }
                catch (Throwable failure) { return CompletableFuture.failedFuture(failure); }
                selected = new Entry(key, opening, now);
                entries.put(key, selected);
                Entry installed = selected;
                opening.whenComplete((handle, failure) -> {
                    if (failure != null) removeFailed(installed);
                    else closeIfCacheClosed(installed, handle);
                });
            }
            selected.references++;
            selected.lastAccessNanos = now;
        }
        evicted.forEach(handle -> handle.closeAsync());
        Entry exact = selected;
        return exact.opening.handle((handle, failure) -> {
            if (failure != null) {
                release(exact);
                throw new java.util.concurrent.CompletionException(failure);
            }
            return new Lease(exact, handle);
        });
    }

    private synchronized List<ReadHandle> evictIdle(long now) {
        List<Entry> candidates = entries.values().stream()
                .filter(entry -> entry.references == 0 && now - entry.lastAccessNanos >= idleNanos)
                .sorted(Comparator.comparingLong(entry -> entry.lastAccessNanos))
                .toList();
        List<ReadHandle> handles = new ArrayList<>();
        for (Entry candidate : candidates) {
            if (entries.remove(candidate.key, candidate)) {
                ReadHandle completed = candidate.opening.isCompletedExceptionally()
                        || candidate.opening.isCancelled() ? null : candidate.opening.getNow(null);
                if (completed != null) handles.add(completed);
                else candidate.opening.thenCompose(ReadHandle::closeAsync);
            }
        }
        return handles;
    }

    private synchronized void release(Entry entry) {
        if (entry.references <= 0) throw new IllegalStateException("BookKeeper handle lease released twice");
        entry.references--;
        entry.lastAccessNanos = System.nanoTime();
    }
    private synchronized void removeFailed(Entry entry) { entries.remove(entry.key, entry); }
    private synchronized void closeIfCacheClosed(Entry entry, ReadHandle handle) {
        if (closed && entries.remove(entry.key, entry)) handle.closeAsync();
    }

    @Override public void close() {
        List<CompletableFuture<ReadHandle>> handles;
        synchronized (this) {
            if (closed) return;
            closed = true;
            handles = entries.values().stream().map(entry -> entry.opening).toList();
            entries.clear();
        }
        handles.forEach(future -> future.thenCompose(ReadHandle::closeAsync));
    }

    private static final class Entry {
        private final Key key;
        private final CompletableFuture<ReadHandle> opening;
        private int references;
        private long lastAccessNanos;
        private Entry(Key key, CompletableFuture<ReadHandle> opening, long now) {
            this.key = key; this.opening = opening; this.lastAccessNanos = now;
        }
    }
}
