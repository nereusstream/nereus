/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Exact-version deterministic backend for unchanged F4 metadata-store contract scenarios. */
public final class InMemoryPartitionedOxiaBackend implements PartitionedOxiaClient.Backend {
    private final Map<StorageKey, Stored> values = new HashMap<>();
    private final List<Watcher> watchers = new ArrayList<>();
    private long nextVersion = 1;

    @Override
    public synchronized CompletableFuture<Optional<PartitionedOxiaClient.VersionedValue>> get(
            String key,
            PartitionKey partitionKey) {
        Stored stored = values.get(new StorageKey(partitionKey.value(), key));
        return completed(stored == null
                ? Optional.empty()
                : Optional.of(versioned(key, stored)));
    }

    @Override
    public synchronized CompletableFuture<PartitionedOxiaClient.WriteResult> putIfAbsent(
            String key,
            byte[] value,
            PartitionKey partitionKey) {
        StorageKey storageKey = new StorageKey(partitionKey.value(), key);
        if (values.containsKey(storageKey)) {
            return failed(new F4MetadataConditionFailedException("key already exists"));
        }
        long version = nextVersion++;
        values.put(storageKey, new Stored(value.clone(), version));
        notifyWatchers(partitionKey, key);
        return completed(new PartitionedOxiaClient.WriteResult(version));
    }

    @Override
    public synchronized CompletableFuture<PartitionedOxiaClient.WriteResult> putIfVersion(
            String key,
            byte[] value,
            long expectedVersion,
            PartitionKey partitionKey) {
        StorageKey storageKey = new StorageKey(partitionKey.value(), key);
        Stored current = values.get(storageKey);
        if (current == null || current.version() != expectedVersion) {
            return failed(new F4MetadataConditionFailedException("version mismatch"));
        }
        long version = nextVersion++;
        values.put(storageKey, new Stored(value.clone(), version));
        notifyWatchers(partitionKey, key);
        return completed(new PartitionedOxiaClient.WriteResult(version));
    }

    @Override
    public synchronized CompletableFuture<Void> deleteIfVersion(
            String key,
            long expectedVersion,
            PartitionKey partitionKey) {
        StorageKey storageKey = new StorageKey(partitionKey.value(), key);
        Stored current = values.get(storageKey);
        if (current == null || current.version() != expectedVersion) {
            return failed(new F4MetadataConditionFailedException("delete version mismatch"));
        }
        values.remove(storageKey);
        notifyWatchers(partitionKey, key);
        return completed(null);
    }

    @Override
    public synchronized CompletableFuture<List<String>> list(
            String fromInclusive,
            String toExclusive,
            PartitionKey partitionKey) {
        return completed(values.keySet().stream()
                .filter(key -> key.partition().equals(partitionKey.value()))
                .map(StorageKey::key)
                .filter(key -> key.compareTo(fromInclusive) >= 0 && key.compareTo(toExclusive) < 0)
                .sorted()
                .toList());
    }

    @Override
    public synchronized CompletableFuture<List<PartitionedOxiaClient.VersionedValue>> rangeScan(
            String fromInclusive,
            String toExclusive,
            int limit,
            PartitionKey partitionKey) {
        return completed(values.entrySet().stream()
                .filter(entry -> entry.getKey().partition().equals(partitionKey.value()))
                .filter(entry -> entry.getKey().key().compareTo(fromInclusive) >= 0
                        && entry.getKey().key().compareTo(toExclusive) < 0)
                .sorted(Map.Entry.comparingByKey())
                .limit(limit)
                .map(entry -> versioned(entry.getKey().key(), entry.getValue()))
                .toList());
    }

    @Override
    public synchronized WatchRegistration watchPrefix(
            String prefix,
            PartitionKey partitionKey,
            Runnable invalidationCallback) {
        Watcher watcher = new Watcher(partitionKey.value(), prefix, invalidationCallback);
        watchers.add(watcher);
        return () -> removeWatcher(watcher);
    }

    public synchronized int size() {
        return values.size();
    }

    private synchronized void removeWatcher(Watcher watcher) {
        watchers.remove(watcher);
    }

    private void notifyWatchers(PartitionKey partitionKey, String key) {
        List<Runnable> callbacks = watchers.stream()
                .filter(watcher -> watcher.partition().equals(partitionKey.value())
                        && key.startsWith(watcher.prefix()))
                .map(Watcher::callback)
                .toList();
        callbacks.forEach(Runnable::run);
    }

    private static PartitionedOxiaClient.VersionedValue versioned(String key, Stored value) {
        return new PartitionedOxiaClient.VersionedValue(key, value.value(), value.version());
    }

    private static <T> CompletableFuture<T> completed(T value) {
        return CompletableFuture.completedFuture(value);
    }

    private static <T> CompletableFuture<T> failed(Throwable failure) {
        return CompletableFuture.failedFuture(failure);
    }

    private record StorageKey(String partition, String key) implements Comparable<StorageKey> {
        @Override
        public int compareTo(StorageKey other) {
            int partitionOrder = partition.compareTo(other.partition);
            return partitionOrder != 0 ? partitionOrder : key.compareTo(other.key);
        }
    }

    private record Stored(byte[] value, long version) {
        private Stored {
            value = value.clone();
        }

        @Override
        public byte[] value() {
            return value.clone();
        }
    }

    private record Watcher(String partition, String prefix, Runnable callback) {
    }
}
