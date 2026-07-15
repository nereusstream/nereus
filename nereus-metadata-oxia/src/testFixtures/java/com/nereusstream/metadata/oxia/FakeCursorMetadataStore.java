/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.records.CursorRetentionRecord;
import com.nereusstream.metadata.oxia.records.CursorStateRecord;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/** Restartable codec-backed fake for the complete F3 cursor metadata contract. */
public final class FakeCursorMetadataStore implements CursorMetadataStore {
    public record StoredValue(String key, String partitionKey, byte[] envelope, long version) {
        public StoredValue {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(partitionKey, "partitionKey");
            envelope = Objects.requireNonNull(envelope, "envelope").clone();
            if (version < 0) {
                throw new IllegalArgumentException("version must be non-negative");
            }
        }

        @Override
        public byte[] envelope() {
            return envelope.clone();
        }
    }

    /** Durable bytes and versions survive construction of a new fake adapter instance. */
    public static final class DurableState {
        private final Map<String, StoredValue> values = new HashMap<>();
        private final AtomicLong nextVersion = new AtomicLong();
        private final CopyOnWriteArrayList<FakeWatch> watches = new CopyOnWriteArrayList<>();
        private volatile boolean watchDeliveryEnabled = true;

        public synchronized Optional<StoredValue> storedValue(String key) {
            StoredValue value = values.get(Objects.requireNonNull(key, "key"));
            return Optional.ofNullable(value == null ? null : copy(value));
        }

        public synchronized List<StoredValue> storedValues() {
            return values.values().stream()
                    .sorted((left, right) -> compareHierarchical(left.key(), right.key()))
                    .map(DurableState::copy)
                    .toList();
        }

        public synchronized void inject(String key, String partitionKey, byte[] envelope) {
            long version = nextVersion.getAndIncrement();
            values.put(key, new StoredValue(key, partitionKey, envelope, version));
        }

        public void setWatchDeliveryEnabled(boolean enabled) {
            watchDeliveryEnabled = enabled;
        }

        private synchronized Optional<PartitionedOxiaClient.VersionedValue> get(
                String key, PartitionKey partitionKey) {
            StoredValue value = values.get(key);
            requirePartition(value, partitionKey, key);
            return Optional.ofNullable(value == null ? null : versioned(value));
        }

        private synchronized PartitionedOxiaClient.WriteResult putIfAbsent(
                String key, byte[] envelope, PartitionKey partitionKey) {
            if (values.containsKey(key)) {
                throw new CursorMetadataConditionFailedException("key already exists: " + key);
            }
            long version = nextVersion.getAndIncrement();
            values.put(key, new StoredValue(key, partitionKey.value(), envelope, version));
            return new PartitionedOxiaClient.WriteResult(version);
        }

        private synchronized PartitionedOxiaClient.WriteResult putIfVersion(
                String key, byte[] envelope, long expectedVersion, PartitionKey partitionKey) {
            StoredValue current = values.get(key);
            if (current == null || current.version() != expectedVersion) {
                throw new CursorMetadataConditionFailedException("unexpected version for key: " + key);
            }
            requirePartition(current, partitionKey, key);
            long version = nextVersion.getAndIncrement();
            values.put(key, new StoredValue(key, partitionKey.value(), envelope, version));
            return new PartitionedOxiaClient.WriteResult(version);
        }

        private synchronized void deleteIfVersion(
                String key, long expectedVersion, PartitionKey partitionKey) {
            StoredValue current = values.get(key);
            if (current == null || current.version() != expectedVersion) {
                throw new F4MetadataConditionFailedException("unexpected version for key: " + key);
            }
            requirePartition(current, partitionKey, key);
            values.remove(key);
        }

        private synchronized List<String> list(String fromInclusive, String toExclusive) {
            return values.keySet().stream()
                    .filter(key -> compareHierarchical(key, fromInclusive) >= 0
                            && compareHierarchical(key, toExclusive) < 0)
                    .sorted(DurableState::compareHierarchical)
                    .toList();
        }

        private synchronized List<PartitionedOxiaClient.VersionedValue> rangeScan(
                String fromInclusive, String toExclusive, int limit, PartitionKey partitionKey) {
            List<PartitionedOxiaClient.VersionedValue> result = new ArrayList<>();
            values.values().stream()
                    .filter(value -> compareHierarchical(value.key(), fromInclusive) >= 0
                            && compareHierarchical(value.key(), toExclusive) < 0)
                    .sorted((left, right) -> compareHierarchical(left.key(), right.key()))
                    .limit(limit)
                    .forEach(value -> {
                        requirePartition(value, partitionKey, value.key());
                        result.add(versioned(value));
                    });
            return List.copyOf(result);
        }

        private WatchRegistration watch(String prefix, PartitionKey partitionKey, Runnable callback) {
            FakeWatch watch = new FakeWatch(prefix, partitionKey.value(), callback, new AtomicBoolean(true));
            watches.add(watch);
            return () -> {
                if (watch.active().compareAndSet(true, false)) {
                    watches.remove(watch);
                }
            };
        }

        private void notifyWatches(String key, PartitionKey partitionKey) {
            if (!watchDeliveryEnabled) {
                return;
            }
            for (FakeWatch watch : watches) {
                if (watch.active().get()
                        && watch.partitionKey().equals(partitionKey.value())
                        && key.startsWith(watch.prefix())) {
                    watch.callback().run();
                }
            }
        }

        private static void requirePartition(StoredValue value, PartitionKey partitionKey, String key) {
            if (value != null && !value.partitionKey().equals(partitionKey.value())) {
                throw new IllegalStateException("partition key mismatch for durable key: " + key);
            }
        }

        private static PartitionedOxiaClient.VersionedValue versioned(StoredValue value) {
            return new PartitionedOxiaClient.VersionedValue(
                    value.key(), value.envelope(), value.version());
        }

        private static StoredValue copy(StoredValue value) {
            return new StoredValue(value.key(), value.partitionKey(), value.envelope(), value.version());
        }

        private static int compareHierarchical(String left, String right) {
            int level = Integer.compare(hierarchyLevel(left), hierarchyLevel(right));
            if (level != 0) {
                return level;
            }
            byte[] leftBytes = left.getBytes(StandardCharsets.UTF_8);
            byte[] rightBytes = right.getBytes(StandardCharsets.UTF_8);
            int common = Math.min(leftBytes.length, rightBytes.length);
            for (int index = 0; index < common; index++) {
                int leftByte = leftBytes[index] == '/' ? 0xff : Byte.toUnsignedInt(leftBytes[index]);
                int rightByte = rightBytes[index] == '/' ? 0xff : Byte.toUnsignedInt(rightBytes[index]);
                if (leftByte != rightByte) {
                    return Integer.compare(leftByte, rightByte);
                }
            }
            return Integer.compare(leftBytes.length, rightBytes.length);
        }

        private static int hierarchyLevel(String key) {
            int separators = 0;
            for (int index = 0; index < key.length(); index++) {
                if (key.charAt(index) == '/') {
                    separators++;
                }
            }
            return key.endsWith("//") ? separators - 1 : separators;
        }
    }

    private record FakeWatch(
            String prefix, String partitionKey, Runnable callback, AtomicBoolean active) {
    }

    private final CursorMetadataStoreCore core;
    private final DurableState durableState;

    public FakeCursorMetadataStore() {
        this(new DurableState(), CursorMetadataStoreConfig.defaults());
    }

    public FakeCursorMetadataStore(
            DurableState durableState, CursorMetadataStoreConfig config) {
        this.durableState = Objects.requireNonNull(durableState, "durableState");
        this.core = new CursorMetadataStoreCore(
                new PartitionedOxiaClient(new FakeBackend(durableState)), config);
    }

    public DurableState durableState() {
        return durableState;
    }

    @Override
    public CompletableFuture<Optional<VersionedCursorState>> getCursor(
            String cluster, StreamId streamId, String cursorName) {
        return core.getCursor(cluster, streamId, cursorName);
    }

    @Override
    public CompletableFuture<VersionedCursorState> createCursor(
            String cluster, CursorStateRecord value) {
        return core.createCursor(cluster, value);
    }

    @Override
    public CompletableFuture<VersionedCursorState> compareAndSetCursor(
            String cluster, CursorStateRecord value, long expectedMetadataVersion) {
        return core.compareAndSetCursor(cluster, value, expectedMetadataVersion);
    }

    @Override
    public CompletableFuture<CursorScanPage> scanCursors(
            String cluster,
            StreamId streamId,
            Optional<CursorScanToken> continuation,
            int pageSize) {
        return core.scanCursors(cluster, streamId, continuation, pageSize);
    }

    @Override
    public CompletableFuture<Optional<VersionedCursorRetention>> getRetention(
            String cluster, StreamId streamId) {
        return core.getRetention(cluster, streamId);
    }

    @Override
    public CompletableFuture<VersionedCursorRetention> createRetention(
            String cluster, CursorRetentionRecord value) {
        return core.createRetention(cluster, value);
    }

    @Override
    public CompletableFuture<VersionedCursorRetention> compareAndSetRetention(
            String cluster, CursorRetentionRecord value, long expectedMetadataVersion) {
        return core.compareAndSetRetention(cluster, value, expectedMetadataVersion);
    }

    @Override
    public WatchRegistration watchStreamCursors(
            String cluster, StreamId streamId, Runnable invalidation) {
        return core.watchStreamCursors(cluster, streamId, invalidation);
    }

    @Override
    public void close() {
        core.close();
    }

    private static final class FakeBackend implements PartitionedOxiaClient.Backend {
        private final DurableState state;

        private FakeBackend(DurableState state) {
            this.state = state;
        }

        @Override
        public CompletableFuture<Optional<PartitionedOxiaClient.VersionedValue>> get(
                String key, PartitionKey partitionKey) {
            return invoke(() -> state.get(key, partitionKey));
        }

        @Override
        public CompletableFuture<PartitionedOxiaClient.WriteResult> putIfAbsent(
                String key, byte[] value, PartitionKey partitionKey) {
            return invoke(() -> {
                PartitionedOxiaClient.WriteResult result = state.putIfAbsent(key, value, partitionKey);
                state.notifyWatches(key, partitionKey);
                return result;
            });
        }

        @Override
        public CompletableFuture<PartitionedOxiaClient.WriteResult> putIfVersion(
                String key, byte[] value, long expectedVersion, PartitionKey partitionKey) {
            return invoke(() -> {
                PartitionedOxiaClient.WriteResult result = state.putIfVersion(
                        key, value, expectedVersion, partitionKey);
                state.notifyWatches(key, partitionKey);
                return result;
            });
        }

        @Override
        public CompletableFuture<Void> deleteIfVersion(
                String key, long expectedVersion, PartitionKey partitionKey) {
            return invoke(() -> {
                state.deleteIfVersion(key, expectedVersion, partitionKey);
                state.notifyWatches(key, partitionKey);
                return null;
            });
        }

        @Override
        public CompletableFuture<List<String>> list(
                String fromInclusive, String toExclusive, PartitionKey partitionKey) {
            return invoke(() -> state.list(fromInclusive, toExclusive));
        }

        @Override
        public CompletableFuture<List<PartitionedOxiaClient.VersionedValue>> rangeScan(
                String fromInclusive, String toExclusive, int limit, PartitionKey partitionKey) {
            return invoke(() -> state.rangeScan(fromInclusive, toExclusive, limit, partitionKey));
        }

        @Override
        public WatchRegistration watchPrefix(
                String prefix, PartitionKey partitionKey, Runnable invalidationCallback) {
            return state.watch(prefix, partitionKey, invalidationCallback);
        }

        private static <T> CompletableFuture<T> invoke(java.util.concurrent.Callable<T> operation) {
            try {
                return CompletableFuture.completedFuture(operation.call());
            } catch (Throwable error) {
                return CompletableFuture.failedFuture(error);
            }
        }
    }
}
