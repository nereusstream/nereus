/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.records.ManagedLedgerProjectionIdentity;
import com.nereusstream.metadata.oxia.records.TopicProjectionRecord;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/** Restartable codec-backed fake for the complete F2 projection metadata contract. */
public final class FakeManagedLedgerProjectionMetadataStore
        implements ManagedLedgerProjectionMetadataStore {
    public enum FailurePoint {
        AFTER_ALLOCATOR_WRITE,
        AFTER_TOPIC_WRITE,
        AFTER_VIRTUAL_LEDGER_WRITE,
        AFTER_POSITION_INDEX_WRITE
    }

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
        private final AtomicLong backendCalls = new AtomicLong();
        private final AtomicReference<CountDownLatch> operationGate = new AtomicReference<>();

        public long backendCalls() {
            return backendCalls.get();
        }

        public synchronized Optional<StoredValue> storedValue(String key) {
            StoredValue value = values.get(Objects.requireNonNull(key, "key"));
            return Optional.ofNullable(value == null ? null : copy(value));
        }

        public synchronized void inject(
                String key,
                String partitionKey,
                byte[] envelope) {
            long version = nextVersion.getAndIncrement();
            values.put(key, new StoredValue(key, partitionKey, envelope, version));
        }

        public void blockBackendOperations() {
            if (!operationGate.compareAndSet(null, new CountDownLatch(1))) {
                throw new IllegalStateException("backend operations are already blocked");
            }
        }

        public void releaseBackendOperations() {
            CountDownLatch gate = operationGate.getAndSet(null);
            if (gate != null) {
                gate.countDown();
            }
        }

        private CountDownLatch registerBackendCall() {
            backendCalls.incrementAndGet();
            return operationGate.get();
        }

        private static void awaitGate(CountDownLatch gate) {
            if (gate != null) {
                try {
                    gate.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new NereusException(
                            ErrorCode.CANCELLED, true, "fake projection backend operation interrupted", e);
                }
            }
        }

        private synchronized Optional<PartitionedOxiaClient.VersionedValue> get(
                String key,
                PartitionKey partitionKey) {
            StoredValue value = values.get(key);
            if (value != null && !value.partitionKey().equals(partitionKey.value())) {
                throw new IllegalStateException("partition key mismatch for durable key: " + key);
            }
            return Optional.ofNullable(value == null ? null : new PartitionedOxiaClient.VersionedValue(
                    value.key(), value.envelope(), value.version()));
        }

        private synchronized PartitionedOxiaClient.WriteResult putIfAbsent(
                String key,
                byte[] envelope,
                PartitionKey partitionKey) {
            if (values.containsKey(key)) {
                throw new ProjectionMetadataConditionFailedException("key already exists: " + key);
            }
            long version = nextVersion.getAndIncrement();
            values.put(key, new StoredValue(key, partitionKey.value(), envelope, version));
            return new PartitionedOxiaClient.WriteResult(version);
        }

        private synchronized PartitionedOxiaClient.WriteResult putIfVersion(
                String key,
                byte[] envelope,
                long expectedVersion,
                PartitionKey partitionKey) {
            StoredValue current = values.get(key);
            if (current == null || current.version() != expectedVersion) {
                throw new ProjectionMetadataConditionFailedException("unexpected version for key: " + key);
            }
            if (!current.partitionKey().equals(partitionKey.value())) {
                throw new IllegalStateException("partition key changed for durable key: " + key);
            }
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
            if (!current.partitionKey().equals(partitionKey.value())) {
                throw new IllegalStateException("partition key changed for durable key: " + key);
            }
            values.remove(key);
        }

        private synchronized List<String> list(String fromInclusive, String toExclusive) {
            return values.keySet().stream()
                    .filter(key -> key.compareTo(fromInclusive) >= 0 && key.compareTo(toExclusive) < 0)
                    .sorted()
                    .toList();
        }

        private synchronized List<PartitionedOxiaClient.VersionedValue> rangeScan(
                String fromInclusive,
                String toExclusive,
                int limit) {
            List<PartitionedOxiaClient.VersionedValue> result = new ArrayList<>();
            values.values().stream()
                    .filter(value -> value.key().compareTo(fromInclusive) >= 0
                            && value.key().compareTo(toExclusive) < 0)
                    .sorted(Comparator.comparing(StoredValue::key))
                    .limit(limit)
                    .forEach(value -> result.add(new PartitionedOxiaClient.VersionedValue(
                            value.key(), value.envelope(), value.version())));
            return List.copyOf(result);
        }

        private static StoredValue copy(StoredValue value) {
            return new StoredValue(value.key(), value.partitionKey(), value.envelope(), value.version());
        }
    }

    private final DurableState durableState;
    private final AtomicReference<FailurePoint> failNext = new AtomicReference<>();
    private final Map<FailurePoint, AtomicLong> successfulWrites = new EnumMap<>(FailurePoint.class);
    private final ProjectionMetadataStoreCore core;

    public FakeManagedLedgerProjectionMetadataStore() {
        this(new DurableState(), ProjectionMetadataStoreConfig.defaults(), Clock.systemUTC());
    }

    public FakeManagedLedgerProjectionMetadataStore(
            DurableState durableState,
            ProjectionMetadataStoreConfig config,
            Clock clock) {
        this.durableState = Objects.requireNonNull(durableState, "durableState");
        for (FailurePoint point : FailurePoint.values()) {
            successfulWrites.put(point, new AtomicLong());
        }
        PartitionedOxiaClient client = new PartitionedOxiaClient(new FakeBackend(durableState));
        this.core = new ProjectionMetadataStoreCore(client, config, clock, this::afterWrite);
    }

    public DurableState durableState() {
        return durableState;
    }

    public void failNext(FailurePoint failurePoint) {
        if (!failNext.compareAndSet(null, Objects.requireNonNull(failurePoint, "failurePoint"))) {
            throw new IllegalStateException("a fake failure is already armed");
        }
    }

    public long successfulWrites(FailurePoint failurePoint) {
        return successfulWrites.get(Objects.requireNonNull(failurePoint, "failurePoint")).get();
    }

    @Override
    public CompletableFuture<Optional<TopicProjectionRecord>> getProjection(
            String cluster,
            String managedLedgerName) {
        return core.getProjection(cluster, managedLedgerName);
    }

    @Override
    public CompletableFuture<ManagedLedgerStreamProjection> getProjectionByStream(
            String cluster, StreamId streamId) {
        return core.getProjectionByStream(cluster, streamId);
    }

    @Override
    public CompletableFuture<TopicProjectionRecord> createFirstProjection(
            String cluster,
            ProjectionCreateRequest request,
            ProjectionPublishGuard publishGuard) {
        return core.createFirstProjection(cluster, request, publishGuard);
    }

    @Override
    public CompletableFuture<TopicProjectionRecord> recreateDeletedProjection(
            String cluster,
            ManagedLedgerProjectionIdentity expectedDeletedIdentity,
            long expectedTopicMetadataVersion,
            ProjectionCreateRequest request,
            ProjectionPublishGuard publishGuard) {
        return core.recreateDeletedProjection(
                cluster, expectedDeletedIdentity, expectedTopicMetadataVersion, request, publishGuard);
    }

    @Override
    public CompletableFuture<TopicProjectionRecord> updateProperties(
            String cluster,
            String managedLedgerName,
            ManagedLedgerProjectionIdentity expectedIdentity,
            long expectedVersion,
            Map<String, String> properties) {
        return core.updateProperties(
                cluster, managedLedgerName, expectedIdentity, expectedVersion, properties);
    }

    @Override
    public CompletableFuture<TopicProjectionRecord> activateCursorProtocol(
            String cluster,
            String managedLedgerName,
            ManagedLedgerProjectionIdentity expectedIdentity,
            long expectedMetadataVersion) {
        return core.activateCursorProtocol(
                cluster, managedLedgerName, expectedIdentity, expectedMetadataVersion);
    }

    @Override
    public CompletableFuture<TopicProjectionRecord> activateGenerationProtocol(
            String cluster,
            String managedLedgerName,
            ManagedLedgerProjectionIdentity expectedIdentity,
            long expectedMetadataVersion) {
        return core.activateGenerationProtocol(
                cluster, managedLedgerName, expectedIdentity, expectedMetadataVersion);
    }

    @Override
    public CompletableFuture<TopicProjectionRecord> mirrorFacadeState(
            String cluster,
            String managedLedgerName,
            ManagedLedgerProjectionIdentity expectedIdentity,
            long expectedVersion,
            ManagedLedgerFacadeState state) {
        return core.mirrorFacadeState(
                cluster, managedLedgerName, expectedIdentity, expectedVersion, state);
    }

    @Override
    public CompletableFuture<ProjectionRepairResult> repairProjectionIndexes(
            String cluster,
            TopicProjectionRecord authoritative) {
        return core.repairProjectionIndexes(cluster, authoritative);
    }

    @Override
    public void close() {
        core.close();
    }

    private void afterWrite(ProjectionMetadataStoreCore.WriteKind kind) {
        FailurePoint point = switch (kind) {
            case ALLOCATOR -> FailurePoint.AFTER_ALLOCATOR_WRITE;
            case TOPIC -> FailurePoint.AFTER_TOPIC_WRITE;
            case VIRTUAL_LEDGER -> FailurePoint.AFTER_VIRTUAL_LEDGER_WRITE;
            case POSITION_INDEX -> FailurePoint.AFTER_POSITION_INDEX_WRITE;
        };
        successfulWrites.get(point).incrementAndGet();
        if (failNext.compareAndSet(point, null)) {
            throw new NereusException(
                    ErrorCode.METADATA_UNAVAILABLE, true, "injected fake failure at " + point);
        }
    }

    private static final class FakeBackend implements PartitionedOxiaClient.Backend {
        private final DurableState state;

        private FakeBackend(DurableState state) {
            this.state = state;
        }

        @Override
        public CompletableFuture<Optional<PartitionedOxiaClient.VersionedValue>> get(
                String key,
                PartitionKey partitionKey) {
            return invoke(() -> state.get(key, partitionKey));
        }

        @Override
        public CompletableFuture<PartitionedOxiaClient.WriteResult> putIfAbsent(
                String key,
                byte[] value,
                PartitionKey partitionKey) {
            return invoke(() -> state.putIfAbsent(key, value, partitionKey));
        }

        @Override
        public CompletableFuture<PartitionedOxiaClient.WriteResult> putIfVersion(
                String key,
                byte[] value,
                long expectedVersion,
                PartitionKey partitionKey) {
            return invoke(() -> state.putIfVersion(key, value, expectedVersion, partitionKey));
        }

        @Override
        public CompletableFuture<Void> deleteIfVersion(
                String key, long expectedVersion, PartitionKey partitionKey) {
            return invoke(() -> {
                state.deleteIfVersion(key, expectedVersion, partitionKey);
                return null;
            });
        }

        @Override
        public CompletableFuture<List<String>> list(
                String fromInclusive,
                String toExclusive,
                PartitionKey partitionKey) {
            return invoke(() -> state.list(fromInclusive, toExclusive));
        }

        @Override
        public CompletableFuture<List<PartitionedOxiaClient.VersionedValue>> rangeScan(
                String fromInclusive,
                String toExclusive,
                int limit,
                PartitionKey partitionKey) {
            return invoke(() -> state.rangeScan(fromInclusive, toExclusive, limit));
        }

        @Override
        public WatchRegistration watchPrefix(
                String prefix,
                PartitionKey partitionKey,
                Runnable invalidationCallback) {
            return () -> { };
        }

        private <T> CompletableFuture<T> invoke(java.util.concurrent.Callable<T> operation) {
            CountDownLatch gate = state.registerBackendCall();
            if (gate != null) {
                return CompletableFuture.supplyAsync(() -> {
                    DurableState.awaitGate(gate);
                    return call(operation);
                });
            }
            try {
                return CompletableFuture.completedFuture(operation.call());
            } catch (Throwable e) {
                return CompletableFuture.failedFuture(e);
            }
        }

        private static <T> T call(java.util.concurrent.Callable<T> operation) {
            try {
                return operation.call();
            } catch (Throwable e) {
                throw new CompletionException(e);
            }
        }
    }
}
