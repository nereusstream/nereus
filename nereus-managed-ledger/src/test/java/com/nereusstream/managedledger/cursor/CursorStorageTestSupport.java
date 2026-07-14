/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import com.nereusstream.api.AppendAttemptId;
import com.nereusstream.api.AppendBatch;
import com.nereusstream.api.AppendOptions;
import com.nereusstream.api.AppendRecoveryOptions;
import com.nereusstream.api.AppendResult;
import com.nereusstream.api.AppendSession;
import com.nereusstream.api.AppendSessionOptions;
import com.nereusstream.api.DeleteOptions;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.ReadResult;
import com.nereusstream.api.ResolveOptions;
import com.nereusstream.api.ResolveResult;
import com.nereusstream.api.SealOptions;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamCreateOptions;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamMetadata;
import com.nereusstream.api.StreamName;
import com.nereusstream.api.StreamState;
import com.nereusstream.api.StreamStorage;
import com.nereusstream.api.TrimOptions;
import com.nereusstream.metadata.oxia.FakeCursorMetadataStore;
import com.nereusstream.metadata.oxia.FakeManagedLedgerProjectionMetadataStore;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionNames;
import com.nereusstream.metadata.oxia.ProjectionCreateRequest;
import com.nereusstream.metadata.oxia.CursorMetadataStore;
import com.nereusstream.metadata.oxia.CursorScanPage;
import com.nereusstream.metadata.oxia.CursorScanToken;
import com.nereusstream.metadata.oxia.VersionedCursorRetention;
import com.nereusstream.metadata.oxia.VersionedCursorState;
import com.nereusstream.metadata.oxia.WatchRegistration;
import com.nereusstream.metadata.oxia.records.CursorRetentionRecord;
import com.nereusstream.metadata.oxia.records.CursorStateRecord;
import com.nereusstream.metadata.oxia.records.TopicProjectionRecord;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.function.Supplier;

final class CursorStorageTestSupport {
    static final String CLUSTER = "cluster/a";
    static final String TOPIC = "persistent://tenant/ns/cursor-m2";
    static final String OWNER_1 = "00112233445566778899aabbccddeeff";
    static final String OWNER_2 = "ffeeddccbbaa99887766554433221100";
    static final String OWNER_3 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    static final Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC);

    private CursorStorageTestSupport() {
    }

    static final class Context implements AutoCloseable {
        final CursorStorageConfig config;
        final FakeManagedLedgerProjectionMetadataStore projectionStore =
                new FakeManagedLedgerProjectionMetadataStore();
        final FakeCursorMetadataStore metadataBackend;
        final ControllableCursorMetadataStore metadataStore;
        final TestStreamStorage streamStorage;
        final InMemorySnapshotStore snapshotStore;
        final ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(4);
        final AtomicInteger activationCalls = new AtomicInteger();
        final AtomicReference<CompletableFuture<Void>> activationGate =
                new AtomicReference<>(CompletableFuture.completedFuture(null));
        final AtomicReference<CountDownLatch> activationArrivals =
                new AtomicReference<>(new CountDownLatch(0));
        final AtomicInteger idSequence = new AtomicInteger();
        final TopicProjectionRecord projection;
        final CursorLedgerIdentity ledger;
        final DefaultCursorRetentionCoordinator retention;
        final DefaultCursorStorage storage;

        Context(long trimOffset, long committedEndOffset) {
            this(trimOffset, committedEndOffset, CursorStorageConfig.defaults());
        }

        Context(
                long trimOffset,
                long committedEndOffset,
                CursorStorageConfig config) {
            this.config = config;
            StreamId streamId = ManagedLedgerProjectionNames.streamId(TOPIC, 1);
            StreamName streamName = ManagedLedgerProjectionNames.streamName(TOPIC, 1);
            StreamMetadata empty = metadata(streamId, streamName, 0, 0, 0);
            projection = projectionStore.createFirstProjection(
                            CLUSTER,
                            new ProjectionCreateRequest(TOPIC, 1, 1, empty, Map.of()),
                            () -> CompletableFuture.completedFuture(null))
                    .join();
            metadataBackend = new FakeCursorMetadataStore();
            metadataStore = new ControllableCursorMetadataStore(metadataBackend);
            ledger = new CursorLedgerIdentity(
                    TOPIC,
                    ManagedLedgerProjectionNames.managedLedgerNameHash(TOPIC),
                    projection.projectionIdentity());
            streamStorage = new TestStreamStorage(metadata(
                    streamId, streamName, trimOffset, committedEndOffset, committedEndOffset * 100));
            snapshotStore = new InMemorySnapshotStore(config);
            CursorProtocolActivationGuard guard = ignored -> {
                activationCalls.incrementAndGet();
                activationArrivals.get().countDown();
                return activationGate.get();
            };
            CursorStateMachine stateMachine = new CursorStateMachine(config);
            retention = new DefaultCursorRetentionCoordinator(
                    CLUSTER,
                    streamStorage,
                    projectionStore,
                    metadataStore,
                    snapshotStore,
                    guard,
                    stateMachine,
                    config,
                    CLOCK,
                    scheduler,
                    () -> String.format("%032x", idSequence.incrementAndGet()),
                    System::nanoTime);
            storage = new DefaultCursorStorage(
                    CLUSTER,
                    streamStorage,
                    projectionStore,
                    metadataStore,
                    snapshotStore,
                    retention,
                    guard,
                    stateMachine,
                    new CursorStatePersistencePlanner(CLUSTER, config),
                    config,
                    CLOCK,
                    scheduler);
        }

        CursorOwnerSession owner(String id) {
            return new CursorOwnerSession(ledger, id);
        }

        void blockActivations(int expectedArrivals) {
            if (expectedArrivals <= 0) {
                throw new IllegalArgumentException("expectedArrivals must be positive");
            }
            CompletableFuture<Void> blocked = new CompletableFuture<>();
            while (true) {
                CompletableFuture<Void> current = activationGate.get();
                if (!current.isDone()) {
                    throw new IllegalStateException("activations are already blocked");
                }
                activationArrivals.set(new CountDownLatch(expectedArrivals));
                if (activationGate.compareAndSet(current, blocked)) {
                    return;
                }
            }
        }

        void awaitBlockedActivations() {
            try {
                if (!activationArrivals.get().await(10, TimeUnit.SECONDS)) {
                    throw new AssertionError("timed out waiting for blocked activation calls");
                }
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted waiting for blocked activation calls", error);
            }
        }

        void releaseActivations() {
            activationGate.getAndSet(CompletableFuture.completedFuture(null)).complete(null);
        }

        @Override
        public void close() {
            releaseActivations();
            storage.close();
            retention.close();
            snapshotStore.close();
            metadataStore.close();
            projectionStore.close();
            scheduler.shutdownNow();
        }

        private static StreamMetadata metadata(
                StreamId streamId,
                StreamName streamName,
                long trimOffset,
                long committedEndOffset,
                long cumulativeSize) {
            return new StreamMetadata(
                    streamId,
                    streamName,
                    StreamState.ACTIVE,
                    StorageProfile.OBJECT_WAL_SYNC_OBJECT,
                    Map.of(
                            ManagedLedgerProjectionNames.PAYLOAD_MAPPING_ATTRIBUTE,
                            ManagedLedgerProjectionNames.PAYLOAD_MAPPING_V1),
                    100,
                    0,
                    committedEndOffset,
                    cumulativeSize,
                    trimOffset);
        }
    }

    static CursorStorageConfig configWithRecordCap(int recordCap) {
        CursorStorageConfig source = CursorStorageConfig.defaults();
        return new CursorStorageConfig(
                source.cursorMetadataValueMaxBytes(),
                source.cursorMetadataSafetyMarginBytes(),
                source.cursorInlineAckMaxBytes(),
                source.cursorInlineDeltaMaxCount(),
                source.cursorNameMaxUtf8Bytes(),
                source.cursorPositionPropertiesMaxBytes(),
                source.cursorPropertiesMaxBytes(),
                source.cursorSnapshotMaxBytes(),
                source.cursorAckPositionsPerRequestMax(),
                source.cursorBatchIndexesMax(),
                source.cursorProtectionIntentMaxBytes(),
                source.cursorTrimReasonMaxUtf8Bytes(),
                Math.min(source.cursorScanPageSize(), recordCap),
                recordCap,
                source.cursorOwnerClaimConcurrency(),
                source.cursorMutationQueueMax(),
                source.cursorMaxCasAttempts(),
                source.cursorHydrationMaxAttempts(),
                source.cursorSnapshotIdMaxAttempts(),
                source.cursorMetadataOperationTimeout(),
                source.cursorSnapshotOperationTimeout());
    }

    static final class TestStreamStorage implements StreamStorage {
        private final AtomicReference<StreamMetadata> metadata;
        private final ArrayList<TrimCall> trims = new ArrayList<>();
        private final AtomicReference<Throwable> failBeforeNextTrim = new AtomicReference<>();
        private final AtomicReference<Throwable> failAfterNextTrim = new AtomicReference<>();

        TestStreamStorage(StreamMetadata metadata) {
            this.metadata = new AtomicReference<>(metadata);
        }

        void setCommittedEnd(long committedEndOffset) {
            metadata.updateAndGet(current -> new StreamMetadata(
                    current.streamId(),
                    current.streamName(),
                    current.state(),
                    current.profile(),
                    current.attributes(),
                    current.createdAtMillis(),
                    current.metadataVersion() + 1,
                    committedEndOffset,
                    Math.max(current.cumulativeSize(), committedEndOffset * 100),
                    current.trimOffset()));
        }

        synchronized java.util.List<TrimCall> trims() {
            return java.util.List.copyOf(trims);
        }

        void failNextTrimBeforeApply(Throwable failure) {
            if (!failBeforeNextTrim.compareAndSet(null, failure)) {
                throw new IllegalStateException("a before-apply trim failure is already armed");
            }
        }

        void failNextTrimAfterApply(Throwable failure) {
            if (!failAfterNextTrim.compareAndSet(null, failure)) {
                throw new IllegalStateException("an after-apply trim failure is already armed");
            }
        }

        @Override
        public CompletableFuture<Void> trim(
                StreamId streamId, long beforeOffset, TrimOptions options) {
            StreamMetadata current = metadata.get();
            if (!current.streamId().equals(streamId)
                    || beforeOffset < current.trimOffset()
                    || beforeOffset > current.committedEndOffset()) {
                return CompletableFuture.failedFuture(new IllegalArgumentException("invalid trim"));
            }
            synchronized (this) {
                trims.add(new TrimCall(beforeOffset, options.reason()));
            }
            Throwable beforeFailure = failBeforeNextTrim.getAndSet(null);
            if (beforeFailure != null) {
                return CompletableFuture.failedFuture(beforeFailure);
            }
            metadata.updateAndGet(value -> new StreamMetadata(
                    value.streamId(),
                    value.streamName(),
                    value.state(),
                    value.profile(),
                    value.attributes(),
                    value.createdAtMillis(),
                    value.metadataVersion() + 1,
                    value.committedEndOffset(),
                    value.cumulativeSize(),
                    beforeOffset));
            Throwable afterFailure = failAfterNextTrim.getAndSet(null);
            if (afterFailure != null) {
                return CompletableFuture.failedFuture(afterFailure);
            }
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<StreamMetadata> getStreamMetadata(StreamId streamId) {
            StreamMetadata current = metadata.get();
            return current.streamId().equals(streamId)
                    ? CompletableFuture.completedFuture(current)
                    : CompletableFuture.failedFuture(new IllegalArgumentException("unknown stream"));
        }

        @Override
        public CompletableFuture<StreamMetadata> createOrGetStream(
                StreamName streamName, StreamCreateOptions options) {
            return unsupported();
        }

        @Override
        public CompletableFuture<AppendSession> acquireAppendSession(
                StreamId streamId, AppendSessionOptions options) {
            return unsupported();
        }

        @Override
        public CompletableFuture<AppendResult> append(
                StreamId streamId, AppendBatch batch, AppendOptions options) {
            return unsupported();
        }

        @Override
        public CompletableFuture<AppendResult> recoverAppend(
                StreamId streamId,
                AppendAttemptId attemptId,
                AppendRecoveryOptions options) {
            return unsupported();
        }

        @Override
        public CompletableFuture<ReadResult> read(
                StreamId streamId, long startOffset, ReadOptions options) {
            return unsupported();
        }

        @Override
        public CompletableFuture<ResolveResult> resolve(
                StreamId streamId, long startOffset, ResolveOptions options) {
            return unsupported();
        }

        @Override
        public CompletableFuture<StreamMetadata> seal(StreamId streamId, SealOptions options) {
            return unsupported();
        }

        @Override
        public CompletableFuture<StreamMetadata> delete(StreamId streamId, DeleteOptions options) {
            return unsupported();
        }

        @Override
        public void close() {
        }

        private static <T> CompletableFuture<T> unsupported() {
            return CompletableFuture.failedFuture(new UnsupportedOperationException("not used by cursor tests"));
        }
    }

    record TrimCall(long offset, String reason) {
    }

    enum MetadataOperation {
        CREATE_CURSOR,
        CAS_CURSOR,
        CREATE_RETENTION,
        CAS_RETENTION
    }

    enum FaultCut {
        BEFORE,
        AFTER
    }

    static final class ControllableCursorMetadataStore implements CursorMetadataStore {
        private final CursorMetadataStore delegate;
        private final List<FaultRule> faults = Collections.synchronizedList(new ArrayList<>());
        private final AtomicReference<CursorCasGate> cursorCasGate = new AtomicReference<>();

        ControllableCursorMetadataStore(CursorMetadataStore delegate) {
            this.delegate = delegate;
        }

        void barrierNextCursorCas(int parties) {
            if (parties < 2) {
                throw new IllegalArgumentException("cursor CAS barrier requires at least two parties");
            }
            if (!cursorCasGate.compareAndSet(null, new CursorCasGate(parties))) {
                throw new IllegalStateException("a cursor CAS barrier is already armed");
            }
        }

        void failNext(
                MetadataOperation operation,
                FaultCut cut,
                Predicate<Object> predicate,
                Throwable failure) {
            faults.add(new FaultRule(operation, cut, predicate, failure));
        }

        void failNext(MetadataOperation operation, FaultCut cut, Throwable failure) {
            failNext(operation, cut, ignored -> true, failure);
        }

        @Override
        public CompletableFuture<Optional<VersionedCursorState>> getCursor(
                String cluster, StreamId streamId, String cursorName) {
            return delegate.getCursor(cluster, streamId, cursorName);
        }

        @Override
        public CompletableFuture<VersionedCursorState> createCursor(
                String cluster, CursorStateRecord value) {
            return invoke(
                    MetadataOperation.CREATE_CURSOR,
                    value,
                    () -> delegate.createCursor(cluster, value));
        }

        @Override
        public CompletableFuture<VersionedCursorState> compareAndSetCursor(
                String cluster, CursorStateRecord value, long expectedMetadataVersion) {
            try {
                awaitCursorCasGate();
            } catch (Throwable error) {
                return CompletableFuture.failedFuture(error);
            }
            return invoke(
                    MetadataOperation.CAS_CURSOR,
                    value,
                    () -> delegate.compareAndSetCursor(
                            cluster, value, expectedMetadataVersion));
        }

        @Override
        public CompletableFuture<CursorScanPage> scanCursors(
                String cluster,
                StreamId streamId,
                Optional<CursorScanToken> continuation,
                int pageSize) {
            return delegate.scanCursors(cluster, streamId, continuation, pageSize);
        }

        @Override
        public CompletableFuture<Optional<VersionedCursorRetention>> getRetention(
                String cluster, StreamId streamId) {
            return delegate.getRetention(cluster, streamId);
        }

        @Override
        public CompletableFuture<VersionedCursorRetention> createRetention(
                String cluster, CursorRetentionRecord value) {
            return invoke(
                    MetadataOperation.CREATE_RETENTION,
                    value,
                    () -> delegate.createRetention(cluster, value));
        }

        @Override
        public CompletableFuture<VersionedCursorRetention> compareAndSetRetention(
                String cluster,
                CursorRetentionRecord value,
                long expectedMetadataVersion) {
            return invoke(
                    MetadataOperation.CAS_RETENTION,
                    value,
                    () -> delegate.compareAndSetRetention(
                            cluster, value, expectedMetadataVersion));
        }

        @Override
        public WatchRegistration watchStreamCursors(
                String cluster, StreamId streamId, Runnable invalidation) {
            return delegate.watchStreamCursors(cluster, streamId, invalidation);
        }

        @Override
        public void close() {
            delegate.close();
        }

        private <T> CompletableFuture<T> invoke(
                MetadataOperation operation,
                Object value,
                Supplier<CompletableFuture<T>> action) {
            FaultRule fault = takeFault(operation, value);
            if (fault != null && fault.cut() == FaultCut.BEFORE) {
                return CompletableFuture.failedFuture(fault.failure());
            }
            final CompletableFuture<T> result;
            try {
                result = action.get();
            } catch (Throwable error) {
                return CompletableFuture.failedFuture(error);
            }
            if (fault == null || fault.cut() != FaultCut.AFTER) {
                return result;
            }
            return result.thenCompose(ignored -> CompletableFuture.failedFuture(fault.failure()));
        }

        private FaultRule takeFault(MetadataOperation operation, Object value) {
            synchronized (faults) {
                for (int index = 0; index < faults.size(); index++) {
                    FaultRule candidate = faults.get(index);
                    if (candidate.operation() == operation
                            && candidate.predicate().test(value)) {
                        faults.remove(index);
                        return candidate;
                    }
                }
            }
            return null;
        }

        private void awaitCursorCasGate()
                throws InterruptedException, BrokenBarrierException, TimeoutException {
            CursorCasGate gate = cursorCasGate.get();
            if (gate == null || !gate.tryEnter()) {
                return;
            }
            try {
                gate.barrier().await(10, TimeUnit.SECONDS);
            } finally {
                if (gate.remaining().get() == 0) {
                    cursorCasGate.compareAndSet(gate, null);
                }
            }
        }
    }

    private record FaultRule(
            MetadataOperation operation,
            FaultCut cut,
            Predicate<Object> predicate,
            Throwable failure) {
    }

    private record CursorCasGate(CyclicBarrier barrier, AtomicInteger remaining) {
        CursorCasGate(int parties) {
            this(new CyclicBarrier(parties), new AtomicInteger(parties));
        }

        boolean tryEnter() {
            while (true) {
                int current = remaining.get();
                if (current == 0) {
                    return false;
                }
                if (remaining.compareAndSet(current, current - 1)) {
                    return true;
                }
            }
        }
    }

    static final class InMemorySnapshotStore implements CursorSnapshotStore {
        private final CursorStorageConfig config;
        private final AtomicInteger ids = new AtomicInteger();
        private final ConcurrentHashMap<ObjectKey, byte[]> objects = new ConcurrentHashMap<>();
        private final AtomicReference<Throwable> nextReadFailure = new AtomicReference<>();

        InMemorySnapshotStore(CursorStorageConfig config) {
            this.config = config;
        }

        @Override
        public CompletableFuture<CursorSnapshotReference> write(CursorSnapshotWriteRequest request) {
            String snapshotId = String.format("%032x", ids.incrementAndGet());
            CursorSnapshotCodecV1.EncodedSnapshot encoded = CursorSnapshotCodecV1.encode(
                    request, snapshotId, config);
            ObjectKey key = new ObjectKey("cursor-test-snapshot/" + snapshotId + ".ncs");
            byte[] bytes = new byte[encoded.payload().remaining()];
            encoded.payload().get(bytes);
            objects.put(key, bytes);
            return CompletableFuture.completedFuture(new CursorSnapshotReference(
                    key,
                    snapshotId,
                    request.identity().cursorGeneration(),
                    request.sourceMutationSequence(),
                    request.fullState().markDeleteOffset(),
                    bytes.length,
                    encoded.storageChecksum(),
                    encoded.formatCrc32c(),
                    1,
                    request.createdAtMillis()));
        }

        @Override
        public CompletableFuture<CursorAckState> read(
                CursorSnapshotReference reference, CursorIdentity expectedIdentity) {
            Throwable injectedFailure = nextReadFailure.getAndSet(null);
            if (injectedFailure != null) {
                return CompletableFuture.failedFuture(injectedFailure);
            }
            byte[] bytes = objects.get(reference.objectKey());
            if (bytes == null) {
                return CompletableFuture.failedFuture(new NereusException(
                        ErrorCode.OBJECT_NOT_FOUND, true, "snapshot missing"));
            }
            try {
                return CompletableFuture.completedFuture(CursorSnapshotCodecV1.decode(
                        ByteBuffer.wrap(bytes), reference, expectedIdentity, config));
            } catch (Throwable error) {
                return CompletableFuture.failedFuture(error);
            }
        }

        int objectCount() {
            return objects.size();
        }

        void remove(ObjectKey key) {
            objects.remove(key);
        }

        void failNextRead(Throwable failure) {
            if (!nextReadFailure.compareAndSet(null, failure)) {
                throw new IllegalStateException("a snapshot read failure is already pending");
            }
        }

        void corrupt(ObjectKey key) {
            objects.compute(key, (ignored, bytes) -> {
                if (bytes == null || bytes.length == 0) {
                    throw new IllegalStateException("snapshot is unavailable for corruption");
                }
                byte[] corrupted = bytes.clone();
                corrupted[corrupted.length - 1] ^= 0x01;
                return corrupted;
            });
        }

        @Override
        public void close() {
        }
    }
}
