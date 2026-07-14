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
import com.nereusstream.metadata.oxia.records.TopicProjectionRecord;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

final class CursorStorageTestSupport {
    static final String CLUSTER = "cluster/a";
    static final String TOPIC = "persistent://tenant/ns/cursor-m2";
    static final String OWNER_1 = "00112233445566778899aabbccddeeff";
    static final String OWNER_2 = "ffeeddccbbaa99887766554433221100";
    static final Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC);

    private CursorStorageTestSupport() {
    }

    static final class Context implements AutoCloseable {
        final CursorStorageConfig config = CursorStorageConfig.defaults();
        final FakeManagedLedgerProjectionMetadataStore projectionStore =
                new FakeManagedLedgerProjectionMetadataStore();
        final FakeCursorMetadataStore metadataStore = new FakeCursorMetadataStore();
        final TestStreamStorage streamStorage;
        final InMemorySnapshotStore snapshotStore;
        final ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(4);
        final AtomicInteger activationCalls = new AtomicInteger();
        final AtomicInteger idSequence = new AtomicInteger();
        final TopicProjectionRecord projection;
        final CursorLedgerIdentity ledger;
        final DefaultCursorRetentionCoordinator retention;
        final DefaultCursorStorage storage;

        Context(long trimOffset, long committedEndOffset) {
            StreamId streamId = ManagedLedgerProjectionNames.streamId(TOPIC, 1);
            StreamName streamName = ManagedLedgerProjectionNames.streamName(TOPIC, 1);
            StreamMetadata empty = metadata(streamId, streamName, 0, 0, 0);
            projection = projectionStore.createFirstProjection(
                            CLUSTER,
                            new ProjectionCreateRequest(TOPIC, 1, 1, empty, Map.of()),
                            () -> CompletableFuture.completedFuture(null))
                    .join();
            ledger = new CursorLedgerIdentity(
                    TOPIC,
                    ManagedLedgerProjectionNames.managedLedgerNameHash(TOPIC),
                    projection.projectionIdentity());
            streamStorage = new TestStreamStorage(metadata(
                    streamId, streamName, trimOffset, committedEndOffset, committedEndOffset * 100));
            snapshotStore = new InMemorySnapshotStore(config);
            CursorProtocolActivationGuard guard = ignored -> {
                activationCalls.incrementAndGet();
                return CompletableFuture.completedFuture(null);
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

        @Override
        public void close() {
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

    static final class TestStreamStorage implements StreamStorage {
        private final AtomicReference<StreamMetadata> metadata;
        private final ArrayList<TrimCall> trims = new ArrayList<>();

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

    static final class InMemorySnapshotStore implements CursorSnapshotStore {
        private final CursorStorageConfig config;
        private final AtomicInteger ids = new AtomicInteger();
        private final ConcurrentHashMap<ObjectKey, byte[]> objects = new ConcurrentHashMap<>();

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
            byte[] bytes = objects.get(reference.objectKey());
            if (bytes == null) {
                return CompletableFuture.failedFuture(new IllegalStateException("snapshot missing"));
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

        @Override
        public void close() {
        }
    }
}
