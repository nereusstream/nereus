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

package com.nereusstream.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.AppendBatch;
import com.nereusstream.api.AppendEntry;
import com.nereusstream.api.AppendOptions;
import com.nereusstream.api.AppendOutcome;
import com.nereusstream.api.AppendResult;
import com.nereusstream.api.AppendSession;
import com.nereusstream.api.AppendSessionOptions;
import com.nereusstream.api.DurabilityLevel;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamCreateOptions;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamMetadata;
import com.nereusstream.api.StreamName;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.testing.FakeOxiaMetadataStore;
import com.nereusstream.objectstore.testing.LocalFileObjectStore;
import com.nereusstream.objectstore.wal.DefaultWalObjectReader;
import com.nereusstream.objectstore.wal.DefaultWalObjectWriter;
import com.nereusstream.objectstore.wal.PreparedWalObject;
import com.nereusstream.objectstore.wal.WalObjectWriter;
import com.nereusstream.objectstore.wal.WalWriteRequest;
import com.nereusstream.objectstore.wal.WalWriteResult;
import com.nereusstream.objectstore.wal.WrittenStreamSlice;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DefaultStreamStorageAppendTest {
    private static final Instant NOW = Instant.parse("2026-07-11T03:04:05Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);

    @TempDir
    Path root;

    @Test
    void appendBuildsManifestAndAdvancesDenseOffsets() {
        try (TestContext context = context(StorageProfile.OBJECT_WAL_SYNC_OBJECT, Runnable::run)) {
            StreamMetadata stream = context.createStream("orders");

            AppendResult first = context.storage.append(
                    stream.streamId(), batch("a", "bb"), appendOptions(Duration.ofSeconds(5))).join();
            AppendResult second = context.storage.append(
                    stream.streamId(), batch("ccc"), appendOptions(Duration.ofSeconds(5))).join();

            assertThat(first.range()).isEqualTo(new OffsetRange(0, 2));
            assertThat(second.range()).isEqualTo(new OffsetRange(2, 3));
            assertThat(first.logicalBytes()).isEqualTo(3);
            assertThat(first.objectId()).isNotEqualTo(second.objectId());
            assertThat(context.metadata.getObjectManifest("cluster/a", first.objectId()).join()).isPresent();
            assertThat(context.metadata.scanOffsetIndex("cluster/a", stream.streamId(), 0, 10).join())
                    .extracting(record -> record.offsetEnd())
                    .containsExactly(2L, 3L);
            StreamMetadata current = context.storage.getStreamMetadata(stream.streamId()).join();
            assertThat(current.committedEndOffset()).isEqualTo(3);
            assertThat(current.cumulativeSize()).isEqualTo(6);
        }
    }

    @Test
    void concurrentSameStreamAppendsAreSequenced() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try (TestContext context = context(StorageProfile.OBJECT_WAL_SYNC_OBJECT, executor)) {
            StreamId streamId = context.createStream("sequenced").streamId();
            List<CompletableFuture<AppendResult>> appends = java.util.stream.IntStream.range(0, 20)
                    .mapToObj(index -> context.storage.append(
                            streamId,
                            batch("value-" + index),
                            appendOptions(Duration.ofSeconds(10))))
                    .toList();
            CompletableFuture.allOf(appends.toArray(CompletableFuture[]::new)).get(10, TimeUnit.SECONDS);

            assertThat(appends)
                    .extracting(future -> future.join().range().startOffset())
                    .containsExactlyInAnyOrderElementsOf(java.util.stream.LongStream.range(0, 20).boxed().toList());
            assertThat(context.metadata.getCommittedEndOffset("cluster/a", streamId).join().committedEndOffset())
                    .isEqualTo(20);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void unsupportedProfileAndDurabilityAreRejectedBeforeWalPreparation() {
        CountingWriter writer = new CountingWriter(newWriter(new LocalFileObjectStore(root)));
        try (TestContext context = context(StorageProfile.BOOKKEEPER_WAL_ONLY, Runnable::run, writer)) {
            StreamId streamId = context.createStream("bk").streamId();
            NereusException profileFailure = appendFailure(context.storage.append(
                    streamId, batch("a"), appendOptions(Duration.ofSeconds(5))));
            assertThat(profileFailure.code()).isEqualTo(ErrorCode.UNSUPPORTED_STORAGE_PROFILE);
            assertThat(profileFailure.appendOutcome()).contains(AppendOutcome.KNOWN_NOT_COMMITTED);
            assertThat(writer.prepareCount).hasValue(0);
        }

        CountingWriter durabilityWriter = new CountingWriter(newWriter(new LocalFileObjectStore(root.resolve("d"))));
        try (TestContext context = context(
                StorageProfile.OBJECT_WAL_SYNC_OBJECT, Runnable::run, durabilityWriter)) {
            StreamId streamId = context.createStream("durability").streamId();
            AppendOptions options = new AppendOptions(
                    Optional.empty(), DurabilityLevel.WAL_DURABLE, Duration.ofSeconds(5), true, Map.of());
            NereusException durabilityFailure = appendFailure(
                    context.storage.append(streamId, batch("a"), options));
            assertThat(durabilityFailure.code()).isEqualTo(ErrorCode.UNSUPPORTED_DURABILITY_LEVEL);
            assertThat(durabilityWriter.prepareCount).hasValue(0);
        }
    }

    @Test
    void uploadTimeoutAndCancellationNeverStartManifestCommit() throws Exception {
        BlockingUploadWriter timeoutWriter = new BlockingUploadWriter(
                newWriter(new LocalFileObjectStore(root.resolve("timeout"))));
        try (TestContext context = context(
                StorageProfile.OBJECT_WAL_SYNC_OBJECT, Runnable::run, timeoutWriter)) {
            StreamId streamId = context.createStream("timeout").streamId();
            CompletableFuture<AppendResult> append = context.storage.append(
                    streamId, batch("a"), appendOptions(Duration.ofSeconds(2)));
            assertThat(timeoutWriter.uploadStarted.await(5, TimeUnit.SECONDS)).isTrue();

            NereusException failure = appendFailure(append);
            assertThat(failure.code()).isEqualTo(ErrorCode.TIMEOUT);
            assertThat(failure.appendOutcome()).contains(AppendOutcome.KNOWN_NOT_COMMITTED);
            PreparedWalObject prepared = timeoutWriter.prepared;
            assertThat(prepared).isNotNull();
            assertThat(context.metadata.getObjectManifest(
                    "cluster/a", prepared.result().objectId()).join()).isEmpty();
        }

        BlockingUploadWriter cancelledWriter = new BlockingUploadWriter(
                newWriter(new LocalFileObjectStore(root.resolve("cancel"))));
        try (TestContext context = context(
                StorageProfile.OBJECT_WAL_SYNC_OBJECT, Runnable::run, cancelledWriter)) {
            StreamId streamId = context.createStream("cancel").streamId();
            CompletableFuture<AppendResult> append = context.storage.append(
                    streamId, batch("a"), appendOptions(Duration.ofSeconds(5)));
            assertThat(cancelledWriter.uploadStarted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(append.cancel(false)).isTrue();
            NereusException failure = appendFailure(append);
            assertThat(failure.code()).isEqualTo(ErrorCode.CANCELLED);
            assertThat(failure.appendOutcome()).contains(AppendOutcome.KNOWN_NOT_COMMITTED);
            assertThat(context.metadata.getObjectManifest(
                    "cluster/a", cancelledWriter.prepared.result().objectId()).join()).isEmpty();
        }
    }

    @Test
    void unconfirmedCommitResponseIsMayHaveCommittedAndSuspendsLane() {
        FakeOxiaMetadataStore fake = new FakeOxiaMetadataStore(CLOCK::millis);
        OxiaMetadataStore delayedCommit = delayMethod(fake, "commitStreamSlice");
        LocalFileObjectStore objectStore = new LocalFileObjectStore(root);
        try (TestContext context = context(
                StorageProfile.OBJECT_WAL_SYNC_OBJECT,
                Runnable::run,
                newWriter(objectStore),
                fake,
                delayedCommit,
                objectStore)) {
            StreamId streamId = context.createStream("uncertain").streamId();
            NereusException failure = appendFailure(context.storage.append(
                    streamId, batch("a"), appendOptions(Duration.ofMillis(40))));
            assertThat(failure.code()).isEqualTo(ErrorCode.TIMEOUT);
            assertThat(failure.appendOutcome()).contains(AppendOutcome.MAY_HAVE_COMMITTED);

            NereusException suspended = appendFailure(context.storage.append(
                    streamId, batch("b"), appendOptions(Duration.ofSeconds(1))));
            assertThat(suspended.code()).isEqualTo(ErrorCode.METADATA_UNAVAILABLE);
            assertThat(suspended.appendOutcome()).contains(AppendOutcome.MAY_HAVE_COMMITTED);
        }
    }

    @Test
    void knownCommittedMaterializationFailureSuspendsLaneWithoutLosingHeadAdvance() {
        try (TestContext context = context(StorageProfile.OBJECT_WAL_SYNC_OBJECT, Runnable::run)) {
            StreamId streamId = context.createStream("known").streamId();
            context.metadata.failNext(FakeOxiaMetadataStore.FailurePoint.AFTER_HEAD_CAS_BEFORE_DERIVED_INDEX);

            NereusException failure = appendFailure(context.storage.append(
                    streamId, batch("a"), appendOptions(Duration.ofSeconds(5))));
            assertThat(failure.appendOutcome()).contains(AppendOutcome.KNOWN_COMMITTED);
            assertThat(context.metadata.getCommittedEndOffset("cluster/a", streamId).join().committedEndOffset())
                    .isEqualTo(1);
            assertThat(context.metadata.scanOffsetIndex("cluster/a", streamId, 0, 10).join()).isEmpty();

            NereusException suspended = appendFailure(context.storage.append(
                    streamId, batch("b"), appendOptions(Duration.ofSeconds(1))));
            assertThat(suspended.code()).isEqualTo(ErrorCode.METADATA_UNAVAILABLE);
            assertThat(suspended.appendOutcome()).contains(AppendOutcome.KNOWN_COMMITTED);
        }
    }

    @Test
    void closeRejectsNewAppendWithStructuredOutcome() {
        TestContext context = context(StorageProfile.OBJECT_WAL_SYNC_OBJECT, Runnable::run);
        StreamId streamId = context.createStream("closed").streamId();
        context.storage.close();

        NereusException failure = appendFailure(context.storage.append(
                streamId, batch("a"), appendOptions(Duration.ofSeconds(1))));
        assertThat(failure.code()).isEqualTo(ErrorCode.STORAGE_CLOSED);
        assertThat(failure.appendOutcome()).contains(AppendOutcome.KNOWN_NOT_COMMITTED);
        context.close();
    }

    @Test
    void inFlightBackpressureRejectsBeforePrepareAndReleasesAfterCancellation() throws Exception {
        LocalFileObjectStore objectStore = new LocalFileObjectStore(root);
        FakeOxiaMetadataStore metadata = new FakeOxiaMetadataStore(CLOCK::millis);
        BlockingUploadWriter writer = new BlockingUploadWriter(newWriter(objectStore));
        StreamStorageConfig oneInFlight = config(1 << 20, 4L << 20, 4L << 20, 1);
        try (DefaultStreamStorage storage = new DefaultStreamStorage(
                oneInFlight,
                metadata,
                writer,
                new DefaultWalObjectReader(objectStore),
                CLOCK,
                Runnable::run)) {
            StreamId streamId = storage.createOrGetStream(
                    new StreamName("backpressure"),
                    new StreamCreateOptions(StorageProfile.OBJECT_WAL_SYNC_OBJECT, Map.of())).join().streamId();
            CompletableFuture<AppendResult> first = storage.append(
                    streamId, batch("a"), appendOptions(Duration.ofSeconds(5)));
            assertThat(writer.uploadStarted.await(5, TimeUnit.SECONDS)).isTrue();

            NereusException rejected = appendFailure(storage.append(
                    streamId, batch("b"), appendOptions(Duration.ofSeconds(1))));
            assertThat(rejected.code()).isEqualTo(ErrorCode.BACKPRESSURE_REJECTED);
            assertThat(rejected.appendOutcome()).contains(AppendOutcome.KNOWN_NOT_COMMITTED);

            first.cancel(false);
            assertThat(appendFailure(first).code()).isEqualTo(ErrorCode.CANCELLED);
        } finally {
            metadata.close();
            objectStore.close();
        }
    }

    @Test
    void nearExpirySuppliedSessionIsRenewedBeforeWalUpload() {
        try (TestContext context = context(StorageProfile.OBJECT_WAL_SYNC_OBJECT, Runnable::run)) {
            StreamId streamId = context.createStream("renew").streamId();
            AppendSession session = context.storage.acquireAppendSession(
                    streamId,
                    new AppendSessionOptions("writer-a", Duration.ofSeconds(1), true)).join();
            long headWritesBeforeAppend = headConditionalWrites(context.metadata);
            AppendOptions supplied = new AppendOptions(
                    Optional.of(session),
                    DurabilityLevel.WAL_DURABLE_AND_INDEX_COMMITTED,
                    Duration.ofSeconds(5),
                    false,
                    Map.of());

            AppendResult result = context.storage.append(streamId, batch("a"), supplied).join();

            assertThat(result.range()).isEqualTo(new OffsetRange(0, 1));
            assertThat(headConditionalWrites(context.metadata) - headWritesBeforeAppend).isEqualTo(2);
        }
    }

    @Test
    void disabledAutoAcquireRequiresAnExplicitSessionEvenWhenCacheIsWarm() {
        try (TestContext context = context(StorageProfile.OBJECT_WAL_SYNC_OBJECT, Runnable::run)) {
            StreamId streamId = context.createStream("explicit-session").streamId();
            context.storage.acquireAppendSession(
                    streamId,
                    new AppendSessionOptions("writer-a", Duration.ofSeconds(30), true)).join();
            AppendOptions noExplicitSession = new AppendOptions(
                    Optional.empty(),
                    DurabilityLevel.WAL_DURABLE_AND_INDEX_COMMITTED,
                    Duration.ofSeconds(1),
                    false,
                    Map.of());

            NereusException failure = appendFailure(
                    context.storage.append(streamId, batch("a"), noExplicitSession));

            assertThat(failure.code()).isEqualTo(ErrorCode.APPEND_SESSION_EXPIRED);
            assertThat(failure.appendOutcome()).contains(AppendOutcome.KNOWN_NOT_COMMITTED);
        }
    }

    @Test
    void mismatchedPreparedSliceIsRejectedBeforeUpload() {
        LocalFileObjectStore objectStore = new LocalFileObjectStore(root);
        FakeOxiaMetadataStore metadata = new FakeOxiaMetadataStore(CLOCK::millis);
        MismatchingWriter writer = new MismatchingWriter(newWriter(objectStore));
        try (TestContext context = context(
                StorageProfile.OBJECT_WAL_SYNC_OBJECT,
                Runnable::run,
                writer,
                metadata,
                metadata,
                objectStore)) {
            StreamId streamId = context.createStream("bad-writer").streamId();

            NereusException failure = appendFailure(context.storage.append(
                    streamId, batch("a"), appendOptions(Duration.ofSeconds(1))));

            assertThat(failure.code()).isEqualTo(ErrorCode.METADATA_INVARIANT_VIOLATION);
            assertThat(failure.appendOutcome()).contains(AppendOutcome.KNOWN_NOT_COMMITTED);
            assertThat(writer.uploadCount).hasValue(0);
        }
    }

    @Test
    void externalOffsetConflictInvalidatesLaneOffsetWithoutRebasingUploadedSlice() throws Exception {
        LocalFileObjectStore objectStore = new LocalFileObjectStore(root);
        FakeOxiaMetadataStore metadata = new FakeOxiaMetadataStore(CLOCK::millis);
        GatedUploadWriter gatedWriter = new GatedUploadWriter(newWriter(objectStore));
        DefaultStreamStorage first = new DefaultStreamStorage(
                config(1 << 20, 4L << 20, 4L << 20),
                metadata,
                gatedWriter,
                new DefaultWalObjectReader(objectStore),
                CLOCK,
                Runnable::run);
        DefaultStreamStorage second = new DefaultStreamStorage(
                config(1 << 20, 4L << 20, 4L << 20),
                metadata,
                newWriter(objectStore),
                new DefaultWalObjectReader(objectStore),
                CLOCK,
                Runnable::run);
        try {
            StreamId streamId = first.createOrGetStream(
                    new StreamName("offset-race"),
                    new StreamCreateOptions(StorageProfile.OBJECT_WAL_SYNC_OBJECT, Map.of())).join().streamId();
            CompletableFuture<AppendResult> losing = first.append(
                    streamId, batch("losing"), appendOptions(Duration.ofSeconds(5)));
            assertThat(gatedWriter.uploadStarted.await(5, TimeUnit.SECONDS)).isTrue();
            AppendResult winner = second.append(
                    streamId, batch("winner"), appendOptions(Duration.ofSeconds(5))).join();
            assertThat(winner.range()).isEqualTo(new OffsetRange(0, 1));

            gatedWriter.release();
            NereusException conflict = appendFailure(losing);
            assertThat(conflict.code()).isEqualTo(ErrorCode.OFFSET_CONFLICT);
            assertThat(conflict.appendOutcome()).contains(AppendOutcome.KNOWN_NOT_COMMITTED);

            AppendResult recovered = first.append(
                    streamId, batch("next"), appendOptions(Duration.ofSeconds(5))).join();
            assertThat(recovered.range()).isEqualTo(new OffsetRange(1, 2));
        } finally {
            first.close();
            second.close();
            metadata.close();
            objectStore.close();
        }
    }

    @Test
    void configRejectsAnObjectLargerThanAppendOrReadMemoryBudgets() {
        assertThatThrownBy(() -> config(1024, 512, 2048))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxObjectBytes must be <= maxBufferedBytes");
        assertThatThrownBy(() -> config(1024, 2048, 512))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxObjectBytes must be <= maxReadBufferBytes");
    }

    private TestContext context(StorageProfile profile, java.util.concurrent.Executor executor) {
        LocalFileObjectStore objectStore = new LocalFileObjectStore(root);
        FakeOxiaMetadataStore metadata = new FakeOxiaMetadataStore(CLOCK::millis);
        return context(profile, executor, newWriter(objectStore), metadata, metadata, objectStore);
    }

    private TestContext context(
            StorageProfile profile,
            java.util.concurrent.Executor executor,
            WalObjectWriter writer) {
        LocalFileObjectStore objectStore = new LocalFileObjectStore(root.resolve("store-" + profile.name()));
        FakeOxiaMetadataStore metadata = new FakeOxiaMetadataStore(CLOCK::millis);
        return context(profile, executor, writer, metadata, metadata, objectStore);
    }

    private TestContext context(
            StorageProfile profile,
            java.util.concurrent.Executor executor,
            WalObjectWriter writer,
            FakeOxiaMetadataStore metadata,
            OxiaMetadataStore storageMetadata,
            LocalFileObjectStore objectStore) {
        DefaultStreamStorage storage = new DefaultStreamStorage(
                config(1 << 20, 4L << 20, 4L << 20),
                storageMetadata,
                writer,
                new DefaultWalObjectReader(objectStore),
                CLOCK,
                executor);
        return new TestContext(storage, metadata, objectStore, profile);
    }

    private static StreamStorageConfig config(
            int maxObjectBytes,
            long maxBufferedBytes,
            long maxReadBufferBytes) {
        return config(maxObjectBytes, maxBufferedBytes, maxReadBufferBytes, 64);
    }

    private static StreamStorageConfig config(
            int maxObjectBytes,
            long maxBufferedBytes,
            long maxReadBufferBytes,
            int maxInFlightAppends) {
        return new StreamStorageConfig(
                "cluster/a",
                "writer-a",
                Duration.ofSeconds(30),
                Duration.ofSeconds(10),
                Duration.ofSeconds(5),
                Duration.ofSeconds(10),
                Duration.ofSeconds(10),
                Duration.ofSeconds(2),
                64,
                10_000,
                256,
                1_000,
                maxInFlightAppends,
                maxBufferedBytes,
                16,
                maxReadBufferBytes,
                maxObjectBytes,
                10_000,
                Duration.ofSeconds(5),
                true,
                false,
                true);
    }

    private static DefaultWalObjectWriter newWriter(LocalFileObjectStore store) {
        return new DefaultWalObjectWriter(store, "test-writer-1", CLOCK);
    }

    private static AppendBatch batch(String... values) {
        List<AppendEntry> entries = java.util.Arrays.stream(values)
                .map(value -> new AppendEntry(
                        value.getBytes(StandardCharsets.UTF_8), 1, NOW.toEpochMilli(), Map.of()))
                .toList();
        return new AppendBatch(
                PayloadFormat.OPAQUE_RECORD_BATCH,
                entries,
                entries.size(),
                entries.size(),
                NOW.toEpochMilli(),
                NOW.toEpochMilli(),
                List.of(),
                Map.of(),
                Optional.empty());
    }

    private static AppendOptions appendOptions(Duration timeout) {
        return new AppendOptions(
                Optional.empty(),
                DurabilityLevel.WAL_DURABLE_AND_INDEX_COMMITTED,
                timeout,
                true,
                Map.of());
    }

    private static NereusException appendFailure(CompletableFuture<?> future) {
        try {
            future.join();
            throw new AssertionError("future completed successfully");
        } catch (CompletionException e) {
            assertThat(e.getCause()).isInstanceOf(NereusException.class);
            return (NereusException) e.getCause();
        }
    }

    private static long headConditionalWrites(FakeOxiaMetadataStore metadata) {
        return metadata.accessLog().stream()
                .filter(access -> access.operation().equals("putIfVersion"))
                .count();
    }

    private static OxiaMetadataStore delayMethod(OxiaMetadataStore delegate, String methodName) {
        return (OxiaMetadataStore) Proxy.newProxyInstance(
                OxiaMetadataStore.class.getClassLoader(),
                new Class<?>[] {OxiaMetadataStore.class},
                (proxy, method, args) -> {
                    if (method.getName().equals(methodName)) {
                        return new CompletableFuture<>();
                    }
                    try {
                        return method.invoke(delegate, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    private static final class CountingWriter implements WalObjectWriter {
        private final WalObjectWriter delegate;
        private final AtomicInteger prepareCount = new AtomicInteger();

        private CountingWriter(WalObjectWriter delegate) {
            this.delegate = delegate;
        }

        @Override
        public PreparedWalObject prepare(WalWriteRequest request) {
            prepareCount.incrementAndGet();
            return delegate.prepare(request);
        }

        @Override
        public CompletableFuture<WalWriteResult> upload(PreparedWalObject preparedObject) {
            return delegate.upload(preparedObject);
        }
    }

    private static final class BlockingUploadWriter implements WalObjectWriter {
        private final WalObjectWriter delegate;
        private final CountDownLatch uploadStarted = new CountDownLatch(1);
        private volatile PreparedWalObject prepared;

        private BlockingUploadWriter(WalObjectWriter delegate) {
            this.delegate = delegate;
        }

        @Override
        public PreparedWalObject prepare(WalWriteRequest request) {
            prepared = delegate.prepare(request);
            return prepared;
        }

        @Override
        public CompletableFuture<WalWriteResult> upload(PreparedWalObject preparedObject) {
            uploadStarted.countDown();
            return new CompletableFuture<>();
        }
    }

    private static final class MismatchingWriter implements WalObjectWriter {
        private final WalObjectWriter delegate;
        private final AtomicInteger uploadCount = new AtomicInteger();

        private MismatchingWriter(WalObjectWriter delegate) {
            this.delegate = delegate;
        }

        @Override
        public PreparedWalObject prepare(WalWriteRequest request) {
            PreparedWalObject prepared = delegate.prepare(request);
            WalWriteResult original = prepared.result();
            WrittenStreamSlice slice = original.slices().getFirst();
            WrittenStreamSlice mismatched = new WrittenStreamSlice(
                    slice.streamId(),
                    slice.sliceId(),
                    slice.objectOffset(),
                    slice.objectLength(),
                    slice.recordCount(),
                    slice.entryCount(),
                    slice.logicalBytes() + 1,
                    slice.schemaRefs(),
                    slice.payloadFormat(),
                    slice.entryIndexRef(),
                    slice.sliceChecksum(),
                    slice.minEventTimeMillis(),
                    slice.maxEventTimeMillis());
            WalWriteResult badResult = new WalWriteResult(
                    original.objectId(),
                    original.objectKey(),
                    original.objectLength(),
                    original.objectChecksum(),
                    original.storageChecksum(),
                    original.formatMajorVersion(),
                    original.formatMinorVersion(),
                    original.writerVersion(),
                    original.createdAtMillis(),
                    List.of(mismatched));
            java.nio.ByteBuffer payload = prepared.payload();
            byte[] bytes = new byte[payload.remaining()];
            payload.get(bytes);
            return new PreparedWalObject(badResult, bytes, prepared.uploadTimeout());
        }

        @Override
        public CompletableFuture<WalWriteResult> upload(PreparedWalObject preparedObject) {
            uploadCount.incrementAndGet();
            return delegate.upload(preparedObject);
        }
    }

    private static final class GatedUploadWriter implements WalObjectWriter {
        private final WalObjectWriter delegate;
        private final CountDownLatch uploadStarted = new CountDownLatch(1);
        private final CompletableFuture<WalWriteResult> pending = new CompletableFuture<>();
        private volatile PreparedWalObject prepared;
        private volatile boolean released;

        private GatedUploadWriter(WalObjectWriter delegate) {
            this.delegate = delegate;
        }

        @Override
        public PreparedWalObject prepare(WalWriteRequest request) {
            prepared = delegate.prepare(request);
            return prepared;
        }

        @Override
        public CompletableFuture<WalWriteResult> upload(PreparedWalObject preparedObject) {
            if (released) {
                return delegate.upload(preparedObject);
            }
            uploadStarted.countDown();
            return pending;
        }

        void release() {
            released = true;
            delegate.upload(prepared).whenComplete((result, error) -> {
                if (error == null) {
                    pending.complete(result);
                } else {
                    pending.completeExceptionally(error);
                }
            });
        }
    }

    private record TestContext(
            DefaultStreamStorage storage,
            FakeOxiaMetadataStore metadata,
            LocalFileObjectStore objectStore,
            StorageProfile profile) implements AutoCloseable {
        StreamMetadata createStream(String name) {
            return storage.createOrGetStream(
                    new StreamName(name), new StreamCreateOptions(profile, Map.of())).join();
        }

        @Override
        public void close() {
            storage.close();
            metadata.close();
            objectStore.close();
        }
    }
}
