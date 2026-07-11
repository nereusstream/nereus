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

import com.nereusstream.api.AppendBatch;
import com.nereusstream.api.AppendEntry;
import com.nereusstream.api.AppendOptions;
import com.nereusstream.api.AppendOutcome;
import com.nereusstream.api.AppendResult;
import com.nereusstream.api.DurabilityLevel;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ReadIsolation;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.ReadResult;
import com.nereusstream.api.ResolveOptions;
import com.nereusstream.api.ResolveResult;
import com.nereusstream.api.SchemaRef;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamCreateOptions;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamMetadata;
import com.nereusstream.api.StreamName;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.core.read.ReadMetricsObserver;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.OffsetIndexEntry;
import com.nereusstream.metadata.oxia.records.OffsetIndexRecord;
import com.nereusstream.metadata.oxia.testing.FakeOxiaMetadataStore;
import com.nereusstream.objectstore.testing.LocalFileObjectStore;
import com.nereusstream.objectstore.wal.DefaultWalObjectReader;
import com.nereusstream.objectstore.wal.DefaultWalObjectWriter;
import com.nereusstream.objectstore.wal.WalObjectReader;
import com.nereusstream.objectstore.wal.WalReadResult;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DefaultStreamStorageReadTest {
    private static final Instant NOW = Instant.parse("2026-07-11T05:06:07Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);
    private static final SchemaRef SCHEMA = new SchemaRef("test", "schema", 1);

    @TempDir
    Path root;

    @Test
    void resolveAndReadAdjacentCommittedSlicesWithExactMetadata() {
        RecordingReadMetrics metrics = new RecordingReadMetrics();
        try (TestContext context = context(defaultConfig(false), metrics)) {
            StreamId streamId = context.createStream("read-adjacent").streamId();
            AppendResult first = context.storage.append(
                    streamId, batch(List.of(SCHEMA), "a", "bb"), appendOptions()).join();
            context.storage.append(streamId, batch(List.of(), "ccc"), appendOptions()).join();

            ResolveResult resolved = context.storage.resolve(
                    streamId, 1, new ResolveOptions(10, true, true)).join();
            ReadResult read = context.storage.read(
                    streamId, 1, readOptions(10, 32, Duration.ofSeconds(5))).join();

            assertThat(resolved.ranges()).hasSize(2);
            assertThat(resolved.resolvedEndOffset()).isEqualTo(3);
            assertThat(((ObjectSliceReadTarget) resolved.ranges().getFirst().readTarget()).sliceChecksum())
                    .isEqualTo(((ObjectSliceReadTarget) first.readTarget()).sliceChecksum());
            assertThat(resolved.ranges().getFirst().schemaRefs()).containsExactly(SCHEMA);
            assertThat(read.batches()).hasSize(2);
            assertThat(read.batches()).extracting(batch -> text(batch.payload()))
                    .containsExactly("bb", "ccc");
            assertThat(read.batches()).extracting(batch -> batch.range())
                    .containsExactly(new OffsetRange(1, 2), new OffsetRange(2, 3));
            assertThat(read.nextOffset()).isEqualTo(3);
            assertThat(read.endOfStream()).isFalse();
            assertThat(read.batches().getFirst().sourceObjectLength()).isEqualTo(2);

            ReadResult eof = context.storage.read(
                    streamId, 3, readOptions(10, 32, Duration.ofSeconds(5))).join();
            assertThat(eof.batches()).isEmpty();
            assertThat(eof.nextOffset()).isEqualTo(3);
            assertThat(eof.endOfStream()).isTrue();
        }
    }

    @Test
    void byteLimitIncludesZeroByteEntriesAndRejectsAnOversizedFirstEntry() {
        try (TestContext context = context(defaultConfig(false), new RecordingReadMetrics())) {
            StreamId streamId = context.createStream("read-limits").streamId();
            context.storage.append(
                    streamId, batch(List.of(), "abc", "", "d"), appendOptions()).join();

            ReadResult limited = context.storage.read(
                    streamId, 0, readOptions(3, 3, Duration.ofSeconds(5))).join();
            assertThat(limited.batches()).hasSize(2);
            assertThat(limited.batches()).extracting(batch -> batch.payload().length)
                    .containsExactly(3, 0);
            assertThat(limited.nextOffset()).isEqualTo(2);

            NereusException tooSmall = failure(context.storage.read(
                    streamId, 0, readOptions(3, 2, Duration.ofSeconds(5))));
            assertThat(tooSmall.code()).isEqualTo(ErrorCode.READ_LIMIT_TOO_SMALL);
            assertThat(tooSmall.retriable()).isTrue();
        }
    }

    @Test
    void trimIsCheckedBeforeCacheAndObjectIo() {
        try (TestContext context = context(defaultConfig(true), new RecordingReadMetrics())) {
            StreamId streamId = context.createStream("trimmed-read").streamId();
            context.storage.append(streamId, batch(List.of(), "a", "b"), appendOptions()).join();
            context.storage.resolve(streamId, 0, new ResolveOptions(10, true, true)).join();
            context.metadata.updateTrim("cluster/a", streamId, 1, "test").join();

            NereusException trimmed = failure(context.storage.read(
                    streamId, 0, readOptions(10, 10, Duration.ofSeconds(5))));
            assertThat(trimmed.code()).isEqualTo(ErrorCode.OFFSET_TRIMMED);

            ReadResult readable = context.storage.read(
                    streamId, 1, readOptions(10, 10, Duration.ofSeconds(5))).join();
            assertThat(readable.batches()).extracting(batch -> text(batch.payload()))
                    .containsExactly("b");
        }
    }

    @Test
    void readRepairsIndexMissingAfterKnownCommittedAppendFailure() {
        try (TestContext context = context(defaultConfig(false), new RecordingReadMetrics())) {
            StreamId streamId = context.createStream("repair-read").streamId();
            context.metadata.failNext(
                    FakeOxiaMetadataStore.FailurePoint.AFTER_HEAD_CAS_BEFORE_DERIVED_INDEX);
            NereusException appendFailure = failure(context.storage.append(
                    streamId, batch(List.of(), "repair-me"), appendOptions()));
            assertThat(appendFailure.appendOutcome()).contains(AppendOutcome.KNOWN_COMMITTED);
            assertThat(context.metadata.scanOffsetIndex("cluster/a", streamId, 0, 10).join()).isEmpty();

            ReadResult repaired = context.storage.read(
                    streamId, 0, readOptions(10, 64, Duration.ofSeconds(5))).join();

            assertThat(repaired.batches()).extracting(batch -> text(batch.payload()))
                    .containsExactly("repair-me");
            assertThat(context.metadata.scanOffsetIndex("cluster/a", streamId, 0, 10).join())
                    .hasSize(1);
        }
    }

    @Test
    void repairBudgetExhaustionIsRetriableAndALargerBudgetCanRecover() {
        LocalFileObjectStore objectStore = new LocalFileObjectStore(root);
        FakeOxiaMetadataStore metadata = new FakeOxiaMetadataStore(CLOCK::millis);
        DefaultStreamStorage first = storage(config(false, 1, 1, 8L << 20), metadata, objectStore,
                new DefaultWalObjectReader(objectStore), new RecordingReadMetrics());
        DefaultStreamStorage second = storage(defaultConfig(false), metadata, objectStore,
                new DefaultWalObjectReader(objectStore), new RecordingReadMetrics());
        try {
            StreamId streamId = first.createOrGetStream(
                    new StreamName("repair-budget"),
                    new StreamCreateOptions(StorageProfile.OBJECT_WAL_SYNC_OBJECT, Map.of())).join().streamId();
            metadata.failNext(FakeOxiaMetadataStore.FailurePoint.AFTER_HEAD_CAS_BEFORE_DERIVED_INDEX);
            failure(first.append(streamId, batch(List.of(), "first"), appendOptions()));
            second.append(streamId, batch(List.of(), "second"), appendOptions()).join();

            NereusException exhausted = failure(first.read(
                    streamId, 0, readOptions(10, 64, Duration.ofSeconds(5))));
            assertThat(exhausted.code()).isEqualTo(ErrorCode.READ_RESOLUTION_FAILED);
            assertThat(exhausted.retriable()).isTrue();

            ReadResult recovered = second.read(
                    streamId, 0, readOptions(10, 64, Duration.ofSeconds(5))).join();
            assertThat(recovered.batches()).extracting(batch -> text(batch.payload()))
                    .containsExactly("first", "second");
        } finally {
            first.close();
            second.close();
            metadata.close();
            objectStore.close();
        }
    }

    @Test
    void resolverSelectsHighestNonTombstonedGeneration() {
        LocalFileObjectStore objectStore = new LocalFileObjectStore(root);
        FakeOxiaMetadataStore metadata = new FakeOxiaMetadataStore(CLOCK::millis);
        DefaultStreamStorage writerStorage = storage(
                defaultConfig(false), metadata, objectStore, new DefaultWalObjectReader(objectStore),
                new RecordingReadMetrics());
        try {
            StreamId streamId = writerStorage.createOrGetStream(
                    new StreamName("generation"),
                    new StreamCreateOptions(StorageProfile.OBJECT_WAL_SYNC_OBJECT, Map.of())).join().streamId();
            writerStorage.append(streamId, batch(List.of(), "a"), appendOptions()).join();
            OffsetIndexEntry base = metadata.scanOffsetIndex("cluster/a", streamId, 0, 10).join().getFirst();
            OffsetIndexEntry higher = withGeneration(base, 7, base.metadataVersion() + 1);
            OxiaMetadataStore overlapping = overrideScan(metadata, List.of(base, higher));
            DefaultStreamStorage readerStorage = storage(
                    defaultConfig(false), overlapping, objectStore, new DefaultWalObjectReader(objectStore),
                    new RecordingReadMetrics());
            try {
                ResolveResult result = readerStorage.resolve(
                        streamId, 0, new ResolveOptions(10, false, true)).join();
                assertThat(result.ranges()).singleElement()
                        .extracting(range -> range.generation())
                        .isEqualTo(7L);
            } finally {
                readerStorage.close();
            }
        } finally {
            writerStorage.close();
            metadata.close();
            objectStore.close();
        }
    }

    @Test
    void smallReadReportsFullSliceAmplification() {
        RecordingReadMetrics metrics = new RecordingReadMetrics();
        try (TestContext context = context(defaultConfig(false), metrics)) {
            StreamId streamId = context.createStream("amplification").streamId();
            byte[] large = new byte[512 * 1024];
            Arrays.fill(large, (byte) 'x');
            context.storage.append(streamId, batchBytes(List.of(), new byte[] {'a'}, large), appendOptions()).join();

            ReadResult result = context.storage.read(
                    streamId, 0, readOptions(1, 1, Duration.ofSeconds(5))).join();

            assertThat(result.batches()).singleElement()
                    .extracting(batch -> batch.payload().length)
                    .isEqualTo(1);
            assertThat(metrics.fullSliceBytes.get()).isEqualTo(large.length + 1L);
            assertThat(metrics.entryIndexBytes.get()).isPositive();
            assertThat(metrics.returnedBytes.get()).isEqualTo(1);
            assertThat(metrics.fullSliceBytes.get() + metrics.entryIndexBytes.get()
                    - metrics.returnedBytes.get()).isGreaterThan(large.length);
        }
    }

    @Test
    void fullSliceChecksumCatchesCorruptionOutsideReturnedSubrange() throws Exception {
        try (TestContext context = context(
                config(false, 10_000, 1, 8L << 20), new RecordingReadMetrics())) {
            StreamId streamId = context.createStream("corruption").streamId();
            AppendResult append = context.storage.append(
                    streamId, batch(List.of(), "a", "b"), appendOptions()).join();
            ObjectSliceReadTarget target = (ObjectSliceReadTarget) append.readTarget();
            Path objectPath = root.resolve(target.objectKey().value());
            byte[] bytes = Files.readAllBytes(objectPath);
            bytes[Math.toIntExact(target.objectOffset() + 1)] ^= 0x01;
            Files.write(objectPath, bytes);

            NereusException corruption = failure(context.storage.read(
                    streamId, 0, readOptions(1, 1, Duration.ofSeconds(5))));
            assertThat(corruption.code()).isEqualTo(ErrorCode.OBJECT_CHECKSUM_MISMATCH);
            assertThat(failure(context.storage.read(
                    streamId, 0, readOptions(1, 1, Duration.ofSeconds(5)))).code())
                    .isEqualTo(ErrorCode.OBJECT_CHECKSUM_MISMATCH);
        }
    }

    @Test
    void concurrentReadBackpressureRejectsBeforeStartingAnotherWalReadAndReleasesOnCancel()
            throws Exception {
        LocalFileObjectStore objectStore = new LocalFileObjectStore(root);
        FakeOxiaMetadataStore metadata = new FakeOxiaMetadataStore(CLOCK::millis);
        DefaultWalObjectReader delegate = new DefaultWalObjectReader(objectStore);
        BlockingWalReader reader = new BlockingWalReader(delegate);
        RecordingReadMetrics metrics = new RecordingReadMetrics();
        DefaultStreamStorage storage = storage(
                config(false, 10_000, 1, 8L << 20), metadata, objectStore, reader, metrics);
        try {
            StreamId streamId = storage.createOrGetStream(
                    new StreamName("read-backpressure"),
                    new StreamCreateOptions(StorageProfile.OBJECT_WAL_SYNC_OBJECT, Map.of())).join().streamId();
            storage.append(streamId, batch(List.of(), "a"), appendOptions()).join();
            CompletableFuture<ReadResult> first = storage.read(
                    streamId, 0, readOptions(10, 10, Duration.ofSeconds(5)));
            assertThat(reader.started.await(5, TimeUnit.SECONDS)).isTrue();

            NereusException rejected = failure(storage.read(
                    streamId, 0, readOptions(10, 10, Duration.ofSeconds(1))));
            assertThat(rejected.code()).isEqualTo(ErrorCode.BACKPRESSURE_REJECTED);
            assertThat(reader.calls).hasValue(1);
            assertThat(metrics.backpressureRejections).hasValue(1);

            assertThat(first.cancel(false)).isTrue();
            assertThat(failure(first).code()).isEqualTo(ErrorCode.CANCELLED);
            CompletableFuture<ReadResult> afterRelease = storage.read(
                    streamId, 0, readOptions(10, 10, Duration.ofSeconds(1)));
            assertThat(reader.calls).hasValue(2);
            afterRelease.cancel(false);
            assertThat(failure(afterRelease).code()).isEqualTo(ErrorCode.CANCELLED);
        } finally {
            storage.close();
            metadata.close();
            objectStore.close();
        }
    }

    @Test
    void readTimeoutReleasesResources() {
        LocalFileObjectStore objectStore = new LocalFileObjectStore(root);
        FakeOxiaMetadataStore metadata = new FakeOxiaMetadataStore(CLOCK::millis);
        BlockingWalReader reader = new BlockingWalReader(new DefaultWalObjectReader(objectStore));
        DefaultStreamStorage storage = storage(
                config(false, 10_000, 1, 8L << 20), metadata, objectStore, reader,
                new RecordingReadMetrics());
        try {
            StreamId streamId = storage.createOrGetStream(
                    new StreamName("read-timeout"),
                    new StreamCreateOptions(StorageProfile.OBJECT_WAL_SYNC_OBJECT, Map.of())).join().streamId();
            storage.append(streamId, batch(List.of(), "a"), appendOptions()).join();

            NereusException timeout = failure(storage.read(
                    streamId, 0, readOptions(10, 10, Duration.ofMillis(40))));
            assertThat(timeout.code()).isEqualTo(ErrorCode.TIMEOUT);
            CompletableFuture<ReadResult> afterTimeout = storage.read(
                    streamId, 0, readOptions(10, 10, Duration.ofSeconds(1)));
            assertThat(reader.calls).hasValue(2);
            afterTimeout.cancel(false);
            assertThat(failure(afterTimeout).code()).isEqualTo(ErrorCode.CANCELLED);
        } finally {
            storage.close();
            metadata.close();
            objectStore.close();
        }
    }

    @Test
    void aggregateReadBufferBudgetRejectsBeforeASecondWalReadEvenWhenPermitIsAvailable()
            throws Exception {
        LocalFileObjectStore objectStore = new LocalFileObjectStore(root);
        FakeOxiaMetadataStore metadata = new FakeOxiaMetadataStore(CLOCK::millis);
        BlockingWalReader reader = new BlockingWalReader(new DefaultWalObjectReader(objectStore));
        RecordingReadMetrics metrics = new RecordingReadMetrics();
        DefaultStreamStorage storage = storage(
                config(false, 10_000, 2, 1L << 20), metadata, objectStore, reader, metrics);
        try {
            StreamId streamId = storage.createOrGetStream(
                    new StreamName("read-buffer"),
                    new StreamCreateOptions(StorageProfile.OBJECT_WAL_SYNC_OBJECT, Map.of())).join().streamId();
            byte[] payload = new byte[700 * 1024];
            storage.append(streamId, batchBytes(List.of(), payload), appendOptions()).join();
            CompletableFuture<ReadResult> first = storage.read(
                    streamId, 0, readOptions(1, payload.length, Duration.ofSeconds(5)));
            assertThat(reader.started.await(5, TimeUnit.SECONDS)).isTrue();

            NereusException rejected = failure(storage.read(
                    streamId, 0, readOptions(1, payload.length, Duration.ofSeconds(1))));
            assertThat(rejected.code()).isEqualTo(ErrorCode.BACKPRESSURE_REJECTED);
            assertThat(reader.calls).hasValue(1);
            assertThat(metrics.backpressureRejections).hasValue(1);

            first.cancel(false);
            assertThat(failure(first).code()).isEqualTo(ErrorCode.CANCELLED);
        } finally {
            storage.close();
            metadata.close();
            objectStore.close();
        }
    }

    @Test
    void positiveCacheHitsAndWatchInvalidatesWithoutPopulatingFromTheEvent() {
        RecordingReadMetrics metrics = new RecordingReadMetrics();
        try (TestContext context = context(defaultConfig(true), metrics)) {
            StreamId streamId = context.createStream("cache-watch").streamId();
            context.storage.append(streamId, batch(List.of(), "a"), appendOptions()).join();

            context.storage.resolve(streamId, 0, new ResolveOptions(10, true, true)).join();
            context.storage.resolve(streamId, 0, new ResolveOptions(10, true, true)).join();
            assertThat(metrics.cacheMisses).hasValue(1);
            assertThat(metrics.cacheHits).hasValue(1);

            context.storage.append(streamId, batch(List.of(), "b"), appendOptions()).join();
            context.storage.resolve(streamId, 0, new ResolveOptions(10, true, true)).join();
            assertThat(metrics.cacheMisses).hasValue(2);

            context.metadata.setNextWatchDelivery(FakeOxiaMetadataStore.WatchDelivery.DROP_NEXT);
            context.storage.append(streamId, batch(List.of(), "c"), appendOptions()).join();
            context.storage.resolve(streamId, 2, new ResolveOptions(10, true, true)).join();
            assertThat(metrics.cacheMisses).hasValue(3);
        }
    }

    @Test
    void unsupportedReadProfileFailsBeforeWalRead() {
        LocalFileObjectStore objectStore = new LocalFileObjectStore(root);
        FakeOxiaMetadataStore metadata = new FakeOxiaMetadataStore(CLOCK::millis);
        BlockingWalReader reader = new BlockingWalReader(new DefaultWalObjectReader(objectStore));
        DefaultStreamStorage storage = storage(defaultConfig(false), metadata, objectStore, reader,
                new RecordingReadMetrics());
        try {
            StreamId streamId = storage.createOrGetStream(
                    new StreamName("bk-read"),
                    new StreamCreateOptions(StorageProfile.BOOKKEEPER_WAL_ONLY, Map.of())).join().streamId();
            NereusException failure = failure(storage.read(
                    streamId, 0, readOptions(10, 10, Duration.ofSeconds(1))));
            assertThat(failure.code()).isEqualTo(ErrorCode.UNSUPPORTED_STORAGE_PROFILE);
            assertThat(reader.calls).hasValue(0);
        } finally {
            storage.close();
            metadata.close();
            objectStore.close();
        }
    }

    @Test
    void missingObjectIsRetriableAndEofIsNeverNegativeCached() {
        RecordingReadMetrics metrics = new RecordingReadMetrics();
        try (TestContext context = context(config(false, 10_000, 1, 8L << 20), metrics)) {
            StreamId streamId = context.createStream("missing-object").streamId();
            ResolveResult empty = context.storage.resolve(
                    streamId, 0, new ResolveOptions(10, true, true)).join();
            assertThat(empty.ranges()).isEmpty();

            context.storage.append(streamId, batch(List.of(), "a"), appendOptions()).join();
            ResolveResult visible = context.storage.resolve(
                    streamId, 0, new ResolveOptions(10, true, true)).join();
            assertThat(visible.ranges()).hasSize(1);
            assertThat(context.storage.read(
                    streamId, 0, readOptions(10, 10, Duration.ofSeconds(1))).join().batches())
                    .hasSize(1);

            context.objectStore.deleteAllForTesting();
            NereusException missing = failure(context.storage.read(
                    streamId, 0, readOptions(10, 10, Duration.ofSeconds(1))));
            assertThat(missing.code()).isEqualTo(ErrorCode.OBJECT_NOT_FOUND);
            assertThat(missing.retriable()).isTrue();
            assertThat(failure(context.storage.read(
                    streamId, 0, readOptions(10, 10, Duration.ofSeconds(1)))).code())
                    .isEqualTo(ErrorCode.OBJECT_NOT_FOUND);
        }
    }

    @Test
    void uploadedManifestWithoutReachableHeadCommitRemainsInvisible() {
        LocalFileObjectStore objectStore = new LocalFileObjectStore(root);
        FakeOxiaMetadataStore metadata = new FakeOxiaMetadataStore(CLOCK::millis);
        OxiaMetadataStore delayedCommit = delayCommit(metadata);
        DefaultStreamStorage storage = storage(
                defaultConfig(false), delayedCommit, objectStore, new DefaultWalObjectReader(objectStore),
                new RecordingReadMetrics());
        try {
            StreamId streamId = storage.createOrGetStream(
                    new StreamName("manifest-only"),
                    new StreamCreateOptions(StorageProfile.OBJECT_WAL_SYNC_OBJECT, Map.of())).join().streamId();
            AppendOptions shortAppend = new AppendOptions(
                    Optional.empty(),
                    DurabilityLevel.WAL_DURABLE_AND_INDEX_COMMITTED,
                    Duration.ofMillis(40),
                    true,
                    Map.of());
            NereusException uncertain = failure(
                    storage.append(streamId, batch(List.of(), "not-visible"), shortAppend));
            assertThat(uncertain.appendOutcome()).contains(AppendOutcome.MAY_HAVE_COMMITTED);
            assertThat(metadata.storedMetadataValuesForTesting())
                    .anyMatch(value -> value.recordType().equals("ObjectManifestRecord"));

            ResolveResult resolved = storage.resolve(
                    streamId, 0, new ResolveOptions(10, false, true)).join();
            assertThat(resolved.ranges()).isEmpty();
        } finally {
            storage.close();
            metadata.close();
            objectStore.close();
        }
    }

    @Test
    void metricsCallbackFailureDoesNotReclassifyAReadOrResolve() {
        ReadMetricsObserver throwingMetrics = new ReadMetricsObserver() {
            @Override
            public void onSliceRead(long fullSlicePayloadBytes, long entryIndexBytes, long returnedPayloadBytes) {
                throw new IllegalStateException("metrics unavailable");
            }

            @Override
            public void onOffsetIndexCacheHit() {
                throw new IllegalStateException("metrics unavailable");
            }

            @Override
            public void onOffsetIndexCacheMiss() {
                throw new IllegalStateException("metrics unavailable");
            }
        };
        LocalFileObjectStore objectStore = new LocalFileObjectStore(root);
        FakeOxiaMetadataStore metadata = new FakeOxiaMetadataStore(CLOCK::millis);
        DefaultStreamStorage storage = storage(
                defaultConfig(false), metadata, objectStore, new DefaultWalObjectReader(objectStore),
                throwingMetrics);
        try {
            StreamId streamId = storage.createOrGetStream(
                    new StreamName("metrics-failure"),
                    new StreamCreateOptions(StorageProfile.OBJECT_WAL_SYNC_OBJECT, Map.of())).join().streamId();
            storage.append(streamId, batch(List.of(), "a"), appendOptions()).join();
            assertThat(storage.resolve(streamId, 0, new ResolveOptions(10, true, true)).join().ranges())
                    .hasSize(1);
            assertThat(storage.read(streamId, 0, readOptions(10, 10, Duration.ofSeconds(1))).join().batches())
                    .hasSize(1);
        } finally {
            storage.close();
            metadata.close();
            objectStore.close();
        }
    }

    private TestContext context(StreamStorageConfig config, RecordingReadMetrics metrics) {
        LocalFileObjectStore objectStore = new LocalFileObjectStore(root);
        FakeOxiaMetadataStore metadata = new FakeOxiaMetadataStore(CLOCK::millis);
        DefaultStreamStorage storage = storage(
                config,
                metadata,
                objectStore,
                new DefaultWalObjectReader(objectStore),
                metrics);
        return new TestContext(storage, metadata, objectStore);
    }

    private static DefaultStreamStorage storage(
            StreamStorageConfig config,
            OxiaMetadataStore metadata,
            LocalFileObjectStore objectStore,
            WalObjectReader reader,
            ReadMetricsObserver metrics) {
        return new DefaultStreamStorage(
                config,
                metadata,
                new DefaultWalObjectWriter(objectStore, "test-writer-1", CLOCK),
                reader,
                CLOCK,
                Runnable::run,
                metrics);
    }

    private static StreamStorageConfig defaultConfig(boolean watch) {
        return config(watch, 10_000, 16, 8L << 20);
    }

    private static StreamStorageConfig config(
            boolean watch,
            int maxCommitChainScan,
            int maxConcurrentReads,
            long maxReadBufferBytes) {
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
                maxCommitChainScan,
                Math.min(256, maxCommitChainScan),
                1_000,
                64,
                8L << 20,
                maxConcurrentReads,
                maxReadBufferBytes,
                1 << 20,
                10_000,
                Duration.ofSeconds(5),
                true,
                watch,
                true);
    }

    private static AppendBatch batch(List<SchemaRef> schemaRefs, String... values) {
        byte[][] payloads = Arrays.stream(values)
                .map(value -> value.getBytes(StandardCharsets.UTF_8))
                .toArray(byte[][]::new);
        return batchBytes(schemaRefs, payloads);
    }

    private static AppendBatch batchBytes(List<SchemaRef> schemaRefs, byte[]... payloads) {
        List<AppendEntry> entries = Arrays.stream(payloads)
                .map(payload -> new AppendEntry(payload, 1, NOW.toEpochMilli(), Map.of()))
                .toList();
        return new AppendBatch(
                PayloadFormat.OPAQUE_RECORD_BATCH,
                entries,
                entries.size(),
                entries.size(),
                NOW.toEpochMilli(),
                NOW.toEpochMilli(),
                schemaRefs,
                Map.of(),
                Optional.empty());
    }

    private static AppendOptions appendOptions() {
        return new AppendOptions(
                Optional.empty(),
                DurabilityLevel.WAL_DURABLE_AND_INDEX_COMMITTED,
                Duration.ofSeconds(5),
                true,
                Map.of());
    }

    private static ReadOptions readOptions(int maxRecords, int maxBytes, Duration timeout) {
        return new ReadOptions(maxRecords, maxBytes, ReadIsolation.COMMITTED, timeout);
    }

    private static NereusException failure(CompletableFuture<?> future) {
        try {
            future.join();
            throw new AssertionError("future completed successfully");
        } catch (CompletionException e) {
            assertThat(e.getCause()).isInstanceOf(NereusException.class);
            return (NereusException) e.getCause();
        }
    }

    private static String text(byte[] payload) {
        return new String(payload, StandardCharsets.UTF_8);
    }

    private static OffsetIndexEntry withGeneration(
            OffsetIndexEntry record,
            long generation,
            long metadataVersion) {
        return new OffsetIndexEntry(
                record.streamId(),
                record.range(),
                generation,
                record.cumulativeSize(),
                record.readTarget(),
                record.payloadFormat(),
                record.recordCount(),
                record.entryCount(),
                record.logicalBytes(),
                record.schemaRefs(),
                record.projectionRef(),
                record.commitVersion(),
                false,
                metadataVersion);
    }

    private static OxiaMetadataStore overrideScan(
            OxiaMetadataStore delegate,
            List<OffsetIndexEntry> records) {
        return (OxiaMetadataStore) Proxy.newProxyInstance(
                OxiaMetadataStore.class.getClassLoader(),
                new Class<?>[] {OxiaMetadataStore.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("scanOffsetIndex")) {
                        return CompletableFuture.completedFuture(records);
                    }
                    try {
                        return method.invoke(delegate, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    private static OxiaMetadataStore delayCommit(OxiaMetadataStore delegate) {
        return (OxiaMetadataStore) Proxy.newProxyInstance(
                OxiaMetadataStore.class.getClassLoader(),
                new Class<?>[] {OxiaMetadataStore.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("commitStableAppend")) {
                        return new CompletableFuture<>();
                    }
                    try {
                        return method.invoke(delegate, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    private static final class BlockingWalReader implements WalObjectReader {
        private final WalObjectReader delegate;
        private final CountDownLatch started = new CountDownLatch(1);
        private final AtomicInteger calls = new AtomicInteger();
        private final CompletableFuture<WalReadResult> pending = new CompletableFuture<>();

        private BlockingWalReader(WalObjectReader delegate) {
            this.delegate = delegate;
        }

        @Override
        public CompletableFuture<WalReadResult> readWithStats(
                long startOffset,
                List<com.nereusstream.api.ResolvedObjectRange> ranges,
                ReadOptions options) {
            calls.incrementAndGet();
            started.countDown();
            return pending;
        }
    }

    private static final class RecordingReadMetrics implements ReadMetricsObserver {
        private final AtomicLong fullSliceBytes = new AtomicLong();
        private final AtomicLong entryIndexBytes = new AtomicLong();
        private final AtomicLong returnedBytes = new AtomicLong();
        private final AtomicInteger backpressureRejections = new AtomicInteger();
        private final AtomicInteger cacheHits = new AtomicInteger();
        private final AtomicInteger cacheMisses = new AtomicInteger();

        @Override
        public void onSliceRead(long fullSlicePayloadBytes, long indexBytes, long returnedPayloadBytes) {
            fullSliceBytes.addAndGet(fullSlicePayloadBytes);
            entryIndexBytes.addAndGet(indexBytes);
            returnedBytes.addAndGet(returnedPayloadBytes);
        }

        @Override
        public void onBackpressureRejected(long requestedBufferBytes) {
            backpressureRejections.incrementAndGet();
        }

        @Override
        public void onOffsetIndexCacheHit() {
            cacheHits.incrementAndGet();
        }

        @Override
        public void onOffsetIndexCacheMiss() {
            cacheMisses.incrementAndGet();
        }
    }

    private record TestContext(
            DefaultStreamStorage storage,
            FakeOxiaMetadataStore metadata,
            LocalFileObjectStore objectStore) implements AutoCloseable {
        StreamMetadata createStream(String name) {
            return storage.createOrGetStream(
                    new StreamName(name),
                    new StreamCreateOptions(StorageProfile.OBJECT_WAL_SYNC_OBJECT, Map.of())).join();
        }

        @Override
        public void close() {
            storage.close();
            metadata.close();
            objectStore.close();
        }
    }
}
