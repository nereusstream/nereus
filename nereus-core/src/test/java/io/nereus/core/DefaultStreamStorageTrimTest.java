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

package io.nereus.core;

import static org.assertj.core.api.Assertions.assertThat;

import io.nereus.api.AppendBatch;
import io.nereus.api.AppendEntry;
import io.nereus.api.AppendOptions;
import io.nereus.api.AppendResult;
import io.nereus.api.DurabilityLevel;
import io.nereus.api.ErrorCode;
import io.nereus.api.NereusException;
import io.nereus.api.PayloadFormat;
import io.nereus.api.ReadIsolation;
import io.nereus.api.ReadOptions;
import io.nereus.api.ResolveOptions;
import io.nereus.api.StorageProfile;
import io.nereus.api.StreamCreateOptions;
import io.nereus.api.StreamId;
import io.nereus.api.StreamMetadata;
import io.nereus.api.StreamName;
import io.nereus.api.TrimOptions;
import io.nereus.core.read.ReadMetricsObserver;
import io.nereus.core.trim.TrimMetricsObserver;
import io.nereus.metadata.oxia.OxiaMetadataStore;
import io.nereus.metadata.oxia.records.TrimRecord;
import io.nereus.metadata.oxia.testing.FakeOxiaMetadataStore;
import io.nereus.objectstore.HeadObjectOptions;
import io.nereus.objectstore.testing.LocalFileObjectStore;
import io.nereus.objectstore.wal.DefaultWalObjectReader;
import io.nereus.objectstore.wal.DefaultWalObjectWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DefaultStreamStorageTrimTest {
    private static final String CLUSTER = "cluster/a";
    private static final Instant NOW = Instant.parse("2026-07-11T07:08:09Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);

    @TempDir
    Path root;

    @Test
    void trimAdvancesLowWatermarkInvalidatesCacheAndPreservesPhysicalData() {
        RecordingTrimMetrics metrics = new RecordingTrimMetrics();
        try (TestContext context = context(new FakeOxiaMetadataStore(CLOCK::millis), metrics)) {
            StreamId streamId = context.createStream("trim-preserves-data").streamId();
            AppendResult append = context.storage.append(
                    streamId, batch("a", "bb", "ccc"), appendOptions()).join();
            context.storage.resolve(streamId, 0, new ResolveOptions(10, true, true)).join();

            int indexCount = context.metadata.scanOffsetIndex(CLUSTER, streamId, 0, 10).join().size();
            long objectLength = context.objectStore.headObject(
                    append.objectKey(), new HeadObjectOptions(Duration.ofSeconds(1))).join().objectLength();

            context.storage.trim(
                    streamId, 2, new TrimOptions(Duration.ofSeconds(1), "consumer-retention")).join();

            assertThat(context.metadata.getTrim(CLUSTER, streamId).join().trimOffset()).isEqualTo(2);
            assertThat(context.metadata.scanOffsetIndex(CLUSTER, streamId, 0, 10).join()).hasSize(indexCount);
            assertThat(context.objectStore.headObject(
                    append.objectKey(), new HeadObjectOptions(Duration.ofSeconds(1))).join().objectLength())
                    .isEqualTo(objectLength);
            assertThat(failure(context.storage.read(
                    streamId, 1, readOptions())).code()).isEqualTo(ErrorCode.OFFSET_TRIMMED);
            assertThat(context.storage.read(streamId, 2, readOptions()).join().batches())
                    .extracting(value -> text(value.payload()))
                    .containsExactly("ccc");
            assertThat(metrics.successes).hasValue(1);
            assertThat(metrics.failures).hasValue(0);
            assertThat(metrics.lastOffset).isEqualTo(2);
        }
    }

    @Test
    void trimRejectsInvalidAndNonMonotonicBoundsWithoutMutation() {
        try (TestContext context = context(new FakeOxiaMetadataStore(CLOCK::millis), TrimMetricsObserver.noop())) {
            StreamId streamId = context.createStream("trim-bounds").streamId();
            context.storage.append(streamId, batch("a", "b"), appendOptions()).join();

            assertThat(failure(context.storage.trim(
                    streamId, -1, new TrimOptions(Duration.ofSeconds(1), "bad"))).code())
                    .isEqualTo(ErrorCode.INVALID_ARGUMENT);
            assertThat(failure(context.storage.trim(streamId, 1, null)).code())
                    .isEqualTo(ErrorCode.INVALID_ARGUMENT);
            context.storage.trim(streamId, 1, new TrimOptions(Duration.ofSeconds(1), "advance")).join();
            assertThat(failure(context.storage.trim(
                    streamId, 0, new TrimOptions(Duration.ofSeconds(1), "decrease"))).code())
                    .isEqualTo(ErrorCode.INVALID_ARGUMENT);
            assertThat(failure(context.storage.trim(
                    streamId, 3, new TrimOptions(Duration.ofSeconds(1), "past-end"))).code())
                    .isEqualTo(ErrorCode.INVALID_ARGUMENT);
            assertThat(context.metadata.getTrim(CLUSTER, streamId).join().trimOffset()).isEqualTo(1);
        }
    }

    @Test
    void trimTimeoutAndCancellationRemainAdvisoryMetadataOutcomes() {
        FakeOxiaMetadataStore metadata = new FakeOxiaMetadataStore(CLOCK::millis);
        CompletableFuture<TrimRecord> pending = new CompletableFuture<>();
        OxiaMetadataStore delayed = delayTrim(metadata, pending);
        RecordingTrimMetrics metrics = new RecordingTrimMetrics();
        try (TestContext context = context(metadata, delayed, metrics, config(Duration.ofSeconds(1)))) {
            StreamId streamId = context.createStream("trim-deadline").streamId();

            NereusException timeout = failure(context.storage.trim(
                    streamId, 0, new TrimOptions(Duration.ofMillis(10), "timeout")));
            assertThat(timeout.code()).isEqualTo(ErrorCode.TIMEOUT);
            assertThat(timeout.retriable()).isTrue();

            CompletableFuture<Void> cancelled = context.storage.trim(
                    streamId, 0, new TrimOptions(Duration.ofSeconds(1), "cancel"));
            assertThat(cancelled.cancel(false)).isTrue();
            assertThat(failure(cancelled).code()).isEqualTo(ErrorCode.CANCELLED);
            assertThat(metrics.failures).hasValue(2);
            pending.complete(new TrimRecord(streamId.value(), 0, "late", NOW.toEpochMilli(), 1));
        }
    }

    @Test
    void closeWaitsForAcceptedTrimAndRejectsNewTrim() throws Exception {
        FakeOxiaMetadataStore metadata = new FakeOxiaMetadataStore(CLOCK::millis);
        CompletableFuture<TrimRecord> pending = new CompletableFuture<>();
        OxiaMetadataStore delayed = delayTrim(metadata, pending);
        TestContext context = context(metadata, delayed, TrimMetricsObserver.noop(), config(Duration.ofSeconds(2)));
        try {
            StreamId streamId = context.createStream("trim-close").streamId();
            CompletableFuture<Void> trim = context.storage.trim(
                    streamId, 0, new TrimOptions(Duration.ofSeconds(2), "close"));
            CompletableFuture<Void> close = CompletableFuture.runAsync(context.storage::close);

            TimeUnit.MILLISECONDS.sleep(50);
            assertThat(close).isNotDone();
            assertThat(failure(context.storage.read(streamId, 0, readOptions())).code())
                    .isEqualTo(ErrorCode.STORAGE_CLOSED);
            NereusException appendAfterClose = failure(context.storage.append(
                    streamId, batch("late"), appendOptions()));
            assertThat(appendAfterClose.code()).isEqualTo(ErrorCode.STORAGE_CLOSED);
            pending.complete(new TrimRecord(streamId.value(), 0, "close", NOW.toEpochMilli(), 1));
            trim.get(1, TimeUnit.SECONDS);
            close.get(1, TimeUnit.SECONDS);

            assertThat(failure(context.storage.trim(
                    streamId, 0, new TrimOptions(Duration.ofSeconds(1), "closed"))).code())
                    .isEqualTo(ErrorCode.STORAGE_CLOSED);
        } finally {
            context.close();
        }
    }

    @Test
    void alreadyExpiredDeadlineStillReturnsAFailedFutureAndReleasesLifecycle() throws Exception {
        FakeOxiaMetadataStore metadata = new FakeOxiaMetadataStore(CLOCK::millis);
        AtomicReference<CompletableFuture<Void>> returned = new AtomicReference<>();
        try (TestContext context = context(metadata, TrimMetricsObserver.noop())) {
            StreamId streamId = context.createStream("trim-expired").streamId();

            returned.set(context.storage.trim(
                    streamId, 0, new TrimOptions(Duration.ofNanos(1), "expired")));

            assertThat(failure(returned.get()).code()).isEqualTo(ErrorCode.TIMEOUT);
            CompletableFuture.runAsync(context.storage::close).get(1, TimeUnit.SECONDS);
        }
    }

    private TestContext context(FakeOxiaMetadataStore metadata, TrimMetricsObserver metrics) {
        return context(metadata, metadata, metrics, config(Duration.ofSeconds(1)));
    }

    private TestContext context(
            FakeOxiaMetadataStore metadata,
            OxiaMetadataStore storageMetadata,
            TrimMetricsObserver metrics,
            StreamStorageConfig config) {
        LocalFileObjectStore objectStore = new LocalFileObjectStore(root.resolve("objects-" + System.nanoTime()));
        DefaultStreamStorage storage = new DefaultStreamStorage(
                config,
                storageMetadata,
                new DefaultWalObjectWriter(objectStore, "test-writer-1", CLOCK),
                new DefaultWalObjectReader(objectStore),
                CLOCK,
                Runnable::run,
                ReadMetricsObserver.noop(),
                metrics);
        return new TestContext(storage, metadata, objectStore);
    }

    private static StreamStorageConfig config(Duration shutdownGrace) {
        StreamStorageConfig defaults = StreamStorageConfig.defaults(CLUSTER, "writer-a");
        return new StreamStorageConfig(
                defaults.cluster(), defaults.writerId(), defaults.appendSessionTtl(),
                defaults.appendSessionRenewBefore(), defaults.appendSessionMinCommitRemaining(),
                defaults.appendTimeout(), defaults.readTimeout(), shutdownGrace,
                defaults.maxResolveRanges(), defaults.maxCommitChainScan(),
                defaults.maxDerivedIndexRepairCommitsPerCall(), defaults.maxInFlightAppends(),
                defaults.maxBufferedBytes(), defaults.maxConcurrentObjectReads(),
                defaults.maxReadBufferBytes(), defaults.maxObjectBytes(), defaults.maxAppendBatchRecords(),
                defaults.offsetIndexCacheTtl(), defaults.autoAcquireAppendSession(), false, true);
    }

    private static OxiaMetadataStore delayTrim(
            OxiaMetadataStore delegate,
            CompletableFuture<TrimRecord> pending) {
        return (OxiaMetadataStore) Proxy.newProxyInstance(
                OxiaMetadataStore.class.getClassLoader(),
                new Class<?>[] {OxiaMetadataStore.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("updateTrim")) {
                        return pending;
                    }
                    try {
                        return method.invoke(delegate, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    private static AppendBatch batch(String... values) {
        List<AppendEntry> entries = Arrays.stream(values)
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

    private static AppendOptions appendOptions() {
        return new AppendOptions(
                Optional.empty(),
                DurabilityLevel.WAL_DURABLE_AND_INDEX_COMMITTED,
                Duration.ofSeconds(1),
                true,
                Map.of());
    }

    private static ReadOptions readOptions() {
        return new ReadOptions(10, 1024, ReadIsolation.COMMITTED, Duration.ofSeconds(1));
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

    private static final class RecordingTrimMetrics implements TrimMetricsObserver {
        private final AtomicInteger successes = new AtomicInteger();
        private final AtomicInteger failures = new AtomicInteger();
        private volatile long lastOffset = -1;

        @Override
        public void onTrimSucceeded(StreamId streamId, long trimOffset) {
            lastOffset = trimOffset;
            successes.incrementAndGet();
        }

        @Override
        public void onTrimFailed(ErrorCode errorCode) {
            failures.incrementAndGet();
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
