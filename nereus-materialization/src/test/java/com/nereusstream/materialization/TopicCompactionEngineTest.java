/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import static com.nereusstream.materialization.MaterializationPlannerTestSupport.STREAM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadIsolation;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import com.nereusstream.objectstore.compacted.CompactedObjectRow;
import com.nereusstream.objectstore.compacted.TopicCompactionKeyEncodingV1;
import com.nereusstream.objectstore.staging.StagingFileManager;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TopicCompactionEngineTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void spillsSelectsLatestKeysRetainsUnkeyedAndReplaysExactSurvivors() throws Exception {
        MaterializationTask task = topicTask();
        TrackingReader reader = new TrackingReader();
        DeterministicDecoder decoder = new DeterministicDecoder(false);
        TopicCompactionRegistry.Binding binding = new TopicCompactionRegistry(
                        List.of(decoder),
                        List.of(new RetainSelectedTombstone()))
                .resolve(task.policy().topicCompaction().orElseThrow());
        Path stagingPath = Files.createDirectory(temporaryDirectory.resolve("staging"));
        Files.setPosixFilePermissions(stagingPath, PosixFilePermissions.fromString("rwx------"));
        try (StagingFileManager staging = new StagingFileManager(
                stagingPath,
                32L << 20,
                StagingFileManager.MIN_UPLOAD_CHUNK_BYTES,
                Duration.ofHours(1),
                Runnable::run)) {
            DefaultTopicCompactionEngine engine = new DefaultTopicCompactionEngine(
                    staging,
                    DefaultTopicCompactionEngine.MIN_IN_MEMORY_KEY_BYTES,
                    32 << 10,
                    2,
                    Runnable::run);

            TopicCompactionPlan plan = engine.prepare(
                            task,
                            reader,
                            readOptions(),
                            binding,
                            5_000)
                    .join();
            assertThat(plan.outputRecordCount()).isEqualTo(5);
            assertThat(staging.reservedBytes()).isZero();

            List<CompactedObjectRow> rows = collect(plan).join();

            assertThat(rows).extracting(CompactedObjectRow::streamOffset)
                    .containsExactly(1L, 3L, 4L, 6L, 7L);
            assertThat(rows).extracting(row -> row.sparseDisposition().orElseThrow())
                    .containsExactly(1, 1, 2, 1, 1);
            assertThat(rows.get(2).exactPayload().remaining()).isZero();
            assertThat(TopicCompactionKeyEncodingV1.decode(
                            rows.get(0).compactionKey().orElseThrow()))
                    .isEqualTo(new TopicCompactionKeyEncodingV1.DecodedKey.Unkeyed(1));
            assertThat(TopicCompactionKeyEncodingV1.decode(
                            rows.get(3).compactionKey().orElseThrow()))
                    .isEqualTo(new TopicCompactionKeyEncodingV1.DecodedKey.Unkeyed(6));
            assertThat(keyedBytes(rows.get(1))).startsWith((byte) 'a');
            assertThat(reader.opened).hasValue(4);
            assertThat(reader.maxActive).hasValue(1);
            assertThat(staging.reservedBytes()).isZero();
            plan.close();
        }
    }

    @Test
    void failsClosedWhenDecoderFactsChangeBetweenPasses() throws Exception {
        MaterializationTask task = topicTask();
        TrackingReader reader = new TrackingReader();
        DeterministicDecoder decoder = new DeterministicDecoder(true);
        TopicCompactionRegistry.Binding binding = new TopicCompactionRegistry(
                        List.of(decoder),
                        List.of(new RetainSelectedTombstone()))
                .resolve(task.policy().topicCompaction().orElseThrow());
        Path stagingPath = Files.createDirectory(temporaryDirectory.resolve("changed-staging"));
        Files.setPosixFilePermissions(stagingPath, PosixFilePermissions.fromString("rwx------"));
        try (StagingFileManager staging = new StagingFileManager(
                stagingPath,
                32L << 20,
                StagingFileManager.MIN_UPLOAD_CHUNK_BYTES,
                Duration.ofHours(1),
                Runnable::run)) {
            TopicCompactionPlan plan = new DefaultTopicCompactionEngine(
                            staging,
                            DefaultTopicCompactionEngine.MIN_IN_MEMORY_KEY_BYTES,
                            32 << 10,
                            2,
                            Runnable::run)
                    .prepare(task, reader, readOptions(), binding, 5_000)
                    .join();

            assertThatThrownBy(() -> collect(plan).join())
                    .hasRootCauseMessage(
                            "topic-compaction pass two differs from the pass-one survivor proof");
            assertThat(staging.reservedBytes()).isZero();
            plan.close();
        }
    }

    @Test
    void rejectsDecodedKeysBeyondTheConfiguredCapAndReleasesSpillBudget() throws Exception {
        MaterializationTask task = topicTask();
        TopicCompactionRegistry.Binding binding = new TopicCompactionRegistry(
                        List.of(new DeterministicDecoder(false)),
                        List.of(new RetainSelectedTombstone()))
                .resolve(task.policy().topicCompaction().orElseThrow());
        Path stagingPath = Files.createDirectory(temporaryDirectory.resolve("oversized-staging"));
        Files.setPosixFilePermissions(stagingPath, PosixFilePermissions.fromString("rwx------"));
        try (StagingFileManager staging = new StagingFileManager(
                stagingPath,
                32L << 20,
                StagingFileManager.MIN_UPLOAD_CHUNK_BYTES,
                Duration.ofHours(1),
                Runnable::run)) {
            DefaultTopicCompactionEngine engine = new DefaultTopicCompactionEngine(
                    staging,
                    DefaultTopicCompactionEngine.MIN_IN_MEMORY_KEY_BYTES,
                    1_024,
                    2,
                    Runnable::run);

            assertThatThrownBy(() -> engine.prepare(
                            task,
                            new TrackingReader(),
                            readOptions(),
                            binding,
                            5_000)
                    .join())
                    .hasRootCauseMessage(
                            "decoded topic-compaction key exceeds the configured byte cap");
            assertThat(staging.reservedBytes()).isZero();
        }
    }

    private static MaterializationTask topicTask() {
        List<VersionedGenerationCandidate> sources = List.of(
                MaterializationPlannerTestSupport.zero("/topic/source-4", 0, 4, 0, 100, 4),
                MaterializationPlannerTestSupport.zero("/topic/source-8", 4, 8, 100, 100, 8));
        MaterializationPolicy policy = MaterializationPolicyFactory.topicCompacted(
                new TopicCompactionSpec("latest", 1, "test-key-v1"),
                2,
                16,
                1_000,
                1_000_000,
                128,
                "ZSTD");
        return MaterializationPlannerTestSupport.planner(sources, List.of(), 0, 8)
                .plan(STREAM, new OffsetRange(0, 8), policy, 1)
                .join()
                .get(0);
    }

    private static ReadOptions readOptions() {
        return new ReadOptions(
                1,
                64 << 10,
                ReadIsolation.COMMITTED,
                Duration.ofSeconds(10));
    }

    private static CompletableFuture<List<CompactedObjectRow>> collect(TopicCompactionPlan plan) {
        CompletableFuture<List<CompactedObjectRow>> result = new CompletableFuture<>();
        List<CompactedObjectRow> rows = new ArrayList<>();
        plan.subscribe(new Flow.Subscriber<>() {
            private Flow.Subscription subscription;

            @Override
            public void onSubscribe(Flow.Subscription value) {
                subscription = value;
                value.request(1);
            }

            @Override
            public void onNext(CompactedObjectRow item) {
                rows.add(item);
                subscription.request(1);
            }

            @Override
            public void onError(Throwable failure) {
                result.completeExceptionally(failure);
            }

            @Override
            public void onComplete() {
                result.complete(List.copyOf(rows));
            }
        });
        return result;
    }

    private static byte[] keyedBytes(CompactedObjectRow row) {
        var decoded = TopicCompactionKeyEncodingV1.decode(
                row.compactionKey().orElseThrow());
        var keyed = (TopicCompactionKeyEncodingV1.DecodedKey.Keyed) decoded;
        ByteBuffer key = keyed.decodedKey();
        byte[] bytes = new byte[key.remaining()];
        key.get(bytes);
        return bytes;
    }

    private static final class DeterministicDecoder implements TopicCompactionDecoder {
        private final boolean mutateSecondPass;
        private final AtomicInteger calls = new AtomicInteger();

        private DeterministicDecoder(boolean mutateSecondPass) {
            this.mutateSecondPass = mutateSecondPass;
        }

        @Override
        public String id() {
            return "test-key-v1";
        }

        @Override
        public Optional<CompactionRecord> decode(long offset, ByteBuffer exactPayload) {
            int call = calls.incrementAndGet();
            if (offset == 1 || offset == 6) {
                return Optional.empty();
            }
            char key = switch ((int) offset) {
                case 0, 3 -> 'a';
                case 2, 5 -> 'b';
                case 4 -> 'c';
                case 7 -> 'd';
                default -> throw new IllegalArgumentException("offset");
            };
            if (mutateSecondPass && call > 8 && offset == 7) {
                key = 'z';
            }
            byte[] largeKey = new byte[20 << 10];
            Arrays.fill(largeKey, (byte) key);
            CompactionDisposition disposition = offset == 4 || offset == 5
                    ? CompactionDisposition.TOMBSTONE
                    : CompactionDisposition.VALUE;
            return Optional.of(new CompactionRecord(
                    offset,
                    ByteBuffer.wrap(largeKey),
                    disposition,
                    OptionalLong.of(1_000 + offset),
                    OptionalLong.empty()));
        }
    }

    private record RetainSelectedTombstone() implements TopicCompactionStrategy {
        @Override
        public String id() {
            return "latest";
        }

        @Override
        public long version() {
            return 1;
        }

        @Override
        public boolean retainTombstone(CompactionRecord tombstone, long planningTimeMillis) {
            return tombstone.streamOffset() == 4 && planningTimeMillis == 5_000;
        }
    }

    private static final class TrackingReader implements ExactSourceRangeReader {
        private final AtomicInteger active = new AtomicInteger();
        private final AtomicInteger maxActive = new AtomicInteger();
        private final AtomicInteger opened = new AtomicInteger();

        @Override
        public CompletableFuture<ExactSourceRead> read(
                SourceGeneration source,
                ReadOptions options) {
            int now = active.incrementAndGet();
            opened.incrementAndGet();
            maxActive.accumulateAndGet(now, Math::max);
            return CompletableFuture.completedFuture(new ExactSourceRead() {
                private final CompletableFuture<ExactSourceReadSummary> completion =
                        new CompletableFuture<>();

                @Override
                public SourceGeneration source() {
                    return source;
                }

                @Override
                public Flow.Publisher<ReadBatch> batches() {
                    return subscriber -> subscriber.onSubscribe(new Flow.Subscription() {
                        private long cursor = source.range().startOffset();
                        private boolean terminal;

                        @Override
                        public void request(long count) {
                            if (terminal) {
                                return;
                            }
                            if (count <= 0) {
                                terminal = true;
                                subscriber.onError(new IllegalArgumentException("demand"));
                                return;
                            }
                            long emitted = 0;
                            while (!terminal
                                    && emitted < count
                                    && cursor < source.range().endOffset()) {
                                ObjectSliceReadTarget target =
                                        (ObjectSliceReadTarget) source.readTarget();
                                subscriber.onNext(new ReadBatch(
                                        new OffsetRange(cursor, cursor + 1),
                                        source.payloadFormat(),
                                        new byte[] {(byte) cursor},
                                        source.schemaRefs(),
                                        target.entryIndexRef(),
                                        source.projectionRef(),
                                        target.objectId(),
                                        target.objectOffset(),
                                        target.objectLength()));
                                cursor++;
                                emitted++;
                            }
                            if (!terminal && cursor == source.range().endOffset()) {
                                terminal = true;
                                active.decrementAndGet();
                                completion.complete(new ExactSourceReadSummary(
                                        source.range(),
                                        source.recordCount(),
                                        source.entryCount(),
                                        source.logicalBytes(),
                                        new Checksum(ChecksumType.SHA256, "a".repeat(64))));
                                subscriber.onComplete();
                            }
                        }

                        @Override
                        public void cancel() {
                            if (!terminal) {
                                terminal = true;
                                active.decrementAndGet();
                            }
                        }
                    });
                }

                @Override
                public CompletableFuture<ExactSourceReadSummary> completion() {
                    return completion;
                }

                @Override
                public void close() {
                }
            });
        }
    }
}
