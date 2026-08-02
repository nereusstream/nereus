/* Licensed under the Apache License, Version 2.0 */

package com.nereusstream.kafka.compaction;

import static com.nereusstream.kafka.codec.KafkaRecordBatchTestSupport.bytes;
import static com.nereusstream.kafka.codec.KafkaRecordBatchTestSupport.readBatch;
import static org.assertj.core.api.Assertions.assertThat;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadIsolation;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.ReadSourceRef;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState.LogConfigHistoryEntry;
import com.nereusstream.kafka.compaction.KafkaCompactionPassOneCollector.Snapshot;
import com.nereusstream.kafka.compaction.KafkaCompactionPlanner.Candidate;
import com.nereusstream.kafka.compaction.KafkaCompactionPlanner.Policy;
import com.nereusstream.kafka.compaction.KafkaCompactionTwoPassExecutor.Limits;
import com.nereusstream.materialization.ExactSourceRangeReader;
import com.nereusstream.materialization.ExactSourceRead;
import com.nereusstream.materialization.ExactSourceReadSummary;
import com.nereusstream.materialization.ExactSourceSet;
import com.nereusstream.materialization.MaterializationPolicy;
import com.nereusstream.materialization.MaterializationPolicyFactory;
import com.nereusstream.materialization.MaterializationTask;
import com.nereusstream.materialization.SourceGeneration;
import com.nereusstream.materialization.TopicCompactionSpec;
import com.nereusstream.objectstore.compacted.KafkaTopicCompactedObjectRow;
import com.nereusstream.objectstore.staging.StagingFileManager;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * F9-M7 exact 128-source / 1,048,576-record NTC2 materialization boundary.
 *
 * <p>Only one 8,192-record Kafka batch is resident while a source is generated. The production
 * executor performs two independent cold passes, spills its winner index, writes a demand-driven
 * NTC2 row spool, and releases every source pin and staging reservation on terminal consumption.
 */
class KafkaMaterializationScaleIntegrationTest {
    private static final int SOURCE_COUNT = 128;
    private static final int RECORD_COUNT = 1_048_576;
    private static final int RECORDS_PER_SOURCE = RECORD_COUNT / SOURCE_COUNT;
    private static final int DISTINCT_KEYS = 2_048;
    private static final long WINNER_MEMORY_BYTES = 64L << 10;

    @TempDir
    Path temporaryDirectory;

    @Test
    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    void scenarioKfScl005() throws Exception {
        assertThat(RECORD_COUNT % SOURCE_COUNT).isZero();
        List<ReadBatch> batches = batches();
        ExactSourceSet sources = sourceSet(batches);
        MaterializationPolicy policy = MaterializationPolicyFactory.kafkaTopicCompacted(
                new TopicCompactionSpec(
                        KafkaCompactionStrategyV1.STRATEGY_ID, KafkaCompactionStrategyV1.STRATEGY_VERSION, "KCK2"),
                2,
                SOURCE_COUNT,
                RECORD_COUNT,
                64L << 20,
                8_192,
                "UNCOMPRESSED");
        MaterializationTask task = MaterializationTask.create(
                new StreamId("stream-f9-kafka-scale-materialization"), sources.coverage(), sources.sources(), policy);
        Policy kafkaPolicy = new Policy(
                1,
                new Checksum(ChecksumType.SHA256, "e".repeat(64)),
                0,
                60_000,
                1_000,
                LogConfigHistoryEntry.CLEANUP_COMPACT_FLAG);
        Candidate candidate =
                new Candidate(sources.coverage(), sources.coverage(), 1, kafkaPolicy, Optional.empty(), 1_000);
        Snapshot snapshot = new Snapshot(
                sources.coverage(),
                sources.coverage(),
                RECORD_COUNT,
                candidate.evaluatedAtMillis(),
                kafkaPolicy.deleteRetentionMs(),
                RECORD_COUNT,
                64,
                WINNER_MEMORY_BYTES,
                List.of(),
                List.of(),
                List.of());
        KafkaCompactionPlan plan =
                KafkaCompactionPlan.create(task, 17, RECORD_COUNT, RECORD_COUNT, candidate, sources, snapshot);
        AtomicInteger sourceCloses = new AtomicInteger();
        Map<Long, ReadBatch> batchByStart = new HashMap<>(SOURCE_COUNT);
        batches.forEach(batch -> batchByStart.put(batch.range().startOffset(), batch));
        ExactSourceRangeReader reader = exactReader(batchByStart, sourceCloses);
        Path stagingPath = Files.createDirectory(temporaryDirectory.resolve("staging"));
        Files.setPosixFilePermissions(stagingPath, PosixFilePermissions.fromString("rwx------"));

        try (StagingFileManager staging = new StagingFileManager(
                stagingPath,
                256L << 20,
                StagingFileManager.MIN_UPLOAD_CHUNK_BYTES,
                Duration.ofHours(1),
                Runnable::run)) {
            KafkaCompactionStreamingExecutor executor = new KafkaCompactionStreamingExecutor(
                    new KafkaTopicCompactionCodecV1(),
                    new KafkaCompactionStrategyV1(),
                    new KafkaCompactionRowMapper(),
                    new Limits(SOURCE_COUNT, DISTINCT_KEYS + 1, 64L << 20),
                    staging,
                    Runnable::run);
            KafkaCompactionBatchSource batchSource = new KafkaCompactionBatchSource(
                    reader,
                    new ReadOptions(RECORDS_PER_SOURCE, 16 << 20, ReadIsolation.COMMITTED, Duration.ofMinutes(1)),
                    Runnable::run);
            KafkaCompactionStreamingExecutor.StreamingResult result =
                    executor.execute(plan, batchSource.open(plan), false).get(2, TimeUnit.MINUTES);
            try (result) {
                assertThat(task.coverage().recordCount()).isEqualTo(RECORD_COUNT);
                assertThat(task.sources()).hasSize(SOURCE_COUNT);
                assertThat(plan.decisionSources().sources()).hasSize(SOURCE_COUNT);
                assertThat(plan.outputSources().sources()).hasSize(SOURCE_COUNT);
                assertThat(result.decisionSourceBatchCount()).isEqualTo(SOURCE_COUNT);
                assertThat(result.outputSourceBatchCount()).isEqualTo(SOURCE_COUNT);
                assertThat(result.outputCoverage().recordCount()).isEqualTo(RECORD_COUNT);
                assertThat(result.decisionHorizon().recordCount()).isEqualTo(RECORD_COUNT);
                assertThat(result.outputRecordCount()).isEqualTo(DISTINCT_KEYS);
                assertThat(result.outputBatchCount()).isEqualTo(DISTINCT_KEYS);
                assertThat(result.spillRunCount()).isPositive();
                assertThat(result.peakInMemoryKeyBytes()).isLessThanOrEqualTo(WINNER_MEMORY_BYTES);
                assertThat(staging.reservedBytes()).isPositive();

                RowAccounting rows = consume(result.rows()).get(30, TimeUnit.SECONDS);
                assertThat(rows.rows()).isEqualTo(DISTINCT_KEYS);
                assertThat(rows.records()).isEqualTo(DISTINCT_KEYS);
                assertThat(rows.firstOffset()).isZero();
                assertThat(rows.lastOffset()).isEqualTo(RECORD_COUNT - 1L);
            }
            assertThat(sourceCloses).hasValue(SOURCE_COUNT * 2);
            assertThat(staging.reservedBytes()).isZero();
        }
    }

    private static List<ReadBatch> batches() {
        ArrayList<ReadBatch> result = new ArrayList<>(SOURCE_COUNT);
        byte[][] keys = new byte[DISTINCT_KEYS][];
        for (int key = 0; key < keys.length; key++) {
            keys[key] = ByteBuffer.allocate(Integer.BYTES).putInt(key).array();
        }
        byte[] value = {(byte) 1};
        for (int source = 0; source < SOURCE_COUNT; source++) {
            long baseOffset = (long) source * RECORDS_PER_SOURCE;
            SimpleRecord[] records = new SimpleRecord[RECORDS_PER_SOURCE];
            for (int index = 0; index < records.length; index++) {
                long offset = baseOffset + index;
                int key = offset < DISTINCT_KEYS ? Math.toIntExact(offset) : DISTINCT_KEYS - 1;
                records[index] = new SimpleRecord(1_000 + offset, keys[key], value);
            }
            byte[] payload = bytes(MemoryRecords.withRecords(baseOffset, Compression.NONE, records));
            result.add(readBatch(
                    new OffsetRange(baseOffset, baseOffset + RECORDS_PER_SOURCE), payload, "scale-" + source));
        }
        return List.copyOf(result);
    }

    private static ExactSourceSet sourceSet(List<ReadBatch> batches) {
        ArrayList<SourceGeneration> sources = new ArrayList<>(SOURCE_COUNT);
        long cumulativeBytes = 0;
        for (ReadBatch batch : batches) {
            long nextCumulative = Math.addExact(cumulativeBytes, batch.payload().length);
            sources.add(new SourceGeneration(
                    ReadView.COMMITTED,
                    batch.range(),
                    batch.source().generation(),
                    batch.source().commitVersion(),
                    "f9/kafka/scale/" + batch.range().startOffset(),
                    batch.range().startOffset(),
                    new Checksum(ChecksumType.SHA256, "d".repeat(64)),
                    batch.source().target(),
                    batch.source().targetIdentity(),
                    Optional.empty(),
                    batch.payloadFormat(),
                    batch.projectionRef(),
                    Math.toIntExact(batch.range().recordCount()),
                    1,
                    batch.payload().length,
                    batch.schemaRefs(),
                    cumulativeBytes,
                    nextCumulative));
            cumulativeBytes = nextCumulative;
        }
        return ExactSourceSet.create(ReadView.COMMITTED, new OffsetRange(0, RECORD_COUNT), sources);
    }

    private static ExactSourceRangeReader exactReader(Map<Long, ReadBatch> batches, AtomicInteger closes) {
        return (source, options) -> {
            ReadBatch original = Optional.ofNullable(batches.get(source.range().startOffset()))
                    .orElseThrow();
            ReadBatch exact = new ReadBatch(
                    source.range(),
                    source.payloadFormat(),
                    original.payload(),
                    source.schemaRefs(),
                    source.projectionRef(),
                    new ReadSourceRef(
                            source.range(),
                            source.generation(),
                            source.commitVersion(),
                            source.readTarget(),
                            source.targetIdentitySha256()));
            return CompletableFuture.completedFuture(new ExactSourceRead() {
                @Override
                public SourceGeneration source() {
                    return source;
                }

                @Override
                public Flow.Publisher<ReadBatch> batches() {
                    return one(exact);
                }

                @Override
                public CompletableFuture<ExactSourceReadSummary> completion() {
                    return CompletableFuture.completedFuture(new ExactSourceReadSummary(
                            source.range(),
                            source.recordCount(),
                            source.entryCount(),
                            source.logicalBytes(),
                            new Checksum(ChecksumType.SHA256, "f".repeat(64))));
                }

                @Override
                public void close() {
                    closes.incrementAndGet();
                }
            });
        };
    }

    private static <T> Flow.Publisher<T> one(T value) {
        return subscriber -> subscriber.onSubscribe(new Flow.Subscription() {
            private boolean terminal;

            @Override
            public void request(long count) {
                if (terminal) {
                    return;
                }
                terminal = true;
                if (count <= 0) {
                    subscriber.onError(new IllegalArgumentException("demand"));
                    return;
                }
                subscriber.onNext(value);
                subscriber.onComplete();
            }

            @Override
            public void cancel() {
                terminal = true;
            }
        });
    }

    private static CompletableFuture<RowAccounting> consume(Flow.Publisher<KafkaTopicCompactedObjectRow> publisher) {
        CompletableFuture<RowAccounting> result = new CompletableFuture<>();
        publisher.subscribe(new Flow.Subscriber<>() {
            private final AtomicInteger rows = new AtomicInteger();
            private final AtomicLong records = new AtomicLong();
            private long firstOffset = -1;
            private long previousOffset = -1;

            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                subscription.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(KafkaTopicCompactedObjectRow row) {
                if (firstOffset < 0) {
                    firstOffset = row.streamOffsetStart();
                }
                assertThat(row.streamOffsetStart()).isGreaterThan(previousOffset);
                previousOffset = row.streamOffsetStart();
                rows.incrementAndGet();
                records.addAndGet(row.recordCount());
            }

            @Override
            public void onError(Throwable failure) {
                result.completeExceptionally(failure);
            }

            @Override
            public void onComplete() {
                result.complete(new RowAccounting(rows.get(), records.get(), firstOffset, previousOffset));
            }
        });
        return result;
    }

    private record RowAccounting(int rows, long records, long firstOffset, long lastOffset) {}
}
