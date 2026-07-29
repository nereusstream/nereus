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

package com.nereusstream.kafka.compaction;

import static com.nereusstream.kafka.codec.KafkaRecordBatchTestSupport.bytes;
import static com.nereusstream.kafka.codec.KafkaRecordBatchTestSupport.readBatch;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
import com.nereusstream.kafka.compaction.KafkaCompactionWriteRequestFactory.Input;
import com.nereusstream.materialization.ExactSourceRangeReader;
import com.nereusstream.materialization.ExactSourceRead;
import com.nereusstream.materialization.ExactSourceReadSummary;
import com.nereusstream.materialization.ExactSourceSet;
import com.nereusstream.materialization.MaterializationPolicy;
import com.nereusstream.materialization.MaterializationPolicyFactory;
import com.nereusstream.materialization.MaterializationTask;
import com.nereusstream.materialization.SourceGeneration;
import com.nereusstream.materialization.TopicCompactionSpec;
import com.nereusstream.objectstore.PutObjectOptions;
import com.nereusstream.objectstore.compacted.ParquetKafkaTopicCompactedReader;
import com.nereusstream.objectstore.compacted.ParquetKafkaTopicCompactedWriter;
import com.nereusstream.objectstore.compacted.ParquetRangedCompactedObjectReader;
import com.nereusstream.objectstore.compacted.RangedCompactedObjectVerificationRequest;
import com.nereusstream.objectstore.compacted.RangedCompactedObjectVerifier;
import com.nereusstream.objectstore.compacted.RangedCompactedObjectWriteResult;
import com.nereusstream.objectstore.staging.StagingFileManager;
import com.nereusstream.objectstore.testing.LocalFileObjectStore;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class KafkaCompactionStreamingExecutorTest {
  @TempDir java.nio.file.Path temporaryDirectory;

  @Test
  void exactTwoPassSpoolStreamsTheSameFullyVerifiedNtc2BytesAsTheReferenceExecutor()
      throws Exception {
    Fixture fixture = fixture();
    AtomicInteger closes = new AtomicInteger();
    KafkaCompactionBatchSource batchSource =
        new KafkaCompactionBatchSource(exactReader(fixture, closes), readOptions(), Runnable::run);
    try (StagingFileManager staging = staging();
        LocalFileObjectStore store =
            new LocalFileObjectStore(temporaryDirectory.resolve("objects"))) {
      KafkaCompactionStreamingExecutor streamingExecutor =
          new KafkaCompactionStreamingExecutor(
              new KafkaTopicCompactionCodecV1(),
              new KafkaCompactionStrategyV1(),
              new KafkaCompactionRowMapper(),
              new Limits(10, 10, 1 << 20),
              staging,
              Runnable::run);
      ParquetKafkaTopicCompactedWriter writer =
          new ParquetKafkaTopicCompactedWriter(staging, Runnable::run);
      KafkaCompactionParquetPublisher publisher =
          new KafkaCompactionParquetPublisher(
              streamingExecutor, new KafkaCompactionWriteRequestFactory(), writer);
      Input input =
          new Input(
              "test-cluster",
              fixture.plan().streamId(),
              fixture.plan().candidate().outputCoverage(),
              "a".repeat(26),
              fixture.outputTask().policyDigestSha256(),
              1 << 20,
              128,
              "UNCOMPRESSED",
              "test-build");

      KafkaCompactionParquetPublisher.PreparedObject streamed =
          publisher.prepare(fixture.plan(), batchSource.open(fixture.plan()), input, false).join();
      assertThat(closes).hasValue(3);
      assertThat(streamed.request().outputRecordCount()).isEqualTo(1);
      assertThat(streamed.request().entryCount()).isEqualTo(1);
      assertThat(streamed.evidence().spillRunCount()).isPositive();

      KafkaCompactionTwoPassExecutor.Result reference =
          new KafkaCompactionTwoPassExecutor(
                  new KafkaTopicCompactionCodecV1(),
                  new KafkaCompactionStrategyV1(),
                  new KafkaCompactionRowMapper(),
                  new Limits(10, 10, 1 << 20),
                  staging)
              .prepare(
                  fixture.plan().passOneSnapshot(),
                  fixture.plan().decisionSources(),
                  List.of(fixture.output(), fixture.tail()))
              .rewrite(fixture.plan().outputSources(), List.of(fixture.output()), false);
      var referenceRequest = new KafkaCompactionWriteRequestFactory().create(input, reference);
      try (RangedCompactedObjectWriteResult referenceWritten =
          writer.write(referenceRequest, listPublisher(reference.rows())).join()) {
        assertThat(streamed.written().contentSha256()).isEqualTo(referenceWritten.contentSha256());
      }

      store
          .putObject(
              streamed.written().objectKey(),
              streamed.written().stagingFile(),
              new PutObjectOptions(
                  "application/vnd.apache.parquet",
                  streamed.written().storageCrc32c(),
                  true,
                  Map.of(),
                  Duration.ofSeconds(10)))
          .join();
      RangedCompactedObjectVerifier verifier =
          new RangedCompactedObjectVerifier(
              store,
              new ParquetRangedCompactedObjectReader(store, Runnable::run),
              new ParquetKafkaTopicCompactedReader(store, Runnable::run));
      verifier
          .verifyExact(
              RangedCompactedObjectVerificationRequest.from(
                  streamed.request(), streamed.written(), Duration.ofSeconds(10)),
              streamed.request())
          .join();
      streamed.close();
      assertThat(staging.reservedBytes()).isZero();
    }
  }

  @Test
  void cancellationDuringDecisionPassClosesExactStreamsAndWinnerSpill() throws Exception {
    Fixture fixture = fixture();
    AtomicInteger closes = new AtomicInteger();
    KafkaCompactionBatchSource batchSource =
        new KafkaCompactionBatchSource(
            stallingDecisionReader(fixture, closes), readOptions(), Runnable::run);
    try (StagingFileManager staging = staging()) {
      KafkaCompactionStreamingExecutor executor =
          new KafkaCompactionStreamingExecutor(
              new KafkaTopicCompactionCodecV1(),
              new KafkaCompactionStrategyV1(),
              new KafkaCompactionRowMapper(),
              new Limits(10, 10, 1 << 20),
              staging,
              Runnable::run);
      CompletableFuture<KafkaCompactionStreamingExecutor.StreamingResult> result =
          executor.execute(fixture.plan(), batchSource.open(fixture.plan()), false);
      assertThat(result).isNotDone();
      assertThat(staging.reservedBytes()).isPositive();

      result.cancel(true);

      assertThatThrownBy(result::join).isInstanceOf(CancellationException.class);
      assertThat(closes).hasPositiveValue();
      assertThat(staging.reservedBytes()).isZero();
    }
  }

  @Test
  void fullySupersededOutputProducesAnEmptyStrictlyReadableNtc2Object() throws Exception {
    Fixture fixture = fixture(false);
    AtomicInteger closes = new AtomicInteger();
    KafkaCompactionBatchSource batchSource =
        new KafkaCompactionBatchSource(exactReader(fixture, closes), readOptions(), Runnable::run);
    try (StagingFileManager staging = staging();
        LocalFileObjectStore store =
            new LocalFileObjectStore(temporaryDirectory.resolve("empty-objects"))) {
      KafkaCompactionParquetPublisher publisher =
          new KafkaCompactionParquetPublisher(
              new KafkaCompactionStreamingExecutor(
                  new KafkaTopicCompactionCodecV1(),
                  new KafkaCompactionStrategyV1(),
                  new KafkaCompactionRowMapper(),
                  new Limits(10, 10, 1 << 20),
                  staging,
                  Runnable::run),
              new KafkaCompactionWriteRequestFactory(),
              new ParquetKafkaTopicCompactedWriter(staging, Runnable::run));
      Input input =
          new Input(
              "test-cluster",
              fixture.plan().streamId(),
              fixture.plan().candidate().outputCoverage(),
              "b".repeat(26),
              fixture.outputTask().policyDigestSha256(),
              1 << 20,
              128,
              "UNCOMPRESSED",
              "test-build");

      KafkaCompactionParquetPublisher.PreparedObject prepared =
          publisher.prepare(fixture.plan(), batchSource.open(fixture.plan()), input, false).join();
      assertThat(prepared.request().outputRecordCount()).isZero();
      assertThat(prepared.request().entryCount()).isZero();
      assertThat(prepared.request().logicalBytes()).isZero();
      store
          .putObject(
              prepared.written().objectKey(),
              prepared.written().stagingFile(),
              new PutObjectOptions(
                  "application/vnd.apache.parquet",
                  prepared.written().storageCrc32c(),
                  true,
                  Map.of(),
                  Duration.ofSeconds(10)))
          .join();
      new RangedCompactedObjectVerifier(
              store,
              new ParquetRangedCompactedObjectReader(store, Runnable::run),
              new ParquetKafkaTopicCompactedReader(store, Runnable::run))
          .verifyExact(
              RangedCompactedObjectVerificationRequest.from(
                  prepared.request(), prepared.written(), Duration.ofSeconds(10)),
              prepared.request())
          .join();
      prepared.close();
      assertThat(staging.reservedBytes()).isZero();
    }
  }

  private Fixture fixture() {
    return fixture(true);
  }

  private Fixture fixture(boolean retainSurvivor) {
    ReadBatch output =
        readBatch(
            new OffsetRange(0, retainSurvivor ? 2 : 1),
            retainSurvivor
                ? bytes(
                    MemoryRecords.withRecords(
                        0,
                        Compression.NONE,
                        new SimpleRecord(1_000, "k".getBytes(), "old".getBytes()),
                        new SimpleRecord(
                            1_001, "keep".getBytes(), "survivor".getBytes())))
                : bytes(
                    MemoryRecords.withRecords(
                        0,
                        Compression.NONE,
                        new SimpleRecord(1_000, "k".getBytes(), "old".getBytes()))),
            "stream-output");
    long tailOffset = output.range().endOffset();
    ReadBatch tail =
        readBatch(
            new OffsetRange(tailOffset, tailOffset + 1),
            bytes(
                MemoryRecords.withRecords(
                    tailOffset,
                    Compression.NONE,
                    new SimpleRecord(1_002, "k".getBytes(), "new".getBytes()))),
            "stream-tail");
    ExactSourceSet outputSources = sourceSet(List.of(output));
    ExactSourceSet decisionSources = sourceSet(List.of(output, tail));
    MaterializationPolicy materializationPolicy =
        MaterializationPolicyFactory.kafkaTopicCompacted(
            new TopicCompactionSpec(
                KafkaCompactionStrategyV1.STRATEGY_ID,
                KafkaCompactionStrategyV1.STRATEGY_VERSION,
                "KCK2"),
            2,
            128,
            1_048_576,
            1 << 20,
            1_024,
            "UNCOMPRESSED");
    MaterializationTask outputTask =
        MaterializationTask.create(
            new StreamId(
                retainSurvivor
                    ? "stream-compaction-streaming"
                    : "stream-compaction-empty-streaming"),
            output.range(),
            outputSources.sources(),
            materializationPolicy);
    Policy policy =
        new Policy(
            11,
            new Checksum(ChecksumType.SHA256, "e".repeat(64)),
            0,
            10_000,
            100,
            LogConfigHistoryEntry.CLEANUP_COMPACT_FLAG);
    Candidate candidate =
        new Candidate(
            output.range(),
            new OffsetRange(0, tail.range().endOffset()),
            1,
            policy,
            Optional.empty(),
            1_000);
    Snapshot snapshot =
        new Snapshot(
            candidate.outputCoverage(),
            candidate.decisionHorizon(),
            tail.range().endOffset(),
            candidate.evaluatedAtMillis(),
            policy.deleteRetentionMs(),
            100,
            1 << 20,
            1,
            List.of(),
            List.of(),
            List.of());
    KafkaCompactionPlan plan =
        KafkaCompactionPlan.create(
            outputTask,
            17,
            output.range().endOffset(),
            tail.range().endOffset(),
            candidate,
            decisionSources,
            snapshot);
    return new Fixture(outputTask, plan, output, tail);
  }

  private ExactSourceRangeReader exactReader(Fixture fixture, AtomicInteger closes) {
    return (source, options) -> {
      ReadBatch original = source.range().startOffset() == 0 ? fixture.output() : fixture.tail();
      ReadBatch exact =
          new ReadBatch(
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
      return CompletableFuture.completedFuture(
          new ExactSourceRead() {
            @Override
            public SourceGeneration source() {
              return source;
            }

            @Override
            public Flow.Publisher<ReadBatch> batches() {
              return listPublisher(List.of(exact));
            }

            @Override
            public CompletableFuture<ExactSourceReadSummary> completion() {
              return CompletableFuture.completedFuture(
                  new ExactSourceReadSummary(
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

  private ExactSourceRangeReader stallingDecisionReader(Fixture fixture, AtomicInteger closes) {
    return (source, options) -> {
      ReadBatch original = source.range().startOffset() == 0 ? fixture.output() : fixture.tail();
      ReadBatch exact = exactBatch(source, original);
      return CompletableFuture.completedFuture(
          new ExactSourceRead() {
            @Override
            public SourceGeneration source() {
              return source;
            }

            @Override
            public Flow.Publisher<ReadBatch> batches() {
              return subscriber ->
                  subscriber.onSubscribe(
                      new Flow.Subscription() {
                        private boolean emitted;
                        private boolean cancelled;

                        @Override
                        public void request(long count) {
                          if (!cancelled && !emitted && count > 0) {
                            emitted = true;
                            subscriber.onNext(exact);
                          }
                        }

                        @Override
                        public void cancel() {
                          cancelled = true;
                        }
                      });
            }

            @Override
            public CompletableFuture<ExactSourceReadSummary> completion() {
              return new CompletableFuture<>();
            }

            @Override
            public void close() {
              closes.incrementAndGet();
            }
          });
    };
  }

  private static ReadBatch exactBatch(SourceGeneration source, ReadBatch original) {
    return new ReadBatch(
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
  }

  private StagingFileManager staging() throws Exception {
    java.nio.file.Path path =
        java.nio.file.Files.createDirectory(temporaryDirectory.resolve("staging"));
    java.nio.file.Files.setPosixFilePermissions(
        path, java.nio.file.attribute.PosixFilePermissions.fromString("rwx------"));
    return new StagingFileManager(
        path,
        64L << 20,
        StagingFileManager.MIN_UPLOAD_CHUNK_BYTES,
        Duration.ofHours(1),
        Runnable::run);
  }

  private static ReadOptions readOptions() {
    return new ReadOptions(64, 1 << 20, ReadIsolation.COMMITTED, Duration.ofSeconds(10));
  }

  private static ExactSourceSet sourceSet(List<ReadBatch> batches) {
    ArrayList<SourceGeneration> sources = new ArrayList<>();
    long cumulativeBytes = 0;
    for (ReadBatch batch : batches) {
      long nextCumulative = Math.addExact(cumulativeBytes, batch.payload().length);
      sources.add(
          new SourceGeneration(
              ReadView.COMMITTED,
              batch.source().resolvedRange(),
              batch.source().generation(),
              batch.source().commitVersion(),
              "test/f9-compaction-streaming/" + batch.range().startOffset(),
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
    return ExactSourceSet.create(
        ReadView.COMMITTED,
        new OffsetRange(
            batches.get(0).range().startOffset(),
            batches.get(batches.size() - 1).range().endOffset()),
        sources);
  }

  private static <T> Flow.Publisher<T> listPublisher(List<T> values) {
    return subscriber ->
        subscriber.onSubscribe(
            new Flow.Subscription() {
              private int index;
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
                while (!terminal && emitted < count && index < values.size()) {
                  subscriber.onNext(values.get(index++));
                  emitted++;
                }
                if (!terminal && index == values.size()) {
                  terminal = true;
                  subscriber.onComplete();
                }
              }

              @Override
              public void cancel() {
                terminal = true;
              }
            });
  }

  private record Fixture(
      MaterializationTask outputTask, KafkaCompactionPlan plan, ReadBatch output, ReadBatch tail) {}
}
