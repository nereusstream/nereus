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
import static com.nereusstream.kafka.compaction.KafkaCompactionStrategyV1.MarkerStatus.DELETE_ELIGIBLE;
import static com.nereusstream.kafka.compaction.KafkaCompactionStrategyV1.MarkerStatus.RETAIN_REQUIRED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import com.nereusstream.kafka.compaction.KafkaCompactionPassOneCollector.MarkerDecision;
import com.nereusstream.kafka.compaction.KafkaCompactionPassOneCollector.Snapshot;
import com.nereusstream.kafka.compaction.KafkaCompactionTwoPassExecutor.Limits;
import com.nereusstream.kafka.compaction.KafkaCompactionTwoPassExecutor.Result;
import com.nereusstream.kafka.compaction.KafkaCompactionWriteRequestFactory.Input;
import com.nereusstream.materialization.ExactSourceSet;
import com.nereusstream.materialization.SourceGeneration;
import com.nereusstream.objectstore.compacted.KafkaCompactionDispositionV2;
import com.nereusstream.objectstore.compacted.KafkaTopicCompactedObjectWriteRequest;
import com.nereusstream.objectstore.staging.StagingFileManager;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class KafkaCompactionTwoPassExecutorTest {
  @TempDir Path temporaryDirectory;

  @Test
  void composesFullHorizonWinnerSelectionWithVerifiedSparseNtc2Rows() {
    ReadBatch output =
        batch(
            0,
            2,
            "output",
            MemoryRecords.withRecords(
                0,
                Compression.gzip().build(),
                new SimpleRecord(1_000, utf8("k"), utf8("old")),
                new SimpleRecord(1_001, utf8("keep"), utf8("survivor"))));
    ReadBatch tail =
        batch(
            2,
            3,
            "tail",
            MemoryRecords.withRecords(
                2, Compression.NONE, new SimpleRecord(1_002, utf8("k"), utf8("new"))));
    KafkaCompactionTwoPassExecutor executor = executor(new Limits(10, 10, 1 << 20));

    Result result =
        executor
            .prepare(
                snapshot(0, 2, 3, List.of()),
                sourceSet(List.of(output, tail)),
                List.of(output, tail))
            .rewrite(sourceSet(List.of(output)), List.of(output), false);

    assertThat(result.decisionSourceBatchCount()).isEqualTo(2);
    assertThat(result.outputSourceBatchCount()).isEqualTo(1);
    assertThat(result.outputRecordCount()).isEqualTo(1);
    assertThat(result.outputBatchCount()).isEqualTo(1);
    assertThat(result.logicalBytes()).isPositive();
    assertThat(result.rows()).hasSize(1);
    assertThat(result.rows().get(0).streamOffsetStart()).isEqualTo(1);
    assertThat(result.rows().get(0).disposition())
        .isEqualTo(KafkaCompactionDispositionV2.RETAIN_VALUE);
    RecordBatch rewritten =
        MemoryRecords.readableRecords(result.rows().get(0).exactPayload())
            .batches()
            .iterator()
            .next();
    assertThat(rewritten.baseOffset()).isEqualTo(1);
    assertThat(rewritten.lastOffset()).isEqualTo(1);
    assertThat(rewritten.isValid()).isTrue();

    KafkaTopicCompactedObjectWriteRequest request =
        new KafkaCompactionWriteRequestFactory()
            .create(
                new Input(
                    "test-cluster",
                    new StreamId("stream-two-pass"),
                    new OffsetRange(0, 2),
                    "a".repeat(26),
                    new Checksum(ChecksumType.SHA256, "e".repeat(64)),
                    1 << 20,
                    1_024,
                    "UNCOMPRESSED",
                    "test-build"),
                result);
    assertThat(request.sourceSetSha256()).isEqualTo(result.outputSourceSetSha256());
    assertThat(request.outputRecordCount()).isEqualTo(1);
    assertThat(request.topicCompaction().strategyId())
        .isEqualTo(KafkaCompactionStrategyV1.STRATEGY_ID);
    assertThat(request.topicCompaction().keyCodecId()).isEqualTo("KCK2");
    assertThat(request.topicCompaction().sourceBatchCount()).isEqualTo(1);

    assertThatThrownBy(
            () ->
                new KafkaCompactionWriteRequestFactory()
                    .create(
                        new Input(
                            "test-cluster",
                            new StreamId("stream-two-pass"),
                            new OffsetRange(0, 1),
                            "a".repeat(26),
                            new Checksum(ChecksumType.SHA256, "e".repeat(64)),
                            1 << 20,
                            1_024,
                            "UNCOMPRESSED",
                            "test-build"),
                        result))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("coverage");
  }

  @Test
  void dropsNullKeyRecordsLikeStockLogCleaner() {
    ReadBatch output =
        batch(
            0,
            1,
            "null-key-output",
            MemoryRecords.withRecords(
                0,
                Compression.NONE,
                new SimpleRecord(1_000, null, utf8("unkeyed"))));
    KafkaCompactionTwoPassExecutor executor = executor(new Limits(10, 10, 1 << 20));

    Result result =
        executor
            .prepare(snapshot(0, 1, 1, List.of()), sourceSet(List.of(output)), List.of(output))
            .rewrite(sourceSet(List.of(output)), List.of(output), false);

    assertThat(result.outputRecordCount()).isZero();
    assertThat(result.rows()).isEmpty();
  }

  @Test
  void productionExecutorUsesTheSharedChecksummedWinnerSpill() throws Exception {
    ReadBatch old =
        batch(
            0,
            1,
            "old",
            MemoryRecords.withRecords(
                0, Compression.NONE, new SimpleRecord(1_000, utf8("k"), utf8("old"))));
    ReadBatch latest =
        batch(
            1,
            2,
            "latest",
            MemoryRecords.withRecords(
                1, Compression.NONE, new SimpleRecord(1_001, utf8("k"), utf8("latest"))));
    Path stagingPath = Files.createDirectory(temporaryDirectory.resolve("winner-spill"));
    Files.setPosixFilePermissions(stagingPath, PosixFilePermissions.fromString("rwx------"));
    try (StagingFileManager staging =
        new StagingFileManager(
            stagingPath,
            32L << 20,
            StagingFileManager.MIN_UPLOAD_CHUNK_BYTES,
            Duration.ofHours(1),
            Runnable::run)) {
      KafkaCompactionTwoPassExecutor executor =
          new KafkaCompactionTwoPassExecutor(
              new KafkaTopicCompactionCodecV1(),
              new KafkaCompactionStrategyV1(),
              new KafkaCompactionRowMapper(),
              new Limits(10, 10, 1 << 20),
              staging);

      KafkaCompactionTwoPassExecutor.Prepared prepared =
          executor.prepare(
              snapshot(0, 2, 2, List.of(), 1),
              sourceSet(List.of(old, latest)),
              List.of(old, latest));

      assertThat(prepared.facts().spillRunCount()).isPositive();
      assertThat(staging.reservedBytes()).isZero();
      assertThat(
              prepared.rewrite(sourceSet(List.of(old, latest)), List.of(old, latest), false).rows())
          .extracting(row -> row.streamOffsetStart())
          .containsExactly(1L);
    }
  }

  @Test
  void rewritesFirstPassTombstoneAndMarkerWithTheFrozenDeleteHorizon() {
    ReadBatch tombstone =
        batch(
            10,
            11,
            "tombstone",
            MemoryRecords.withRecords(
                10, Compression.NONE, new SimpleRecord(2_000, utf8("deleted"), null)));
    ReadBatch marker =
        batch(
            11,
            12,
            "marker",
            MemoryRecords.withEndTransactionMarker(
                11, 2_001, 3, 9, (short) 2, new EndTransactionMarker(ControlRecordType.ABORT, 4)));

    Result result =
        executor(new Limits(10, 10, 1 << 20))
            .prepare(
                snapshot(10, 12, 12, List.of(new MarkerDecision(11, DELETE_ELIGIBLE))),
                sourceSet(List.of(tombstone, marker)),
                List.of(tombstone, marker))
            .rewrite(sourceSet(List.of(tombstone, marker)), List.of(tombstone, marker), false);

    assertThat(result.rows())
        .extracting(row -> row.disposition())
        .containsExactly(
            KafkaCompactionDispositionV2.RETAIN_TOMBSTONE,
            KafkaCompactionDispositionV2.RETAIN_CONTROL);
    assertThat(result.rows())
        .allSatisfy(
            row ->
                assertThat(
                        MemoryRecords.readableRecords(row.exactPayload())
                            .batches()
                            .iterator()
                            .next()
                            .deleteHorizonMs())
                    .hasValue(1_100));
  }

  @Test
  void doesNotIntroduceDeleteHorizonForARequiredControlMarker() {
    ReadBatch marker =
        batch(
            12,
            13,
            "required-marker",
            MemoryRecords.withEndTransactionMarker(
                12, 2_002, 3, 10, (short) 2, new EndTransactionMarker(ControlRecordType.COMMIT, 4)));

    Result result =
        executor(new Limits(10, 10, 1 << 20))
            .prepare(
                snapshot(12, 13, 13, List.of(new MarkerDecision(12, RETAIN_REQUIRED))),
                sourceSet(List.of(marker)),
                List.of(marker))
            .rewrite(sourceSet(List.of(marker)), List.of(marker), false);

    RecordBatch rewritten =
        MemoryRecords.readableRecords(result.rows().get(0).exactPayload())
            .batches()
            .iterator()
            .next();
    assertThat(rewritten.deleteHorizonMs()).isEmpty();
  }

  @Test
  void rejectsPassTwoSourceDriftBeforeReturningRows() {
    ReadBatch original =
        batch(
            20,
            21,
            "original",
            MemoryRecords.withRecords(
                20, Compression.NONE, new SimpleRecord(3_000, utf8("k"), utf8("one"))));
    ReadBatch changed =
        changedPayload(
            original,
            MemoryRecords.withRecords(
                20, Compression.NONE, new SimpleRecord(3_001, utf8("k"), utf8("two"))));
    KafkaCompactionTwoPassExecutor.Prepared prepared =
        executor(new Limits(10, 10, 1 << 20))
            .prepare(
                snapshot(20, 21, 21, List.of()), sourceSet(List.of(original)), List.of(original));

    assertThatThrownBy(
            () -> prepared.rewrite(sourceSet(List.of(original)), List.of(changed), false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("differ");
  }

  @Test
  void enforcesOutputBudgetsBeforeProducingAResult() {
    ReadBatch source =
        batch(
            30,
            31,
            "limited",
            MemoryRecords.withRecords(
                30, Compression.NONE, new SimpleRecord(4_000, utf8("k"), utf8("value"))));

    assertThatThrownBy(
            () ->
                executor(new Limits(10, 10, 1))
                    .prepare(
                        snapshot(30, 31, 31, List.of()),
                        sourceSet(List.of(source)),
                        List.of(source))
                    .rewrite(sourceSet(List.of(source)), List.of(source), false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("output-byte limit");
  }

  @Test
  void rejectsAReResolvedOutputTargetEvenWhenItsLogicalBytesAreIdentical() {
    MemoryRecords records =
        MemoryRecords.withRecords(
            40, Compression.NONE, new SimpleRecord(5_000, utf8("k"), utf8("value")));
    ReadBatch original = batch(40, 41, "frozen-target", records);
    ReadBatch replacement = batch(40, 41, "replacement-target", records);
    KafkaCompactionTwoPassExecutor.Prepared prepared =
        executor(new Limits(10, 10, 1 << 20))
            .prepare(
                snapshot(40, 41, 41, List.of()), sourceSet(List.of(original)), List.of(original));

    assertThatThrownBy(
            () -> prepared.rewrite(sourceSet(List.of(replacement)), List.of(replacement), false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("exact decision-source prefix");
  }

  private static KafkaCompactionTwoPassExecutor executor(Limits limits) {
    return new KafkaCompactionTwoPassExecutor(
        new KafkaTopicCompactionCodecV1(),
        new KafkaCompactionStrategyV1(),
        new KafkaCompactionRowMapper(),
        limits);
  }

  private static Snapshot snapshot(
      long start, long outputEnd, long horizonEnd, List<MarkerDecision> markers) {
    return snapshot(start, outputEnd, horizonEnd, markers, 1 << 20);
  }

  private static Snapshot snapshot(
      long start,
      long outputEnd,
      long horizonEnd,
      List<MarkerDecision> markers,
      long maxInMemoryKeyBytes) {
    return new Snapshot(
        new OffsetRange(start, outputEnd),
        new OffsetRange(start, horizonEnd),
        horizonEnd,
        100,
        1_000,
        100,
        1 << 20,
        maxInMemoryKeyBytes,
        List.of(),
        List.of(),
        markers);
  }

  private static ReadBatch batch(long start, long end, String suffix, MemoryRecords records) {
    return readBatch(new OffsetRange(start, end), bytes(records), suffix);
  }

  private static ReadBatch changedPayload(ReadBatch original, MemoryRecords records) {
    return new ReadBatch(
        original.range(),
        original.payloadFormat(),
        bytes(records),
        original.schemaRefs(),
        original.projectionRef(),
        original.source());
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
              "test/f9-compaction/" + batch.range().startOffset(),
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

  private static byte[] utf8(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }
}
