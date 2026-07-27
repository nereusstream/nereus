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

import com.nereusstream.api.Checksum;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadView;
import com.nereusstream.kafka.compaction.KafkaCompactionPassOneCollector.Facts;
import com.nereusstream.kafka.compaction.KafkaCompactionPassOneCollector.PassTwoVerifier;
import com.nereusstream.kafka.compaction.KafkaCompactionPassOneCollector.Snapshot;
import com.nereusstream.materialization.CompactionRewriteContext;
import com.nereusstream.materialization.ExactSourceSet;
import com.nereusstream.materialization.ExactSourceSetVerifier;
import com.nereusstream.materialization.RewrittenCompactionRecord;
import com.nereusstream.objectstore.compacted.KafkaTopicCompactedObjectRow;
import com.nereusstream.objectstore.staging.StagingFileManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.kafka.common.record.RecordBatch;

/**
 * Deterministic in-memory composition of the F9 Kafka pass-one, pass-two, rewrite, and NTC2-row
 * boundaries.
 *
 * <p>The production constructor composes durable exact-source replay with checksum-verified sorted
 * spill. Streaming NTC2 upload and coverage activation remain workflow responsibilities.
 */
public final class KafkaCompactionTwoPassExecutor {
  private final KafkaTopicCompactionCodecV1 codec;
  private final KafkaCompactionStrategyV1 strategy;
  private final KafkaCompactionRowMapper rowMapper;
  private final Limits limits;
  private final StagingFileManager stagingFiles;

  public KafkaCompactionTwoPassExecutor(
      KafkaTopicCompactionCodecV1 codec,
      KafkaCompactionStrategyV1 strategy,
      KafkaCompactionRowMapper rowMapper,
      Limits limits) {
    this(codec, strategy, rowMapper, limits, null);
  }

  public KafkaCompactionTwoPassExecutor(
      KafkaTopicCompactionCodecV1 codec,
      KafkaCompactionStrategyV1 strategy,
      KafkaCompactionRowMapper rowMapper,
      Limits limits,
      StagingFileManager stagingFiles) {
    this.codec = Objects.requireNonNull(codec, "codec");
    this.strategy = Objects.requireNonNull(strategy, "strategy");
    this.rowMapper = Objects.requireNonNull(rowMapper, "rowMapper");
    this.limits = Objects.requireNonNull(limits, "limits");
    this.stagingFiles = stagingFiles;
  }

  public Prepared prepare(
      Snapshot snapshot,
      ExactSourceSet decisionHorizonSources,
      Iterable<ReadBatch> decisionHorizonBatches) {
    Objects.requireNonNull(snapshot, "snapshot");
    Objects.requireNonNull(decisionHorizonSources, "decisionHorizonSources");
    Objects.requireNonNull(decisionHorizonBatches, "decisionHorizonBatches");
    if (!decisionHorizonSources.coverage().equals(snapshot.decisionHorizon())) {
      throw new IllegalArgumentException(
          "Kafka compaction decision sources do not match the frozen horizon");
    }
    requireKafkaCommitted(decisionHorizonSources);
    ExactSourceSetVerifier sourceVerifier = new ExactSourceSetVerifier(decisionHorizonSources);
    try (KafkaCompactionPassOneCollector collector =
        stagingFiles == null
            ? new KafkaCompactionPassOneCollector(snapshot)
            : new KafkaCompactionPassOneCollector(snapshot, stagingFiles)) {
      long sourceBatches = 0;
      for (ReadBatch batch : decisionHorizonBatches) {
        sourceBatches = Math.addExact(sourceBatches, 1);
        if (sourceBatches > limits.maxSourceBatches()) {
          throw new IllegalArgumentException(
              "Kafka compaction decision horizon exceeded its source-batch limit");
        }
        ReadBatch exact = Objects.requireNonNull(batch, "decisionHorizonBatch");
        sourceVerifier.accept(exact);
        codec.decode(exact, collector::accept);
      }
      sourceVerifier.finish();
      return new Prepared(collector.finish(), decisionHorizonSources, sourceBatches);
    }
  }

  public final class Prepared {
    private final Facts facts;
    private final ExactSourceSet decisionHorizonSources;
    private final long decisionSourceBatchCount;

    private Prepared(
        Facts facts, ExactSourceSet decisionHorizonSources, long decisionSourceBatchCount) {
      this.facts = facts;
      this.decisionHorizonSources = decisionHorizonSources;
      this.decisionSourceBatchCount = decisionSourceBatchCount;
    }

    public Result rewrite(
        ExactSourceSet outputCoverageSources,
        Iterable<ReadBatch> outputCoverageBatches,
        boolean allowUncompressedFallback) {
      Objects.requireNonNull(outputCoverageSources, "outputCoverageSources");
      Objects.requireNonNull(outputCoverageBatches, "outputCoverageBatches");
      if (!outputCoverageSources.coverage().equals(facts.outputCoverage())) {
        throw new IllegalArgumentException(
            "Kafka compaction output sources do not match output coverage");
      }
      requireKafkaCommitted(outputCoverageSources);
      int prefixSize = outputCoverageSources.sources().size();
      if (prefixSize > decisionHorizonSources.sources().size()
          || !outputCoverageSources
              .sources()
              .equals(decisionHorizonSources.sources().subList(0, prefixSize))
          || outputCoverageSources.sources().get(prefixSize - 1).range().endOffset()
              != facts.outputCoverage().endOffset()) {
        throw new IllegalArgumentException(
            "Kafka compaction output sources are not the exact decision-source prefix");
      }
      ExactSourceSetVerifier sourceVerifier = new ExactSourceSetVerifier(outputCoverageSources);
      PassTwoVerifier verifier = facts.newPassTwoVerifier();
      ArrayList<KafkaTopicCompactedObjectRow> rows = new ArrayList<>();
      long sourceBatches = 0;
      long logicalBytes = 0;
      for (ReadBatch batch : outputCoverageBatches) {
        sourceBatches = Math.addExact(sourceBatches, 1);
        if (sourceBatches > limits.maxSourceBatches()) {
          throw new IllegalArgumentException(
              "Kafka compaction output coverage exceeded its source-batch limit");
        }
        ReadBatch exact = Objects.requireNonNull(batch, "outputCoverageBatch");
        sourceVerifier.accept(exact);
        codec.decode(
            exact,
            record -> {
              verifier.accept(record);
              KafkaCompactionStrategyV1.Decision decision =
                  strategy.decide(record, facts.contextFor(record));
              if (decision.retained()) {
                RewrittenCompactionRecord rewritten =
                    codec.rewrite(
                        record,
                        new CompactionRewriteContext(
                            RecordBatch.MAGIC_VALUE_V2,
                            codec.messageFormatSha256(),
                            allowUncompressedFallback,
                            facts.rewriteDeleteHorizon(record)));
                KafkaTopicCompactedObjectRow row = rowMapper.toNtc2Row(rewritten);
                if (rows.size() >= limits.maxOutputBatches()) {
                  throw new IllegalArgumentException(
                      "Kafka compaction exceeded its output-batch limit");
                }
                rows.add(row);
              }
            });
      }
      sourceVerifier.finish();
      verifier.finish();
      long previousOffset = -1;
      for (KafkaTopicCompactedObjectRow row : rows) {
        if (row.streamOffsetStart() <= previousOffset) {
          throw new IllegalStateException("Kafka compaction NTC2 rows are not strictly ordered");
        }
        previousOffset = row.streamOffsetStart();
        logicalBytes = Math.addExact(logicalBytes, row.exactPayload().remaining());
        if (logicalBytes > limits.maxOutputBytes()) {
          throw new IllegalArgumentException("Kafka compaction exceeded its output-byte limit");
        }
      }
      return new Result(
          rows,
          facts.outputCoverage(),
          facts.decisionHorizon(),
          decisionSourceBatchCount,
          sourceBatches,
          rows.size(),
          rows.size(),
          logicalBytes,
          decisionHorizonSources.sourceSetSha256(),
          outputCoverageSources.sourceSetSha256(),
          facts.fullFactSha256(),
          facts.outputFactSha256());
    }

    public Facts facts() {
      return facts;
    }
  }

  public record Limits(long maxSourceBatches, int maxOutputBatches, long maxOutputBytes) {
    public Limits {
      if (maxSourceBatches <= 0 || maxOutputBatches <= 0 || maxOutputBytes <= 0) {
        throw new IllegalArgumentException("invalid Kafka two-pass compaction limits");
      }
    }
  }

  private static void requireKafkaCommitted(ExactSourceSet sources) {
    if (sources.view() != ReadView.COMMITTED
        || sources.sources().stream()
            .anyMatch(source -> source.payloadFormat() != PayloadFormat.KAFKA_RECORD_BATCH)) {
      throw new IllegalArgumentException(
          "Kafka compaction requires exact COMMITTED KAFKA_RECORD_BATCH sources");
    }
  }

  public record Result(
      List<KafkaTopicCompactedObjectRow> rows,
      OffsetRange outputCoverage,
      OffsetRange decisionHorizon,
      long decisionSourceBatchCount,
      long outputSourceBatchCount,
      long outputRecordCount,
      int outputBatchCount,
      long logicalBytes,
      Checksum decisionSourceSetSha256,
      Checksum outputSourceSetSha256,
      Checksum fullFactSha256,
      Checksum outputFactSha256) {
    public Result {
      rows = List.copyOf(Objects.requireNonNull(rows, "rows"));
      Objects.requireNonNull(outputCoverage, "outputCoverage");
      Objects.requireNonNull(decisionHorizon, "decisionHorizon");
      Objects.requireNonNull(decisionSourceSetSha256, "decisionSourceSetSha256");
      Objects.requireNonNull(outputSourceSetSha256, "outputSourceSetSha256");
      Objects.requireNonNull(fullFactSha256, "fullFactSha256");
      Objects.requireNonNull(outputFactSha256, "outputFactSha256");
      if (outputCoverage.isEmpty()
          || decisionHorizon.isEmpty()
          || outputCoverage.startOffset() != decisionHorizon.startOffset()
          || outputCoverage.endOffset() > decisionHorizon.endOffset()
          || decisionSourceBatchCount <= 0
          || outputSourceBatchCount <= 0
          || outputRecordCount < 0
          || outputRecordCount > outputCoverage.recordCount()
          || outputBatchCount < 0
          || outputRecordCount != outputBatchCount
          || outputBatchCount != rows.size()
          || logicalBytes < 0
          || (rows.isEmpty() != (logicalBytes == 0))) {
        throw new IllegalArgumentException("invalid Kafka two-pass compaction result");
      }
    }
  }
}
