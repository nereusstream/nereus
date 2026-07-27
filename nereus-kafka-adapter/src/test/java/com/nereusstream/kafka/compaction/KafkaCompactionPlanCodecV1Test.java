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
import static com.nereusstream.kafka.compaction.KafkaCompactionStrategyV1.MarkerStatus.RETAIN_REQUIRED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState.LogConfigHistoryEntry;
import com.nereusstream.kafka.compaction.KafkaCompactionPassOneCollector.AbortedTransactionRange;
import com.nereusstream.kafka.compaction.KafkaCompactionPassOneCollector.MarkerDecision;
import com.nereusstream.kafka.compaction.KafkaCompactionPassOneCollector.OpenTransactionRange;
import com.nereusstream.kafka.compaction.KafkaCompactionPassOneCollector.Snapshot;
import com.nereusstream.kafka.compaction.KafkaCompactionPlanner.Candidate;
import com.nereusstream.kafka.compaction.KafkaCompactionPlanner.Policy;
import com.nereusstream.materialization.ExactSourceSet;
import com.nereusstream.materialization.MaterializationPolicy;
import com.nereusstream.materialization.MaterializationPolicyFactory;
import com.nereusstream.materialization.MaterializationTask;
import com.nereusstream.materialization.SourceGeneration;
import com.nereusstream.materialization.TopicCompactionSpec;
import com.nereusstream.metadata.oxia.KafkaPartitionId;
import com.nereusstream.metadata.oxia.records.KafkaCompactionPlanRecord;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.junit.jupiter.api.Test;

class KafkaCompactionPlanCodecV1Test {
  private final KafkaCompactionPlanCodecV1 codec = new KafkaCompactionPlanCodecV1();

  @Test
  void roundTripsAPlanThatLinksTheOutputTaskAndFullDecisionHorizon() {
    Fixture fixture = fixture("UNCOMPRESSED");

    byte[] first = codec.encode(fixture.plan());
    KafkaCompactionPlan decoded = codec.decode(first);

    assertThat(decoded).isEqualTo(fixture.plan());
    assertThat(codec.encode(decoded)).isEqualTo(first);
    assertThat(decoded.outputSources().sources())
        .containsExactlyElementsOf(fixture.outputTask().sources());
    assertThat(decoded.decisionSources().sources()).hasSize(2);
    assertThat(decoded.passOneSnapshot().abortedTransactions()).hasSize(1);
    assertThat(decoded.passOneSnapshot().openTransactions()).hasSize(1);
    assertThat(decoded.passOneSnapshot().markerDecisions()).hasSize(1);
    decoded.requireMaterializationTask(fixture.outputTask());
  }

  @Test
  void rejectsPlanIdentityCorruptionTruncationAndAnotherMaterializationTask() {
    Fixture fixture = fixture("UNCOMPRESSED");
    byte[] encoded = codec.encode(fixture.plan());
    byte[] corruptId = encoded.clone();
    corruptId[15] = corruptId[15] == 'a' ? (byte) 'b' : (byte) 'a';
    byte[] truncated = java.util.Arrays.copyOf(encoded, encoded.length - 1);

    assertThatThrownBy(() -> codec.decode(corruptId))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("planId");
    assertThatThrownBy(() -> codec.decode(truncated))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("malformed Kafka compaction plan");
    assertThatThrownBy(
            () -> fixture.plan().requireMaterializationTask(fixture("ZSTD").outputTask()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does not match");
  }

  @Test
  void mapsCanonicalKcp1BytesToAnIdentityCheckedOxiaAttachment() {
    Fixture fixture = fixture("UNCOMPRESSED");
    KafkaCompactionPlanRecordMapper mapper = new KafkaCompactionPlanRecordMapper();
    KafkaPartitionId partition = new KafkaPartitionId("kraft", topicId(9), 3);

    KafkaCompactionPlanRecord record = mapper.toRecord(partition, fixture.plan(), 2_000);

    assertThat(record.identity()).isEqualTo(partition);
    assertThat(mapper.fromRecord(record)).isEqualTo(fixture.plan());
    assertThatThrownBy(
            () ->
                mapper.fromRecord(
                    new KafkaCompactionPlanRecord(
                        record.formatVersion(),
                        record.kafkaClusterId(),
                        record.topicId(),
                        record.partitionId(),
                        "another-stream",
                        record.planId(),
                        record.materializationTaskId(),
                        record.outputStartOffset(),
                        record.outputEndOffset(),
                        record.decisionEndOffset(),
                        record.planSha256(),
                        record.planBytes(),
                        record.createdAtMillis(),
                        record.metadataVersion())))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("differs");
  }

  static Fixture fixture(String compression) {
    ReadBatch output =
        readBatch(
            new OffsetRange(0, 2),
            bytes(
                MemoryRecords.withRecords(
                    0,
                    Compression.NONE,
                    new SimpleRecord("k1".getBytes(), "v1".getBytes()),
                    new SimpleRecord("k2".getBytes(), "v2".getBytes()))),
            "plan-output");
    ReadBatch tail =
        readBatch(
            new OffsetRange(2, 3),
            bytes(
                MemoryRecords.withRecords(
                    2, Compression.NONE, new SimpleRecord("k1".getBytes(), "v3".getBytes()))),
            "plan-tail");
    ExactSourceSet outputSources = sourceSet(List.of(output));
    ExactSourceSet decisionSources = sourceSet(List.of(output, tail));
    MaterializationPolicy materializationPolicy =
        MaterializationPolicyFactory.topicCompacted(
            new TopicCompactionSpec(
                KafkaCompactionStrategyV1.STRATEGY_ID,
                KafkaCompactionStrategyV1.STRATEGY_VERSION,
                "KCK2"),
            2,
            128,
            1_048_576,
            1 << 20,
            1_024,
            compression);
    MaterializationTask outputTask =
        MaterializationTask.create(
            new StreamId("stream-compaction-plan"),
            new OffsetRange(0, 2),
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
            new OffsetRange(0, 2), new OffsetRange(0, 3), 1, policy, Optional.empty(), 1_000);
    Snapshot snapshot =
        new Snapshot(
            candidate.outputCoverage(),
            candidate.decisionHorizon(),
            4,
            candidate.evaluatedAtMillis(),
            policy.deleteRetentionMs(),
            1_000,
            1 << 20,
            1 << 20,
            List.of(new AbortedTransactionRange(7, 0, 1)),
            List.of(new OpenTransactionRange(8, 2)),
            List.of(new MarkerDecision(1, RETAIN_REQUIRED)));
    KafkaCompactionPlan plan =
        KafkaCompactionPlan.create(outputTask, 17, 2, 3, candidate, decisionSources, snapshot);
    return new Fixture(outputTask, plan);
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
              "test/f9-compaction-plan/" + batch.range().startOffset(),
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

  private static String topicId(int value) {
    byte[] bytes = new byte[16];
    bytes[12] = (byte) (value >>> 24);
    bytes[13] = (byte) (value >>> 16);
    bytes[14] = (byte) (value >>> 8);
    bytes[15] = (byte) value;
    return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
  }

  record Fixture(MaterializationTask outputTask, KafkaCompactionPlan plan) {}
}
