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
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.StreamId;
import com.nereusstream.objectstore.compacted.CompactedObjectFormatV2;
import com.nereusstream.objectstore.compacted.KafkaCompactionKeyEncodingV2;
import com.nereusstream.objectstore.compacted.KafkaTopicCompactedFormatSpecV2;
import com.nereusstream.objectstore.compacted.KafkaTopicCompactedObjectWriteRequest;
import java.util.Objects;

/** Exact NTC2 writer request derived from a verified two-pass result and frozen task facts. */
public final class KafkaCompactionWriteRequestFactory {

  public KafkaTopicCompactedObjectWriteRequest create(
      Input input, KafkaCompactionTwoPassExecutor.Result result) {
    Objects.requireNonNull(input, "input");
    Objects.requireNonNull(result, "result");
    if (!input.outputCoverage().equals(result.outputCoverage())) {
      throw new IllegalArgumentException(
          "Kafka NTC2 write coverage does not match the verified two-pass result");
    }
    return new KafkaTopicCompactedObjectWriteRequest(
        input.cluster(),
        input.streamId(),
        input.outputCoverage(),
        input.outputAttemptId(),
        result.outputSourceSetSha256(),
        input.policySha256(),
        result.outputRecordCount(),
        result.outputBatchCount(),
        result.logicalBytes(),
        input.cumulativeSizeAtEnd(),
        input.targetRowGroupRecords(),
        input.compression(),
        input.writerBuild(),
        new KafkaTopicCompactedFormatSpecV2(
            KafkaCompactionStrategyV1.STRATEGY_ID,
            KafkaCompactionStrategyV1.STRATEGY_VERSION,
            KafkaCompactionKeyEncodingV2.ID,
            CompactedObjectFormatV2.KAFKA_REWRITE_CODEC,
            KafkaTopicCompactionCodecV1.MESSAGE_FORMAT_SHA256,
            result.outputSourceBatchCount(),
            result.outputBatchCount()));
  }

  public record Input(
      String cluster,
      StreamId streamId,
      OffsetRange outputCoverage,
      String outputAttemptId,
      Checksum policySha256,
      long cumulativeSizeAtEnd,
      int targetRowGroupRecords,
      String compression,
      String writerBuild) {
    public Input {
      Objects.requireNonNull(cluster, "cluster");
      Objects.requireNonNull(streamId, "streamId");
      Objects.requireNonNull(outputCoverage, "outputCoverage");
      Objects.requireNonNull(outputAttemptId, "outputAttemptId");
      Objects.requireNonNull(policySha256, "policySha256");
      Objects.requireNonNull(compression, "compression");
      Objects.requireNonNull(writerBuild, "writerBuild");
      if (cluster.isBlank()
          || outputCoverage.isEmpty()
          || policySha256.type() != ChecksumType.SHA256
          || cumulativeSizeAtEnd < 0
          || targetRowGroupRecords <= 0
          || compression.isBlank()
          || writerBuild.isBlank()) {
        throw new IllegalArgumentException("invalid Kafka NTC2 write-request input");
      }
    }
  }
}
