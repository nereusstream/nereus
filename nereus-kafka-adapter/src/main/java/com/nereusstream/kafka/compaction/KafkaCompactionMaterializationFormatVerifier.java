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

import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.materialization.MaterializationFormatVerifier;
import com.nereusstream.materialization.MaterializationOutput;
import com.nereusstream.materialization.MaterializationPolicy;
import com.nereusstream.materialization.MaterializationTask;
import com.nereusstream.objectstore.compacted.CompactedObjectFormatException;
import com.nereusstream.objectstore.compacted.CompactedObjectFormatV2;
import com.nereusstream.objectstore.compacted.KafkaTopicCompactedFormatSpecV2;
import com.nereusstream.objectstore.compacted.RangedCompactedObjectMetadata;
import com.nereusstream.objectstore.compacted.RangedCompactedObjectVerificationRequest;
import com.nereusstream.objectstore.compacted.RangedCompactedObjectVerifier;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Task-aware bridge from generic F4 publication to strict full-file NTC2 verification. */
public final class KafkaCompactionMaterializationFormatVerifier
    implements MaterializationFormatVerifier {
  private final RangedCompactedObjectVerifier verifier;

  public KafkaCompactionMaterializationFormatVerifier(RangedCompactedObjectVerifier verifier) {
    this.verifier = Objects.requireNonNull(verifier, "verifier");
  }

  @Override
  public CompletableFuture<Void> verify(
      MaterializationTask task, MaterializationOutput output, Duration timeout) {
    try {
      MaterializationTask exactTask = Objects.requireNonNull(task, "task");
      MaterializationOutput exactOutput = Objects.requireNonNull(output, "output");
      if (exactTask.view() != ReadView.TOPIC_COMPACTED
          || !exactTask
              .policy()
              .targetPhysicalFormat()
              .equals(MaterializationPolicy.KAFKA_TOPIC_COMPACTED_FORMAT)
          || !(exactOutput.readTarget() instanceof ObjectSliceReadTarget target)
          || exactOutput.payloadFormat() != PayloadFormat.KAFKA_RECORD_BATCH) {
        throw new CompactedObjectFormatException(
            "Kafka materialization verification requires an NTC2 task/output");
      }
      RangedCompactedObjectVerificationRequest request =
          new RangedCompactedObjectVerificationRequest(
              exactOutput.streamId(),
              exactOutput.view(),
              exactOutput.coverage(),
              target,
              exactOutput.payloadFormat(),
              exactOutput.storageCrc32c(),
              exactOutput.contentSha256(),
              timeout);
      return verifier
          .verify(request)
          .thenAccept(metadata -> requireAgreement(exactTask, exactOutput, metadata));
    } catch (Throwable failure) {
      return CompletableFuture.failedFuture(failure);
    }
  }

  private static void requireAgreement(
      MaterializationTask task,
      MaterializationOutput output,
      RangedCompactedObjectMetadata metadata) {
    KafkaTopicCompactedFormatSpecV2 topic = metadata.topicCompaction().orElseThrow();
    var policyTopic = task.policy().topicCompaction().orElseThrow();
    if (metadata.view() != ReadView.TOPIC_COMPACTED
        || !metadata.streamId().equals(task.streamId())
        || !metadata.sourceCoverage().equals(task.coverage())
        || !metadata.sourceSetSha256().equals(task.sourceSetSha256())
        || !metadata.policySha256().equals(task.policyDigestSha256())
        || !metadata.outputAttemptId().equals(output.outputAttemptId())
        || metadata.payloadFormat() != PayloadFormat.KAFKA_RECORD_BATCH
        || !metadata.logicalFormat().equals(CompactedObjectFormatV2.KAFKA_LOGICAL_FORMAT)
        || metadata.sourceRecordCount() != output.sourceRecordCount()
        || metadata.outputRecordCount() != output.outputRecordCount()
        || metadata.entryCount() != output.entryCount()
        || metadata.logicalBytes() != output.logicalBytes()
        || metadata.cumulativeSizeAtEnd() != output.cumulativeSizeAtEnd()
        || !metadata.compression().equals(task.policy().compression())
        || metadata.targetRowGroupRecords() != task.policy().targetRowGroupRecords()
        || !topic.strategyId().equals(policyTopic.strategyId())
        || topic.strategyVersion() != policyTopic.strategyVersion()
        || !topic.keyCodecId().equals(policyTopic.keyCodecId())
        || !topic.rewriteCodecId().equals(CompactedObjectFormatV2.KAFKA_REWRITE_CODEC)
        || !topic.messageFormatSha256().equals(KafkaTopicCompactionCodecV1.MESSAGE_FORMAT_SHA256)
        || !output.physicalFormat().equals(task.policy().targetPhysicalFormat())) {
      throw new CompactedObjectFormatException(
          "NTC2 metadata does not match task/output publication facts");
    }
  }
}
