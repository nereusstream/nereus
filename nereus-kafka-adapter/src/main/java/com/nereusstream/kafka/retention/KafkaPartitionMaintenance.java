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

package com.nereusstream.kafka.retention;

import com.nereusstream.kafka.checkpoint.KafkaCanonicalCheckpointState;
import com.nereusstream.kafka.checkpoint.KafkaCheckpointSourceState;
import com.nereusstream.kafka.compaction.KafkaCompactionPartitionPass;
import com.nereusstream.kafka.compaction.KafkaCompactionPlanner;
import com.nereusstream.materialization.MaterializationPolicy;
import com.nereusstream.metadata.oxia.VersionedKafkaPartitionBinding;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Product-owned maintenance boundary for one recovered Kafka leader.
 *
 * <p>The Kafka fork supplies only a partition-lock-frozen canonical image and the exact local-log
 * publication callback. Binding, stream-head, checkpoint, trim, and response-loss state machines
 * remain product-owned.
 */
public interface KafkaPartitionMaintenance {

  CompletableFuture<KafkaRetentionCoordinator.RunResult> runRetention(Hooks hooks);

  CompletableFuture<KafkaDeleteRecordsCoordinator.Result> deleteRecords(
      Hooks hooks, long normalizedRequestedOffset);

  /**
   * Freezes one compaction pass from the same recovered leader and durable authority domain.
   *
   * <p>The product loads and validates the binding/source head. The fork supplies the
   * partition-lock-owned canonical state and performs the stock transaction-marker pre-scan only
   * after the product has selected the exact decision horizon.
   */
  default CompletableFuture<KafkaCompactionPartitionPass.Capture> captureCompaction(
      CompactionHooks hooks) {
    Objects.requireNonNull(hooks, "hooks");
    return CompletableFuture.failedFuture(
        new UnsupportedOperationException(
            "Kafka partition maintenance does not provide compaction capture"));
  }

  interface Hooks {

    CompletableFuture<Capture> capture(KafkaCheckpointSourceState currentSource);

    CompletableFuture<Void> advanceLogStart(
        KafkaTrimBarrier.Snapshot revalidated,
        long durableTrimOffset,
        VersionedKafkaPartitionBinding publishedBinding);
  }

  record Capture(
      KafkaCanonicalCheckpointState canonicalState,
      long highWatermark,
      long lastStableOffset) {
    public Capture {
      Objects.requireNonNull(canonicalState, "canonicalState");
      if (lastStableOffset < canonicalState.logStartOffset()
          || highWatermark < lastStableOffset
          || highWatermark > canonicalState.stableEndOffset()) {
        throw new IllegalArgumentException(
            "Kafka maintenance visibility offsets are outside the canonical state");
      }
    }
  }

  interface CompactionHooks {

    CompletableFuture<CompactionState> capture(KafkaCheckpointSourceState currentSource);

    CompletableFuture<KafkaCompactionPartitionPass.PassOneInputs> capturePassOne(
        KafkaCheckpointSourceState currentSource,
        KafkaCompactionPlanner.Candidate candidate,
        CompactionState state);
  }

  /** Fork-owned immutable facts that do not require reading the selected decision horizon. */
  record CompactionState(
      KafkaCanonicalCheckpointState canonicalState,
      long highWatermark,
      long lastStableOffset,
      MaterializationPolicy outputPolicy,
      KafkaCompactionPartitionPass.WriteSettings writeSettings) {
    public CompactionState {
      Objects.requireNonNull(canonicalState, "canonicalState");
      Objects.requireNonNull(outputPolicy, "outputPolicy");
      Objects.requireNonNull(writeSettings, "writeSettings");
      if (lastStableOffset < canonicalState.logStartOffset()
          || highWatermark < lastStableOffset
          || highWatermark > canonicalState.stableEndOffset()) {
        throw new IllegalArgumentException(
            "Kafka compaction visibility offsets are outside the canonical state");
      }
    }
  }
}
