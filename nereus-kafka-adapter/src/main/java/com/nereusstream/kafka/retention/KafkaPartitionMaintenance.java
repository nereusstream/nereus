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
}
