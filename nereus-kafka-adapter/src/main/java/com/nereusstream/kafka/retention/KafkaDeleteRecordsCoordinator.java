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

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState.LogConfigHistoryEntry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Product-side DeleteRecords boundary after Kafka has applied its stock request validation and
 * converted the high-watermark sentinel to an exact logical offset.
 */
public final class KafkaDeleteRecordsCoordinator {
  private final KafkaTrimBarrier barrier;

  public KafkaDeleteRecordsCoordinator(KafkaTrimBarrier barrier) {
    this.barrier = Objects.requireNonNull(barrier, "barrier");
  }

  public CompletableFuture<Result> deleteTo(
      KafkaTrimBarrier.Snapshot captured, long normalizedRequestedOffset) {
    Objects.requireNonNull(captured, "captured");
    if (normalizedRequestedOffset < 0) {
      return CompletableFuture.failedFuture(
          new NereusException(
              ErrorCode.INVALID_ARGUMENT,
              false,
              "Kafka DeleteRecords requires a non-negative normalized offset"));
    }
    if (!deleteEnabled(captured.retention().policy())) {
      return CompletableFuture.failedFuture(
          new NereusException(
              ErrorCode.INVALID_ARGUMENT,
              false,
              "Kafka DeleteRecords requires cleanup.policy containing delete"));
    }
    if (normalizedRequestedOffset > captured.retention().highWatermark()) {
      return CompletableFuture.failedFuture(
          new NereusException(
              ErrorCode.OFFSET_NOT_AVAILABLE,
              false,
              "Kafka DeleteRecords offset exceeds the frozen high watermark"));
    }

    long currentLogStart = captured.sourceHead().trimOffset();
    if (normalizedRequestedOffset <= currentLogStart) {
      return CompletableFuture.completedFuture(
          new Result(normalizedRequestedOffset, currentLogStart, Optional.empty()));
    }
    return barrier
        .advanceDeleteRecords(captured, normalizedRequestedOffset)
        .thenApply(
            trim ->
                new Result(normalizedRequestedOffset, trim.durableTrimOffset(), Optional.of(trim)));
  }

  private static boolean deleteEnabled(KafkaRetentionPlanner.Policy policy) {
    return (policy.cleanupPolicyFlags() & LogConfigHistoryEntry.CLEANUP_DELETE_FLAG) != 0;
  }

  public record Result(
      long requestedOffset,
      long durableLowWatermark,
      Optional<KafkaTrimBarrier.Result> trimResult) {
    public Result {
      trimResult = Objects.requireNonNull(trimResult, "trimResult");
      if (requestedOffset < 0
          || durableLowWatermark < requestedOffset
          || (trimResult.isPresent()
              && (trimResult.orElseThrow().requestedTrimOffset() != requestedOffset
                  || trimResult.orElseThrow().durableTrimOffset() != durableLowWatermark))) {
        throw new IllegalArgumentException("invalid Kafka DeleteRecords result");
      }
    }

    public boolean advanced() {
      return trimResult.isPresent();
    }
  }
}
