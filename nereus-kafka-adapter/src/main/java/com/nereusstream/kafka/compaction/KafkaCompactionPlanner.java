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
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState.LogConfigHistoryEntry;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState.SegmentState;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState.VirtualSegment;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Pure selection of the next closed virtual-segment range eligible for Kafka compaction. */
public final class KafkaCompactionPlanner {

  public Candidate select(Snapshot snapshot) {
    Objects.requireNonNull(snapshot, "snapshot");
    KafkaVirtualSegmentState state = snapshot.virtualSegments();
    long firstDirtyOffset =
        snapshot
            .mandatoryCoverage()
            .map(MandatoryCoverage::endOffset)
            .orElse(state.logStartOffset());
    OffsetRange decisionHorizon = new OffsetRange(firstDirtyOffset, state.stableEndOffset());
    if (!snapshot.policy().compactEnabled() || firstDirtyOffset >= state.stableEndOffset()) {
      return Candidate.none(snapshot, firstDirtyOffset, decisionHorizon);
    }

    long upperBound = snapshot.lastStableOffset();
    int selectedSegments = 0;
    long outputEnd = firstDirtyOffset;
    for (VirtualSegment segment : state.segments()) {
      if (segment.endOffset() <= firstDirtyOffset) {
        continue;
      }
      if (segment.state() != SegmentState.CLOSED
          || segment.endOffset() > upperBound
          || withinMinimumLag(
              segment, snapshot.nowMillis(), snapshot.policy().minCompactionLagMs())) {
        break;
      }
      outputEnd = segment.endOffset();
      selectedSegments++;
    }
    if (outputEnd == firstDirtyOffset) {
      return Candidate.none(snapshot, firstDirtyOffset, decisionHorizon);
    }
    return new Candidate(
        new OffsetRange(firstDirtyOffset, outputEnd),
        decisionHorizon,
        selectedSegments,
        snapshot.policy(),
        snapshot.mandatoryCoverage(),
        snapshot.nowMillis());
  }

  private static boolean withinMinimumLag(
      VirtualSegment segment, long nowMillis, long minCompactionLagMs) {
    if (minCompactionLagMs == 0) {
      return false;
    }
    long segmentTimestamp =
        segment.largestTimestamp() >= 0 ? segment.largestTimestamp() : segment.closedAtMillis();
    return segmentTimestamp > nowMillis - minCompactionLagMs;
  }

  public record Snapshot(
      KafkaVirtualSegmentState virtualSegments,
      Policy policy,
      Optional<MandatoryCoverage> mandatoryCoverage,
      long lastStableOffset,
      long highWatermark,
      long nowMillis) {
    public Snapshot {
      Objects.requireNonNull(virtualSegments, "virtualSegments");
      Objects.requireNonNull(policy, "policy");
      mandatoryCoverage = Objects.requireNonNull(mandatoryCoverage, "mandatoryCoverage");
      if (nowMillis < 0
          || lastStableOffset < virtualSegments.logStartOffset()
          || highWatermark < lastStableOffset
          || highWatermark > virtualSegments.stableEndOffset()) {
        throw new IllegalArgumentException("invalid Kafka compaction snapshot bounds");
      }
      List<LogConfigHistoryEntry> history = virtualSegments.configHistory();
      if (!history.isEmpty() && !policy.equals(Policy.from(history.get(history.size() - 1)))) {
        throw new IllegalArgumentException(
            "Kafka compaction policy does not match current config history");
      }
      mandatoryCoverage.ifPresent(
          coverage -> {
            if (coverage.startOffset() != virtualSegments.logStartOffset()
                || coverage.endOffset() > virtualSegments.stableEndOffset()) {
              throw new IllegalArgumentException(
                  "Kafka mandatory compaction coverage does not match retained bounds");
            }
          });
    }
  }

  public record Policy(
      long metadataOffset,
      Checksum configDigest,
      long minCompactionLagMs,
      long maxCompactionLagMs,
      long deleteRetentionMs,
      int cleanupPolicyFlags) {
    public Policy {
      Objects.requireNonNull(configDigest, "configDigest");
      if (metadataOffset < 0
          || configDigest.type() != ChecksumType.SHA256
          || minCompactionLagMs < 0
          || maxCompactionLagMs < minCompactionLagMs
          || deleteRetentionMs < 0
          || cleanupPolicyFlags == 0
          || (cleanupPolicyFlags & ~LogConfigHistoryEntry.KNOWN_CLEANUP_POLICY_FLAGS) != 0) {
        throw new IllegalArgumentException("invalid Kafka compaction policy");
      }
    }

    public static Policy from(LogConfigHistoryEntry config) {
      Objects.requireNonNull(config, "config");
      return new Policy(
          config.metadataOffset(),
          config.configDigest(),
          config.minCompactionLagMs(),
          config.maxCompactionLagMs(),
          config.deleteRetentionMs(),
          config.cleanupPolicyFlags());
    }

    public boolean compactEnabled() {
      return (cleanupPolicyFlags & LogConfigHistoryEntry.CLEANUP_COMPACT_FLAG) != 0;
    }
  }

  public record MandatoryCoverage(
      long startOffset,
      long endOffset,
      long activationEpoch,
      Checksum generationSetSha256,
      Checksum policySha256) {
    public MandatoryCoverage {
      Objects.requireNonNull(generationSetSha256, "generationSetSha256");
      Objects.requireNonNull(policySha256, "policySha256");
      if (startOffset < 0
          || endOffset <= startOffset
          || activationEpoch <= 0
          || generationSetSha256.type() != ChecksumType.SHA256
          || policySha256.type() != ChecksumType.SHA256) {
        throw new IllegalArgumentException("invalid Kafka mandatory compaction coverage");
      }
    }
  }

  public record Candidate(
      OffsetRange outputCoverage,
      OffsetRange decisionHorizon,
      int selectedSegmentCount,
      Policy policy,
      Optional<MandatoryCoverage> previousMandatoryCoverage,
      long evaluatedAtMillis) {
    public Candidate {
      Objects.requireNonNull(outputCoverage, "outputCoverage");
      Objects.requireNonNull(decisionHorizon, "decisionHorizon");
      Objects.requireNonNull(policy, "policy");
      previousMandatoryCoverage =
          Objects.requireNonNull(previousMandatoryCoverage, "previousMandatoryCoverage");
      if (selectedSegmentCount < 0
          || evaluatedAtMillis < 0
          || outputCoverage.startOffset() != decisionHorizon.startOffset()
          || outputCoverage.endOffset() > decisionHorizon.endOffset()
          || (selectedSegmentCount == 0) != outputCoverage.isEmpty()) {
        throw new IllegalArgumentException("invalid Kafka compaction candidate");
      }
    }

    private static Candidate none(
        Snapshot snapshot, long firstDirtyOffset, OffsetRange decisionHorizon) {
      return new Candidate(
          new OffsetRange(firstDirtyOffset, firstDirtyOffset),
          decisionHorizon,
          0,
          snapshot.policy(),
          snapshot.mandatoryCoverage(),
          snapshot.nowMillis());
    }

    public boolean shouldCompact() {
      return !outputCoverage.isEmpty();
    }
  }
}
