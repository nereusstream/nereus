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

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState.LogConfigHistoryEntry;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState.SegmentState;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState.VirtualSegment;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Pure stock-compatible retention calculation over one frozen Kafka partition snapshot. */
public final class KafkaRetentionPlanner {

  public Plan plan(Snapshot snapshot) {
    Objects.requireNonNull(snapshot, "snapshot");
    KafkaVirtualSegmentState state = snapshot.virtualSegments();
    Policy policy = snapshot.policy();
    List<VirtualSegment> segments = state.segments();
    long totalLogicalBytes = totalLogicalBytes(segments);
    if (segments.isEmpty()
        || (policy.cleanupPolicyFlags() & LogConfigHistoryEntry.CLEANUP_DELETE_FLAG) == 0) {
      return Plan.none(snapshot, totalLogicalBytes);
    }

    int closedCount = segments.size() - 1;
    long deletionUpperBound = Math.min(snapshot.highWatermark(), state.stableEndOffset());
    int timePrefixCount =
        timePrefixCount(
            segments, closedCount, deletionUpperBound, snapshot.nowMillis(), policy.retentionMs());
    int sizePrefixCount =
        sizePrefixCount(
            segments, closedCount, deletionUpperBound, totalLogicalBytes, policy.retentionBytes());
    int selectedCount = Math.max(timePrefixCount, sizePrefixCount);
    if (selectedCount == 0) {
      return Plan.none(snapshot, totalLogicalBytes);
    }

    long deletedLogicalBytes = 0;
    for (int index = 0; index < selectedCount; index++) {
      deletedLogicalBytes = Math.addExact(deletedLogicalBytes, segments.get(index).logicalBytes());
    }
    long candidateLogStartOffset = segments.get(selectedCount - 1).endOffset();
    if (candidateLogStartOffset <= state.logStartOffset()
        || candidateLogStartOffset > deletionUpperBound) {
      throw new IllegalStateException(
          "Kafka retention planner selected an invalid segment boundary");
    }
    ArrayList<Reason> reasons = new ArrayList<>(2);
    if (timePrefixCount > 0) {
      reasons.add(Reason.TIME);
    }
    if (sizePrefixCount > 0) {
      reasons.add(Reason.SIZE);
    }
    return new Plan(
        state.logStartOffset(),
        candidateLogStartOffset,
        selectedCount,
        deletedLogicalBytes,
        totalLogicalBytes,
        timePrefixCount,
        sizePrefixCount,
        List.copyOf(reasons),
        policy,
        snapshot.nowMillis());
  }

  private static int timePrefixCount(
      List<VirtualSegment> segments,
      int closedCount,
      long deletionUpperBound,
      long nowMillis,
      long retentionMs) {
    if (retentionMs < 0) {
      return 0;
    }
    int selected = 0;
    while (selected < closedCount) {
      VirtualSegment segment = segments.get(selected);
      if (segment.state() != SegmentState.CLOSED || segment.endOffset() > deletionUpperBound) {
        break;
      }
      long retentionTimestamp =
          segment.largestTimestamp() >= 0 ? segment.largestTimestamp() : segment.closedAtMillis();
      if (nowMillis - retentionTimestamp <= retentionMs) {
        break;
      }
      selected++;
    }
    return selected;
  }

  private static int sizePrefixCount(
      List<VirtualSegment> segments,
      int closedCount,
      long deletionUpperBound,
      long totalLogicalBytes,
      long retentionBytes) {
    if (retentionBytes < 0 || totalLogicalBytes < retentionBytes) {
      return 0;
    }
    long excessBytes = totalLogicalBytes - retentionBytes;
    int selected = 0;
    while (selected < closedCount) {
      VirtualSegment segment = segments.get(selected);
      if (segment.state() != SegmentState.CLOSED
          || segment.endOffset() > deletionUpperBound
          || segment.logicalBytes() > excessBytes) {
        break;
      }
      excessBytes -= segment.logicalBytes();
      selected++;
    }
    return selected;
  }

  private static long totalLogicalBytes(List<VirtualSegment> segments) {
    long total = 0;
    for (VirtualSegment segment : segments) {
      total = Math.addExact(total, segment.logicalBytes());
    }
    return total;
  }

  public record Snapshot(
      KafkaVirtualSegmentState virtualSegments,
      Policy policy,
      long lastStableOffset,
      long highWatermark,
      long nowMillis) {
    public Snapshot {
      Objects.requireNonNull(virtualSegments, "virtualSegments");
      Objects.requireNonNull(policy, "policy");
      if (nowMillis < 0
          || lastStableOffset < virtualSegments.logStartOffset()
          || highWatermark < lastStableOffset
          || highWatermark > virtualSegments.stableEndOffset()) {
        throw new IllegalArgumentException("invalid Kafka retention snapshot bounds");
      }
      if (!virtualSegments.configHistory().isEmpty()) {
        LogConfigHistoryEntry current =
            virtualSegments.configHistory().get(virtualSegments.configHistory().size() - 1);
        if (!policy.equals(Policy.from(current))) {
          throw new IllegalArgumentException(
              "Kafka retention policy does not match current config history");
        }
      }
    }
  }

  public record Policy(
      long metadataOffset,
      Checksum configDigest,
      long retentionBytes,
      long retentionMs,
      int cleanupPolicyFlags) {
    public Policy {
      Objects.requireNonNull(configDigest, "configDigest");
      if (metadataOffset < 0
          || configDigest.type() != ChecksumType.SHA256
          || retentionBytes < -1
          || retentionMs < -1
          || cleanupPolicyFlags == 0
          || (cleanupPolicyFlags & ~LogConfigHistoryEntry.KNOWN_CLEANUP_POLICY_FLAGS) != 0) {
        throw new IllegalArgumentException("invalid Kafka retention policy");
      }
    }

    public static Policy from(LogConfigHistoryEntry config) {
      Objects.requireNonNull(config, "config");
      return new Policy(
          config.metadataOffset(),
          config.configDigest(),
          config.retentionBytes(),
          config.retentionMs(),
          config.cleanupPolicyFlags());
    }
  }

  public record Plan(
      long previousLogStartOffset,
      long candidateLogStartOffset,
      int selectedSegmentCount,
      long deletedLogicalBytes,
      long totalLogicalBytes,
      int timePrefixCount,
      int sizePrefixCount,
      List<Reason> reasons,
      Policy policy,
      long evaluatedAtMillis) {
    public Plan {
      reasons = List.copyOf(Objects.requireNonNull(reasons, "reasons"));
      Objects.requireNonNull(policy, "policy");
      if (previousLogStartOffset < 0
          || candidateLogStartOffset < previousLogStartOffset
          || selectedSegmentCount < 0
          || deletedLogicalBytes < 0
          || totalLogicalBytes < deletedLogicalBytes
          || timePrefixCount < 0
          || sizePrefixCount < 0
          || timePrefixCount > selectedSegmentCount
          || sizePrefixCount > selectedSegmentCount
          || evaluatedAtMillis < 0
          || (selectedSegmentCount == 0
              && (candidateLogStartOffset != previousLogStartOffset
                  || deletedLogicalBytes != 0
                  || !reasons.isEmpty()))
          || (selectedSegmentCount > 0
              && (candidateLogStartOffset == previousLogStartOffset || reasons.isEmpty()))) {
        throw new IllegalArgumentException("invalid Kafka retention plan");
      }
    }

    private static Plan none(Snapshot snapshot, long totalLogicalBytes) {
      return new Plan(
          snapshot.virtualSegments().logStartOffset(),
          snapshot.virtualSegments().logStartOffset(),
          0,
          0,
          totalLogicalBytes,
          0,
          0,
          List.of(),
          snapshot.policy(),
          snapshot.nowMillis());
    }

    public boolean shouldTrim() {
      return candidateLogStartOffset > previousLogStartOffset;
    }

    public String trimReason() {
      if (!shouldTrim()) {
        throw new IllegalStateException("Kafka retention plan does not advance log start");
      }
      String reason =
          reasons.stream()
              .map(Enum::name)
              .reduce((left, right) -> left + "+" + right)
              .orElseThrow();
      return "KAFKA_RETENTION_V1:"
          + reason
          + ":config="
          + policy.metadataOffset()
          + "/"
          + policy.configDigest().value()
          + ":from="
          + previousLogStartOffset
          + ":to="
          + candidateLogStartOffset
          + ":segments="
          + selectedSegmentCount
          + ":bytes="
          + deletedLogicalBytes
          + ":now="
          + evaluatedAtMillis;
    }
  }

  public enum Reason {
    TIME,
    SIZE
  }
}
