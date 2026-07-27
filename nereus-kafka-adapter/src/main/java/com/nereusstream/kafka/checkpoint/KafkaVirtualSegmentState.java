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

package com.nereusstream.kafka.checkpoint;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Kafka-artifact-neutral canonical image for NKC1 virtual segments and log-config history. */
public record KafkaVirtualSegmentState(
    long logStartOffset,
    long stableEndOffset,
    List<VirtualSegment> segments,
    List<LogConfigHistoryEntry> configHistory) {

  public KafkaVirtualSegmentState {
    if (logStartOffset < 0 || stableEndOffset < logStartOffset) {
      throw new IllegalArgumentException("invalid Kafka virtual-segment bounds");
    }
    segments = List.copyOf(Objects.requireNonNull(segments, "segments"));
    configHistory = List.copyOf(Objects.requireNonNull(configHistory, "configHistory"));
    validateConfigHistory(configHistory, stableEndOffset);
    validateSegments(segments, configHistory, logStartOffset, stableEndOffset);
  }

  public void requireBounds(long expectedLogStartOffset, long expectedStableEndOffset) {
    if (logStartOffset != expectedLogStartOffset || stableEndOffset != expectedStableEndOffset) {
      throw new IllegalArgumentException(
          "Kafka virtual-segment state does not match checkpoint bounds");
    }
  }

  private static void validateConfigHistory(
      List<LogConfigHistoryEntry> history, long stableEndOffset) {
    long previousMetadataOffset = -1;
    long previousEffectiveOffset = -1;
    for (LogConfigHistoryEntry entry : history) {
      Objects.requireNonNull(entry, "configHistoryEntry");
      if (entry.metadataOffset() <= previousMetadataOffset
          || entry.effectiveFromOffset() < previousEffectiveOffset
          || entry.effectiveFromOffset() > stableEndOffset) {
        throw new IllegalArgumentException(
            "Kafka log-config history must be ordered and checkpoint-bounded");
      }
      previousMetadataOffset = entry.metadataOffset();
      previousEffectiveOffset = entry.effectiveFromOffset();
    }
  }

  private static void validateSegments(
      List<VirtualSegment> segments,
      List<LogConfigHistoryEntry> history,
      long logStartOffset,
      long stableEndOffset) {
    if (segments.isEmpty()) {
      if (stableEndOffset != logStartOffset) {
        throw new IllegalArgumentException("non-empty Kafka log requires a virtual segment");
      }
      return;
    }
    if (history.isEmpty()) {
      throw new IllegalArgumentException("Kafka virtual segments require log-config history");
    }
    if (segments.get(0).baseOffset() > logStartOffset) {
      throw new IllegalArgumentException("first Kafka virtual segment must cover log start");
    }
    Map<Checksum, LogConfigHistoryEntry> configByDigest = new HashMap<>();
    history.forEach(entry -> configByDigest.putIfAbsent(entry.configDigest(), entry));
    long previousBaseOffset = -1;
    long previousEndOffset = -1;
    long previousRollSequence = -1;
    long previousCreatedAt = -1;
    long previousClosedAt = -1;
    long previousCumulativeBytes = -1;
    for (int index = 0; index < segments.size(); index++) {
      VirtualSegment segment = Objects.requireNonNull(segments.get(index), "segment");
      if (segment.baseOffset() <= previousBaseOffset
          || segment.rollSequence() <= previousRollSequence
          || segment.createdAtMillis() < previousCreatedAt
          || (index > 0 && segment.createdAtMillis() < previousClosedAt)) {
        throw new IllegalArgumentException("Kafka virtual segments must be strictly ordered");
      }
      if (index > 0
          && (segment.baseOffset() != previousEndOffset
              || segment.firstCumulativeBytes() != previousCumulativeBytes)) {
        throw new IllegalArgumentException(
            "Kafka virtual segment offset and byte ranges must be dense");
      }
      boolean last = index == segments.size() - 1;
      if (last != (segment.state() == SegmentState.ACTIVE)) {
        throw new IllegalArgumentException("only the final Kafka virtual segment may be active");
      }
      LogConfigHistoryEntry config = configByDigest.get(segment.configDigest());
      if (config == null) {
        throw new IllegalArgumentException("Kafka virtual segment references unknown log config");
      }
      if (config.effectiveFromOffset() > segment.baseOffset()) {
        throw new IllegalArgumentException(
            "Kafka virtual segment references a log config that was not yet effective");
      }
      if (segment.rollJitterMillis() > config.segmentJitterMillis()) {
        throw new IllegalArgumentException("Kafka virtual segment roll jitter exceeds its config");
      }
      previousBaseOffset = segment.baseOffset();
      previousEndOffset = segment.endOffset();
      previousRollSequence = segment.rollSequence();
      previousCreatedAt = segment.createdAtMillis();
      previousClosedAt = segment.closedAtMillis();
      previousCumulativeBytes = segment.lastCumulativeBytes();
    }
    if (previousEndOffset != stableEndOffset) {
      throw new IllegalArgumentException("final Kafka virtual segment must end at stable end");
    }
  }

  public record VirtualSegment(
      long baseOffset,
      long endOffset,
      long rollSequence,
      long createdAtMillis,
      long closedAtMillis,
      long rollJitterMillis,
      long largestTimestamp,
      long maxTimestampOffset,
      long logicalBytes,
      long firstCumulativeBytes,
      long lastCumulativeBytes,
      Checksum configDigest,
      RollReason rollReason,
      SegmentState state) {
    public VirtualSegment {
      Objects.requireNonNull(configDigest, "configDigest");
      Objects.requireNonNull(rollReason, "rollReason");
      Objects.requireNonNull(state, "state");
      if (configDigest.type() != ChecksumType.SHA256) {
        throw new IllegalArgumentException("configDigest must use SHA256");
      }
      if (baseOffset < 0
          || endOffset < baseOffset
          || rollSequence < 0
          || createdAtMillis < 0
          || rollJitterMillis < 0
          || logicalBytes < 0
          || firstCumulativeBytes < 0
          || lastCumulativeBytes < firstCumulativeBytes
          || logicalBytes != lastCumulativeBytes - firstCumulativeBytes) {
        throw new IllegalArgumentException("invalid Kafka virtual-segment numeric fields");
      }
      if (state == SegmentState.ACTIVE && closedAtMillis != 0
          || state == SegmentState.CLOSED
              && (closedAtMillis <= 0 || closedAtMillis < createdAtMillis)) {
        throw new IllegalArgumentException("Kafka virtual-segment lifecycle is inconsistent");
      }
      if (logicalBytes == 0) {
        if (largestTimestamp != -1 || maxTimestampOffset != -1) {
          throw new IllegalArgumentException(
              "empty Kafka virtual segment cannot have a max timestamp");
        }
      } else if (largestTimestamp < 0
          || maxTimestampOffset < baseOffset
          || maxTimestampOffset >= endOffset) {
        throw new IllegalArgumentException(
            "Kafka virtual-segment max timestamp is outside its range");
      }
      if (state == SegmentState.CLOSED && endOffset == baseOffset) {
        throw new IllegalArgumentException("closed Kafka virtual segment cannot be empty");
      }
    }
  }

  public record LogConfigHistoryEntry(
      long metadataOffset,
      long effectiveFromOffset,
      long segmentBytes,
      long segmentMs,
      long segmentJitterMillis,
      int segmentIndexBytes,
      int indexIntervalBytes,
      long retentionBytes,
      long retentionMs,
      long fileDeleteDelayMs,
      long deleteRetentionMs,
      long minCompactionLagMs,
      long maxCompactionLagMs,
      double minCleanableDirtyRatio,
      int cleanupPolicyFlags,
      Checksum configDigest) {
    public static final int CLEANUP_DELETE_FLAG = 1;
    public static final int CLEANUP_COMPACT_FLAG = 1 << 1;
    public static final int KNOWN_CLEANUP_POLICY_FLAGS = CLEANUP_DELETE_FLAG | CLEANUP_COMPACT_FLAG;

    public LogConfigHistoryEntry {
      Objects.requireNonNull(configDigest, "configDigest");
      if (metadataOffset < 0
          || effectiveFromOffset < 0
          || segmentBytes <= 0
          || segmentMs <= 0
          || segmentJitterMillis < 0
          || segmentJitterMillis > segmentMs
          || segmentIndexBytes <= 0
          || indexIntervalBytes <= 0
          || retentionBytes < -1
          || retentionMs < -1
          || fileDeleteDelayMs < 0
          || deleteRetentionMs < 0
          || minCompactionLagMs < 0
          || maxCompactionLagMs < minCompactionLagMs
          || !Double.isFinite(minCleanableDirtyRatio)
          || minCleanableDirtyRatio < 0
          || minCleanableDirtyRatio > 1
          || Double.doubleToRawLongBits(minCleanableDirtyRatio) == Long.MIN_VALUE
          || cleanupPolicyFlags == 0
          || (cleanupPolicyFlags & ~KNOWN_CLEANUP_POLICY_FLAGS) != 0) {
        throw new IllegalArgumentException("invalid Kafka log-config history fields");
      }
      if (configDigest.type() != ChecksumType.SHA256
          || !configDigest.equals(
              computeConfigDigest(
                  segmentBytes,
                  segmentMs,
                  segmentJitterMillis,
                  segmentIndexBytes,
                  indexIntervalBytes,
                  retentionBytes,
                  retentionMs,
                  fileDeleteDelayMs,
                  deleteRetentionMs,
                  minCompactionLagMs,
                  maxCompactionLagMs,
                  minCleanableDirtyRatio,
                  cleanupPolicyFlags))) {
        throw new IllegalArgumentException(
            "Kafka log-config digest does not match its canonical fields");
      }
    }

    public static LogConfigHistoryEntry create(
        long metadataOffset,
        long effectiveFromOffset,
        long segmentBytes,
        long segmentMs,
        long segmentJitterMillis,
        int segmentIndexBytes,
        int indexIntervalBytes,
        long retentionBytes,
        long retentionMs,
        long fileDeleteDelayMs,
        long deleteRetentionMs,
        long minCompactionLagMs,
        long maxCompactionLagMs,
        double minCleanableDirtyRatio,
        int cleanupPolicyFlags) {
      return new LogConfigHistoryEntry(
          metadataOffset,
          effectiveFromOffset,
          segmentBytes,
          segmentMs,
          segmentJitterMillis,
          segmentIndexBytes,
          indexIntervalBytes,
          retentionBytes,
          retentionMs,
          fileDeleteDelayMs,
          deleteRetentionMs,
          minCompactionLagMs,
          maxCompactionLagMs,
          minCleanableDirtyRatio,
          cleanupPolicyFlags,
          computeConfigDigest(
              segmentBytes,
              segmentMs,
              segmentJitterMillis,
              segmentIndexBytes,
              indexIntervalBytes,
              retentionBytes,
              retentionMs,
              fileDeleteDelayMs,
              deleteRetentionMs,
              minCompactionLagMs,
              maxCompactionLagMs,
              minCleanableDirtyRatio,
              cleanupPolicyFlags));
    }

    private static Checksum computeConfigDigest(
        long segmentBytes,
        long segmentMs,
        long segmentJitterMillis,
        int segmentIndexBytes,
        int indexIntervalBytes,
        long retentionBytes,
        long retentionMs,
        long fileDeleteDelayMs,
        long deleteRetentionMs,
        long minCompactionLagMs,
        long maxCompactionLagMs,
        double minCleanableDirtyRatio,
        int cleanupPolicyFlags) {
      ByteBuffer fields =
          ByteBuffer.allocate(Long.BYTES * 10 + Integer.BYTES * 3).order(ByteOrder.BIG_ENDIAN);
      fields.putLong(segmentBytes);
      fields.putLong(segmentMs);
      fields.putLong(segmentJitterMillis);
      fields.putInt(segmentIndexBytes);
      fields.putInt(indexIntervalBytes);
      fields.putLong(retentionBytes);
      fields.putLong(retentionMs);
      fields.putLong(fileDeleteDelayMs);
      fields.putLong(deleteRetentionMs);
      fields.putLong(minCompactionLagMs);
      fields.putLong(maxCompactionLagMs);
      fields.putLong(Double.doubleToLongBits(minCleanableDirtyRatio));
      fields.putInt(cleanupPolicyFlags);
      try {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        digest.update("NEREUS_NKC1_LOG_CONFIG_V1\0".getBytes(StandardCharsets.US_ASCII));
        return new Checksum(
            ChecksumType.SHA256, HexFormat.of().formatHex(digest.digest(fields.array())));
      } catch (NoSuchAlgorithmException failure) {
        throw new IllegalStateException("SHA-256 is unavailable", failure);
      }
    }
  }

  public enum RollReason {
    INITIAL(1),
    SIZE(2),
    TIME(3),
    RELATIVE_OFFSET_OVERFLOW(4),
    INDEX_FULL(5),
    CONFIG(6),
    MANUAL(7),
    TEST(8);

    private final int wireId;

    RollReason(int wireId) {
      this.wireId = wireId;
    }

    public int wireId() {
      return wireId;
    }

    public static RollReason fromWireId(int wireId) {
      for (RollReason value : values()) {
        if (value.wireId == wireId) {
          return value;
        }
      }
      throw new IllegalArgumentException("unknown Kafka virtual-segment roll reason");
    }
  }

  public enum SegmentState {
    ACTIVE(1),
    CLOSED(2);

    private final int wireId;

    SegmentState(int wireId) {
      this.wireId = wireId;
    }

    public int wireId() {
      return wireId;
    }

    public static SegmentState fromWireId(int wireId) {
      for (SegmentState value : values()) {
        if (value.wireId == wireId) {
          return value;
        }
      }
      throw new IllegalArgumentException("unknown Kafka virtual-segment state");
    }
  }
}
