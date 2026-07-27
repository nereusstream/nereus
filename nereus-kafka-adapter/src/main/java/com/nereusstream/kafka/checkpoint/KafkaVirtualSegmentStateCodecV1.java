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
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState.LogConfigHistoryEntry;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState.RollReason;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState.SegmentState;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState.VirtualSegment;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointFormatException;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointFormatV1;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointSection;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointSectionType;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;

/** Strict big-endian V1 codec for NKC1 section 4. */
public final class KafkaVirtualSegmentStateCodecV1 {
  private static final int PAYLOAD_VERSION = 1;
  private static final int SHA256_BYTES = 32;
  private static final int SEGMENT_BYTES = Long.BYTES * 11 + SHA256_BYTES + Integer.BYTES * 2;
  private static final int CONFIG_BYTES = Long.BYTES * 12 + Integer.BYTES * 3 + SHA256_BYTES;

  public KafkaCheckpointSection encodeSection(
      KafkaVirtualSegmentState state, long expectedLogStartOffset, long expectedStableEndOffset) {
    KafkaVirtualSegmentState exact = Objects.requireNonNull(state, "state");
    exact.requireBounds(expectedLogStartOffset, expectedStableEndOffset);
    return KafkaCheckpointSection.required(
        KafkaCheckpointSectionType.VIRTUAL_SEGMENT_DESCRIPTORS, encodePayload(exact));
  }

  public KafkaVirtualSegmentState decodeSection(
      List<KafkaCheckpointSection> sections,
      long expectedLogStartOffset,
      long expectedStableEndOffset) {
    if (expectedLogStartOffset < 0 || expectedStableEndOffset < expectedLogStartOffset) {
      throw new IllegalArgumentException("invalid Kafka virtual-segment checkpoint bounds");
    }
    try {
      KafkaCheckpointSection section =
          locate(List.copyOf(Objects.requireNonNull(sections, "sections")));
      Reader input = new Reader(section.payload());
      input.requireVersion();
      int segmentCount = input.readCount("segmentCount", SEGMENT_BYTES);
      ArrayList<VirtualSegment> segments = new ArrayList<>(boundedCapacity(segmentCount));
      for (int index = 0; index < segmentCount; index++) {
        segments.add(
            new VirtualSegment(
                input.readLong("baseOffset"),
                input.readLong("endOffset"),
                input.readLong("rollSequence"),
                input.readLong("createdAtMillis"),
                input.readLong("closedAtMillis"),
                input.readLong("rollJitterMillis"),
                input.readLong("largestTimestamp"),
                input.readLong("maxTimestampOffset"),
                input.readLong("logicalBytes"),
                input.readLong("firstCumulativeBytes"),
                input.readLong("lastCumulativeBytes"),
                input.readSha256("configDigest"),
                RollReason.fromWireId(input.readInt("rollReasonId")),
                SegmentState.fromWireId(input.readInt("stateId"))));
      }
      int configCount = input.readCount("configCount", CONFIG_BYTES);
      ArrayList<LogConfigHistoryEntry> history = new ArrayList<>(boundedCapacity(configCount));
      for (int index = 0; index < configCount; index++) {
        history.add(
            new LogConfigHistoryEntry(
                input.readLong("metadataOffset"),
                input.readLong("effectiveFromOffset"),
                input.readLong("segmentBytes"),
                input.readLong("segmentMs"),
                input.readLong("segmentJitterMillis"),
                input.readInt("segmentIndexBytes"),
                input.readInt("indexIntervalBytes"),
                input.readLong("retentionBytes"),
                input.readLong("retentionMs"),
                input.readLong("fileDeleteDelayMs"),
                input.readLong("deleteRetentionMs"),
                input.readLong("minCompactionLagMs"),
                input.readLong("maxCompactionLagMs"),
                Double.longBitsToDouble(input.readLong("minCleanableDirtyRatioBits")),
                input.readInt("cleanupPolicyFlags"),
                input.readSha256("configDigest")));
      }
      input.requireEnd();
      return new KafkaVirtualSegmentState(
          expectedLogStartOffset, expectedStableEndOffset, segments, history);
    } catch (KafkaCheckpointFormatException failure) {
      throw failure;
    } catch (IllegalArgumentException failure) {
      throw malformed("malformed NKC1 virtual-segment state", failure);
    }
  }

  private static byte[] encodePayload(KafkaVirtualSegmentState state) {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      DataOutputStream output = new DataOutputStream(bytes);
      output.writeShort(PAYLOAD_VERSION);
      output.writeInt(state.segments().size());
      for (VirtualSegment segment : state.segments()) {
        output.writeLong(segment.baseOffset());
        output.writeLong(segment.endOffset());
        output.writeLong(segment.rollSequence());
        output.writeLong(segment.createdAtMillis());
        output.writeLong(segment.closedAtMillis());
        output.writeLong(segment.rollJitterMillis());
        output.writeLong(segment.largestTimestamp());
        output.writeLong(segment.maxTimestampOffset());
        output.writeLong(segment.logicalBytes());
        output.writeLong(segment.firstCumulativeBytes());
        output.writeLong(segment.lastCumulativeBytes());
        output.write(digestBytes(segment.configDigest()));
        output.writeInt(segment.rollReason().wireId());
        output.writeInt(segment.state().wireId());
      }
      output.writeInt(state.configHistory().size());
      for (LogConfigHistoryEntry config : state.configHistory()) {
        output.writeLong(config.metadataOffset());
        output.writeLong(config.effectiveFromOffset());
        output.writeLong(config.segmentBytes());
        output.writeLong(config.segmentMs());
        output.writeLong(config.segmentJitterMillis());
        output.writeInt(config.segmentIndexBytes());
        output.writeInt(config.indexIntervalBytes());
        output.writeLong(config.retentionBytes());
        output.writeLong(config.retentionMs());
        output.writeLong(config.fileDeleteDelayMs());
        output.writeLong(config.deleteRetentionMs());
        output.writeLong(config.minCompactionLagMs());
        output.writeLong(config.maxCompactionLagMs());
        output.writeLong(Double.doubleToLongBits(config.minCleanableDirtyRatio()));
        output.writeInt(config.cleanupPolicyFlags());
        output.write(digestBytes(config.configDigest()));
      }
      output.flush();
      byte[] payload = bytes.toByteArray();
      if (payload.length > KafkaCheckpointFormatV1.MAX_SECTION_BYTES) {
        throw malformed("NKC1 virtual-segment section exceeds its hard limit");
      }
      return payload;
    } catch (IOException failure) {
      throw malformed("failed to encode NKC1 virtual-segment section", failure);
    }
  }

  private static KafkaCheckpointSection locate(List<KafkaCheckpointSection> sections) {
    KafkaCheckpointSection found = null;
    for (KafkaCheckpointSection section : sections) {
      Objects.requireNonNull(section, "section");
      if (section.sectionType()
          != KafkaCheckpointSectionType.VIRTUAL_SEGMENT_DESCRIPTORS.wireId()) {
        continue;
      }
      if (found != null) {
        throw malformed("duplicate NKC1 virtual-segment section");
      }
      if (!section.required()
          || section.sectionVersion() != PAYLOAD_VERSION
          || section.sectionFlags() != KafkaCheckpointFormatV1.SECTION_REQUIRED_FLAG) {
        throw malformed("unsupported NKC1 virtual-segment section header");
      }
      found = section;
    }
    if (found == null) {
      throw malformed("missing NKC1 virtual-segment section");
    }
    return found;
  }

  private static byte[] digestBytes(Checksum checksum) {
    if (checksum.type() != ChecksumType.SHA256) {
      throw new IllegalArgumentException("checkpoint digest must use SHA256");
    }
    return HexFormat.of().parseHex(checksum.value());
  }

  private static int boundedCapacity(int count) {
    return Math.min(count, 1 << 16);
  }

  private static KafkaCheckpointFormatException malformed(String message) {
    return new KafkaCheckpointFormatException(message);
  }

  private static KafkaCheckpointFormatException malformed(String message, Throwable cause) {
    return new KafkaCheckpointFormatException(message, cause);
  }

  private static final class Reader {
    private final ByteBuffer input;

    private Reader(byte[] payload) {
      Objects.requireNonNull(payload, "payload");
      if (payload.length > KafkaCheckpointFormatV1.MAX_SECTION_BYTES) {
        throw malformed("NKC1 virtual-segment section exceeds its hard limit");
      }
      input = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
    }

    private void requireVersion() {
      requireRemaining(Short.BYTES, "payloadVersion");
      if (Short.toUnsignedInt(input.getShort()) != PAYLOAD_VERSION) {
        throw malformed("unsupported NKC1 virtual-segment payload version");
      }
    }

    private int readCount(String field, int minimumEntryBytes) {
      requireRemaining(Integer.BYTES, field);
      long count = Integer.toUnsignedLong(input.getInt());
      if (count > Integer.MAX_VALUE || count > input.remaining() / minimumEntryBytes) {
        throw malformed("invalid NKC1 virtual-segment " + field);
      }
      return (int) count;
    }

    private int readInt(String field) {
      requireRemaining(Integer.BYTES, field);
      return input.getInt();
    }

    private long readLong(String field) {
      requireRemaining(Long.BYTES, field);
      return input.getLong();
    }

    private Checksum readSha256(String field) {
      requireRemaining(SHA256_BYTES, field);
      byte[] digest = new byte[SHA256_BYTES];
      input.get(digest);
      return new Checksum(ChecksumType.SHA256, HexFormat.of().formatHex(digest));
    }

    private void requireEnd() {
      if (input.hasRemaining()) {
        throw malformed("NKC1 virtual-segment section contains trailing bytes");
      }
    }

    private void requireRemaining(int bytes, String field) {
      if (input.remaining() < bytes) {
        throw malformed("truncated NKC1 virtual-segment " + field);
      }
    }
  }
}
