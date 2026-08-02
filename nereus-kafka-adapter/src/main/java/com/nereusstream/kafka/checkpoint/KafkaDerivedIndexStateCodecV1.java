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

import com.nereusstream.kafka.checkpoint.KafkaDerivedIndexState.LogicalByteSample;
import com.nereusstream.kafka.checkpoint.KafkaDerivedIndexState.SegmentLogicalByteIndex;
import com.nereusstream.kafka.checkpoint.KafkaDerivedIndexState.SegmentTimeIndex;
import com.nereusstream.kafka.checkpoint.KafkaDerivedIndexState.TimeIndexEntry;
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
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Strict big-endian V1 codec for NKC1 sections 5 and 6.
 */
public final class KafkaDerivedIndexStateCodecV1 {
    private static final int PAYLOAD_VERSION = 1;
    private static final int TIME_SEGMENT_BYTES = Long.BYTES + Integer.BYTES;
    private static final int TIME_ENTRY_BYTES = Long.BYTES * 2;
    private static final int BYTE_SEGMENT_BYTES = Long.BYTES * 2 + Integer.BYTES;
    private static final int BYTE_SAMPLE_BYTES = Long.BYTES * 2;

    public List<KafkaCheckpointSection> encodeSections(
            KafkaDerivedIndexState state, long expectedLogStartOffset, long expectedStableEndOffset) {
        KafkaDerivedIndexState exact = Objects.requireNonNull(state, "state");
        exact.requireBounds(expectedLogStartOffset, expectedStableEndOffset);
        return List.of(
                required(KafkaCheckpointSectionType.TIME_INDEX, encodeTimeIndexes(exact.timeIndexes())),
                required(
                        KafkaCheckpointSectionType.LOGICAL_BYTE_POSITION_INDEX,
                        encodeLogicalByteIndexes(exact.logicalByteIndexes())));
    }

    public KafkaDerivedIndexState decodeSections(
            List<KafkaCheckpointSection> sections, long expectedLogStartOffset, long expectedStableEndOffset) {
        if (expectedLogStartOffset < 0 || expectedStableEndOffset < expectedLogStartOffset) {
            throw new IllegalArgumentException("invalid Kafka derived-index checkpoint bounds");
        }
        try {
            Map<KafkaCheckpointSectionType, KafkaCheckpointSection> located =
                    locate(List.copyOf(Objects.requireNonNull(sections, "sections")));
            return new KafkaDerivedIndexState(
                    expectedLogStartOffset,
                    expectedStableEndOffset,
                    decodeTimeIndexes(
                            located.get(KafkaCheckpointSectionType.TIME_INDEX).payload()),
                    decodeLogicalByteIndexes(located.get(KafkaCheckpointSectionType.LOGICAL_BYTE_POSITION_INDEX)
                            .payload()));
        } catch (KafkaCheckpointFormatException failure) {
            throw failure;
        } catch (IllegalArgumentException failure) {
            throw malformed("malformed NKC1 derived-index state", failure);
        }
    }

    private static byte[] encodeTimeIndexes(List<SegmentTimeIndex> indexes) {
        return writePayload(output -> {
            output.writeShort(PAYLOAD_VERSION);
            output.writeInt(indexes.size());
            for (SegmentTimeIndex segment : indexes) {
                output.writeLong(segment.segmentBaseOffset());
                output.writeInt(segment.entries().size());
                for (TimeIndexEntry entry : segment.entries()) {
                    output.writeLong(entry.timestamp());
                    output.writeLong(entry.offset());
                }
            }
        });
    }

    private static byte[] encodeLogicalByteIndexes(List<SegmentLogicalByteIndex> indexes) {
        return writePayload(output -> {
            output.writeShort(PAYLOAD_VERSION);
            output.writeInt(indexes.size());
            for (SegmentLogicalByteIndex segment : indexes) {
                output.writeLong(segment.segmentBaseOffset());
                output.writeLong(segment.segmentLogicalBytes());
                output.writeInt(segment.samples().size());
                for (LogicalByteSample sample : segment.samples()) {
                    output.writeLong(sample.entryStartOffset());
                    output.writeLong(sample.cumulativeLogicalBytes());
                }
            }
        });
    }

    private static List<SegmentTimeIndex> decodeTimeIndexes(byte[] payload) {
        Reader input = new Reader(payload, "time index");
        input.requireVersion();
        int segmentCount = input.readCount("segmentCount", TIME_SEGMENT_BYTES);
        ArrayList<SegmentTimeIndex> indexes = new ArrayList<>(boundedCapacity(segmentCount));
        for (int segmentIndex = 0; segmentIndex < segmentCount; segmentIndex++) {
            long segmentBaseOffset = input.readLong("segmentBaseOffset");
            int entryCount = input.readCount("entryCount", TIME_ENTRY_BYTES);
            ArrayList<TimeIndexEntry> entries = new ArrayList<>(boundedCapacity(entryCount));
            for (int entryIndex = 0; entryIndex < entryCount; entryIndex++) {
                entries.add(new TimeIndexEntry(input.readLong("timestamp"), input.readLong("offset")));
            }
            indexes.add(new SegmentTimeIndex(segmentBaseOffset, entries));
        }
        input.requireEnd();
        return List.copyOf(indexes);
    }

    private static List<SegmentLogicalByteIndex> decodeLogicalByteIndexes(byte[] payload) {
        Reader input = new Reader(payload, "logical-byte index");
        input.requireVersion();
        int segmentCount = input.readCount("segmentCount", BYTE_SEGMENT_BYTES);
        ArrayList<SegmentLogicalByteIndex> indexes = new ArrayList<>(boundedCapacity(segmentCount));
        for (int segmentIndex = 0; segmentIndex < segmentCount; segmentIndex++) {
            long segmentBaseOffset = input.readLong("segmentBaseOffset");
            long segmentLogicalBytes = input.readLong("segmentLogicalBytes");
            int sampleCount = input.readCount("sampleCount", BYTE_SAMPLE_BYTES);
            ArrayList<LogicalByteSample> samples = new ArrayList<>(boundedCapacity(sampleCount));
            for (int sampleIndex = 0; sampleIndex < sampleCount; sampleIndex++) {
                samples.add(new LogicalByteSample(
                        input.readLong("entryStartOffset"), input.readLong("cumulativeLogicalBytes")));
            }
            indexes.add(new SegmentLogicalByteIndex(segmentBaseOffset, segmentLogicalBytes, samples));
        }
        input.requireEnd();
        return List.copyOf(indexes);
    }

    private static Map<KafkaCheckpointSectionType, KafkaCheckpointSection> locate(
            List<KafkaCheckpointSection> sections) {
        EnumMap<KafkaCheckpointSectionType, KafkaCheckpointSection> found =
                new EnumMap<>(KafkaCheckpointSectionType.class);
        for (KafkaCheckpointSection section : sections) {
            Objects.requireNonNull(section, "section");
            KafkaCheckpointSectionType type;
            if (section.sectionType() == KafkaCheckpointSectionType.TIME_INDEX.wireId()) {
                type = KafkaCheckpointSectionType.TIME_INDEX;
            } else if (section.sectionType() == KafkaCheckpointSectionType.LOGICAL_BYTE_POSITION_INDEX.wireId()) {
                type = KafkaCheckpointSectionType.LOGICAL_BYTE_POSITION_INDEX;
            } else {
                continue;
            }
            if (found.putIfAbsent(type, section) != null) {
                throw malformed("duplicate NKC1 " + type + " section");
            }
            if (!section.required()
                    || section.sectionVersion() != PAYLOAD_VERSION
                    || section.sectionFlags() != KafkaCheckpointFormatV1.SECTION_REQUIRED_FLAG) {
                throw malformed("unsupported NKC1 " + type + " section header");
            }
        }
        if (!found.containsKey(KafkaCheckpointSectionType.TIME_INDEX)
                || !found.containsKey(KafkaCheckpointSectionType.LOGICAL_BYTE_POSITION_INDEX)) {
            throw malformed("missing NKC1 derived-index section");
        }
        return found;
    }

    private static KafkaCheckpointSection required(KafkaCheckpointSectionType type, byte[] payload) {
        return KafkaCheckpointSection.required(type, payload);
    }

    private static byte[] writePayload(PayloadWriter writer) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream output = new DataOutputStream(bytes);
            writer.write(output);
            output.flush();
            byte[] payload = bytes.toByteArray();
            if (payload.length > KafkaCheckpointFormatV1.MAX_SECTION_BYTES) {
                throw malformed("NKC1 derived-index section exceeds its hard limit");
            }
            return payload;
        } catch (IOException failure) {
            throw malformed("failed to encode NKC1 derived-index section", failure);
        }
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

    @FunctionalInterface
    private interface PayloadWriter {
        void write(DataOutputStream output) throws IOException;
    }

    private static final class Reader {
        private final ByteBuffer input;
        private final String section;

        private Reader(byte[] payload, String section) {
            Objects.requireNonNull(payload, "payload");
            if (payload.length > KafkaCheckpointFormatV1.MAX_SECTION_BYTES) {
                throw malformed("NKC1 " + section + " section exceeds its hard limit");
            }
            this.input = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
            this.section = section;
        }

        private void requireVersion() {
            requireRemaining(Short.BYTES, "payloadVersion");
            if (Short.toUnsignedInt(input.getShort()) != PAYLOAD_VERSION) {
                throw malformed("unsupported NKC1 " + section + " payload version");
            }
        }

        private int readCount(String field, int minimumEntryBytes) {
            requireRemaining(Integer.BYTES, field);
            long count = Integer.toUnsignedLong(input.getInt());
            if (count > Integer.MAX_VALUE || count > input.remaining() / minimumEntryBytes) {
                throw malformed("invalid NKC1 " + section + " " + field);
            }
            return (int) count;
        }

        private long readLong(String field) {
            requireRemaining(Long.BYTES, field);
            return input.getLong();
        }

        private void requireEnd() {
            if (input.hasRemaining()) {
                throw malformed("NKC1 " + section + " contains trailing bytes");
            }
        }

        private void requireRemaining(int bytes, String field) {
            if (input.remaining() < bytes) {
                throw malformed("truncated NKC1 " + section + " " + field);
            }
        }
    }
}
