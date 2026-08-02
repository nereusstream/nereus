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

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Kafka-artifact-neutral canonical image for NKC1 time and logical-byte position indexes.
 */
public record KafkaDerivedIndexState(
        long logStartOffset,
        long stableEndOffset,
        List<SegmentTimeIndex> timeIndexes,
        List<SegmentLogicalByteIndex> logicalByteIndexes) {

    public KafkaDerivedIndexState {
        if (logStartOffset < 0 || stableEndOffset < logStartOffset) {
            throw new IllegalArgumentException("invalid Kafka derived-index bounds");
        }
        timeIndexes = List.copyOf(Objects.requireNonNull(timeIndexes, "timeIndexes"));
        logicalByteIndexes = List.copyOf(Objects.requireNonNull(logicalByteIndexes, "logicalByteIndexes"));
        validateLogicalByteIndexes(logicalByteIndexes, logStartOffset, stableEndOffset);
        validateTimeIndexes(timeIndexes, logicalByteIndexes, logStartOffset, stableEndOffset);
    }

    public void requireBounds(long expectedLogStartOffset, long expectedStableEndOffset) {
        if (logStartOffset != expectedLogStartOffset || stableEndOffset != expectedStableEndOffset) {
            throw new IllegalArgumentException("Kafka derived-index state does not match checkpoint bounds");
        }
    }

    private static void validateLogicalByteIndexes(
            List<SegmentLogicalByteIndex> indexes, long logStartOffset, long stableEndOffset) {
        long previousBaseOffset = -1;
        for (int index = 0; index < indexes.size(); index++) {
            SegmentLogicalByteIndex segment = Objects.requireNonNull(indexes.get(index), "logicalByteIndex");
            if (segment.segmentBaseOffset() <= previousBaseOffset
                    || segment.segmentBaseOffset() > stableEndOffset
                    || (index > 0 && segment.segmentBaseOffset() < logStartOffset)) {
                throw new IllegalArgumentException(
                        "Kafka logical-byte segment bases must be strictly ordered and bounded");
            }
            if (segment.segmentBaseOffset() == stableEndOffset && segment.segmentLogicalBytes() != 0) {
                throw new IllegalArgumentException("Kafka logical-byte segment at stable end must be empty");
            }
            previousBaseOffset = segment.segmentBaseOffset();
            long previousOffset = -1;
            long previousBytes = -1;
            for (LogicalByteSample sample : segment.samples()) {
                if (sample.entryStartOffset() < Math.max(segment.segmentBaseOffset(), logStartOffset)
                        || sample.entryStartOffset() >= stableEndOffset
                        || sample.entryStartOffset() <= previousOffset
                        || sample.cumulativeLogicalBytes() <= previousBytes
                        || sample.cumulativeLogicalBytes() >= segment.segmentLogicalBytes()) {
                    throw new IllegalArgumentException(
                            "Kafka logical-byte samples must be strictly ordered and bounded");
                }
                previousOffset = sample.entryStartOffset();
                previousBytes = sample.cumulativeLogicalBytes();
            }
        }
    }

    private static void validateTimeIndexes(
            List<SegmentTimeIndex> indexes,
            List<SegmentLogicalByteIndex> logicalIndexes,
            long logStartOffset,
            long stableEndOffset) {
        Set<Long> logicalBases = new HashSet<>();
        logicalIndexes.forEach(index -> logicalBases.add(index.segmentBaseOffset()));
        long previousBaseOffset = -1;
        for (SegmentTimeIndex segment : indexes) {
            Objects.requireNonNull(segment, "timeIndex");
            if (segment.segmentBaseOffset() <= previousBaseOffset
                    || !logicalBases.contains(segment.segmentBaseOffset())) {
                throw new IllegalArgumentException(
                        "Kafka time indexes must reference strictly ordered logical-byte segments");
            }
            previousBaseOffset = segment.segmentBaseOffset();
            long previousTimestamp = -1;
            long previousOffset = -1;
            for (TimeIndexEntry entry : segment.entries()) {
                if (entry.offset() < Math.max(segment.segmentBaseOffset(), logStartOffset)
                        || entry.offset() >= stableEndOffset
                        || entry.offset() <= previousOffset
                        || entry.timestamp() < previousTimestamp) {
                    throw new IllegalArgumentException("Kafka time-index entries must be monotonic and bounded");
                }
                previousTimestamp = entry.timestamp();
                previousOffset = entry.offset();
            }
        }
    }

    public record SegmentTimeIndex(long segmentBaseOffset, List<TimeIndexEntry> entries) {
        public SegmentTimeIndex {
            if (segmentBaseOffset < 0) {
                throw new IllegalArgumentException("segmentBaseOffset must be non-negative");
            }
            entries = List.copyOf(Objects.requireNonNull(entries, "entries"));
            entries.forEach(entry -> Objects.requireNonNull(entry, "entry"));
        }
    }

    public record TimeIndexEntry(long timestamp, long offset) {
        public TimeIndexEntry {
            if (timestamp < 0 || offset < 0) {
                throw new IllegalArgumentException("invalid Kafka time-index entry");
            }
        }
    }

    public record SegmentLogicalByteIndex(
            long segmentBaseOffset, long segmentLogicalBytes, List<LogicalByteSample> samples) {
        public SegmentLogicalByteIndex {
            if (segmentBaseOffset < 0 || segmentLogicalBytes < 0) {
                throw new IllegalArgumentException("invalid Kafka logical-byte segment");
            }
            samples = List.copyOf(Objects.requireNonNull(samples, "samples"));
            samples.forEach(sample -> Objects.requireNonNull(sample, "sample"));
            if (segmentLogicalBytes == 0 && !samples.isEmpty()) {
                throw new IllegalArgumentException("empty Kafka logical-byte segment cannot contain samples");
            }
        }
    }

    public record LogicalByteSample(long entryStartOffset, long cumulativeLogicalBytes) {
        public LogicalByteSample {
            if (entryStartOffset < 0 || cumulativeLogicalBytes < 0) {
                throw new IllegalArgumentException("invalid Kafka logical-byte sample");
            }
        }
    }
}
