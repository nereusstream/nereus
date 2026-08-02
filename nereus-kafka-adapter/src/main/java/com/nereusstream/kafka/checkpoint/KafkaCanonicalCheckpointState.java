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

import com.nereusstream.kafka.checkpoint.KafkaDerivedIndexState.SegmentLogicalByteIndex;
import com.nereusstream.kafka.checkpoint.KafkaDerivedIndexState.SegmentTimeIndex;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState.VirtualSegment;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Complete Kafka-artifact-neutral canonical image for all seven required NKC1 V1 sections.
 */
public record KafkaCanonicalCheckpointState(
        long checkpointOffset,
        long logStartOffset,
        long stableEndOffset,
        KafkaProducerTransactionState producerTransactionState,
        KafkaLeaderEpochState leaderEpochState,
        KafkaVirtualSegmentState virtualSegmentState,
        KafkaDerivedIndexState derivedIndexState) {

    public KafkaCanonicalCheckpointState {
        if (checkpointOffset < 0
                || logStartOffset < 0
                || stableEndOffset < logStartOffset
                || checkpointOffset != stableEndOffset) {
            throw new IllegalArgumentException("canonical Kafka checkpoint must capture the exact stable end");
        }
        producerTransactionState = Objects.requireNonNull(producerTransactionState, "producerTransactionState");
        leaderEpochState = Objects.requireNonNull(leaderEpochState, "leaderEpochState");
        virtualSegmentState = Objects.requireNonNull(virtualSegmentState, "virtualSegmentState");
        derivedIndexState = Objects.requireNonNull(derivedIndexState, "derivedIndexState");
        producerTransactionState.requireCheckpointOffset(checkpointOffset);
        leaderEpochState.requireBounds(logStartOffset, stableEndOffset);
        virtualSegmentState.requireBounds(logStartOffset, stableEndOffset);
        derivedIndexState.requireBounds(logStartOffset, stableEndOffset);
        validateSegmentIndexes(virtualSegmentState.segments(), derivedIndexState);
    }

    private static void validateSegmentIndexes(List<VirtualSegment> segments, KafkaDerivedIndexState indexes) {
        List<SegmentLogicalByteIndex> logical = indexes.logicalByteIndexes();
        if (segments.size() != logical.size()) {
            throw new IllegalArgumentException("NKC1 virtual and logical-byte segment sets differ");
        }
        Map<Long, VirtualSegment> byBase = new HashMap<>();
        for (int index = 0; index < segments.size(); index++) {
            VirtualSegment segment = segments.get(index);
            SegmentLogicalByteIndex logicalSegment = logical.get(index);
            if (segment.baseOffset() != logicalSegment.segmentBaseOffset()
                    || segment.logicalBytes() != logicalSegment.segmentLogicalBytes()) {
                throw new IllegalArgumentException("NKC1 virtual and logical-byte segment facts differ");
            }
            logicalSegment.samples().forEach(sample -> {
                if (sample.entryStartOffset() >= segment.endOffset()) {
                    throw new IllegalArgumentException("NKC1 logical-byte sample crosses its virtual segment");
                }
            });
            byBase.put(segment.baseOffset(), segment);
        }
        for (SegmentTimeIndex timeIndex : indexes.timeIndexes()) {
            VirtualSegment segment = byBase.get(timeIndex.segmentBaseOffset());
            if (segment == null) {
                throw new IllegalArgumentException("NKC1 time index references an unknown virtual segment");
            }
            timeIndex.entries().forEach(entry -> {
                if (entry.offset() >= segment.endOffset() || entry.timestamp() > segment.largestTimestamp()) {
                    throw new IllegalArgumentException("NKC1 time-index entry exceeds its virtual segment");
                }
            });
        }
    }
}
