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
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import com.nereusstream.kafka.partition.KafkaPartitionIdentity;
import com.nereusstream.kafka.partition.KafkaStableSnapshot;
import com.nereusstream.kafka.partition.KafkaStorageReadRequest;
import com.nereusstream.metadata.oxia.VersionedKafkaPartitionBinding;
import com.nereusstream.metadata.oxia.records.KafkaCompactionCoverageRecord;
import com.nereusstream.metadata.oxia.records.KafkaPartitionBindingRecord;
import com.nereusstream.metadata.oxia.records.KafkaPartitionLifecycle;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Builds one immutable Kafka Fetch view plan from a linearizable partition binding root.
 *
 * <p>The mandatory compacted prefix is correctness state, not a cleanup-policy hint. Once the
 * binding activates it, this planner never emits a COMMITTED segment below its exclusive end.
 */
public final class KafkaCompactedFetchPlanner {
    public Plan plan(
            KafkaPartitionIdentity identity,
            StreamId streamId,
            KafkaStableSnapshot stableSnapshot,
            KafkaStorageReadRequest request,
            VersionedKafkaPartitionBinding binding) {
        Objects.requireNonNull(identity, "identity");
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(stableSnapshot, "stableSnapshot");
        Objects.requireNonNull(request, "request");
        VersionedKafkaPartitionBinding exactBinding = Objects.requireNonNull(binding, "binding");
        KafkaPartitionBindingRecord root = exactBinding.value();
        if (!root.identity().equals(identity.durableId())
                || !root.streamId().equals(streamId.value())
                || root.lifecycle() != KafkaPartitionLifecycle.ACTIVE) {
            throw invariant("Kafka Fetch binding does not identify the exact ACTIVE partition stream");
        }
        long authoritativeLogStart = Math.max(stableSnapshot.logStartOffset(), root.observedLogStartOffset());
        if (request.startOffset() < authoritativeLogStart) {
            throw new NereusException(
                    ErrorCode.OFFSET_TRIMMED,
                    false,
                    "Kafka Fetch offset precedes the authoritative partition log start");
        }
        long upperBound = Math.min(request.maxOffsetExclusive(), stableSnapshot.stableEndOffset());
        if (request.startOffset() >= upperBound) {
            return new Plan(exactBinding, new OffsetRange(request.startOffset(), request.startOffset()), List.of());
        }

        KafkaCompactionCoverageRecord coverage = root.compactionCoverage();
        ArrayList<Segment> segments = new ArrayList<>(2);
        long cursor = request.startOffset();
        if (coverage.coverageVersion() != 0 && cursor < coverage.endOffset()) {
            if (coverage.startOffset() > authoritativeLogStart
                    || coverage.endOffset() > stableSnapshot.stableEndOffset()) {
                throw invariant("Kafka mandatory compaction coverage is outside the readable partition window");
            }
            long compactedEnd = Math.min(coverage.endOffset(), upperBound);
            segments.add(new Segment(
                    ReadView.TOPIC_COMPACTED,
                    new OffsetRange(cursor, compactedEnd),
                    Optional.of(new MandatoryAuthority(
                            coverage.activationEpoch(),
                            sha256(coverage.generationSetSha256()),
                            sha256(coverage.policySha256())))));
            cursor = compactedEnd;
        }
        if (cursor < upperBound) {
            segments.add(new Segment(ReadView.COMMITTED, new OffsetRange(cursor, upperBound), Optional.empty()));
        }
        return new Plan(exactBinding, new OffsetRange(request.startOffset(), upperBound), segments);
    }

    public Plan committedOnly(KafkaStableSnapshot stableSnapshot, KafkaStorageReadRequest request) {
        Objects.requireNonNull(stableSnapshot, "stableSnapshot");
        Objects.requireNonNull(request, "request");
        if (request.startOffset() < stableSnapshot.logStartOffset()) {
            throw new NereusException(
                    ErrorCode.OFFSET_TRIMMED, false, "Kafka Fetch offset precedes the partition log start");
        }
        long upperBound = Math.min(request.maxOffsetExclusive(), stableSnapshot.stableEndOffset());
        OffsetRange range = new OffsetRange(request.startOffset(), Math.max(request.startOffset(), upperBound));
        List<Segment> segments =
                range.isEmpty() ? List.of() : List.of(new Segment(ReadView.COMMITTED, range, Optional.empty()));
        return new Plan(null, range, segments);
    }

    public record Plan(VersionedKafkaPartitionBinding binding, OffsetRange requestRange, List<Segment> segments) {
        public Plan {
            Objects.requireNonNull(requestRange, "requestRange");
            segments = List.copyOf(Objects.requireNonNull(segments, "segments"));
            long cursor = requestRange.startOffset();
            boolean committedSeen = false;
            for (Segment segment : segments) {
                if (segment.range().startOffset() != cursor
                        || segment.range().endOffset() > requestRange.endOffset()
                        || (committedSeen && segment.view() != ReadView.COMMITTED)) {
                    throw new IllegalArgumentException(
                            "Kafka Fetch plan segments must be ordered, gap-free and view-monotonic");
                }
                committedSeen |= segment.view() == ReadView.COMMITTED;
                cursor = segment.range().endOffset();
            }
            if (cursor != requestRange.endOffset()) {
                throw new IllegalArgumentException("Kafka Fetch plan does not cover the requested readable range");
            }
        }

        public boolean hasMandatoryCompactedPrefix() {
            return !segments.isEmpty() && segments.get(0).view() == ReadView.TOPIC_COMPACTED;
        }
    }

    public record Segment(ReadView view, OffsetRange range, Optional<MandatoryAuthority> mandatoryAuthority) {
        public Segment {
            Objects.requireNonNull(view, "view");
            Objects.requireNonNull(range, "range");
            mandatoryAuthority = Objects.requireNonNull(mandatoryAuthority, "mandatoryAuthority");
            if (range.isEmpty() || (view == ReadView.TOPIC_COMPACTED) != mandatoryAuthority.isPresent()) {
                throw new IllegalArgumentException("Kafka Fetch segment view and mandatory authority are inconsistent");
            }
        }
    }

    public record MandatoryAuthority(long activationEpoch, Checksum generationSetSha256, Checksum policySha256) {
        public MandatoryAuthority {
            Objects.requireNonNull(generationSetSha256, "generationSetSha256");
            Objects.requireNonNull(policySha256, "policySha256");
            if (activationEpoch <= 0
                    || generationSetSha256.type() != ChecksumType.SHA256
                    || policySha256.type() != ChecksumType.SHA256) {
                throw new IllegalArgumentException("invalid Kafka mandatory compaction read authority");
            }
        }
    }

    private static Checksum sha256(byte[] value) {
        return new Checksum(ChecksumType.SHA256, HexFormat.of().formatHex(value));
    }

    private static NereusException invariant(String message) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }
}
