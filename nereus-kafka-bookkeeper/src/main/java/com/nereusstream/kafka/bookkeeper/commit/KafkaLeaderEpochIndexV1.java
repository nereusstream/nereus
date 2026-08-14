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

package com.nereusstream.kafka.bookkeeper.commit;

import java.util.Collections;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

/** Kafka leader epoch to first committed offset; Owner and Storage Epoch never enter this index. */
public record KafkaLeaderEpochIndexV1(NavigableMap<Integer, Long> startOffsets) {
    public KafkaLeaderEpochIndexV1 {
        startOffsets = Collections.unmodifiableNavigableMap(
                new TreeMap<>(Objects.requireNonNull(startOffsets, "startOffsets")));
        long previousOffset = -1;
        for (var entry : startOffsets.entrySet()) {
            if (entry.getKey() < 0 || entry.getValue() < 0 || entry.getValue() < previousOffset) {
                throw new IllegalArgumentException("leader epoch index regresses epoch or start offset");
            }
            previousOffset = entry.getValue();
        }
    }

    public static KafkaLeaderEpochIndexV1 empty() {
        return new KafkaLeaderEpochIndexV1(new TreeMap<>());
    }

    public KafkaLeaderEpochIndexV1 observe(int leaderEpoch, long startOffset) {
        if (leaderEpoch < 0 || startOffset < 0) {
            throw new IllegalArgumentException("leader epoch observation is outside its domain");
        }
        Long existing = startOffsets.get(leaderEpoch);
        if (existing != null) {
            if (existing != startOffset && existing > startOffset) {
                throw new IllegalArgumentException("leader epoch start offset regresses");
            }
            return this;
        }
        if (!startOffsets.isEmpty()
                && (leaderEpoch <= startOffsets.lastKey()
                        || startOffset < startOffsets.lastEntry().getValue())) {
            throw new IllegalArgumentException("leader epoch observation is not monotonic");
        }
        TreeMap<Integer, Long> replacement = new TreeMap<>(startOffsets);
        replacement.put(leaderEpoch, startOffset);
        return new KafkaLeaderEpochIndexV1(replacement);
    }
}
