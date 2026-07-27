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

import java.util.List;
import java.util.Objects;

/** Kafka-artifact-neutral canonical image of the stock leader-epoch cache. */
public record KafkaLeaderEpochState(
        long logStartOffset,
        long stableEndOffset,
        List<LeaderEpochRange> ranges) {

    public KafkaLeaderEpochState {
        if (logStartOffset < 0 || stableEndOffset < logStartOffset) {
            throw new IllegalArgumentException("invalid Kafka leader-epoch state bounds");
        }
        ranges = List.copyOf(Objects.requireNonNull(ranges, "ranges"));
        int previousEpoch = -1;
        long previousStartOffset = -1;
        for (int index = 0; index < ranges.size(); index++) {
            LeaderEpochRange range =
                    Objects.requireNonNull(ranges.get(index), "range");
            if (range.leaderEpoch() <= previousEpoch
                    || range.startOffset() <= previousStartOffset
                    || range.startOffset() > stableEndOffset
                    || (index > 0 && range.startOffset() < logStartOffset)) {
                throw new IllegalArgumentException(
                        "Kafka leader epochs and start offsets must be strictly ordered and bounded");
            }
            previousEpoch = range.leaderEpoch();
            previousStartOffset = range.startOffset();
        }
    }

    public void requireBounds(
            long expectedLogStartOffset,
            long expectedStableEndOffset) {
        if (logStartOffset != expectedLogStartOffset
                || stableEndOffset != expectedStableEndOffset) {
            throw new IllegalArgumentException(
                    "Kafka leader-epoch state does not match checkpoint bounds");
        }
    }

    /**
     * One epoch start. Only the first retained range may start before log start; its position makes it the explicit
     * carried-forward range needed to answer the epoch containing the retained floor.
     */
    public record LeaderEpochRange(int leaderEpoch, long startOffset) {
        public LeaderEpochRange {
            if (leaderEpoch < 0 || startOffset < 0) {
                throw new IllegalArgumentException(
                        "invalid Kafka leader-epoch range");
            }
        }
    }
}
