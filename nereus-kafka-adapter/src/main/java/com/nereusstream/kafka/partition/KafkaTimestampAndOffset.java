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

package com.nereusstream.kafka.partition;

import java.util.Objects;
import java.util.OptionalInt;

/** Exact Kafka record timestamp, logical offset, and optional batch leader epoch returned by the fork inspector. */
public record KafkaTimestampAndOffset(
        long timestampMillis,
        long offset,
        OptionalInt leaderEpoch) {
    public KafkaTimestampAndOffset {
        Objects.requireNonNull(leaderEpoch, "leaderEpoch");
        if (timestampMillis < 0 || offset < 0 || (leaderEpoch.isPresent() && leaderEpoch.orElseThrow() < 0)) {
            throw new IllegalArgumentException("invalid Kafka timestamp/offset result");
        }
    }
}
