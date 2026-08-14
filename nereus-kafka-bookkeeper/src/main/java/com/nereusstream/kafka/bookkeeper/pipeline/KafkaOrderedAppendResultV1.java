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

package com.nereusstream.kafka.bookkeeper.pipeline;

import java.util.Objects;
import java.util.OptionalLong;

/** Closed K4 result with a Kafka range only after assignment occurred. */
public record KafkaOrderedAppendResultV1(
        KafkaOrderedAppendOutcomeV1 outcome, OptionalLong startOffset, OptionalLong endOffsetExclusive) {
    public KafkaOrderedAppendResultV1 {
        Objects.requireNonNull(outcome, "outcome");
        Objects.requireNonNull(startOffset, "startOffset");
        Objects.requireNonNull(endOffsetExclusive, "endOffsetExclusive");
        if (startOffset.isPresent() != endOffsetExclusive.isPresent()
                || startOffset.isPresent() && endOffsetExclusive.getAsLong() <= startOffset.getAsLong()) {
            throw new IllegalArgumentException("ordered append result range is incomplete or empty");
        }
    }

    public static KafkaOrderedAppendResultV1 beforeAssignment(KafkaOrderedAppendOutcomeV1 outcome) {
        return new KafkaOrderedAppendResultV1(outcome, OptionalLong.empty(), OptionalLong.empty());
    }

    public static KafkaOrderedAppendResultV1 assigned(
            KafkaOrderedAppendOutcomeV1 outcome, long startOffset, long endOffsetExclusive) {
        return new KafkaOrderedAppendResultV1(
                outcome, OptionalLong.of(startOffset), OptionalLong.of(endOffsetExclusive));
    }
}
