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

import java.util.Objects;

/** One protocol delta after its exact Kafka batch range is known. */
public record KafkaAssignedProtocolBatchV1(long startOffset, long endOffsetExclusive, KafkaProtocolBatchDeltaV1 delta) {
    public KafkaAssignedProtocolBatchV1 {
        Objects.requireNonNull(delta, "delta");
        if (startOffset < 0
                || endOffsetExclusive <= startOffset
                || endOffsetExclusive - startOffset != delta.logicalOffsetCount()) {
            throw new IllegalArgumentException("assigned protocol batch range differs from its logical coverage");
        }
    }
}
