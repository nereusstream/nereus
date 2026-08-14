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

/** Original committed Kafka Offset Range retained for one native duplicate identity. */
public record KafkaProducerBatchResultV1(
        KafkaBatchDuplicateIdentityV1 identity, long startOffset, long endOffsetExclusive) {
    public KafkaProducerBatchResultV1 {
        Objects.requireNonNull(identity, "identity");
        if (startOffset < 0 || endOffsetExclusive <= startOffset) {
            throw new IllegalArgumentException("producer batch result range is invalid");
        }
    }
}
