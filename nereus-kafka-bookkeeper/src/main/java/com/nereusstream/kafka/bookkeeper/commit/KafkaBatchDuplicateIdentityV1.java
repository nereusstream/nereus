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

/** Kafka-native per-RecordBatch duplicate identity; storage digests are deliberately absent. */
public record KafkaBatchDuplicateIdentityV1(long producerId, short producerEpoch, int baseSequence, int lastSequence) {
    public KafkaBatchDuplicateIdentityV1 {
        if (producerId < 0 || producerEpoch < 0 || baseSequence < 0 || lastSequence < 0) {
            throw new IllegalArgumentException("Kafka producer identity fields must be non-negative");
        }
    }
}
