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

package com.nereusstream.kafka.bookkeeper.protocol;

import java.util.Objects;

/** One immutable coherent Kafka partition protocol publication root. */
public record KafkaPartitionProtocolStateV1(
        KafkaPartitionFenceV1 fence,
        long stateVersion,
        KafkaPartitionFrontiersV1 frontiers,
        KafkaPartitionStateReferencesV1 references)
        implements KafkaPartitionReadSnapshotV1 {
    public KafkaPartitionProtocolStateV1 {
        Objects.requireNonNull(fence, "fence");
        Objects.requireNonNull(frontiers, "frontiers");
        Objects.requireNonNull(references, "references");
        if (stateVersion < 0 || stateVersion == Long.MAX_VALUE) {
            throw new IllegalArgumentException("state version must be non-negative and incrementable");
        }
    }
}
