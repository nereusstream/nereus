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

/** One competing Binding/Storage/Owner/Kafka-leader transition on the same partition publication cut. */
public record KafkaPartitionFenceTransitionV1(
        KafkaPartitionFenceV1 expectedFence,
        long predecessorStateVersion,
        KafkaPartitionFenceV1 replacementFence,
        KafkaPartitionFrontiersV1 replacementFrontiers,
        KafkaPartitionStateReferencesV1 replacementReferences) {
    public KafkaPartitionFenceTransitionV1 {
        Objects.requireNonNull(expectedFence, "expectedFence");
        Objects.requireNonNull(replacementFence, "replacementFence");
        Objects.requireNonNull(replacementFrontiers, "replacementFrontiers");
        Objects.requireNonNull(replacementReferences, "replacementReferences");
        if (predecessorStateVersion < 0) {
            throw new IllegalArgumentException("predecessor state version must be non-negative");
        }
        if (!expectedFence.samePartitionIdentity(replacementFence)
                || replacementFence.bindingGeneration() < expectedFence.bindingGeneration()
                || replacementFence.ownerEpoch() < expectedFence.ownerEpoch()
                || replacementFence.kafkaLeaderEpoch() < expectedFence.kafkaLeaderEpoch()
                || replacementFence.equals(expectedFence)) {
            throw new IllegalArgumentException("fence transition changes identity, regresses, or changes no fence");
        }
    }
}
