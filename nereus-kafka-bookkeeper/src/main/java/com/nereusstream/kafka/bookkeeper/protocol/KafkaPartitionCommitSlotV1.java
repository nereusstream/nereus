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

/** Hidden, contiguous commit candidate that can become visible only through the publication cell. */
public record KafkaPartitionCommitSlotV1(
        KafkaPartitionFenceV1 expectedFence,
        long predecessorStateVersion,
        long commitStartOffset,
        long commitEndOffset,
        KafkaPartitionFrontiersV1 replacementFrontiers,
        KafkaPartitionStateReferencesV1 replacementReferences) {
    public KafkaPartitionCommitSlotV1 {
        Objects.requireNonNull(expectedFence, "expectedFence");
        Objects.requireNonNull(replacementFrontiers, "replacementFrontiers");
        Objects.requireNonNull(replacementReferences, "replacementReferences");
        if (predecessorStateVersion < 0 || commitStartOffset < 0 || commitEndOffset <= commitStartOffset) {
            throw new IllegalArgumentException("commit slot version/range is outside its domain");
        }
        if (replacementFrontiers.readableEndOffset() != commitEndOffset
                || replacementFrontiers.durableEndOffset() < commitEndOffset
                || replacementFrontiers.allocatedEndOffset() < commitEndOffset) {
            throw new IllegalArgumentException("commit replacement does not publish the exact durable readable end");
        }
    }
}
