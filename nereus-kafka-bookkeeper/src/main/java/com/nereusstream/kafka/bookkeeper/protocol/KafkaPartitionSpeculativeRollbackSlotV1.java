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

/** Exact whole speculative-suffix removal candidate; only Allocated and its queue reference may change. */
public record KafkaPartitionSpeculativeRollbackSlotV1(
        KafkaPartitionFenceV1 expectedFence,
        long predecessorStateVersion,
        long rollbackStartOffset,
        KafkaPartitionStateReferencesV1 replacementReferences) {
    public KafkaPartitionSpeculativeRollbackSlotV1 {
        Objects.requireNonNull(expectedFence, "expectedFence");
        Objects.requireNonNull(replacementReferences, "replacementReferences");
        if (predecessorStateVersion < 0 || rollbackStartOffset < 0) {
            throw new IllegalArgumentException("speculative rollback version/range is outside its domain");
        }
    }
}
