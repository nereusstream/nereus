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

import java.util.Optional;
import java.util.concurrent.CompletionStage;

/** Two-stage protocol hook: validate without offsets, then install the exact assigned speculative delta. */
public interface KafkaAppendProtocolHooksV1 {
    /** Called under the pipeline admission lock; duplicates join before capacity or offset allocation. */
    default Optional<CompletionStage<KafkaOrderedAppendResultV1>> findDuplicateBeforeOffsetAssignment() {
        return Optional.empty();
    }

    void validateBeforeOffsetAssignment();

    void prepareAfterOffsetAssignment(KafkaOffsetAssignedAppendV1 assigned);

    /** Registers the original result before storage submission can complete. */
    default void registerAssignedResult(
            long startOffset, long endOffsetExclusive, CompletionStage<KafkaOrderedAppendResultV1> result) {}

    static KafkaAppendProtocolHooksV1 none() {
        return new KafkaAppendProtocolHooksV1() {
            @Override
            public void validateBeforeOffsetAssignment() {}

            @Override
            public void prepareAfterOffsetAssignment(KafkaOffsetAssignedAppendV1 assigned) {}
        };
    }
}
