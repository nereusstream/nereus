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

/** Coherently published immutable reference set; capture never deep-copies the referenced views. */
public record KafkaPartitionStateReferencesV1(
        KafkaPartitionStateReferenceV1 runTable,
        KafkaPartitionStateReferenceV1 activeTail,
        KafkaPartitionStateReferenceV1 sourceMap,
        KafkaPartitionStateReferenceV1 committedProducerState,
        KafkaPartitionStateReferenceV1 speculativeProducerQueue,
        KafkaPartitionStateReferenceV1 transactionIndex,
        KafkaPartitionStateReferenceV1 leaderEpochIndex,
        KafkaPartitionStateReferenceV1 checkpointVector,
        KafkaPartitionStateReferenceV1 sourceProtection) {
    public KafkaPartitionStateReferencesV1 {
        Objects.requireNonNull(runTable, "runTable");
        Objects.requireNonNull(activeTail, "activeTail");
        Objects.requireNonNull(sourceMap, "sourceMap");
        Objects.requireNonNull(committedProducerState, "committedProducerState");
        Objects.requireNonNull(speculativeProducerQueue, "speculativeProducerQueue");
        Objects.requireNonNull(transactionIndex, "transactionIndex");
        Objects.requireNonNull(leaderEpochIndex, "leaderEpochIndex");
        Objects.requireNonNull(checkpointVector, "checkpointVector");
        Objects.requireNonNull(sourceProtection, "sourceProtection");
    }

    public boolean doesNotRegress(KafkaPartitionStateReferencesV1 previous) {
        Objects.requireNonNull(previous, "previous");
        return runTable.doesNotRegress(previous.runTable)
                && activeTail.doesNotRegress(previous.activeTail)
                && sourceMap.doesNotRegress(previous.sourceMap)
                && committedProducerState.doesNotRegress(previous.committedProducerState)
                && speculativeProducerQueue.doesNotRegress(previous.speculativeProducerQueue)
                && transactionIndex.doesNotRegress(previous.transactionIndex)
                && leaderEpochIndex.doesNotRegress(previous.leaderEpochIndex)
                && checkpointVector.doesNotRegress(previous.checkpointVector)
                && sourceProtection.doesNotRegress(previous.sourceProtection);
    }
}
