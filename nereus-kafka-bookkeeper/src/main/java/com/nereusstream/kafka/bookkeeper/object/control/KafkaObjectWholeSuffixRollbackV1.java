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

package com.nereusstream.kafka.bookkeeper.object.control;

import com.nereusstream.kafka.bookkeeper.commit.KafkaCoherentCommitCoordinatorV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectCoherentProtocolSnapshotV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectCompletionTrackerV1;
import java.util.Objects;

/** Kafka-only whole-suffix rollback derived from the production tracker lifecycle. */
public final class KafkaObjectWholeSuffixRollbackV1 {
    private final KafkaObjectCompletionTrackerV1 tracker;

    public KafkaObjectWholeSuffixRollbackV1(KafkaObjectCompletionTrackerV1 tracker) {
        this.tracker = Objects.requireNonNull(tracker, "tracker");
    }

    /** CASes the repository-backed M2 rollback root, then consumes the exact tracker-issued plan. */
    public synchronized KafkaObjectCoherentProtocolSnapshotV1 rollbackM2Suffix(
            long startOffset, KafkaCoherentCommitCoordinatorV1 coordinator) {
        Objects.requireNonNull(coordinator, "coordinator");
        KafkaObjectCompletionTrackerV1.RollbackPlan plan = tracker.prepareRollbackSuffix(startOffset);
        KafkaObjectCoherentProtocolSnapshotV1 published = coordinator.rollbackObjectSuffix(startOffset, plan.commits());
        tracker.completeRollbackAfterRootCas(plan);
        return published;
    }
}
