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

package com.nereusstream.kafka.bookkeeper.replication;

import com.nereusstream.kafka.bookkeeper.recovery.KafkaElectionKindV1;
import java.util.Objects;
import java.util.OptionalLong;

/** Applies the mandatory Applied-through-adoptable rule without inventing a native election boundary. */
public final class KafkaReplicaElectionValidatorV1 {
    private KafkaReplicaElectionValidatorV1() {}

    public static KafkaReplicaElectionValidationV1 validate(
            KafkaReplicaProgressSnapshotV1 progress,
            KafkaElectionKindV1 electionKind,
            long electionAdoptableEndOffset) {
        Objects.requireNonNull(progress, "progress");
        Objects.requireNonNull(electionKind, "electionKind");
        KafkaReplicaElectionValidationOutcomeV1 outcome;
        if (progress.observedEndOffset() < electionAdoptableEndOffset) {
            outcome = KafkaReplicaElectionValidationOutcomeV1.OBSERVED_SHORTFALL;
        } else if (progress.appliedEndOffset() < electionAdoptableEndOffset) {
            outcome = KafkaReplicaElectionValidationOutcomeV1.APPLIED_SHORTFALL;
        } else {
            outcome = KafkaReplicaElectionValidationOutcomeV1.ELIGIBLE;
        }
        return new KafkaReplicaElectionValidationV1(
                electionKind,
                electionAdoptableEndOffset,
                outcome,
                outcome == KafkaReplicaElectionValidationOutcomeV1.ELIGIBLE
                        ? OptionalLong.of(electionAdoptableEndOffset)
                        : OptionalLong.empty());
    }
}
