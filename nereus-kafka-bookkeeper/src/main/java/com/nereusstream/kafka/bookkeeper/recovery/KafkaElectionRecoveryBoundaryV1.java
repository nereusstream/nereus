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

package com.nereusstream.kafka.bookkeeper.recovery;

import java.util.Objects;

/** Native election facts kept independent from the shared physical candidate tail. */
public record KafkaElectionRecoveryBoundaryV1(
        KafkaElectionKindV1 electionKind,
        long electedReplicaObservedEndOffset,
        long replicaAppliedEndOffset,
        long electionAdoptableEndOffset) {
    public KafkaElectionRecoveryBoundaryV1 {
        Objects.requireNonNull(electionKind, "electionKind");
        if (electedReplicaObservedEndOffset < 0
                || replicaAppliedEndOffset < 0
                || electionAdoptableEndOffset < 0
                || replicaAppliedEndOffset > electedReplicaObservedEndOffset
                || electionAdoptableEndOffset > electedReplicaObservedEndOffset) {
            throw new IllegalArgumentException("native election recovery frontiers are inconsistent");
        }
    }

    public boolean appliedThroughAdoptableBoundary() {
        return replicaAppliedEndOffset >= electionAdoptableEndOffset;
    }
}
