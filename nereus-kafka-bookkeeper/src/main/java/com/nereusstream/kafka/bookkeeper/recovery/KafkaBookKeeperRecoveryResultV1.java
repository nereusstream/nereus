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

import com.nereusstream.kafka.bookkeeper.admission.KafkaBookKeeperRecoveryProgressV1;
import com.nereusstream.kafka.bookkeeper.checkpoint.KafkaProtocolCheckpointStateV1;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

/** Physical candidate, election cut, protocol state, and cumulative envelope accounting. */
public record KafkaBookKeeperRecoveryResultV1(
        KafkaBookKeeperRecoveryOutcomeV1 outcome,
        long physicalRecoveredEndOffset,
        OptionalLong newLeaderLeo,
        Optional<KafkaProtocolCheckpointStateV1> recoveredProtocolState,
        KafkaBookKeeperRecoveryProgressV1 progress,
        OptionalLong conflictEntryId,
        String detail) {
    public KafkaBookKeeperRecoveryResultV1 {
        Objects.requireNonNull(outcome, "outcome");
        Objects.requireNonNull(newLeaderLeo, "newLeaderLeo");
        Objects.requireNonNull(recoveredProtocolState, "recoveredProtocolState");
        Objects.requireNonNull(progress, "progress");
        Objects.requireNonNull(conflictEntryId, "conflictEntryId");
        Objects.requireNonNull(detail, "detail");
        boolean recovered = outcome == KafkaBookKeeperRecoveryOutcomeV1.RECOVERED_EXACT
                || outcome == KafkaBookKeeperRecoveryOutcomeV1.RECOVERED_WITH_INERT_RESIDUE;
        if (physicalRecoveredEndOffset < 0
                || recovered != newLeaderLeo.isPresent()
                || recovered != recoveredProtocolState.isPresent()
                || recovered
                        && recoveredProtocolState.orElseThrow().vector().recoveryCoveredThrough()
                                != newLeaderLeo.getAsLong()) {
            throw new IllegalArgumentException("recovery result state and boundaries are inconsistent");
        }
    }

    public boolean recovered() {
        return outcome == KafkaBookKeeperRecoveryOutcomeV1.RECOVERED_EXACT
                || outcome == KafkaBookKeeperRecoveryOutcomeV1.RECOVERED_WITH_INERT_RESIDUE;
    }
}
