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

import com.nereusstream.kafka.bookkeeper.admission.KafkaBookKeeperRecoveryEnvelopeV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import java.util.Objects;
import java.util.OptionalLong;

/** Exact prior-run and native-election inputs for one fail-closed takeover attempt. */
public record KafkaBookKeeperRecoveryRequestV1(
        Nbke2RunBindingV1 runBinding,
        RunLedgerHandleV1 handle,
        long kafkaStartOffset,
        OptionalLong hintedCheckpointEntryId,
        KafkaBookKeeperRecoveryEnvelopeV1 envelope,
        KafkaElectionRecoveryBoundaryV1 electionBoundary,
        KafkaPartitionFenceV1 recoveredStateFence) {
    public KafkaBookKeeperRecoveryRequestV1 {
        Objects.requireNonNull(runBinding, "runBinding");
        Objects.requireNonNull(handle, "handle");
        Objects.requireNonNull(hintedCheckpointEntryId, "hintedCheckpointEntryId");
        Objects.requireNonNull(envelope, "envelope");
        Objects.requireNonNull(electionBoundary, "electionBoundary");
        Objects.requireNonNull(recoveredStateFence, "recoveredStateFence");
        if (kafkaStartOffset < 0
                || hintedCheckpointEntryId.isPresent() && hintedCheckpointEntryId.getAsLong() <= 0
                || !runBinding.providerScopeId().equals(handle.providerScopeId())
                || !runBinding.runId().equals(handle.runId())
                || !runBinding.bindingId().equals(recoveredStateFence.bindingId())
                || !runBinding.topicIncarnation().equals(recoveredStateFence.topicIncarnation())
                || runBinding.partitionId() != recoveredStateFence.partitionId()
                || !runBinding.storageEpochId().equals(recoveredStateFence.storageEpochId())
                || recoveredStateFence.ownerEpoch() < runBinding.creatorOwnerEpoch()
                || recoveredStateFence.kafkaLeaderEpoch() < runBinding.kafkaLeaderEpoch()
                || electionBoundary.electionAdoptableEndOffset() < kafkaStartOffset) {
            throw new IllegalArgumentException("recovery request changes run identity or regresses its bounds");
        }
    }
}
