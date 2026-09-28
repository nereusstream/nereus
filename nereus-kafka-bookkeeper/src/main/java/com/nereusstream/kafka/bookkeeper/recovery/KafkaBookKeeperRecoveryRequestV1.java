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

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.kafka.bookkeeper.admission.KafkaBookKeeperRecoveryEnvelopeV1;
import com.nereusstream.kafka.bookkeeper.checkpoint.KafkaProtocolCheckpointStateV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.api.kafka.KafkaOwnerAdmissionV1;
import com.nereusstream.storage.api.kafka.KafkaRunRootRecordV2;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

/** Exact closed-history and prior-run inputs for one fail-closed takeover attempt. */
public record KafkaBookKeeperRecoveryRequestV1(
        Nbke2RunBindingV1 runBinding,
        RunLedgerHandleV1 handle,
        long kafkaStartOffset,
        OptionalLong hintedCheckpointEntryId,
        KafkaBookKeeperRecoveryEnvelopeV1 envelope,
        KafkaOwnerAdmissionV1 closedOwner,
        KafkaRunRootRecordV2 selectedRoot,
        KafkaPartitionFenceV1 recoveredStateFence,
        Optional<KafkaProtocolCheckpointStateV1> precedingState) {
    public KafkaBookKeeperRecoveryRequestV1 {
        Objects.requireNonNull(runBinding, "runBinding");
        Objects.requireNonNull(handle, "handle");
        Objects.requireNonNull(hintedCheckpointEntryId, "hintedCheckpointEntryId");
        Objects.requireNonNull(envelope, "envelope");
        Objects.requireNonNull(closedOwner, "closedOwner");
        Objects.requireNonNull(selectedRoot, "selectedRoot");
        Objects.requireNonNull(precedingState, "precedingState");
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
                || !closedOwner.closed()
                || closedOwner.owner().ownerEpoch() != runBinding.creatorOwnerEpoch()
                || closedOwner.owner().kafkaLeaderEpoch() != runBinding.kafkaLeaderEpoch()
                || !closedOwner
                        .scopeSha256()
                        .equals(Sha256Digest.hash(KafkaRunRootRecordV2.Scope.of(selectedRoot.root())
                                .encode()))
                || !closedOwner.runs().contains(selectedRoot.initialLink())
                || selectedRoot
                        .recoveryCut()
                        .filter(cut -> !cut.closedOwnerSha256().equals(Sha256Digest.hash(closedOwner.encode())))
                        .isPresent()
                || !selectedRoot.root().runId().equals(runBinding.runId())
                || !selectedRoot.root().ledgerIdentity().equals(handle.ledgerIdentity())
                || selectedRoot.root().kafkaStartOffset() != kafkaStartOffset
                || selectedRoot.root().creatorOwnerEpoch() != runBinding.creatorOwnerEpoch()
                || selectedRoot.root().kafkaLeaderEpoch() != runBinding.kafkaLeaderEpoch()
                || !selectedRoot.root().bindingId().equals(runBinding.bindingId())
                || !selectedRoot.root().topicIncarnation().equals(runBinding.topicIncarnation())
                || selectedRoot.root().partitionId() != runBinding.partitionId()
                || !selectedRoot.root().storageEpochId().equals(runBinding.storageEpochId())
                || !selectedRoot.root().providerScopeId().equals(runBinding.providerScopeId())
                || selectedRoot.retired()
                || precedingState
                        .filter(value -> !value.vector().isAlignedCompoundCheckpoint()
                                || value.vector().recoveryCoveredThrough() != kafkaStartOffset
                                || !value.vector().runBinding().bindingId().equals(runBinding.bindingId())
                                || !value.vector()
                                        .runBinding()
                                        .topicIncarnation()
                                        .equals(runBinding.topicIncarnation())
                                || value.vector().runBinding().partitionId() != runBinding.partitionId()
                                || !value.vector().runBinding().storageEpochId().equals(runBinding.storageEpochId()))
                        .isPresent()) {
            throw new IllegalArgumentException("recovery request changes run identity or regresses its bounds");
        }
    }
}
