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

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.kafka.bookkeeper.pipeline.KafkaOrderedDurableCommitV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionProtocolStateV1;
import java.util.Objects;

/** Compact post-publication descriptor that followers validate before reporting Observed. */
public record KafkaReplicaCommitDescriptorV1(
        KafkaPartitionFenceV1 fence,
        long validatedStateVersion,
        long startOffset,
        long endOffsetExclusive,
        long encodedDataBytes,
        Sha256Digest aggregateAssignedPayloadSha256,
        KafkaReplicaSourceReferenceV1 source,
        KafkaReplicaProtocolProofV1 protocolProof,
        KafkaReplicaObservationModeV1 observationMode) {
    public KafkaReplicaCommitDescriptorV1 {
        Objects.requireNonNull(fence, "fence");
        Objects.requireNonNull(aggregateAssignedPayloadSha256, "aggregateAssignedPayloadSha256");
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(protocolProof, "protocolProof");
        Objects.requireNonNull(observationMode, "observationMode");
        if (validatedStateVersion <= 0
                || startOffset < 0
                || endOffsetExclusive <= startOffset
                || encodedDataBytes <= 0
                || aggregateAssignedPayloadSha256.isZero()
                || source.kafkaStartOffset() != startOffset
                || source.kafkaEndOffsetExclusive() != endOffsetExclusive
                || !source.payloadContentDigest().equals(aggregateAssignedPayloadSha256)) {
            throw new IllegalArgumentException("replica descriptor coverage/content is inconsistent");
        }
    }

    public static KafkaReplicaCommitDescriptorV1 fromBookKeeper(
            KafkaOrderedDurableCommitV1 commit,
            KafkaPartitionProtocolStateV1 published,
            KafkaReplicaObservationModeV1 observationMode) {
        Objects.requireNonNull(commit, "commit");
        Objects.requireNonNull(published, "published");
        Objects.requireNonNull(observationMode, "observationMode");
        var binding = commit.runBinding();
        var fence = published.fence();
        if (!binding.bindingId().equals(fence.bindingId())
                || !binding.topicIncarnation().equals(fence.topicIncarnation())
                || binding.partitionId() != fence.partitionId()
                || !binding.storageEpochId().equals(fence.storageEpochId())
                || binding.creatorOwnerEpoch() != fence.ownerEpoch()
                || binding.kafkaLeaderEpoch() != fence.kafkaLeaderEpoch()
                || published.frontiers().durableEndOffset() != commit.endOffsetExclusive()
                || published.frontiers().readableEndOffset() != commit.endOffsetExclusive()) {
            throw new IllegalArgumentException("durable commit differs from the exact coherent publication cut");
        }
        KafkaReplicaSourceReferenceV1 source = KafkaReplicaSourceReferenceV1.bookKeeper(
                commit, published.references().sourceMap().generation());
        return new KafkaReplicaCommitDescriptorV1(
                fence,
                published.stateVersion(),
                commit.startOffset(),
                commit.endOffsetExclusive(),
                commit.encodedDataBytes(),
                commit.aggregateAssignedPayloadSha256(),
                source,
                KafkaReplicaProtocolProofV1.from(published.references()),
                observationMode);
    }
}
