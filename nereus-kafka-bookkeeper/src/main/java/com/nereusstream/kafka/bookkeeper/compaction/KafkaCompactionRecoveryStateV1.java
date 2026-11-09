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

package com.nereusstream.kafka.bookkeeper.compaction;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.codec.TopicIncarnationIdentityCodecV1;
import com.nereusstream.kafka.bookkeeper.checkpoint.KafkaProtocolCheckpointComponentsV1;
import com.nereusstream.kafka.bookkeeper.checkpoint.KafkaProtocolCheckpointStateV1;
import com.nereusstream.kafka.bookkeeper.checkpoint.KafkaRecoveryCheckpointVectorV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaCoherentCommitCoordinatorV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaCoherentProtocolSnapshotV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectCoherentProtocolSnapshotV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionProtocolStateV1;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.MaterializationSourceCut;
import java.util.Objects;

/**
 * Full protocol state for a compacted output. The input checkpoint digest is provenance, not a physical read source.
 * Selection, native output identity and capacity admission remain the enclosing publisher's responsibility.
 */
public record KafkaCompactionRecoveryStateV1(
        KafkaPartitionFenceV1 sourceFence,
        long coveredThrough,
        Sha256Digest sourceCheckpointSha256,
        KafkaProtocolCheckpointComponentsV1 components) {
    public KafkaCompactionRecoveryStateV1 {
        Objects.requireNonNull(sourceFence, "sourceFence");
        KafkaBookKeeperInventoryV2.requireDigest(sourceCheckpointSha256);
        Objects.requireNonNull(components, "components");
        components.requireCoverage(coveredThrough, coveredThrough, coveredThrough);
    }

    /** Requires the actual resolved checkpoint named by the supplied coherent cut; never reconstructs it from data. */
    public static KafkaCompactionRecoveryStateV1 capture(
            KafkaCoherentProtocolSnapshotV1 snapshot, KafkaProtocolCheckpointStateV1 checkpoint) {
        return capture(snapshot.root(), checkpoint);
    }

    public static KafkaCompactionRecoveryStateV1 capture(
            KafkaObjectCoherentProtocolSnapshotV1 snapshot, KafkaProtocolCheckpointStateV1 checkpoint) {
        return capture(snapshot.root(), checkpoint);
    }

    private static KafkaCompactionRecoveryStateV1 capture(
            KafkaPartitionProtocolStateV1 root, KafkaProtocolCheckpointStateV1 checkpoint) {
        Objects.requireNonNull(checkpoint, "checkpoint");
        var vector = checkpoint.vector();
        var fence = root.fence();
        var binding = vector.runBinding();
        var digest = KafkaCoherentCommitCoordinatorV1.recoveryCheckpointContentDigest(checkpoint);
        if (!vector.isAlignedCompoundCheckpoint()
                || vector.recoveryCoveredThrough() > root.frontiers().durableEndOffset()
                || !digest.equals(root.references().checkpointVector().contentDigest())
                || !samePartition(fence, binding)
                || binding.creatorOwnerEpoch() > fence.ownerEpoch()
                || binding.kafkaLeaderEpoch() > fence.kafkaLeaderEpoch()) {
            throw new IllegalArgumentException("compaction recovery state differs from its coherent checkpoint cut");
        }
        return new KafkaCompactionRecoveryStateV1(
                fence, vector.recoveryCoveredThrough(), digest, checkpoint.components());
    }

    /**
     * Reanchors these components to an independently verified output run. No original-run locator is transferred.
     * This creates checkpoint data only; it grants no selection, writer or recovery authority.
     */
    public KafkaProtocolCheckpointStateV1 checkpointFor(Nbke2RunBindingV1 outputBinding) {
        Objects.requireNonNull(outputBinding, "outputBinding");
        if (!samePartition(sourceFence, outputBinding)
                || outputBinding.creatorOwnerEpoch() < sourceFence.ownerEpoch()
                || outputBinding.kafkaLeaderEpoch() < sourceFence.kafkaLeaderEpoch()) {
            throw new IllegalArgumentException(
                    "compaction recovery state cannot move to a foreign or older output run");
        }
        return new KafkaProtocolCheckpointStateV1(
                new KafkaRecoveryCheckpointVectorV1(
                        outputBinding, coveredThrough, coveredThrough, coveredThrough, coveredThrough),
                components.producerState(),
                components.transactionState(),
                components.leaderEpochIndex());
    }

    public CanonicalBytes encode() {
        return KafkaCompactionRecoveryStateCodecV1.encode(this);
    }

    public Sha256Digest sha256() {
        return Sha256Digest.hash(encode());
    }

    /** Checks carrier provenance and coverage; current native input and publication admission are still required. */
    public void requireSourceCut(MaterializationSourceCut cut) {
        var identity = cut.identity();
        var binding = identity.binding();
        if (!sourceFence.bindingId().equals(binding.bindingId())
                || !Sha256Digest.hash(TopicIncarnationIdentityCodecV1.encode(sourceFence.topicIncarnation()))
                        .equals(binding.incarnationSha256())
                || !sourceFence.storageEpochId().digest().equals(binding.storageEpochSha256())
                || sourceFence.ownerEpoch() != identity.ownerEpoch()
                || coveredThrough != cut.coverage().exclusiveEnd()
                || !sourceCheckpointSha256.equals(cut.recoveryCheckpointRootSha256())) {
            throw new IllegalArgumentException("compaction recovery state differs from its source cut");
        }
    }

    public void requirePlan(KafkaCompactionRecordsV1.CompactionPlan plan) {
        requireSourceCut(plan.sourceCut());
        if (!sourceCheckpointSha256.equals(plan.protocolRoots().recoveryCheckpointSha256())) {
            throw new IllegalArgumentException("compaction recovery state differs from its plan checkpoint root");
        }
    }

    private static boolean samePartition(KafkaPartitionFenceV1 fence, Nbke2RunBindingV1 binding) {
        return fence.bindingId().equals(binding.bindingId())
                && fence.topicIncarnation().equals(binding.topicIncarnation())
                && fence.partitionId() == binding.partitionId()
                && fence.storageEpochId().equals(binding.storageEpochId());
    }
}
