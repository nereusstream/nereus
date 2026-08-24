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

package com.nereusstream.kafka.bookkeeper.commit;

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.kafka.bookkeeper.checkpoint.KafkaProtocolCheckpointStateV1;
import com.nereusstream.kafka.bookkeeper.object.nwkcp1.KafkaObjectRecoveredTailV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectBindingKeyV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectCoherentProtocolSnapshotV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectCompletionTrackerV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectNativeStateV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectSourceProtectionTrackerV1;
import com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectStateCodecV1;
import com.nereusstream.kafka.bookkeeper.object.read.KafkaObjectActiveTailStateV1;
import com.nereusstream.kafka.bookkeeper.pipeline.KafkaAppendProtocolHooksV1;
import com.nereusstream.kafka.bookkeeper.pipeline.KafkaOffsetAssignedAppendV1;
import com.nereusstream.kafka.bookkeeper.pipeline.KafkaOrderedDurableCommitObserver;
import com.nereusstream.kafka.bookkeeper.pipeline.KafkaOrderedDurableCommitV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionCommitSlotV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFrontiersV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionObjectTailRetirementSlotV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionProtocolStateV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionPublicationCellV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionPublicationObserver;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionPublicationResultV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionSpeculativeRollbackSlotV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionSpeculativeSlotV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionStateReferenceV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionStateReferencesV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import java.util.List;
import java.util.Objects;

/** K5 bridge from post-offset speculative admission to one ordered K1 coherent publication per durable group. */
public final class KafkaCoherentCommitCoordinatorV1 implements KafkaOrderedDurableCommitObserver {
    private enum StorageProfile {
        BOOKKEEPER,
        OBJECT
    }

    private final KafkaPartitionPublicationCellV1 publicationCell;
    private final KafkaProtocolStateRepositoryV1 repository;
    private final RunLedgerHandleV1 expectedHandle;
    private final StorageProfile storageProfile;

    private KafkaCoherentCommitCoordinatorV1(
            KafkaPartitionPublicationCellV1 publicationCell,
            KafkaProtocolStateRepositoryV1 repository,
            RunLedgerHandleV1 expectedHandle,
            StorageProfile storageProfile) {
        this.publicationCell = publicationCell;
        this.repository = repository;
        this.expectedHandle = expectedHandle;
        this.storageProfile = Objects.requireNonNull(storageProfile, "storageProfile");
    }

    public static KafkaCoherentCommitCoordinatorV1 bootstrap(
            KafkaPartitionFenceV1 fence,
            long startOffset,
            RunLedgerHandleV1 expectedHandle,
            KafkaPartitionPublicationObserver observer) {
        Objects.requireNonNull(fence, "fence");
        Objects.requireNonNull(expectedHandle, "expectedHandle");
        Objects.requireNonNull(observer, "observer");
        if (startOffset < 0) {
            throw new IllegalArgumentException("coherent protocol start offset must be non-negative");
        }
        KafkaProtocolStateRepositoryV1 repository = new KafkaProtocolStateRepositoryV1();
        KafkaActiveTailStateV1 activeTail = KafkaActiveTailStateV1.empty(startOffset);
        KafkaCommittedProducerStateV1 producers = KafkaCommittedProducerStateV1.empty();
        KafkaSpeculativeQueueV1 speculative = KafkaSpeculativeQueueV1.empty();
        KafkaTransactionStateV1 transactions = KafkaTransactionStateV1.empty();
        KafkaLeaderEpochIndexV1 leaderEpochs = KafkaLeaderEpochIndexV1.empty();
        KafkaPartitionStateReferencesV1 references = new KafkaPartitionStateReferencesV1(
                seed("K5-RUN-TABLE-SEED-V1"),
                repository.store(0, KafkaProtocolStateCodecV1.activeTail(activeTail), activeTail),
                seed("K5-SOURCE-MAP-SEED-V1"),
                repository.store(0, KafkaProtocolStateCodecV1.producers(producers), producers),
                repository.store(0, KafkaProtocolStateCodecV1.speculative(speculative), speculative),
                repository.store(0, KafkaProtocolStateCodecV1.transactions(transactions), transactions),
                repository.store(0, KafkaProtocolStateCodecV1.leaderEpochs(leaderEpochs), leaderEpochs),
                seed("K5-CHECKPOINT-VECTOR-SEED-V1"),
                seed("K5-SOURCE-PROTECTION-SEED-V1"));
        KafkaPartitionFrontiersV1 frontiers = new KafkaPartitionFrontiersV1(
                startOffset, startOffset, startOffset, startOffset, startOffset, startOffset);
        KafkaPartitionProtocolStateV1 initial = new KafkaPartitionProtocolStateV1(fence, 0, frontiers, references);
        return new KafkaCoherentCommitCoordinatorV1(
                new KafkaPartitionPublicationCellV1(initial, observer),
                repository,
                expectedHandle,
                StorageProfile.BOOKKEEPER);
    }

    /** M3 Object-WAL profile bootstrap; it does not activate a native Kafka broker/controller path. */
    public static KafkaCoherentCommitCoordinatorV1 bootstrapObject(
            KafkaPartitionFenceV1 fence, long startOffset, KafkaPartitionPublicationObserver observer) {
        Objects.requireNonNull(fence, "fence");
        Objects.requireNonNull(observer, "observer");
        if (startOffset < 0) {
            throw new IllegalArgumentException("coherent Object protocol start offset must be non-negative");
        }
        KafkaObjectBindingKeyV1 binding = binding(fence);
        KafkaProtocolStateRepositoryV1 repository = new KafkaProtocolStateRepositoryV1();
        KafkaObjectActiveTailStateV1 activeTail = KafkaObjectActiveTailStateV1.empty(binding, startOffset);
        KafkaCommittedProducerStateV1 producers = KafkaCommittedProducerStateV1.empty();
        KafkaSpeculativeQueueV1 speculative = KafkaSpeculativeQueueV1.empty();
        KafkaTransactionStateV1 transactions = KafkaTransactionStateV1.empty();
        KafkaLeaderEpochIndexV1 leaderEpochs = KafkaLeaderEpochIndexV1.empty();
        KafkaPartitionStateReferencesV1 references = new KafkaPartitionStateReferencesV1(
                seed("M3-OBJECT-RUN-TABLE-SEED-V1"),
                repository.store(0, KafkaObjectStateCodecV1.activeTail(activeTail), activeTail),
                seed("M3-OBJECT-SOURCE-MAP-SEED-V1"),
                repository.store(0, KafkaProtocolStateCodecV1.producers(producers), producers),
                repository.store(0, KafkaProtocolStateCodecV1.speculative(speculative), speculative),
                repository.store(0, KafkaProtocolStateCodecV1.transactions(transactions), transactions),
                repository.store(0, KafkaProtocolStateCodecV1.leaderEpochs(leaderEpochs), leaderEpochs),
                seed("M3-OBJECT-CHECKPOINT-VECTOR-SEED-V1"),
                seed("M3-OBJECT-SOURCE-PROTECTION-SEED-V1"));
        KafkaPartitionFrontiersV1 frontiers = new KafkaPartitionFrontiersV1(
                startOffset, startOffset, startOffset, startOffset, startOffset, startOffset);
        KafkaPartitionProtocolStateV1 initial = new KafkaPartitionProtocolStateV1(fence, 0, frontiers, references);
        return new KafkaCoherentCommitCoordinatorV1(
                new KafkaPartitionPublicationCellV1(initial, observer), repository, null, StorageProfile.OBJECT);
    }

    /** Restores an Object-profile M2 root from one verified compound checkpoint without activating M6 broker paths. */
    public static KafkaCoherentCommitCoordinatorV1 bootstrapObjectRecovered(
            KafkaPartitionFenceV1 fence,
            long trimStartOffset,
            long newLeaderLeo,
            long nativeHighWatermark,
            KafkaProtocolCheckpointStateV1 recovered,
            KafkaObjectRecoveredTailV1 recoveredTail,
            KafkaPartitionPublicationObserver observer) {
        Objects.requireNonNull(fence, "fence");
        Objects.requireNonNull(recovered, "recovered");
        Objects.requireNonNull(recoveredTail, "recoveredTail");
        Objects.requireNonNull(observer, "observer");
        var oldRun = recovered.vector().runBinding();
        if (trimStartOffset < 0
                || trimStartOffset > nativeHighWatermark
                || nativeHighWatermark > newLeaderLeo
                || recovered.vector().recoveryCoveredThrough() != newLeaderLeo
                || !recovered.vector().isAlignedCompoundCheckpoint()
                || !oldRun.bindingId().equals(fence.bindingId())
                || !oldRun.topicIncarnation().equals(fence.topicIncarnation())
                || oldRun.partitionId() != fence.partitionId()
                || !oldRun.storageEpochId().equals(fence.storageEpochId())
                || oldRun.creatorOwnerEpoch() > fence.ownerEpoch()
                || oldRun.kafkaLeaderEpoch() > fence.kafkaLeaderEpoch()) {
            throw new IllegalArgumentException("recovered Object protocol state differs from the new owner boundary");
        }
        KafkaObjectActiveTailStateV1 activeTail = recoveredTail.activeTail();
        if (!recoveredTail
                        .walRunRootSha()
                        .equals(
                                activeTail.locators().isEmpty()
                                        ? recoveredTail.walRunRootSha()
                                        : activeTail.locators().get(0).extent().walRunRootSha())
                || !activeTail.binding().equals(binding(fence))
                || activeTail.startOffset() != trimStartOffset
                || activeTail.endOffsetExclusive() != newLeaderLeo
                || trimStartOffset < newLeaderLeo && activeTail.locators().isEmpty()) {
            throw new IllegalArgumentException(
                    "recovered Object active tail lacks exact manifest/locator/source protection");
        }
        long firstUnstable = recovered
                .transactionState()
                .firstUnstableOffset(nativeHighWatermark)
                .orElse(nativeHighWatermark);
        long lastStableOffset = Math.min(nativeHighWatermark, firstUnstable);
        if (lastStableOffset < trimStartOffset) {
            throw new IllegalArgumentException("native HW leaves an unstable transaction before Object Log Start");
        }
        KafkaProtocolStateRepositoryV1 repository = new KafkaProtocolStateRepositoryV1();
        KafkaSpeculativeQueueV1 speculative = KafkaSpeculativeQueueV1.empty();
        KafkaPartitionStateReferencesV1 references = new KafkaPartitionStateReferencesV1(
                seed("M3-OBJECT-RECOVERED-RUN-TABLE-SEED-V1"),
                repository.store(0, KafkaObjectStateCodecV1.activeTail(activeTail), activeTail),
                seed("M3-OBJECT-RECOVERED-SOURCE-MAP-SEED-V1"),
                repository.store(
                        0, KafkaProtocolStateCodecV1.producers(recovered.producerState()), recovered.producerState()),
                repository.store(0, KafkaProtocolStateCodecV1.speculative(speculative), speculative),
                repository.store(
                        0,
                        KafkaProtocolStateCodecV1.transactions(recovered.transactionState()),
                        recovered.transactionState()),
                repository.store(
                        0,
                        KafkaProtocolStateCodecV1.leaderEpochs(recovered.leaderEpochIndex()),
                        recovered.leaderEpochIndex()),
                repository.store(0, KafkaProtocolStateCodecV1.checkpoint(recovered), recovered),
                new KafkaPartitionStateReferenceV1(0, recoveredTail.sourceProtectionDigest()));
        KafkaPartitionFrontiersV1 frontiers = new KafkaPartitionFrontiersV1(
                trimStartOffset, newLeaderLeo, newLeaderLeo, newLeaderLeo, nativeHighWatermark, lastStableOffset);
        KafkaPartitionProtocolStateV1 initial = new KafkaPartitionProtocolStateV1(fence, 0, frontiers, references);
        return new KafkaCoherentCommitCoordinatorV1(
                new KafkaPartitionPublicationCellV1(initial, observer), repository, null, StorageProfile.OBJECT);
    }

    public static KafkaCoherentCommitCoordinatorV1 bootstrapRecovered(
            KafkaPartitionFenceV1 fence,
            long trimStartOffset,
            long newLeaderLeo,
            long nativeHighWatermark,
            KafkaProtocolCheckpointStateV1 recovered,
            RunLedgerHandleV1 expectedNewRunHandle,
            KafkaPartitionPublicationObserver observer) {
        Objects.requireNonNull(fence, "fence");
        Objects.requireNonNull(recovered, "recovered");
        Objects.requireNonNull(expectedNewRunHandle, "expectedNewRunHandle");
        Objects.requireNonNull(observer, "observer");
        var oldRun = recovered.vector().runBinding();
        if (trimStartOffset < 0
                || trimStartOffset > nativeHighWatermark
                || nativeHighWatermark > newLeaderLeo
                || recovered.vector().recoveryCoveredThrough() != newLeaderLeo
                || !recovered.vector().isAlignedCompoundCheckpoint()
                || !oldRun.bindingId().equals(fence.bindingId())
                || !oldRun.topicIncarnation().equals(fence.topicIncarnation())
                || oldRun.partitionId() != fence.partitionId()
                || !oldRun.storageEpochId().equals(fence.storageEpochId())
                || oldRun.creatorOwnerEpoch() > fence.ownerEpoch()
                || oldRun.kafkaLeaderEpoch() > fence.kafkaLeaderEpoch()
                || !expectedNewRunHandle.providerScopeId().equals(oldRun.providerScopeId())
                || expectedNewRunHandle.runId().equals(oldRun.runId())) {
            throw new IllegalArgumentException("recovered protocol state differs from the new leader/run boundary");
        }
        long firstUnstable = recovered
                .transactionState()
                .firstUnstableOffset(nativeHighWatermark)
                .orElse(nativeHighWatermark);
        long lastStableOffset = Math.min(nativeHighWatermark, firstUnstable);
        if (lastStableOffset < trimStartOffset) {
            throw new IllegalArgumentException("native HW leaves an unstable transaction before Log Start");
        }
        KafkaProtocolStateRepositoryV1 repository = new KafkaProtocolStateRepositoryV1();
        KafkaActiveTailStateV1 activeTail = KafkaActiveTailStateV1.empty(newLeaderLeo);
        KafkaSpeculativeQueueV1 speculative = KafkaSpeculativeQueueV1.empty();
        KafkaPartitionStateReferencesV1 references = new KafkaPartitionStateReferencesV1(
                seed("K7-RECOVERED-RUN-TABLE-SEED-V1"),
                repository.store(0, KafkaProtocolStateCodecV1.activeTail(activeTail), activeTail),
                seed("K7-RECOVERED-SOURCE-MAP-SEED-V1"),
                repository.store(
                        0, KafkaProtocolStateCodecV1.producers(recovered.producerState()), recovered.producerState()),
                repository.store(0, KafkaProtocolStateCodecV1.speculative(speculative), speculative),
                repository.store(
                        0,
                        KafkaProtocolStateCodecV1.transactions(recovered.transactionState()),
                        recovered.transactionState()),
                repository.store(
                        0,
                        KafkaProtocolStateCodecV1.leaderEpochs(recovered.leaderEpochIndex()),
                        recovered.leaderEpochIndex()),
                repository.store(0, KafkaProtocolStateCodecV1.checkpoint(recovered), recovered),
                seed("K7-RECOVERED-SOURCE-PROTECTION-SEED-V1"));
        KafkaPartitionFrontiersV1 frontiers = new KafkaPartitionFrontiersV1(
                trimStartOffset, newLeaderLeo, newLeaderLeo, newLeaderLeo, nativeHighWatermark, lastStableOffset);
        KafkaPartitionProtocolStateV1 initial = new KafkaPartitionProtocolStateV1(fence, 0, frontiers, references);
        return new KafkaCoherentCommitCoordinatorV1(
                new KafkaPartitionPublicationCellV1(initial, observer),
                repository,
                expectedNewRunHandle,
                StorageProfile.BOOKKEEPER);
    }

    public KafkaPartitionPublicationCellV1 publicationCell() {
        return publicationCell;
    }

    public KafkaAppendProtocolHooksV1 protocolHooks(KafkaProtocolAppendPlanV1 plan) {
        Objects.requireNonNull(plan, "plan");
        return new KafkaAppendProtocolHooksV1() {
            @Override
            public void validateBeforeOffsetAssignment() {
                validate(plan);
            }

            @Override
            public void prepareAfterOffsetAssignment(KafkaOffsetAssignedAppendV1 assigned) {
                stage(plan, assigned);
            }
        };
    }

    public synchronized KafkaCoherentProtocolSnapshotV1 capture() {
        requireProfile(StorageProfile.BOOKKEEPER);
        ProtocolComponents components = captureComponents();
        return new KafkaCoherentProtocolSnapshotV1(
                components.root(),
                (KafkaActiveTailStateV1) components.activeTail(),
                components.producers(),
                components.speculative(),
                components.transactions(),
                components.leaderEpochs());
    }

    public synchronized KafkaObjectCoherentProtocolSnapshotV1 captureObject() {
        requireProfile(StorageProfile.OBJECT);
        ProtocolComponents components = captureComponents();
        return new KafkaObjectCoherentProtocolSnapshotV1(
                components.root(),
                (KafkaObjectActiveTailStateV1) components.activeTail(),
                components.producers(),
                components.speculative(),
                components.transactions(),
                components.leaderEpochs());
    }

    @Override
    public synchronized void onOrderedDurable(KafkaOrderedDurableCommitV1 commit) {
        requireProfile(StorageProfile.BOOKKEEPER);
        Objects.requireNonNull(commit, "commit");
        KafkaCoherentProtocolSnapshotV1 before = capture();
        KafkaSpeculativeCommitV1 head = before.speculativeQueue().head();
        if (!commit.handle().equals(expectedHandle)
                || !head.expectedFence().equals(before.root().fence())
                || head.startOffset() != commit.startOffset()
                || head.endOffsetExclusive() != commit.endOffsetExclusive()) {
            throw new KafkaCoherentPublicationException(
                    "ordered durable locator differs from the speculative queue head or active run");
        }

        KafkaBookKeeperActiveTailLocatorV1 locator = KafkaBookKeeperActiveTailLocatorV1.from(commit);
        KafkaActiveTailStateV1 activeTail = before.activeTail().append(locator);
        KafkaCommittedProducerStateV1 producers = applyProducers(before.committedProducerState(), head);
        KafkaSpeculativeQueueV1 speculative = before.speculativeQueue().removeHead();
        KafkaTransactionStateV1 transactions = applyTransactions(before.transactionState(), head);
        KafkaLeaderEpochIndexV1 leaderEpochs =
                before.leaderEpochIndex().observe(before.root().fence().kafkaLeaderEpoch(), head.startOffset());

        KafkaPartitionStateReferencesV1 current = before.root().references();
        KafkaPartitionStateReferencesV1 replacement = new KafkaPartitionStateReferencesV1(
                current.runTable(),
                repository.store(
                        Math.addExact(current.activeTail().generation(), 1),
                        KafkaProtocolStateCodecV1.activeTail(activeTail),
                        activeTail),
                current.sourceMap(),
                storeProducerReplacement(current.committedProducerState(), before.committedProducerState(), producers),
                repository.store(
                        Math.addExact(current.speculativeProducerQueue().generation(), 1),
                        KafkaProtocolStateCodecV1.speculative(speculative),
                        speculative),
                storeTransactionReplacement(current.transactionIndex(), before.transactionState(), transactions),
                storeLeaderEpochReplacement(current.leaderEpochIndex(), before.leaderEpochIndex(), leaderEpochs),
                current.checkpointVector(),
                current.sourceProtection());
        KafkaPartitionFrontiersV1 frontiers = before.root().frontiers();
        KafkaPartitionCommitSlotV1 slot = new KafkaPartitionCommitSlotV1(
                before.root().fence(),
                before.root().stateVersion(),
                commit.startOffset(),
                commit.endOffsetExclusive(),
                new KafkaPartitionFrontiersV1(
                        frontiers.trimStartOffset(),
                        frontiers.allocatedEndOffset(),
                        commit.endOffsetExclusive(),
                        commit.endOffsetExclusive(),
                        frontiers.highWatermark(),
                        frontiers.lastStableOffset()),
                replacement);
        KafkaPartitionPublicationResultV1 result = publicationCell.publish(slot);
        if (!result.published()) {
            throw new KafkaCoherentPublicationException("coherent commit publication failed: " + result.outcome());
        }
    }

    /** Publishes one tracker-complete Object locator/native-state cut through the M2 root CAS. */
    public synchronized KafkaObjectCoherentProtocolSnapshotV1 publishObject(
            KafkaObjectCompletionTrackerV1.ReadyCompletion completion) {
        requireProfile(StorageProfile.OBJECT);
        Objects.requireNonNull(completion, "completion");
        KafkaObjectCoherentProtocolSnapshotV1 before = captureObject();
        KafkaSpeculativeCommitV1 head = before.speculativeQueue().head();
        if (!head.equals(completion.commitSet())
                || !head.expectedFence().equals(before.root().fence())
                || !completion.locator().binding().equals(binding(before.root().fence()))
                || completion.locator().startOffset() != head.startOffset()
                || completion.locator().endOffsetExclusive() != head.endOffsetExclusive()) {
            throw new KafkaCoherentPublicationException(
                    "Object locator differs from the speculative queue head or coherent binding");
        }
        KafkaCommittedProducerStateV1 producers = applyProducers(before.committedProducerState(), head);
        KafkaTransactionStateV1 transactions = applyTransactions(before.transactionState(), head);
        KafkaLeaderEpochIndexV1 leaderEpochs =
                before.leaderEpochIndex().observe(before.root().fence().kafkaLeaderEpoch(), head.startOffset());
        KafkaObjectNativeStateV1 nativeState = completion.nativeState();
        long recomputedLastStableOffset = Math.min(
                nativeState.highWatermark(),
                transactions.firstUnstableOffset(nativeState.highWatermark()).orElse(nativeState.highWatermark()));
        if (!nativeState.producerState().equals(producers)
                || !nativeState.transactionState().equals(transactions)
                || !nativeState.leaderEpochIndex().equals(leaderEpochs)) {
            throw new KafkaCoherentPublicationException(
                    "Object completion native state differs from the exact speculative transition");
        }
        if (nativeState.highWatermark() > head.endOffsetExclusive()
                || nativeState.lastStableOffset() != recomputedLastStableOffset) {
            throw new KafkaCoherentPublicationException(
                    "Object completion HW/LSO differs from the selected transaction state");
        }
        KafkaObjectActiveTailStateV1 activeTail = before.activeTail().append(completion.locator());
        KafkaSpeculativeQueueV1 speculative = before.speculativeQueue().removeHead();
        KafkaPartitionStateReferencesV1 current = before.root().references();
        KafkaPartitionStateReferencesV1 replacement = new KafkaPartitionStateReferencesV1(
                current.runTable(),
                repository.store(
                        Math.addExact(current.activeTail().generation(), 1),
                        KafkaObjectStateCodecV1.activeTail(activeTail),
                        activeTail),
                current.sourceMap(),
                storeProducerReplacement(current.committedProducerState(), before.committedProducerState(), producers),
                repository.store(
                        Math.addExact(current.speculativeProducerQueue().generation(), 1),
                        KafkaProtocolStateCodecV1.speculative(speculative),
                        speculative),
                storeTransactionReplacement(current.transactionIndex(), before.transactionState(), transactions),
                storeLeaderEpochReplacement(current.leaderEpochIndex(), before.leaderEpochIndex(), leaderEpochs),
                current.checkpointVector(),
                current.sourceProtection());
        KafkaPartitionFrontiersV1 frontiers = before.root().frontiers();
        KafkaPartitionCommitSlotV1 slot = new KafkaPartitionCommitSlotV1(
                before.root().fence(),
                before.root().stateVersion(),
                head.startOffset(),
                head.endOffsetExclusive(),
                new KafkaPartitionFrontiersV1(
                        frontiers.trimStartOffset(),
                        frontiers.allocatedEndOffset(),
                        head.endOffsetExclusive(),
                        head.endOffsetExclusive(),
                        nativeState.highWatermark(),
                        nativeState.lastStableOffset()),
                replacement);
        KafkaPartitionPublicationResultV1 result = publicationCell.publishObject(slot);
        if (!result.published()) {
            throw new KafkaCoherentPublicationException(
                    "coherent Object commit publication failed: " + result.outcome());
        }
        KafkaObjectCoherentProtocolSnapshotV1 published = captureObject();
        if (!published.root().equals(result.observedState())
                || !published.activeTail().equals(activeTail)
                || !published.committedProducerState().equals(producers)
                || !published.speculativeQueue().equals(speculative)
                || !published.transactionState().equals(transactions)
                || !published.leaderEpochIndex().equals(leaderEpochs)) {
            throw new KafkaCoherentPublicationException("Object root CAS selected a different component cut");
        }
        return published;
    }

    /** Removes one exact speculative suffix by storing its real queue bytes and CASing the same M2 root. */
    public synchronized KafkaObjectCoherentProtocolSnapshotV1 rollbackObjectSuffix(
            long rollbackStartOffset, List<KafkaSpeculativeCommitV1> exactSuffix) {
        requireProfile(StorageProfile.OBJECT);
        exactSuffix = List.copyOf(Objects.requireNonNull(exactSuffix, "exactSuffix"));
        if (exactSuffix.isEmpty() || exactSuffix.get(0).startOffset() != rollbackStartOffset) {
            throw new IllegalArgumentException("Object rollback suffix does not begin at its exact boundary");
        }
        KafkaObjectCoherentProtocolSnapshotV1 before = captureObject();
        List<KafkaSpeculativeCommitV1> queue = before.speculativeQueue().commits();
        if (queue.size() < exactSuffix.size()
                || !queue.subList(queue.size() - exactSuffix.size(), queue.size())
                        .equals(exactSuffix)
                || before.root().frontiers().allocatedEndOffset()
                        != exactSuffix.get(exactSuffix.size() - 1).endOffsetExclusive()) {
            throw new IllegalArgumentException("M2 queue/root differs from the exact Object rollback suffix");
        }
        KafkaSpeculativeQueueV1 replacementQueue =
                new KafkaSpeculativeQueueV1(queue.subList(0, queue.size() - exactSuffix.size()));
        KafkaPartitionStateReferencesV1 current = before.root().references();
        KafkaPartitionStateReferencesV1 replacement = new KafkaPartitionStateReferencesV1(
                current.runTable(),
                current.activeTail(),
                current.sourceMap(),
                current.committedProducerState(),
                repository.store(
                        Math.addExact(current.speculativeProducerQueue().generation(), 1),
                        KafkaProtocolStateCodecV1.speculative(replacementQueue),
                        replacementQueue),
                current.transactionIndex(),
                current.leaderEpochIndex(),
                current.checkpointVector(),
                current.sourceProtection());
        KafkaPartitionPublicationResultV1 result =
                publicationCell.rollbackSpeculative(new KafkaPartitionSpeculativeRollbackSlotV1(
                        before.root().fence(), before.root().stateVersion(), rollbackStartOffset, replacement));
        if (!result.published()) {
            throw new KafkaCoherentPublicationException("coherent Object suffix rollback failed: " + result.outcome());
        }
        KafkaObjectCoherentProtocolSnapshotV1 published = captureObject();
        if (!published.root().equals(result.observedState())
                || !published.activeTail().equals(before.activeTail())
                || !published.committedProducerState().equals(before.committedProducerState())
                || !published.speculativeQueue().equals(replacementQueue)
                || !published.transactionState().equals(before.transactionState())
                || !published.leaderEpochIndex().equals(before.leaderEpochIndex())) {
            throw new KafkaCoherentPublicationException("Object rollback root selected a different component cut");
        }
        return published;
    }

    /** Retires a manifest-covered locator prefix and releases its retained budget only after one exact root CAS. */
    public synchronized KafkaObjectCoherentProtocolSnapshotV1 retireObjectTail(
            KafkaObjectSourceProtectionTrackerV1 sourceProtection,
            KafkaObjectSourceProtectionTrackerV1.RetirementPlan retirementPlan,
            KafkaObjectCompletionTrackerV1 completionTracker) {
        requireProfile(StorageProfile.OBJECT);
        Objects.requireNonNull(sourceProtection, "sourceProtection");
        Objects.requireNonNull(retirementPlan, "retirementPlan");
        Objects.requireNonNull(completionTracker, "completionTracker");
        KafkaObjectCoherentProtocolSnapshotV1 before = captureObject();
        if (!retirementPlan.binds(before.activeTail())) {
            throw new IllegalArgumentException("Object retirement plan differs from the selected active tail");
        }
        KafkaObjectActiveTailStateV1 activeTail = before.activeTail().retirePrefix(retirementPlan);
        if (!activeTail.equals(retirementPlan.replacement())) {
            throw new IllegalStateException("Object retirement plan produced a different active-tail cut");
        }
        KafkaPartitionStateReferencesV1 current = before.root().references();
        KafkaPartitionStateReferencesV1 replacement = new KafkaPartitionStateReferencesV1(
                current.runTable(),
                repository.store(
                        Math.addExact(current.activeTail().generation(), 1),
                        KafkaObjectStateCodecV1.activeTail(activeTail),
                        activeTail),
                current.sourceMap(),
                current.committedProducerState(),
                current.speculativeProducerQueue(),
                current.transactionIndex(),
                current.leaderEpochIndex(),
                current.checkpointVector(),
                new KafkaPartitionStateReferenceV1(
                        Math.addExact(current.sourceProtection().generation(), 1),
                        retirementPlan.sourceProtectionDigest()));
        KafkaPartitionPublicationResultV1 result =
                publicationCell.retireObjectTail(new KafkaPartitionObjectTailRetirementSlotV1(
                        before.root().fence(), before.root().stateVersion(), replacement));
        if (!result.published()) {
            throw new KafkaCoherentPublicationException("coherent Object tail retirement failed: " + result.outcome());
        }
        KafkaObjectCoherentProtocolSnapshotV1 selected = captureObject();
        if (!selected.root().equals(result.observedState())
                || !selected.activeTail().equals(activeTail)) {
            throw new KafkaCoherentPublicationException("Object retirement root selected a different component cut");
        }
        var authority = sourceProtection.completeAfterRootCas(retirementPlan, selected);
        completionTracker.releaseRetainedAfterRootRetirement(authority);
        return selected;
    }

    private synchronized void stage(KafkaProtocolAppendPlanV1 plan, KafkaOffsetAssignedAppendV1 assigned) {
        ProtocolComponents before = captureComponents();
        if (!plan.expectedFence().equals(before.root().fence())
                || assigned.startOffset() != before.root().frontiers().allocatedEndOffset()) {
            throw new KafkaCoherentPublicationException(
                    "assigned append differs from the current protocol fence or Allocated frontier");
        }
        KafkaSpeculativeCommitV1 commit =
                KafkaSpeculativeCommitV1.assign(plan, assigned.startOffset(), assigned.endOffsetExclusive());
        KafkaSpeculativeQueueV1 candidate =
                before.speculative().append(commit, before.root().frontiers().allocatedEndOffset());

        KafkaPartitionStateReferencesV1 references = before.root().references();
        KafkaPartitionStateReferencesV1 replacement = new KafkaPartitionStateReferencesV1(
                references.runTable(),
                references.activeTail(),
                references.sourceMap(),
                references.committedProducerState(),
                repository.store(
                        Math.addExact(references.speculativeProducerQueue().generation(), 1),
                        KafkaProtocolStateCodecV1.speculative(candidate),
                        candidate),
                references.transactionIndex(),
                references.leaderEpochIndex(),
                references.checkpointVector(),
                references.sourceProtection());
        KafkaPartitionPublicationResultV1 result = publicationCell.stageSpeculative(new KafkaPartitionSpeculativeSlotV1(
                before.root().fence(),
                before.root().stateVersion(),
                assigned.startOffset(),
                assigned.endOffsetExclusive(),
                replacement));
        if (!result.published()) {
            throw new KafkaCoherentPublicationException("speculative protocol publication failed: " + result.outcome());
        }
    }

    private synchronized void validate(KafkaProtocolAppendPlanV1 plan) {
        ProtocolComponents before = captureComponents();
        if (!plan.expectedFence().equals(before.root().fence())) {
            throw new KafkaCoherentPublicationException("protocol plan fence differs before offset assignment");
        }
        long startOffset = before.root().frontiers().allocatedEndOffset();
        KafkaSpeculativeCommitV1 candidate = KafkaSpeculativeCommitV1.assign(
                plan, startOffset, Math.addExact(startOffset, plan.logicalOffsetCount()));
        KafkaCommittedProducerStateV1 effectiveProducers = before.producers();
        KafkaTransactionStateV1 effectiveTransactions = before.transactions();
        try {
            for (KafkaSpeculativeCommitV1 existing : before.speculative().commits()) {
                effectiveProducers = effectiveProducers.apply(existing);
                effectiveTransactions = effectiveTransactions.apply(existing);
            }
            effectiveProducers.apply(candidate);
            effectiveTransactions.apply(candidate);
        } catch (IllegalArgumentException failure) {
            throw new KafkaCoherentPublicationException(
                    "protocol delta is invalid against committed plus ordered speculative state", failure);
        }
    }

    private static KafkaCommittedProducerStateV1 applyProducers(
            KafkaCommittedProducerStateV1 before, KafkaSpeculativeCommitV1 commit) {
        try {
            return before.apply(commit);
        } catch (IllegalArgumentException failure) {
            throw new KafkaCoherentPublicationException("committed producer transition failed", failure);
        }
    }

    private static KafkaTransactionStateV1 applyTransactions(
            KafkaTransactionStateV1 before, KafkaSpeculativeCommitV1 commit) {
        try {
            return before.apply(commit);
        } catch (IllegalArgumentException failure) {
            throw new KafkaCoherentPublicationException("committed transaction transition failed", failure);
        }
    }

    private KafkaPartitionStateReferenceV1 storeProducerReplacement(
            KafkaPartitionStateReferenceV1 reference,
            KafkaCommittedProducerStateV1 before,
            KafkaCommittedProducerStateV1 after) {
        return before.equals(after)
                ? reference
                : repository.store(
                        Math.addExact(reference.generation(), 1), KafkaProtocolStateCodecV1.producers(after), after);
    }

    private KafkaPartitionStateReferenceV1 storeTransactionReplacement(
            KafkaPartitionStateReferenceV1 reference, KafkaTransactionStateV1 before, KafkaTransactionStateV1 after) {
        return before.equals(after)
                ? reference
                : repository.store(
                        Math.addExact(reference.generation(), 1), KafkaProtocolStateCodecV1.transactions(after), after);
    }

    private KafkaPartitionStateReferenceV1 storeLeaderEpochReplacement(
            KafkaPartitionStateReferenceV1 reference, KafkaLeaderEpochIndexV1 before, KafkaLeaderEpochIndexV1 after) {
        return before.equals(after)
                ? reference
                : repository.store(
                        Math.addExact(reference.generation(), 1), KafkaProtocolStateCodecV1.leaderEpochs(after), after);
    }

    private static KafkaPartitionStateReferenceV1 seed(String label) {
        return new KafkaPartitionStateReferenceV1(0, Sha256Digest.hash(KafkaProtocolStateCodecV1.label(label)));
    }

    private ProtocolComponents captureComponents() {
        KafkaPartitionProtocolStateV1 root = publicationCell.capture();
        KafkaPartitionStateReferencesV1 references = root.references();
        Object activeTail = storageProfile == StorageProfile.BOOKKEEPER
                ? repository.resolve(references.activeTail(), KafkaActiveTailStateV1.class)
                : repository.resolve(references.activeTail(), KafkaObjectActiveTailStateV1.class);
        return new ProtocolComponents(
                root,
                activeTail,
                repository.resolve(references.committedProducerState(), KafkaCommittedProducerStateV1.class),
                repository.resolve(references.speculativeProducerQueue(), KafkaSpeculativeQueueV1.class),
                repository.resolve(references.transactionIndex(), KafkaTransactionStateV1.class),
                repository.resolve(references.leaderEpochIndex(), KafkaLeaderEpochIndexV1.class));
    }

    private void requireProfile(StorageProfile expected) {
        if (storageProfile != expected) {
            throw new IllegalStateException("Kafka coherent coordinator belongs to another storage profile");
        }
    }

    private static KafkaObjectBindingKeyV1 binding(KafkaPartitionFenceV1 fence) {
        return new KafkaObjectBindingKeyV1(
                fence.bindingId(), fence.topicIncarnation().topicId(), fence.partitionId(), fence.storageEpochId());
    }

    private record ProtocolComponents(
            KafkaPartitionProtocolStateV1 root,
            Object activeTail,
            KafkaCommittedProducerStateV1 producers,
            KafkaSpeculativeQueueV1 speculative,
            KafkaTransactionStateV1 transactions,
            KafkaLeaderEpochIndexV1 leaderEpochs) {}
}
