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
import com.nereusstream.kafka.bookkeeper.pipeline.KafkaAppendProtocolHooksV1;
import com.nereusstream.kafka.bookkeeper.pipeline.KafkaOffsetAssignedAppendV1;
import com.nereusstream.kafka.bookkeeper.pipeline.KafkaOrderedDurableCommitObserver;
import com.nereusstream.kafka.bookkeeper.pipeline.KafkaOrderedDurableCommitV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionCommitSlotV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFrontiersV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionProtocolStateV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionPublicationCellV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionPublicationObserver;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionPublicationResultV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionSpeculativeSlotV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionStateReferenceV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionStateReferencesV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import java.util.Objects;

/** K5 bridge from post-offset speculative admission to one ordered K1 coherent publication per durable group. */
public final class KafkaCoherentCommitCoordinatorV1 implements KafkaOrderedDurableCommitObserver {
    private final KafkaPartitionPublicationCellV1 publicationCell;
    private final KafkaProtocolStateRepositoryV1 repository;
    private final RunLedgerHandleV1 expectedHandle;

    private KafkaCoherentCommitCoordinatorV1(
            KafkaPartitionPublicationCellV1 publicationCell,
            KafkaProtocolStateRepositoryV1 repository,
            RunLedgerHandleV1 expectedHandle) {
        this.publicationCell = publicationCell;
        this.repository = repository;
        this.expectedHandle = expectedHandle;
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
                new KafkaPartitionPublicationCellV1(initial, observer), repository, expectedHandle);
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
        KafkaPartitionProtocolStateV1 root = publicationCell.capture();
        KafkaPartitionStateReferencesV1 references = root.references();
        return new KafkaCoherentProtocolSnapshotV1(
                root,
                repository.resolve(references.activeTail(), KafkaActiveTailStateV1.class),
                repository.resolve(references.committedProducerState(), KafkaCommittedProducerStateV1.class),
                repository.resolve(references.speculativeProducerQueue(), KafkaSpeculativeQueueV1.class),
                repository.resolve(references.transactionIndex(), KafkaTransactionStateV1.class),
                repository.resolve(references.leaderEpochIndex(), KafkaLeaderEpochIndexV1.class));
    }

    @Override
    public synchronized void onOrderedDurable(KafkaOrderedDurableCommitV1 commit) {
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

    private synchronized void stage(KafkaProtocolAppendPlanV1 plan, KafkaOffsetAssignedAppendV1 assigned) {
        KafkaCoherentProtocolSnapshotV1 before = capture();
        if (!plan.expectedFence().equals(before.root().fence())
                || assigned.startOffset() != before.root().frontiers().allocatedEndOffset()) {
            throw new KafkaCoherentPublicationException(
                    "assigned append differs from the current protocol fence or Allocated frontier");
        }
        KafkaSpeculativeCommitV1 commit =
                KafkaSpeculativeCommitV1.assign(plan, assigned.startOffset(), assigned.endOffsetExclusive());
        KafkaSpeculativeQueueV1 candidate = before.speculativeQueue()
                .append(commit, before.root().frontiers().allocatedEndOffset());

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
        KafkaCoherentProtocolSnapshotV1 before = capture();
        if (!plan.expectedFence().equals(before.root().fence())) {
            throw new KafkaCoherentPublicationException("protocol plan fence differs before offset assignment");
        }
        long startOffset = before.root().frontiers().allocatedEndOffset();
        KafkaSpeculativeCommitV1 candidate = KafkaSpeculativeCommitV1.assign(
                plan, startOffset, Math.addExact(startOffset, plan.logicalOffsetCount()));
        KafkaCommittedProducerStateV1 effectiveProducers = before.committedProducerState();
        KafkaTransactionStateV1 effectiveTransactions = before.transactionState();
        try {
            for (KafkaSpeculativeCommitV1 existing : before.speculativeQueue().commits()) {
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
}
