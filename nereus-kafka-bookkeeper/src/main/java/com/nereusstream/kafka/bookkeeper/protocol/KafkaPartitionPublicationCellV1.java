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

package com.nereusstream.kafka.bookkeeper.protocol;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/** One CAS-linearized coherent partition state root shared by commits and fence transitions. */
public final class KafkaPartitionPublicationCellV1 {
    private final AtomicReference<KafkaPartitionProtocolStateV1> state;
    private final KafkaPartitionPublicationObserver observer;

    public KafkaPartitionPublicationCellV1(
            KafkaPartitionProtocolStateV1 initialState, KafkaPartitionPublicationObserver observer) {
        this.state = new AtomicReference<>(Objects.requireNonNull(initialState, "initialState"));
        this.observer = Objects.requireNonNull(observer, "observer");
    }

    public KafkaPartitionProtocolStateV1 capture() {
        return state.get();
    }

    public KafkaPartitionReadSnapshotV1 captureReadSnapshot() {
        return state.get();
    }

    public KafkaPartitionPublicationResultV1 publish(KafkaPartitionCommitSlotV1 slot) {
        Objects.requireNonNull(slot, "slot");
        KafkaPartitionProtocolStateV1 current = state.get();
        KafkaPartitionPublicationResultV1 mismatch =
                exactPredecessor(current, slot.expectedFence(), slot.predecessorStateVersion());
        if (mismatch != null) {
            return mismatch;
        }
        if (slot.commitStartOffset() != current.frontiers().readableEndOffset()) {
            return result(KafkaPartitionPublicationOutcomeV1.NON_CONTIGUOUS_COMMIT, current);
        }
        if (!validCommitReplacement(current, slot)) {
            return result(KafkaPartitionPublicationOutcomeV1.INVALID_COMMIT_REPLACEMENT, current);
        }
        KafkaPartitionProtocolStateV1 replacement = new KafkaPartitionProtocolStateV1(
                current.fence(),
                Math.addExact(current.stateVersion(), 1),
                slot.replacementFrontiers(),
                slot.replacementReferences());
        return replace(current, replacement, KafkaPartitionPublicationKindV1.COMMIT);
    }

    /** Object-WAL commit cut: active tail, native state, speculative queue and all frontiers share one root CAS. */
    public KafkaPartitionPublicationResultV1 publishObject(KafkaPartitionCommitSlotV1 slot) {
        Objects.requireNonNull(slot, "slot");
        KafkaPartitionProtocolStateV1 current = state.get();
        KafkaPartitionPublicationResultV1 mismatch =
                exactPredecessor(current, slot.expectedFence(), slot.predecessorStateVersion());
        if (mismatch != null) {
            return mismatch;
        }
        if (slot.commitStartOffset() != current.frontiers().readableEndOffset()
                || slot.commitStartOffset() != current.frontiers().durableEndOffset()) {
            return result(KafkaPartitionPublicationOutcomeV1.NON_CONTIGUOUS_COMMIT, current);
        }
        if (!validObjectCommitReplacement(current, slot)) {
            return result(KafkaPartitionPublicationOutcomeV1.INVALID_COMMIT_REPLACEMENT, current);
        }
        KafkaPartitionProtocolStateV1 replacement = new KafkaPartitionProtocolStateV1(
                current.fence(),
                Math.addExact(current.stateVersion(), 1),
                slot.replacementFrontiers(),
                slot.replacementReferences());
        return replace(current, replacement, KafkaPartitionPublicationKindV1.COMMIT);
    }

    public KafkaPartitionPublicationResultV1 stageSpeculative(KafkaPartitionSpeculativeSlotV1 slot) {
        Objects.requireNonNull(slot, "slot");
        KafkaPartitionProtocolStateV1 current = state.get();
        KafkaPartitionPublicationResultV1 mismatch =
                exactPredecessor(current, slot.expectedFence(), slot.predecessorStateVersion());
        if (mismatch != null) {
            return mismatch;
        }
        if (slot.allocationStartOffset() != current.frontiers().allocatedEndOffset()) {
            return result(KafkaPartitionPublicationOutcomeV1.NON_CONTIGUOUS_ALLOCATION, current);
        }
        if (!validSpeculativeReplacement(current.references(), slot.replacementReferences())) {
            return result(KafkaPartitionPublicationOutcomeV1.INVALID_SPECULATIVE_REPLACEMENT, current);
        }
        KafkaPartitionFrontiersV1 before = current.frontiers();
        KafkaPartitionProtocolStateV1 replacement = new KafkaPartitionProtocolStateV1(
                current.fence(),
                Math.addExact(current.stateVersion(), 1),
                new KafkaPartitionFrontiersV1(
                        before.trimStartOffset(),
                        slot.allocationEndOffset(),
                        before.durableEndOffset(),
                        before.readableEndOffset(),
                        before.highWatermark(),
                        before.lastStableOffset()),
                slot.replacementReferences());
        if (!state.compareAndSet(current, replacement)) {
            return result(KafkaPartitionPublicationOutcomeV1.STATE_VERSION_MISMATCH, state.get());
        }
        return result(KafkaPartitionPublicationOutcomeV1.PUBLISHED, replacement);
    }

    /** Regresses only Allocated and replaces only the speculative queue after exact whole-suffix validation. */
    public KafkaPartitionPublicationResultV1 rollbackSpeculative(KafkaPartitionSpeculativeRollbackSlotV1 slot) {
        Objects.requireNonNull(slot, "slot");
        KafkaPartitionProtocolStateV1 current = state.get();
        KafkaPartitionPublicationResultV1 mismatch =
                exactPredecessor(current, slot.expectedFence(), slot.predecessorStateVersion());
        if (mismatch != null) {
            return mismatch;
        }
        KafkaPartitionFrontiersV1 before = current.frontiers();
        if (slot.rollbackStartOffset() < before.durableEndOffset()
                || slot.rollbackStartOffset() >= before.allocatedEndOffset()
                || !validSpeculativeRollback(current.references(), slot.replacementReferences())) {
            return result(KafkaPartitionPublicationOutcomeV1.INVALID_SPECULATIVE_ROLLBACK, current);
        }
        KafkaPartitionProtocolStateV1 replacement = new KafkaPartitionProtocolStateV1(
                current.fence(),
                Math.addExact(current.stateVersion(), 1),
                new KafkaPartitionFrontiersV1(
                        before.trimStartOffset(),
                        slot.rollbackStartOffset(),
                        before.durableEndOffset(),
                        before.readableEndOffset(),
                        before.highWatermark(),
                        before.lastStableOffset()),
                slot.replacementReferences());
        return replace(current, replacement, KafkaPartitionPublicationKindV1.SPECULATIVE_ROLLBACK);
    }

    /** Selects manifest-backed active-tail retirement and its drained source-protection state in one root CAS. */
    public KafkaPartitionPublicationResultV1 retireObjectTail(KafkaPartitionObjectTailRetirementSlotV1 slot) {
        Objects.requireNonNull(slot, "slot");
        KafkaPartitionProtocolStateV1 current = state.get();
        KafkaPartitionPublicationResultV1 mismatch =
                exactPredecessor(current, slot.expectedFence(), slot.predecessorStateVersion());
        if (mismatch != null) {
            return mismatch;
        }
        KafkaPartitionStateReferencesV1 before = current.references();
        KafkaPartitionStateReferencesV1 after = slot.replacementReferences();
        if (!after.doesNotRegress(before)
                || !exactGenerationAdvance(before.activeTail(), after.activeTail(), true)
                || !exactGenerationAdvance(before.sourceProtection(), after.sourceProtection(), true)
                || !after.runTable().equals(before.runTable())
                || !after.sourceMap().equals(before.sourceMap())
                || !after.committedProducerState().equals(before.committedProducerState())
                || !after.speculativeProducerQueue().equals(before.speculativeProducerQueue())
                || !after.transactionIndex().equals(before.transactionIndex())
                || !after.leaderEpochIndex().equals(before.leaderEpochIndex())
                || !after.checkpointVector().equals(before.checkpointVector())) {
            return result(KafkaPartitionPublicationOutcomeV1.INVALID_OBJECT_TAIL_RETIREMENT, current);
        }
        KafkaPartitionProtocolStateV1 replacement = new KafkaPartitionProtocolStateV1(
                current.fence(), Math.addExact(current.stateVersion(), 1), current.frontiers(), after);
        return replace(current, replacement, KafkaPartitionPublicationKindV1.OBJECT_TAIL_RETIREMENT);
    }

    public KafkaPartitionPublicationResultV1 transition(KafkaPartitionFenceTransitionV1 transition) {
        Objects.requireNonNull(transition, "transition");
        KafkaPartitionProtocolStateV1 current = state.get();
        KafkaPartitionPublicationResultV1 mismatch =
                exactPredecessor(current, transition.expectedFence(), transition.predecessorStateVersion());
        if (mismatch != null) {
            return mismatch;
        }
        if (!transition.replacementReferences().doesNotRegress(current.references())) {
            return result(KafkaPartitionPublicationOutcomeV1.INVALID_FENCE_TRANSITION, current);
        }
        KafkaPartitionProtocolStateV1 replacement = new KafkaPartitionProtocolStateV1(
                transition.replacementFence(),
                Math.addExact(current.stateVersion(), 1),
                transition.replacementFrontiers(),
                transition.replacementReferences());
        return replace(current, replacement, KafkaPartitionPublicationKindV1.FENCE_TRANSITION);
    }

    private KafkaPartitionPublicationResultV1 replace(
            KafkaPartitionProtocolStateV1 expected,
            KafkaPartitionProtocolStateV1 replacement,
            KafkaPartitionPublicationKindV1 kind) {
        if (!state.compareAndSet(expected, replacement)) {
            return result(KafkaPartitionPublicationOutcomeV1.STATE_VERSION_MISMATCH, state.get());
        }
        KafkaPartitionPublicationEventV1 event = new KafkaPartitionPublicationEventV1(kind, expected, replacement);
        try {
            observer.onPublished(event);
            return result(KafkaPartitionPublicationOutcomeV1.PUBLISHED, replacement);
        } catch (RuntimeException notificationFailure) {
            return result(KafkaPartitionPublicationOutcomeV1.PUBLISHED_NOTIFICATION_FAILED, replacement);
        }
    }

    private static KafkaPartitionPublicationResultV1 exactPredecessor(
            KafkaPartitionProtocolStateV1 current, KafkaPartitionFenceV1 expectedFence, long expectedVersion) {
        if (!current.fence().equals(expectedFence)) {
            return result(KafkaPartitionPublicationOutcomeV1.FENCE_MISMATCH, current);
        }
        if (current.stateVersion() != expectedVersion) {
            return result(KafkaPartitionPublicationOutcomeV1.STATE_VERSION_MISMATCH, current);
        }
        return null;
    }

    private static boolean validCommitReplacement(
            KafkaPartitionProtocolStateV1 current, KafkaPartitionCommitSlotV1 slot) {
        KafkaPartitionFrontiersV1 before = current.frontiers();
        KafkaPartitionFrontiersV1 after = slot.replacementFrontiers();
        return after.noRegressionFrom(before)
                && after.trimStartOffset() == before.trimStartOffset()
                && after.highWatermark() == before.highWatermark()
                && after.lastStableOffset() == before.lastStableOffset()
                && slot.replacementReferences().doesNotRegress(current.references())
                && slot.replacementReferences().activeTail().generation()
                        > current.references().activeTail().generation();
    }

    private static boolean validObjectCommitReplacement(
            KafkaPartitionProtocolStateV1 current, KafkaPartitionCommitSlotV1 slot) {
        KafkaPartitionFrontiersV1 before = current.frontiers();
        KafkaPartitionFrontiersV1 after = slot.replacementFrontiers();
        KafkaPartitionStateReferencesV1 beforeReferences = current.references();
        KafkaPartitionStateReferencesV1 afterReferences = slot.replacementReferences();
        return after.noRegressionFrom(before)
                && after.trimStartOffset() == before.trimStartOffset()
                && after.allocatedEndOffset() == before.allocatedEndOffset()
                && after.durableEndOffset() == slot.commitEndOffset()
                && after.readableEndOffset() == slot.commitEndOffset()
                && afterReferences.doesNotRegress(beforeReferences)
                && afterReferences.runTable().equals(beforeReferences.runTable())
                && exactGenerationAdvance(beforeReferences.activeTail(), afterReferences.activeTail(), true)
                && afterReferences.sourceMap().equals(beforeReferences.sourceMap())
                && exactGenerationAdvance(
                        beforeReferences.committedProducerState(), afterReferences.committedProducerState(), false)
                && exactGenerationAdvance(
                        beforeReferences.speculativeProducerQueue(), afterReferences.speculativeProducerQueue(), true)
                && exactGenerationAdvance(
                        beforeReferences.transactionIndex(), afterReferences.transactionIndex(), false)
                && exactGenerationAdvance(
                        beforeReferences.leaderEpochIndex(), afterReferences.leaderEpochIndex(), false)
                && afterReferences.checkpointVector().equals(beforeReferences.checkpointVector())
                && afterReferences.sourceProtection().equals(beforeReferences.sourceProtection());
    }

    private static boolean validSpeculativeReplacement(
            KafkaPartitionStateReferencesV1 before, KafkaPartitionStateReferencesV1 after) {
        return after.doesNotRegress(before)
                && after.runTable().equals(before.runTable())
                && after.activeTail().equals(before.activeTail())
                && after.sourceMap().equals(before.sourceMap())
                && after.committedProducerState().equals(before.committedProducerState())
                && after.speculativeProducerQueue().generation()
                        > before.speculativeProducerQueue().generation()
                && after.transactionIndex().equals(before.transactionIndex())
                && after.leaderEpochIndex().equals(before.leaderEpochIndex())
                && after.checkpointVector().equals(before.checkpointVector())
                && after.sourceProtection().equals(before.sourceProtection());
    }

    private static boolean validSpeculativeRollback(
            KafkaPartitionStateReferencesV1 before, KafkaPartitionStateReferencesV1 after) {
        return after.runTable().equals(before.runTable())
                && after.activeTail().equals(before.activeTail())
                && after.sourceMap().equals(before.sourceMap())
                && after.committedProducerState().equals(before.committedProducerState())
                && exactGenerationAdvance(before.speculativeProducerQueue(), after.speculativeProducerQueue(), true)
                && after.transactionIndex().equals(before.transactionIndex())
                && after.leaderEpochIndex().equals(before.leaderEpochIndex())
                && after.checkpointVector().equals(before.checkpointVector())
                && after.sourceProtection().equals(before.sourceProtection());
    }

    private static boolean exactGenerationAdvance(
            KafkaPartitionStateReferenceV1 before, KafkaPartitionStateReferenceV1 after, boolean mustAdvance) {
        if (after.equals(before)) {
            return !mustAdvance;
        }
        return after.generation() == Math.addExact(before.generation(), 1);
    }

    private static KafkaPartitionPublicationResultV1 result(
            KafkaPartitionPublicationOutcomeV1 outcome, KafkaPartitionProtocolStateV1 observed) {
        return new KafkaPartitionPublicationResultV1(outcome, observed);
    }
}
