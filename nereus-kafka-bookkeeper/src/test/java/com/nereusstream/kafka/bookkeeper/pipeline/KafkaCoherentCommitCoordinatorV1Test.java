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

package com.nereusstream.kafka.bookkeeper.pipeline;

import static org.assertj.core.api.Assertions.assertThat;
import com.nereusstream.kafka.bookkeeper.commit.KafkaBatchDuplicateIdentityV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaCoherentCommitCoordinatorV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaCoherentProtocolSnapshotV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaProtocolAppendPlanV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaProtocolBatchDeltaV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaTransactionBatchKindV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceTransitionV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFrontiersV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionPublicationEventV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionPublicationKindV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionPublicationObserver;
import com.nereusstream.kafka.bookkeeper.run.KafkaBookKeeperRunLifecycleV1;
import com.nereusstream.kafka.bookkeeper.run.KafkaRunTestFixtures;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class KafkaCoherentCommitCoordinatorV1Test {
    @Test
    void onePublicationAdvancesLocatorProducerResultsLeaderEpochDurableAndLeo() {
        Context context = context();
        KafkaPipelineTestFixtures.Plan storage = KafkaPipelineTestFixtures.plan(context.lifecycle, 2, 100);
        KafkaProtocolAppendPlanV1 protocol = plan(
                context.fence,
                batch(7, 0, KafkaTransactionBatchKindV1.NONE, -1),
                batch(8, 0, KafkaTransactionBatchKindV1.NONE, -1));

        KafkaOrderedAppendResultV1 result = context.pipeline
                .submit(storage.request(), storage::assignment, context.coordinator.protocolHooks(protocol))
                .toCompletableFuture()
                .join();
        KafkaCoherentProtocolSnapshotV1 snapshot = context.coordinator.capture();

        assertThat(result.outcome()).isEqualTo(KafkaOrderedAppendOutcomeV1.COMMITTED_ORDERED);
        assertThat(snapshot.root().frontiers()).isEqualTo(new KafkaPartitionFrontiersV1(100, 102, 102, 102, 100, 100));
        assertThat(snapshot.activeTail().locators()).singleElement().satisfies(locator -> {
            assertThat(locator.startOffset()).isEqualTo(100);
            assertThat(locator.endOffsetExclusive()).isEqualTo(102);
            assertThat(locator.firstDataEntryId()).isEqualTo(1);
            assertThat(locator.lastDataEntryId()).isEqualTo(2);
            assertThat(locator.memberCount()).isEqualTo(2);
        });
        assertThat(snapshot.committedProducerState().producers()).containsOnlyKeys(7L, 8L);
        assertThat(snapshot.committedProducerState()
                        .findDuplicate(new KafkaBatchDuplicateIdentityV1(8, (short) 0, 0, 0))
                        .orElseThrow()
                        .startOffset())
                .isEqualTo(101);
        assertThat(snapshot.speculativeQueue().commits()).isEmpty();
        assertThat(snapshot.leaderEpochIndex().startOffsets()).containsEntry(5, 100L);
        assertThat(context.events)
                .singleElement()
                .extracting(KafkaPartitionPublicationEventV1::kind)
                .isEqualTo(KafkaPartitionPublicationKindV1.COMMIT);
    }

    @Test
    void invalidProducerSequenceFailsBeforeOffsetAssignmentAndLeaksNoCapacity() {
        Context context = context();
        KafkaPipelineTestFixtures.Plan storage = KafkaPipelineTestFixtures.plan(context.lifecycle, 1, 100);
        KafkaProtocolAppendPlanV1 invalid = plan(
                context.fence,
                new KafkaProtocolBatchDeltaV1(
                        1,
                        Optional.of(new KafkaBatchDuplicateIdentityV1(7, (short) 0, 2, 2)),
                        KafkaTransactionBatchKindV1.NONE,
                        -1,
                        -1));
        AtomicBoolean allocatorInvoked = new AtomicBoolean();

        KafkaOrderedAppendResultV1 result = context.pipeline
                .submit(
                        storage.request(),
                        () -> {
                            allocatorInvoked.set(true);
                            return storage.assignment();
                        },
                        context.coordinator.protocolHooks(invalid))
                .toCompletableFuture()
                .join();

        assertThat(result.outcome()).isEqualTo(KafkaOrderedAppendOutcomeV1.PROTOCOL_VALIDATION_FAILED);
        assertThat(allocatorInvoked).isFalse();
        assertThat(context.pipeline.speculativeEndOffset()).isEqualTo(100);
        assertThat(context.pipeline.fenced()).isFalse();
        assertThat(context.partition.snapshot()).isEqualTo(new KafkaAppendCapacitySnapshotV1(0, 0, 0));
        assertThat(context.coordinator.capture().root().frontiers().allocatedEndOffset())
                .isEqualTo(100);
    }

    @Test
    void fenceBetweenNativeAssignmentAndSpeculativeInstallCreatesNoVisibleStateAndFencesWriter() {
        Context context = context();
        KafkaPipelineTestFixtures.Plan storage = KafkaPipelineTestFixtures.plan(context.lifecycle, 1, 100);
        KafkaProtocolAppendPlanV1 protocol = plan(context.fence, batch(7, 0, KafkaTransactionBatchKindV1.NONE, -1));

        KafkaOrderedAppendResultV1 result = context.pipeline
                .submit(
                        storage.request(),
                        () -> {
                            var beforeFence = context.coordinator.capture().root();
                            assertThat(context.coordinator
                                            .publicationCell()
                                            .transition(new KafkaPartitionFenceTransitionV1(
                                                    beforeFence.fence(),
                                                    beforeFence.stateVersion(),
                                                    successorFence(beforeFence.fence()),
                                                    beforeFence.frontiers(),
                                                    beforeFence.references()))
                                            .published())
                                    .isTrue();
                            return storage.assignment();
                        },
                        context.coordinator.protocolHooks(protocol))
                .toCompletableFuture()
                .join();

        assertThat(result.outcome()).isEqualTo(KafkaOrderedAppendOutcomeV1.PROTOCOL_PREPARATION_FAILED);
        assertThat(context.pipeline.speculativeEndOffset()).isEqualTo(101);
        assertThat(context.pipeline.fenced()).isTrue();
        KafkaCoherentProtocolSnapshotV1 after = context.coordinator.capture();
        assertThat(after.root().frontiers()).isEqualTo(new KafkaPartitionFrontiersV1(100, 100, 100, 100, 100, 100));
        assertThat(after.speculativeQueue().commits()).isEmpty();
        assertThat(context.partition.snapshot()).isEqualTo(new KafkaAppendCapacitySnapshotV1(0, 0, 0));
        assertThat(context.session.entries).containsOnlyKeys(0L);
    }

    @Test
    void bDurableBeforeAStaysHiddenWhileAllocatedAndThenPublishesInOrder() {
        Context context = context();
        context.session.delayedEntryId = 1;
        KafkaPipelineTestFixtures.Plan aStorage = KafkaPipelineTestFixtures.plan(context.lifecycle, 1, 100);
        KafkaPipelineTestFixtures.Plan bStorage = KafkaPipelineTestFixtures.plan(context.lifecycle, 1, 101);
        CompletionStage<KafkaOrderedAppendResultV1> a = context.pipeline.submit(
                aStorage.request(),
                aStorage::assignment,
                context.coordinator.protocolHooks(
                        plan(context.fence, batch(7, 0, KafkaTransactionBatchKindV1.NONE, -1))));
        CompletionStage<KafkaOrderedAppendResultV1> b = context.pipeline.submit(
                bStorage.request(),
                bStorage::assignment,
                context.coordinator.protocolHooks(
                        plan(context.fence, batch(7, 1, KafkaTransactionBatchKindV1.NONE, -1))));

        KafkaCoherentProtocolSnapshotV1 blocked = context.coordinator.capture();
        assertThat(blocked.root().frontiers()).isEqualTo(new KafkaPartitionFrontiersV1(100, 102, 100, 100, 100, 100));
        assertThat(blocked.speculativeQueue().commits()).hasSize(2);
        assertThat(b.toCompletableFuture()).isNotDone();
        assertThat(context.events).isEmpty();

        context.session.completeDelayedAppend();

        assertThat(a.toCompletableFuture().join().outcome()).isEqualTo(KafkaOrderedAppendOutcomeV1.COMMITTED_ORDERED);
        assertThat(b.toCompletableFuture().join().outcome()).isEqualTo(KafkaOrderedAppendOutcomeV1.COMMITTED_ORDERED);
        KafkaCoherentProtocolSnapshotV1 published = context.coordinator.capture();
        assertThat(published.root().frontiers().readableEndOffset()).isEqualTo(102);
        assertThat(published.committedProducerState().producers().get(7L).lastSequence())
                .isEqualTo(1);
        assertThat(published.activeTail().locators())
                .extracting(locator -> locator.startOffset())
                .containsExactly(100L, 101L);
        assertThat(context.events).hasSize(2);
    }

    @Test
    void transactionAbortAndFirstUnstableStateShareTheReadablePublicationCut() {
        Context context = context();
        KafkaPipelineTestFixtures.Plan dataStorage = KafkaPipelineTestFixtures.plan(context.lifecycle, 1, 100);
        KafkaPipelineTestFixtures.Plan abortStorage = KafkaPipelineTestFixtures.plan(context.lifecycle, 1, 101);

        context.pipeline
                .submit(
                        dataStorage.request(),
                        dataStorage::assignment,
                        context.coordinator.protocolHooks(
                                plan(context.fence, batch(9, 0, KafkaTransactionBatchKindV1.TRANSACTIONAL_DATA, -1))))
                .toCompletableFuture()
                .join();
        KafkaCoherentProtocolSnapshotV1 open = context.coordinator.capture();
        assertThat(open.firstUnstableOffset()).hasValue(100);
        assertThat(open.root().frontiers().readableEndOffset()).isEqualTo(101);

        context.pipeline
                .submit(
                        abortStorage.request(),
                        abortStorage::assignment,
                        context.coordinator.protocolHooks(
                                plan(context.fence, batch(9, 1, KafkaTransactionBatchKindV1.ABORT_MARKER, 6))))
                .toCompletableFuture()
                .join();
        KafkaCoherentProtocolSnapshotV1 aborted = context.coordinator.capture();
        assertThat(aborted.firstUnstableOffset()).hasValue(100);
        assertThat(aborted.transactionState().abortedTransactions())
                .singleElement()
                .satisfies(transaction -> {
                    assertThat(transaction.firstOffset()).isEqualTo(100);
                    assertThat(transaction.markerEndOffsetExclusive()).isEqualTo(102);
                });
        assertThat(aborted.root().frontiers().lastStableOffset()).isEqualTo(100);
        assertThat(aborted.root().frontiers().readableEndOffset()).isEqualTo(102);
    }

    @Test
    void fenceWinningAfterSpeculationPreventsLocatorProtocolAndLeoPublication() {
        Context context = context();
        context.session.delayedEntryId = 1;
        KafkaPipelineTestFixtures.Plan storage = KafkaPipelineTestFixtures.plan(context.lifecycle, 1, 100);
        CompletionStage<KafkaOrderedAppendResultV1> pending = context.pipeline.submit(
                storage.request(),
                storage::assignment,
                context.coordinator.protocolHooks(
                        plan(context.fence, batch(7, 0, KafkaTransactionBatchKindV1.NONE, -1))));
        var staged = context.coordinator.capture().root();

        assertThat(context.coordinator
                        .publicationCell()
                        .transition(new KafkaPartitionFenceTransitionV1(
                                staged.fence(),
                                staged.stateVersion(),
                                successorFence(staged.fence()),
                                staged.frontiers(),
                                staged.references()))
                        .published())
                .isTrue();
        context.session.completeDelayedAppend();

        assertThat(pending.toCompletableFuture().join().outcome())
                .isEqualTo(KafkaOrderedAppendOutcomeV1.OUTCOME_UNKNOWN);
        KafkaCoherentProtocolSnapshotV1 after = context.coordinator.capture();
        assertThat(after.root().frontiers()).isEqualTo(new KafkaPartitionFrontiersV1(100, 101, 100, 100, 100, 100));
        assertThat(after.activeTail().locators()).isEmpty();
        assertThat(after.committedProducerState().producers()).isEmpty();
        assertThat(after.leaderEpochIndex().startOffsets()).isEmpty();
    }

    @Test
    void publicationWinningBeforeFenceRemainsLegalOldEpochData() {
        Context context = context();
        KafkaPipelineTestFixtures.Plan storage = KafkaPipelineTestFixtures.plan(context.lifecycle, 1, 100);
        KafkaOrderedAppendResultV1 append = context.pipeline
                .submit(
                        storage.request(),
                        storage::assignment,
                        context.coordinator.protocolHooks(
                                plan(context.fence, batch(7, 0, KafkaTransactionBatchKindV1.NONE, -1))))
                .toCompletableFuture()
                .join();
        var committed = context.coordinator.capture().root();

        assertThat(context.coordinator
                        .publicationCell()
                        .transition(new KafkaPartitionFenceTransitionV1(
                                committed.fence(),
                                committed.stateVersion(),
                                successorFence(committed.fence()),
                                committed.frontiers(),
                                committed.references()))
                        .published())
                .isTrue();

        assertThat(append.outcome()).isEqualTo(KafkaOrderedAppendOutcomeV1.COMMITTED_ORDERED);
        KafkaCoherentProtocolSnapshotV1 after = context.coordinator.capture();
        assertThat(after.root().frontiers().readableEndOffset()).isEqualTo(101);
        assertThat(after.activeTail().locators()).hasSize(1);
        assertThat(after.committedProducerState().producers()).containsKey(7L);
    }

    @Test
    void notificationFailureCannotRollBackAnAlreadyPublishedCoherentRoot() {
        Context context = context(event -> {
            throw new IllegalStateException("waiter notification failed");
        });
        KafkaPipelineTestFixtures.Plan storage = KafkaPipelineTestFixtures.plan(context.lifecycle, 1, 100);

        KafkaOrderedAppendResultV1 result = context.pipeline
                .submit(
                        storage.request(),
                        storage::assignment,
                        context.coordinator.protocolHooks(
                                plan(context.fence, batch(7, 0, KafkaTransactionBatchKindV1.NONE, -1))))
                .toCompletableFuture()
                .join();

        assertThat(result.outcome()).isEqualTo(KafkaOrderedAppendOutcomeV1.COMMITTED_ORDERED);
        assertThat(context.coordinator.capture().root().frontiers().readableEndOffset())
                .isEqualTo(101);
        assertThat(context.coordinator.capture().activeTail().locators()).hasSize(1);
    }

    private static Context context() {
        List<KafkaPartitionPublicationEventV1> events = new ArrayList<>();
        return context(events::add, events);
    }

    private static Context context(KafkaPartitionPublicationObserver observer) {
        return context(observer, new ArrayList<>());
    }

    private static Context context(
            KafkaPartitionPublicationObserver observer, List<KafkaPartitionPublicationEventV1> events) {
        KafkaRunTestFixtures.FakeSession session = new KafkaRunTestFixtures.FakeSession();
        KafkaBookKeeperRunLifecycleV1 lifecycle =
                KafkaPipelineTestFixtures.lifecycle(session, new KafkaRunTestFixtures.FakeRootAuthority());
        KafkaPartitionFenceV1 fence = fence(lifecycle.snapshot().runBinding());
        KafkaCoherentCommitCoordinatorV1 coordinator = KafkaCoherentCommitCoordinatorV1.bootstrap(
                fence, 100, lifecycle.snapshot().handle(), observer);
        KafkaAppendCapacityControllerV1 partition =
                new KafkaAppendCapacityControllerV1(new KafkaAppendCapacityBudgetV1(5, 20, 100_000));
        KafkaAppendCapacityControllerV1 global =
                new KafkaAppendCapacityControllerV1(new KafkaAppendCapacityBudgetV1(5, 20, 100_000));
        KafkaBookKeeperOrderedPipelineV1 pipeline =
                new KafkaBookKeeperOrderedPipelineV1(session, lifecycle, partition, global, coordinator);
        return new Context(session, lifecycle, fence, coordinator, partition, events, pipeline);
    }

    private static KafkaPartitionFenceV1 fence(Nbke2RunBindingV1 binding) {
        return new KafkaPartitionFenceV1(
                binding.bindingId(),
                binding.topicIncarnation(),
                binding.partitionId(),
                13,
                binding.storageEpochId(),
                binding.creatorOwnerEpoch(),
                binding.kafkaLeaderEpoch());
    }

    private static KafkaPartitionFenceV1 successorFence(KafkaPartitionFenceV1 current) {
        return new KafkaPartitionFenceV1(
                current.bindingId(),
                current.topicIncarnation(),
                current.partitionId(),
                current.bindingGeneration(),
                current.storageEpochId(),
                current.ownerEpoch() + 1,
                current.kafkaLeaderEpoch() + 1);
    }

    private static KafkaProtocolAppendPlanV1 plan(KafkaPartitionFenceV1 fence, KafkaProtocolBatchDeltaV1... batches) {
        return new KafkaProtocolAppendPlanV1(fence, List.of(batches));
    }

    private static KafkaProtocolBatchDeltaV1 batch(
            long producerId, int sequence, KafkaTransactionBatchKindV1 kind, int coordinatorEpoch) {
        return new KafkaProtocolBatchDeltaV1(
                1,
                Optional.of(new KafkaBatchDuplicateIdentityV1(producerId, (short) 0, sequence, sequence)),
                kind,
                kind == KafkaTransactionBatchKindV1.NONE ? -1 : producerId,
                coordinatorEpoch);
    }

    private record Context(
            KafkaRunTestFixtures.FakeSession session,
            KafkaBookKeeperRunLifecycleV1 lifecycle,
            KafkaPartitionFenceV1 fence,
            KafkaCoherentCommitCoordinatorV1 coordinator,
            KafkaAppendCapacityControllerV1 partition,
            List<KafkaPartitionPublicationEventV1> events,
            KafkaBookKeeperOrderedPipelineV1 pipeline) {}
}
