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
import com.nereusstream.kafka.bookkeeper.run.KafkaBookKeeperRunLifecycleV1;
import com.nereusstream.kafka.bookkeeper.run.KafkaRunTestFixtures;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class KafkaBookKeeperPipelineAdmissionV1Test {
    @Test
    void reservesPartitionAndGlobalCapacityBeforeInvokingOffsetAssignment() {
        Context context = context(2, 10, 10_000);
        KafkaPipelineTestFixtures.Plan plan = KafkaPipelineTestFixtures.plan(context.lifecycle, 1, 100);
        AtomicBoolean sawReservations = new AtomicBoolean();

        KafkaOrderedAppendResultV1 result = context.pipeline
                .submit(plan.request(), () -> {
                    sawReservations.set(context.partition.snapshot().groups() == 1
                            && context.global.snapshot().groups() == 1);
                    return plan.assignment();
                })
                .toCompletableFuture()
                .join();

        assertThat(sawReservations).isTrue();
        assertThat(result.outcome()).isEqualTo(KafkaOrderedAppendOutcomeV1.COMMITTED_ORDERED);
    }

    @Test
    void capacityRejectionNeverInvokesOffsetAssignment() {
        Context context = context(1, 1, 1);
        KafkaPipelineTestFixtures.Plan plan = KafkaPipelineTestFixtures.plan(context.lifecycle, 1, 100);
        AtomicBoolean invoked = new AtomicBoolean();

        KafkaOrderedAppendResultV1 result = context.pipeline
                .submit(plan.request(), () -> {
                    invoked.set(true);
                    return plan.assignment();
                })
                .toCompletableFuture()
                .join();

        assertThat(invoked).isFalse();
        assertThat(result.outcome()).isEqualTo(KafkaOrderedAppendOutcomeV1.CAPACITY_REJECTED);
        assertThat(result.startOffset()).isEmpty();
    }

    @Test
    void offsetAssignmentFailureReleasesBothScopesWithoutAdvancingSpeculation() {
        Context context = context(2, 10, 10_000);
        KafkaPipelineTestFixtures.Plan plan = KafkaPipelineTestFixtures.plan(context.lifecycle, 1, 100);

        KafkaOrderedAppendResultV1 result = context.pipeline
                .submit(plan.request(), () -> {
                    throw new IllegalStateException("native assignment failed");
                })
                .toCompletableFuture()
                .join();

        assertThat(result.outcome()).isEqualTo(KafkaOrderedAppendOutcomeV1.OFFSET_ASSIGNMENT_FAILED);
        assertThat(context.pipeline.speculativeEndOffset()).isEqualTo(100);
        assertThat(context.partition.snapshot().groups()).isZero();
        assertThat(context.global.snapshot().groups()).isZero();
    }

    @Test
    void validMultiMemberAppendSubmitsContiguousEntriesAndReleasesCapacityAfterOrder() {
        Context context = context(2, 10, 10_000);
        KafkaPipelineTestFixtures.Plan plan = KafkaPipelineTestFixtures.plan(context.lifecycle, 2, 100);

        KafkaOrderedAppendResultV1 result = context.pipeline
                .submit(plan.request(), plan::assignment)
                .toCompletableFuture()
                .join();

        assertThat(result.outcome()).isEqualTo(KafkaOrderedAppendOutcomeV1.COMMITTED_ORDERED);
        assertThat(context.session.entries).containsKeys(0L, 1L, 2L);
        assertThat(context.commits).containsExactly("100:102");
        assertThat(context.pipeline.committedEndOffset()).isEqualTo(102);
        assertThat(context.partition.snapshot().groups()).isZero();
        assertThat(context.global.snapshot().groups()).isZero();
    }

    @Test
    void encodedByteSubstitutionFencesBeforeAnyDataSubmission() {
        Context context = context(2, 10, 10_000);
        KafkaPipelineTestFixtures.Plan plan = KafkaPipelineTestFixtures.plan(context.lifecycle, 1, 100);
        KafkaAppendAdmissionRequestV1 wrong =
                new KafkaAppendAdmissionRequestV1(1, plan.request().encodedDataBytes() + 1);

        KafkaOrderedAppendResultV1 result = context.pipeline
                .submit(wrong, plan::assignment)
                .toCompletableFuture()
                .join();

        assertThat(result.outcome()).isEqualTo(KafkaOrderedAppendOutcomeV1.INVALID_ASSIGNMENT);
        assertThat(context.pipeline.fenced()).isTrue();
        assertThat(context.session.entries).containsOnlyKeys(0L);
        assertThat(context.partition.snapshot().groups()).isZero();
    }

    @Test
    void nonContiguousNativeAssignmentFencesThePipeline() {
        Context context = context(2, 10, 10_000);
        KafkaPipelineTestFixtures.Plan wrong = KafkaPipelineTestFixtures.plan(context.lifecycle, 1, 101);

        KafkaOrderedAppendResultV1 result = context.pipeline
                .submit(wrong.request(), wrong::assignment)
                .toCompletableFuture()
                .join();

        assertThat(result.outcome()).isEqualTo(KafkaOrderedAppendOutcomeV1.INVALID_ASSIGNMENT);
        assertThat(context.pipeline.fenced()).isTrue();
        assertThat(context.pipeline.committedEndOffset()).isEqualTo(100);
    }

    @Test
    void globalScopeCanRejectAfterPartitionReservationWithoutLeakingEitherPermit() {
        KafkaRunTestFixtures.FakeSession session = new KafkaRunTestFixtures.FakeSession();
        KafkaBookKeeperRunLifecycleV1 lifecycle =
                KafkaPipelineTestFixtures.lifecycle(session, new KafkaRunTestFixtures.FakeRootAuthority());
        KafkaAppendCapacityControllerV1 partition = controller(2, 10, 10_000);
        KafkaAppendCapacityControllerV1 global = controller(1, 1, 1);
        KafkaBookKeeperOrderedPipelineV1 pipeline =
                new KafkaBookKeeperOrderedPipelineV1(session, lifecycle, partition, global, (start, end) -> {});
        KafkaPipelineTestFixtures.Plan plan = KafkaPipelineTestFixtures.plan(lifecycle, 1, 100);

        KafkaOrderedAppendResultV1 result = pipeline.submit(plan.request(), plan::assignment)
                .toCompletableFuture()
                .join();

        assertThat(result.outcome()).isEqualTo(KafkaOrderedAppendOutcomeV1.CAPACITY_REJECTED);
        assertThat(partition.snapshot().groups()).isZero();
        assertThat(global.snapshot().groups()).isZero();
    }

    @Test
    void concurrentSubmitCannotInsertAContiguousSuccessorAheadOfItsPredecessor() throws Exception {
        Context context = context(3, 10, 20_000);
        KafkaPipelineTestFixtures.Plan a = KafkaPipelineTestFixtures.plan(context.lifecycle, 1, 100);
        KafkaPipelineTestFixtures.Plan b = KafkaPipelineTestFixtures.plan(context.lifecycle, 1, 101);
        CountDownLatch aFactoryEntered = new CountDownLatch(1);
        CountDownLatch releaseAFactory = new CountDownLatch(1);
        CountDownLatch bCallStarted = new CountDownLatch(1);
        CountDownLatch bOffsetAssignmentEntered = new CountDownLatch(1);

        CompletableFuture<java.util.concurrent.CompletionStage<KafkaOrderedAppendResultV1>> aCall =
                CompletableFuture.supplyAsync(() -> context.pipeline.submit(a.request(), () -> {
                    KafkaOffsetAssignedAppendV1 assigned = a.assignment();
                    return new KafkaOffsetAssignedAppendV1(
                            assigned.startOffset(), assigned.endOffsetExclusive(), firstEntryId -> {
                                aFactoryEntered.countDown();
                                awaitUnchecked(releaseAFactory);
                                return assigned.physicalGroupFactory().apply(firstEntryId);
                            });
                }));

        assertThat(aFactoryEntered.await(1, TimeUnit.SECONDS)).isTrue();
        CompletableFuture<java.util.concurrent.CompletionStage<KafkaOrderedAppendResultV1>> bCall =
                CompletableFuture.supplyAsync(() -> {
                    bCallStarted.countDown();
                    return context.pipeline.submit(b.request(), () -> {
                        bOffsetAssignmentEntered.countDown();
                        return b.assignment();
                    });
                });
        try {
            assertThat(bCallStarted.await(1, TimeUnit.SECONDS)).isTrue();
            assertThat(bOffsetAssignmentEntered.await(200, TimeUnit.MILLISECONDS))
                    .isFalse();
        } finally {
            releaseAFactory.countDown();
        }

        assertThat(aCall.join().toCompletableFuture().join().outcome())
                .isEqualTo(KafkaOrderedAppendOutcomeV1.COMMITTED_ORDERED);
        assertThat(bCall.join().toCompletableFuture().join().outcome())
                .isEqualTo(KafkaOrderedAppendOutcomeV1.COMMITTED_ORDERED);
        assertThat(context.commits).containsExactly("100:101", "101:102");
    }

    static Context context(long groups, long entries, long bytes) {
        KafkaRunTestFixtures.FakeSession session = new KafkaRunTestFixtures.FakeSession();
        KafkaBookKeeperRunLifecycleV1 lifecycle =
                KafkaPipelineTestFixtures.lifecycle(session, new KafkaRunTestFixtures.FakeRootAuthority());
        KafkaAppendCapacityControllerV1 partition = controller(groups, entries, bytes);
        KafkaAppendCapacityControllerV1 global = controller(groups, entries, bytes);
        List<String> commits = new ArrayList<>();
        KafkaBookKeeperOrderedPipelineV1 pipeline = new KafkaBookKeeperOrderedPipelineV1(
                session, lifecycle, partition, global, (start, end) -> commits.add(start + ":" + end));
        return new Context(session, lifecycle, partition, global, commits, pipeline);
    }

    private static KafkaAppendCapacityControllerV1 controller(long groups, long entries, long bytes) {
        return new KafkaAppendCapacityControllerV1(new KafkaAppendCapacityBudgetV1(groups, entries, bytes));
    }

    private static void awaitUnchecked(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while holding the test assignment cut", failure);
        }
    }

    record Context(
            KafkaRunTestFixtures.FakeSession session,
            KafkaBookKeeperRunLifecycleV1 lifecycle,
            KafkaAppendCapacityControllerV1 partition,
            KafkaAppendCapacityControllerV1 global,
            List<String> commits,
            KafkaBookKeeperOrderedPipelineV1 pipeline) {}
}
