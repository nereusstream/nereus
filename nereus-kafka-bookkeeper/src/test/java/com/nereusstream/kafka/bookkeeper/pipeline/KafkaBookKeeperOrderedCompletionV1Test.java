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
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.kafka.bookkeeper.run.KafkaBookKeeperRunLifecycleV1;
import com.nereusstream.kafka.bookkeeper.run.KafkaRunTestFixtures;
import com.nereusstream.storage.api.bookkeeper.AppendQuorumProofV1;
import com.nereusstream.storage.api.bookkeeper.ProviderMutationResultV1;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

class KafkaBookKeeperOrderedCompletionV1Test {
    @Test
    void bDurableBeforeAWaitsAndThenCommitsInAdmissionOrder() {
        KafkaBookKeeperPipelineAdmissionV1Test.Context context =
                KafkaBookKeeperPipelineAdmissionV1Test.context(3, 10, 20_000);
        context.session().delayedEntryId = 1;
        KafkaPipelineTestFixtures.Plan a = KafkaPipelineTestFixtures.plan(context.lifecycle(), 1, 100);
        KafkaPipelineTestFixtures.Plan b = KafkaPipelineTestFixtures.plan(context.lifecycle(), 1, 101);

        CompletionStage<KafkaOrderedAppendResultV1> aResult = context.pipeline().submit(a.request(), a::assignment);
        CompletionStage<KafkaOrderedAppendResultV1> bResult = context.pipeline().submit(b.request(), b::assignment);

        assertThat(aResult.toCompletableFuture()).isNotDone();
        assertThat(bResult.toCompletableFuture()).isNotDone();
        assertThat(context.commits()).isEmpty();
        context.session().completeDelayedAppend();
        assertThat(aResult.toCompletableFuture().join().outcome())
                .isEqualTo(KafkaOrderedAppendOutcomeV1.COMMITTED_ORDERED);
        assertThat(bResult.toCompletableFuture().join().outcome())
                .isEqualTo(KafkaOrderedAppendOutcomeV1.COMMITTED_ORDERED);
        assertThat(context.commits()).containsExactly("100:101", "101:102");
    }

    @Test
    void definitiveAFailureFencesAlreadyDurableB() {
        KafkaBookKeeperPipelineAdmissionV1Test.Context context =
                KafkaBookKeeperPipelineAdmissionV1Test.context(3, 10, 20_000);
        context.session().delayedEntryId = 1;
        context.session().nextAppendOverride = ProviderMutationResultV1.definitelyNotApplied();
        KafkaPipelineTestFixtures.Plan a = KafkaPipelineTestFixtures.plan(context.lifecycle(), 1, 100);
        KafkaPipelineTestFixtures.Plan b = KafkaPipelineTestFixtures.plan(context.lifecycle(), 1, 101);
        CompletionStage<KafkaOrderedAppendResultV1> aResult = context.pipeline().submit(a.request(), a::assignment);
        CompletionStage<KafkaOrderedAppendResultV1> bResult = context.pipeline().submit(b.request(), b::assignment);

        context.session().completeDelayedAppend();

        assertThat(aResult.toCompletableFuture().join().outcome())
                .isEqualTo(KafkaOrderedAppendOutcomeV1.DEFINITIVELY_FAILED);
        assertThat(bResult.toCompletableFuture().join().outcome())
                .isEqualTo(KafkaOrderedAppendOutcomeV1.FENCED_BY_PREDECESSOR);
        assertThat(context.commits()).isEmpty();
    }

    @Test
    void unknownAFailureFencesAlreadyDurableB() {
        KafkaBookKeeperPipelineAdmissionV1Test.Context context =
                KafkaBookKeeperPipelineAdmissionV1Test.context(3, 10, 20_000);
        context.session().delayedEntryId = 1;
        context.session().nextAppendOverride = ProviderMutationResultV1.outcomeUnknown();
        KafkaPipelineTestFixtures.Plan a = KafkaPipelineTestFixtures.plan(context.lifecycle(), 1, 100);
        KafkaPipelineTestFixtures.Plan b = KafkaPipelineTestFixtures.plan(context.lifecycle(), 1, 101);
        CompletionStage<KafkaOrderedAppendResultV1> aResult = context.pipeline().submit(a.request(), a::assignment);
        CompletionStage<KafkaOrderedAppendResultV1> bResult = context.pipeline().submit(b.request(), b::assignment);

        context.session().completeDelayedAppend();

        assertThat(aResult.toCompletableFuture().join().outcome())
                .isEqualTo(KafkaOrderedAppendOutcomeV1.OUTCOME_UNKNOWN);
        assertThat(bResult.toCompletableFuture().join().outcome())
                .isEqualTo(KafkaOrderedAppendOutcomeV1.FENCED_BY_PREDECESSOR);
        assertThat(context.pipeline().fenced()).isTrue();
    }

    @Test
    void substitutedQuorumProofIsOutcomeUnknownAndNeverCommits() {
        KafkaBookKeeperPipelineAdmissionV1Test.Context context =
                KafkaBookKeeperPipelineAdmissionV1Test.context(2, 10, 20_000);
        KafkaPipelineTestFixtures.Plan a = KafkaPipelineTestFixtures.plan(context.lifecycle(), 1, 100);
        context.session().nextAppendOverride = ProviderMutationResultV1.appliedExact(new AppendQuorumProofV1(
                context.lifecycle().snapshot().handle(),
                99,
                1,
                Sha256Digest.hash(CanonicalBytes.copyOf(new byte[] {1})),
                2));

        KafkaOrderedAppendResultV1 result = context.pipeline()
                .submit(a.request(), a::assignment)
                .toCompletableFuture()
                .join();

        assertThat(result.outcome()).isEqualTo(KafkaOrderedAppendOutcomeV1.OUTCOME_UNKNOWN);
        assertThat(context.commits()).isEmpty();
        assertThat(context.pipeline().committedEndOffset()).isEqualTo(100);
    }

    @Test
    void orderedObserverFailureFencesWithoutReportingCommit() {
        KafkaRunTestFixtures.FakeSession session = new KafkaRunTestFixtures.FakeSession();
        KafkaBookKeeperRunLifecycleV1 lifecycle =
                KafkaPipelineTestFixtures.lifecycle(session, new KafkaRunTestFixtures.FakeRootAuthority());
        KafkaAppendCapacityControllerV1 partition = controller();
        KafkaAppendCapacityControllerV1 global = controller();
        KafkaBookKeeperOrderedPipelineV1 pipeline =
                new KafkaBookKeeperOrderedPipelineV1(session, lifecycle, partition, global, commit -> {
                    throw new IllegalStateException("publication seam failed");
                });
        KafkaPipelineTestFixtures.Plan plan = KafkaPipelineTestFixtures.plan(lifecycle, 1, 100);

        KafkaOrderedAppendResultV1 result = pipeline.submit(plan.request(), plan::assignment)
                .toCompletableFuture()
                .join();

        assertThat(result.outcome()).isEqualTo(KafkaOrderedAppendOutcomeV1.OUTCOME_UNKNOWN);
        assertThat(pipeline.fenced()).isTrue();
        assertThat(pipeline.committedEndOffset()).isEqualTo(100);
    }

    @Test
    void fencedPendingSuccessorRetainsCapacityUntilItsOwnProviderTerminal() {
        KafkaBookKeeperPipelineAdmissionV1Test.Context context =
                KafkaBookKeeperPipelineAdmissionV1Test.context(3, 10, 20_000);
        context.session().delayedEntryIds.addAll(java.util.Set.of(1L, 2L));
        context.session().nextAppendOverride = ProviderMutationResultV1.definitelyNotApplied();
        KafkaPipelineTestFixtures.Plan a = KafkaPipelineTestFixtures.plan(context.lifecycle(), 1, 100);
        KafkaPipelineTestFixtures.Plan b = KafkaPipelineTestFixtures.plan(context.lifecycle(), 1, 101);
        CompletionStage<KafkaOrderedAppendResultV1> aResult = context.pipeline().submit(a.request(), a::assignment);
        CompletionStage<KafkaOrderedAppendResultV1> bResult = context.pipeline().submit(b.request(), b::assignment);

        assertThat(context.partition().snapshot().groups()).isEqualTo(2);
        assertThat(context.global().snapshot().groups()).isEqualTo(2);
        context.session().completeDelayedEntry(1);
        assertThat(aResult.toCompletableFuture().join().outcome())
                .isEqualTo(KafkaOrderedAppendOutcomeV1.DEFINITIVELY_FAILED);
        assertThat(bResult.toCompletableFuture()).isNotDone();
        assertThat(context.partition().snapshot().groups()).isEqualTo(1);
        assertThat(context.global().snapshot().groups()).isEqualTo(1);
        context.session().completeDelayedEntry(2);
        assertThat(bResult.toCompletableFuture().join().outcome())
                .isEqualTo(KafkaOrderedAppendOutcomeV1.FENCED_BY_PREDECESSOR);

        assertThat(context.partition().snapshot()).isEqualTo(new KafkaAppendCapacitySnapshotV1(0, 0, 0));
        assertThat(context.global().snapshot()).isEqualTo(new KafkaAppendCapacitySnapshotV1(0, 0, 0));
    }

    private static KafkaAppendCapacityControllerV1 controller() {
        return new KafkaAppendCapacityControllerV1(new KafkaAppendCapacityBudgetV1(2, 10, 20_000));
    }
}
