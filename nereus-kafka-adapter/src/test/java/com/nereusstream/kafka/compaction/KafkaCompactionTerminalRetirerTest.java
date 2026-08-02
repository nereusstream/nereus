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

package com.nereusstream.kafka.compaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.kafka.compaction.KafkaCompactionPlanCodecV1Test.Fixture;
import com.nereusstream.materialization.KafkaCompactionTaskTestSupport;
import com.nereusstream.materialization.MaterializationTask;
import com.nereusstream.metadata.oxia.KafkaCompactionPlanMetadataStore;
import com.nereusstream.metadata.oxia.KafkaCompactionPlanScanPage;
import com.nereusstream.metadata.oxia.KafkaCompactionPlanScanToken;
import com.nereusstream.metadata.oxia.KafkaPartitionId;
import com.nereusstream.metadata.oxia.VersionedKafkaCompactionPlan;
import com.nereusstream.metadata.oxia.VersionedMaterializationTask;
import com.nereusstream.metadata.oxia.records.KafkaCompactionPlanRecord;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class KafkaCompactionTerminalRetirerTest {
    @Test
    void deletesTheTerminalTaskBeforeItsExactPlan() {
        Context context = context();
        AtomicInteger guardCalls = new AtomicInteger();

        KafkaCompactionTerminalRetirer.RetirementResult result = context.retirer()
                .retire(context.partition(), context.taskStore().current, context.planStore().current, () -> {
                    context.events().add("guard");
                    guardCalls.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                })
                .join();

        assertThat(result).isEqualTo(new KafkaCompactionTerminalRetirer.RetirementResult(2, true, true, 1, 1));
        assertThat(guardCalls).hasValue(3);
        assertThat(context.events())
                .containsSubsequence(
                        "task-get",
                        "source-protection-release",
                        "guard",
                        "task-delete",
                        "guard",
                        "task-get",
                        "plan-get",
                        "plan-delete");
        assertThat(context.taskStore().current).isNull();
        assertThat(context.planStore().current).isNull();
    }

    @Test
    void responseLossAfterTaskDeleteConvergesWithoutRepeatingTheDelete() {
        Context context = context();
        context.taskStore().loseDeleteResponse = true;

        KafkaCompactionTerminalRetirer.RetirementResult result = retire(context);

        assertThat(result.taskDeleteAttempts()).isEqualTo(1);
        assertThat(context.taskStore().deleteCalls).isEqualTo(1);
        assertThat(result.planDeleteAttempts()).isEqualTo(1);
        assertThat(context.planStore().current).isNull();
    }

    @Test
    void planOnlyRecoveryDoesNotRequireTheAlreadyDeletedTasksReleaseAuthority() {
        Context context = context();
        VersionedMaterializationTask deletedTask = context.taskStore().current;
        context.taskStore().current = null;

        KafkaCompactionTerminalRetirer.RetirementResult result = context.retirer()
                .retire(
                        context.partition(),
                        deletedTask,
                        context.planStore().current,
                        () -> CompletableFuture.completedFuture(null))
                .join();

        assertThat(result).isEqualTo(new KafkaCompactionTerminalRetirer.RetirementResult(0, false, true, 0, 1));
        assertThat(context.events()).doesNotContain("source-protection-release", "task-delete");
        assertThat(context.planStore().current).isNull();
    }

    @Test
    void responseLossAfterPlanDeleteConvergesByExactReload() {
        Context context = context();
        context.planStore().loseDeleteResponse = true;

        KafkaCompactionTerminalRetirer.RetirementResult result = retire(context);

        assertThat(result.planDeleteAttempts()).isEqualTo(1);
        assertThat(context.planStore().deleteCalls).isEqualTo(1);
        assertThat(context.taskStore().current).isNull();
        assertThat(context.planStore().current).isNull();
    }

    @Test
    void rejectsNonTerminalTaskWithoutDeletingEitherRoot() {
        Context context = context();
        context.taskStore().current =
                KafkaCompactionTaskTestSupport.planned(context.fixture().outputTask(), 2);

        assertThatThrownBy(() -> retire(context))
                .isInstanceOf(CompletionException.class)
                .hasRootCauseMessage("Kafka compaction task is not terminal");
        assertThat(context.taskStore().deleteCalls).isZero();
        assertThat(context.planStore().deleteCalls).isZero();
    }

    @Test
    void failsClosedWhenTaskFactsChangeAfterAnUncertainDelete() {
        Context context = context();
        context.taskStore().replaceOnDeleteFailure = true;

        assertThatThrownBy(() -> retire(context))
                .isInstanceOf(CompletionException.class)
                .hasMessageContaining("Kafka compaction terminal task changed after uncertain delete");
        assertThat(context.planStore().deleteCalls).isZero();
        assertThat(context.planStore().current).isNotNull();
    }

    @Test
    void rejectsATaskRootThatChangedBeforeTheFirstConditionalDelete() {
        Context context = context();
        VersionedMaterializationTask expected = context.taskStore().current;
        context.taskStore().current =
                KafkaCompactionTaskTestSupport.cancelled(context.fixture().outputTask(), 2);

        assertThatThrownBy(() -> context.retirer()
                        .retire(
                                context.partition(),
                                expected,
                                context.planStore().current,
                                () -> CompletableFuture.completedFuture(null))
                        .join())
                .hasRootCauseMessage("Kafka compaction terminal task changed before retirement");
        assertThat(context.taskStore().deleteCalls).isZero();
        assertThat(context.planStore().deleteCalls).isZero();
    }

    @Test
    void rejectsAPlanFromAnotherPartitionBeforeMetadataIo() {
        Context context = context();
        KafkaPartitionId other = new KafkaPartitionId(
                context.partition().kafkaClusterId(),
                topicId(99),
                context.partition().partitionId());

        assertThatThrownBy(() -> context.retirer()
                        .retire(
                                other,
                                context.taskStore().current,
                                context.planStore().current,
                                () -> CompletableFuture.completedFuture(null))
                        .join())
                .hasRootCauseMessage("expected Kafka compaction plan does not match its partition/version wrapper");
        assertThat(context.events()).isEmpty();
    }

    private static KafkaCompactionTerminalRetirer.RetirementResult retire(Context context) {
        return context.retirer()
                .retire(
                        context.partition(),
                        context.taskStore().current,
                        context.planStore().current,
                        () -> CompletableFuture.completedFuture(null))
                .join();
    }

    private static Context context() {
        Fixture fixture = KafkaCompactionPlanCodecV1Test.fixture("UNCOMPRESSED");
        KafkaPartitionId partition = new KafkaPartitionId("kraft", topicId(31), 1);
        ArrayList<String> events = new ArrayList<>();
        FakePlanStore plans = new FakePlanStore(events, partition, fixture.plan());
        FakeTaskStore tasks = new FakeTaskStore(
                events, fixture.outputTask(), KafkaCompactionTaskTestSupport.cancelled(fixture.outputTask(), 1));
        KafkaCompactionTerminalRetirer retirer = new KafkaCompactionTerminalRetirer(
                plans,
                tasks,
                (terminalTask, guard) -> {
                    events.add("source-protection-release");
                    return guard.revalidate().thenApply(ignored -> 2);
                },
                new KafkaCompactionPlanRecordMapper());
        return new Context(fixture, partition, events, plans, tasks, retirer);
    }

    private static String topicId(int value) {
        byte[] bytes = new byte[16];
        bytes[15] = (byte) value;
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    private static final class FakeTaskStore implements KafkaCompactionTerminalRetirer.TaskRoots {
        private final List<String> events;
        private final MaterializationTask expectedTask;
        private VersionedMaterializationTask current;
        private int deleteCalls;
        private boolean loseDeleteResponse;
        private boolean replaceOnDeleteFailure;

        private FakeTaskStore(
                List<String> events, MaterializationTask expectedTask, VersionedMaterializationTask current) {
            this.events = events;
            this.expectedTask = expectedTask;
            this.current = current;
        }

        @Override
        public CompletableFuture<Optional<VersionedMaterializationTask>> get(
                com.nereusstream.api.StreamId streamId, String taskId) {
            events.add("task-get");
            if (!streamId.equals(expectedTask.streamId()) || !taskId.equals(expectedTask.taskId())) {
                return CompletableFuture.failedFuture(new AssertionError("wrong task lookup"));
            }
            return CompletableFuture.completedFuture(Optional.ofNullable(current));
        }

        @Override
        public CompletableFuture<Void> delete(VersionedMaterializationTask expected) {
            events.add("task-delete");
            deleteCalls++;
            if (!expected.equals(current)) {
                return CompletableFuture.failedFuture(new IllegalStateException("stale task"));
            }
            if (replaceOnDeleteFailure) {
                current = KafkaCompactionTaskTestSupport.cancelled(expectedTask, 2);
                return CompletableFuture.failedFuture(new IllegalStateException("uncertain task delete"));
            }
            current = null;
            return loseDeleteResponse
                    ? CompletableFuture.failedFuture(new IllegalStateException("lost task delete response"))
                    : CompletableFuture.completedFuture(null);
        }

        @Override
        public MaterializationTask requireTask(VersionedMaterializationTask durable) {
            return expectedTask;
        }
    }

    private static final class FakePlanStore implements KafkaCompactionPlanMetadataStore {
        private final List<String> events;
        private final KafkaPartitionId partition;
        private VersionedKafkaCompactionPlan current;
        private int deleteCalls;
        private boolean loseDeleteResponse;

        private FakePlanStore(List<String> events, KafkaPartitionId partition, KafkaCompactionPlan plan) {
            this.events = events;
            this.partition = partition;
            KafkaCompactionPlanRecord value = new KafkaCompactionPlanRecordMapper().toRecord(partition, plan, 1_000);
            current = new VersionedKafkaCompactionPlan(
                    "plans/" + value.materializationTaskId(),
                    value.withMetadataVersion(3),
                    3,
                    new Checksum(ChecksumType.SHA256, "d".repeat(64)));
        }

        @Override
        public CompletableFuture<Optional<VersionedKafkaCompactionPlan>> getCompactionPlan(
                KafkaPartitionId id, String materializationTaskId) {
            events.add("plan-get");
            if (!partition.equals(id)
                    || (current != null
                            && !current.value().materializationTaskId().equals(materializationTaskId))) {
                return CompletableFuture.failedFuture(new AssertionError("wrong plan lookup"));
            }
            return CompletableFuture.completedFuture(Optional.ofNullable(current));
        }

        @Override
        public CompletableFuture<VersionedKafkaCompactionPlan> putCompactionPlanIfAbsent(
                KafkaCompactionPlanRecord value) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }

        @Override
        public CompletableFuture<KafkaCompactionPlanScanPage> scanCompactionPlans(
                KafkaPartitionId id, Optional<KafkaCompactionPlanScanToken> continuation, int limit) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }

        @Override
        public CompletableFuture<Void> deleteCompactionPlan(VersionedKafkaCompactionPlan expected) {
            events.add("plan-delete");
            deleteCalls++;
            if (!expected.equals(current)) {
                return CompletableFuture.failedFuture(new IllegalStateException("stale plan"));
            }
            current = null;
            return loseDeleteResponse
                    ? CompletableFuture.failedFuture(new IllegalStateException("lost plan delete response"))
                    : CompletableFuture.completedFuture(null);
        }
    }

    private record Context(
            Fixture fixture,
            KafkaPartitionId partition,
            ArrayList<String> events,
            FakePlanStore planStore,
            FakeTaskStore taskStore,
            KafkaCompactionTerminalRetirer retirer) {}
}
