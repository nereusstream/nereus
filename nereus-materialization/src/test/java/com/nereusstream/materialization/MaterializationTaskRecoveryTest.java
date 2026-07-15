/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import static com.nereusstream.materialization.MaterializationPlannerTestSupport.CLUSTER;
import static com.nereusstream.materialization.MaterializationPlannerTestSupport.STREAM;
import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.OffsetRange;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationMetadataStoreTestFactory;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import com.nereusstream.metadata.oxia.VersionedMaterializationTask;
import com.nereusstream.metadata.oxia.records.MaterializationTaskRecord;
import com.nereusstream.metadata.oxia.records.TaskFailureClass;
import com.nereusstream.metadata.oxia.records.TaskLifecycle;
import com.nereusstream.metadata.oxia.records.WorkerClaimRecord;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class MaterializationTaskRecoveryTest {
    @Test
    void expiresAClaimOnlyAfterClockSkewAndPersistsRetryWithoutCurrentConfigDependency() {
        List<VersionedGenerationCandidate> candidates = List.of(
                MaterializationPlannerTestSupport.zero("/index/z-2", 0, 2, 0, 100, 2),
                MaterializationPlannerTestSupport.zero("/index/z-4", 2, 4, 100, 100, 4));
        MaterializationPolicy policy = MaterializationPlannerTestSupport.policy();
        MaterializationTask task = MaterializationPlannerTestSupport.planner(
                        candidates, List.of(), 0, 4)
                .plan(STREAM, new OffsetRange(0, 4), policy, 1)
                .join()
                .get(0);
        GenerationMetadataStore durable = GenerationMetadataStoreTestFactory.inMemory(
                Clock.fixed(Instant.ofEpochMilli(400), ZoneOffset.UTC));
        MaterializationTaskStore taskStore = new MaterializationTaskStore(
                CLUSTER,
                MaterializationPlannerTestSupport.generationStore(candidates, List.of(), durable),
                Clock.fixed(Instant.ofEpochMilli(100), ZoneOffset.UTC));
        VersionedMaterializationTask planned = taskStore.create(task).join();
        MaterializationTaskRecord claimedRecord = claimed(planned.value(), 120, 300);
        VersionedMaterializationTask claimed = taskStore.compareAndSet(
                claimedRecord, planned.metadataVersion()).join();
        AtomicInteger dispatches = new AtomicInteger();
        AtomicInteger guardCalls = new AtomicInteger();
        MaterializationTaskRecovery recovery = new MaterializationTaskRecovery(
                taskStore,
                new GenerationPublicationReconciler((ignoredTask, ignoredOutput) ->
                        CompletableFuture.completedFuture(null)),
                (ignoredDurable, ignoredTask) -> {
                    dispatches.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                },
                Clock.fixed(Instant.ofEpochMilli(400), ZoneOffset.UTC),
                Duration.ofMillis(50),
                Duration.ofMillis(100));

        MaterializationTaskRecoveryAction action = recovery.recover(claimed, () -> {
            guardCalls.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }).join();

        assertThat(action).isEqualTo(MaterializationTaskRecoveryAction.EXPIRED_CLAIM_REQUEUED);
        assertThat(dispatches).hasValue(0);
        assertThat(guardCalls).hasValue(1);
        VersionedMaterializationTask retry = taskStore.get(STREAM, task.taskId()).join().orElseThrow();
        assertThat(retry.value().lifecycle()).isEqualTo(TaskLifecycle.RETRY_WAIT);
        assertThat(retry.value().failureClassId()).isEqualTo(TaskFailureClass.CLOSED.wireId());
        assertThat(retry.value().retryNotBeforeMillis()).isEqualTo(500);
        assertThat(taskStore.requireTask(retry)).isEqualTo(task);
        assertThat(retry.value().policy()).isEqualTo(MaterializationRecordMapper.policyRecord(policy));
    }

    @Test
    void reentersPublicationFromTheDurableOutputAndEmbeddedHistoricalPolicy() {
        try (GenerationPublicationTestSupport.Context context = GenerationPublicationTestSupport.context()) {
            MaterializationTaskStore taskStore = new MaterializationTaskStore(
                    GenerationPublicationTestSupport.CLUSTER,
                    context.generations(),
                    GenerationPublicationTestSupport.CLOCK);
            VersionedMaterializationTask ready = taskStore.get(
                            context.task().streamId(), context.task().taskId())
                    .join()
                    .orElseThrow();
            AtomicReference<MaterializationTask> recoveredTask = new AtomicReference<>();
            AtomicReference<MaterializationOutput> recoveredOutput = new AtomicReference<>();
            MaterializationTaskRecovery recovery = new MaterializationTaskRecovery(
                    taskStore,
                    new GenerationPublicationReconciler((task, output) -> {
                        recoveredTask.set(task);
                        recoveredOutput.set(output);
                        return CompletableFuture.completedFuture(null);
                    }),
                    (ignoredDurable, ignoredTask) -> CompletableFuture.failedFuture(
                            new AssertionError("output-ready task must not be worker-dispatched")),
                    GenerationPublicationTestSupport.CLOCK,
                    Duration.ZERO,
                    Duration.ofSeconds(1));

            assertThat(recovery.recover(ready).join())
                    .isEqualTo(MaterializationTaskRecoveryAction.PUBLICATION_RECONCILED);
            assertThat(recoveredTask.get()).isEqualTo(context.task());
            assertThat(recoveredOutput.get()).isEqualTo(context.output());
        }
    }

    private static MaterializationTaskRecord claimed(
            MaterializationTaskRecord source,
            long claimedAt,
            long expiresAt) {
        return new MaterializationTaskRecord(
                source.schemaVersion(), source.taskId(), source.taskSequence(), source.streamId(),
                source.readViewId(), source.taskKindId(), source.offsetStart(), source.offsetEnd(),
                source.sources(), source.sourceSetSha256(), source.policyId(), source.policyVersion(),
                source.policySha256(), source.policy(), TaskLifecycle.CLAIMED, 1,
                Optional.of(new WorkerClaimRecord(
                        "c".repeat(26), "d".repeat(26), 1, claimedAt, expiresAt)),
                Optional.empty(), OptionalLong.empty(), "", TaskFailureClass.NONE.wireId(), "", 0,
                source.createdAtMillis(), Math.max(source.updatedAtMillis(), claimedAt), 0);
    }
}
