/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import static com.nereusstream.materialization.MaterializationPlannerTestSupport.CLUSTER;
import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.core.capability.GenerationActivationProof;
import com.nereusstream.core.capability.GenerationOperation;
import com.nereusstream.core.capability.GenerationProtocolActivationGuard;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationMetadataStoreTestFactory;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class RegisteredMaterializationStreamScannerTest {
    @Test
    void scansAllShardsAndConvergesAnUnownedStreamIntoOneDurableTask() {
        List<VersionedGenerationCandidate> candidates = List.of(
                MaterializationPlannerTestSupport.zero("/index/z-2", 0, 2, 0, 100, 2),
                MaterializationPlannerTestSupport.zero("/index/z-4", 2, 4, 100, 100, 4));
        Clock clock = Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC);
        GenerationMetadataStore durable = GenerationMetadataStoreTestFactory.inMemory(clock);
        GenerationMetadataStore generations = MaterializationPlannerTestSupport.generationStore(
                candidates, List.of(), durable);
        MaterializationPolicy policy = MaterializationPlannerTestSupport.policy();
        MaterializationTaskStore taskStore = new MaterializationTaskStore(CLUSTER, generations, clock);
        DefaultMaterializationPlanner planner = new DefaultMaterializationPlanner(
                CLUSTER,
                MaterializationPlannerTestSupport.l0Store(
                        MaterializationPlannerTestSupport.snapshot(0, 4)),
                generations,
                2);
        AtomicInteger dispatches = new AtomicInteger();
        List<MaterializationTask> dispatched = new ArrayList<>();
        MaterializationTaskRecovery recovery = new MaterializationTaskRecovery(
                taskStore,
                new GenerationPublicationReconciler((ignoredTask, ignoredOutput) ->
                        CompletableFuture.completedFuture(null)),
                (ignoredDurable, task) -> {
                    dispatches.incrementAndGet();
                    dispatched.add(task);
                    return CompletableFuture.completedFuture(null);
                },
                clock,
                Duration.ZERO,
                Duration.ofSeconds(1));
        TaskRecoveryScanner recoveryScanner = new TaskRecoveryScanner(taskStore, recovery, 1);
        AtomicInteger readinessChecks = new AtomicInteger();
        AtomicInteger revalidations = new AtomicInteger();
        GenerationProtocolActivationGuard guard = countingGuard(readinessChecks, revalidations);
        RegisteredMaterializationStreamScanner scanner = new RegisteredMaterializationStreamScanner(
                CLUSTER,
                MaterializationPlannerTestSupport.l0Store(
                        MaterializationPlannerTestSupport.snapshot(0, 4)),
                generations,
                guard,
                planner,
                taskStore,
                recovery,
                recoveryScanner,
                policy,
                1,
                10);

        RegisteredMaterializationScanResult first = scanner.scanOnce().join();
        RegisteredMaterializationScanResult second = scanner.scanOnce().join();

        assertThat(first.shardsScanned()).isEqualTo(64);
        assertThat(first.registrationsScanned()).isEqualTo(1);
        assertThat(first.registrationsAdmitted()).isEqualTo(1);
        assertThat(first.registrationsSkipped()).isZero();
        assertThat(first.existingTasksRecovered()).isZero();
        assertThat(first.plannedTasksConverged()).isEqualTo(1);
        assertThat(second.shardsScanned()).isEqualTo(64);
        assertThat(second.existingTasksRecovered()).isEqualTo(1);
        assertThat(second.plannedTasksConverged()).isZero();
        assertThat(dispatches).hasValue(2);
        assertThat(dispatched).hasSize(2).allMatch(task -> task.equals(dispatched.get(0)));
        assertThat(readinessChecks).hasValue(2);
        assertThat(revalidations.get()).isGreaterThanOrEqualTo(3);
    }

    private static GenerationProtocolActivationGuard countingGuard(
            AtomicInteger readinessChecks,
            AtomicInteger revalidations) {
        GenerationProtocolActivationGuard delegate = GenerationPublicationTestSupport.successfulGuard();
        return new GenerationProtocolActivationGuard() {
            @Override
            public CompletableFuture<GenerationActivationProof> requireReady(
                    GenerationOperation operation,
                    com.nereusstream.core.capability.GenerationActivationSubject subject,
                    boolean activateLiveProjectionIfAbsent) {
                readinessChecks.incrementAndGet();
                return delegate.requireReady(operation, subject, activateLiveProjectionIfAbsent);
            }

            @Override
            public CompletableFuture<Void> revalidate(GenerationActivationProof proof) {
                revalidations.incrementAndGet();
                return delegate.revalidate(proof);
            }
        };
    }
}
