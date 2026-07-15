/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import static com.nereusstream.materialization.MaterializationPlannerTestSupport.CLUSTER;
import static com.nereusstream.materialization.MaterializationPlannerTestSupport.STREAM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationMetadataStoreTestFactory;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import com.nereusstream.metadata.oxia.records.MaterializationCheckpointRecord;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class MaterializationCheckpointReconcilerTest {
    private static final Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC);

    @Test
    void advancesOnlyAcrossContiguousHealthyCurrentPolicyGenerations() {
        MaterializationPolicy policy = MaterializationPlannerTestSupport.policy();
        List<VersionedGenerationCandidate> candidates = List.of(
                MaterializationPlannerTestSupport.higher(
                        "/index/current-2", 0, 2, 1, 0, 100, 2,
                        policy.digestSha256(), policy.targetPhysicalFormat()),
                MaterializationPlannerTestSupport.higher(
                        "/index/current-4", 2, 4, 2, 100, 100, 4,
                        policy.digestSha256(), policy.targetPhysicalFormat()),
                MaterializationPlannerTestSupport.higher(
                        "/index/old-policy-6", 4, 6, 3, 200, 100, 6,
                        MaterializationPlannerTestSupport.sha('b'), policy.targetPhysicalFormat()));
        GenerationMetadataStore durable = GenerationMetadataStoreTestFactory.inMemory(CLOCK);
        GenerationMetadataStore generations = MaterializationPlannerTestSupport.generationStore(
                candidates, List.of(), durable);
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        AtomicInteger guardCalls = new AtomicInteger();
        try {
            DefaultMaterializationCheckpointReconciler reconciler = reconciler(
                    generations, 0, 6, scheduler);

            var first = reconciler.reconcile(STREAM, policy, () -> {
                guardCalls.incrementAndGet();
                return CompletableFuture.completedFuture(null);
            }).join();
            var replay = reconciler.reconcile(
                    STREAM, policy, MaterializationTaskMutationGuard.noOp()).join();

            assertThat(first.value().contiguousCoveredOffset()).isEqualTo(4);
            assertThat(first.value().observedCommitVersion()).isEqualTo(4);
            assertThat(first.value().lastTaskSequence()).isEqualTo(4);
            assertThat(first.value().lastTaskId()).isEqualTo(
                    ((com.nereusstream.metadata.oxia.VersionedGenerationIndex) candidates.get(1))
                            .value().taskId());
            assertThat(replay).isEqualTo(first);
            assertThat(guardCalls).hasValue(2);
        } finally {
            scheduler.shutdownNow();
            generations.close();
        }
    }

    @Test
    void reloadsTheExactCheckpointAfterALostCommittedCasResponse() {
        MaterializationPolicy policy = MaterializationPlannerTestSupport.policy();
        List<VersionedGenerationCandidate> candidates = List.of(
                MaterializationPlannerTestSupport.higher(
                        "/index/lost-2", 0, 2, 1, 0, 100, 2,
                        policy.digestSha256(), policy.targetPhysicalFormat()));
        GenerationMetadataStore durable = GenerationMetadataStoreTestFactory.inMemory(CLOCK);
        GenerationMetadataStore base = MaterializationPlannerTestSupport.generationStore(
                candidates, List.of(), durable);
        GenerationMetadataStore lossy = loseFirstCheckpointCasResponse(base);
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            var checkpoint = reconciler(lossy, 0, 2, scheduler)
                    .reconcile(STREAM, policy, MaterializationTaskMutationGuard.noOp())
                    .join();

            assertThat(checkpoint.value().contiguousCoveredOffset()).isEqualTo(2);
            assertThat(durable.getMaterializationCheckpoint(
                            CLUSTER, STREAM, policy.policyId(), policy.policyVersion())
                    .join()).contains(checkpoint);
        } finally {
            scheduler.shutdownNow();
            lossy.close();
        }
    }

    @Test
    void rejectsAnAheadCheckpointInsteadOfAllowingItToHideAGap() {
        MaterializationPolicy policy = MaterializationPlannerTestSupport.policy();
        List<VersionedGenerationCandidate> candidates = List.of(
                MaterializationPlannerTestSupport.higher(
                        "/index/gap-2", 0, 2, 1, 0, 100, 2,
                        policy.digestSha256(), policy.targetPhysicalFormat()));
        GenerationMetadataStore durable = GenerationMetadataStoreTestFactory.inMemory(CLOCK);
        var bootstrap = durable.getOrCreateMaterializationCheckpoint(
                CLUSTER,
                STREAM,
                policy.policyId(),
                policy.policyVersion(),
                policy.digestSha256()).join();
        MaterializationCheckpointRecord value = bootstrap.value();
        durable.compareAndSetMaterializationCheckpoint(
                CLUSTER,
                new MaterializationCheckpointRecord(
                        value.schemaVersion(),
                        value.streamId(),
                        value.policyId(),
                        value.policyVersion(),
                        value.policySha256(),
                        4,
                        4,
                        4,
                        "ahead-task",
                        value.updatedAtMillis(),
                        0),
                bootstrap.metadataVersion()).join();
        GenerationMetadataStore generations = MaterializationPlannerTestSupport.generationStore(
                candidates, List.of(), durable);
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            assertThatThrownBy(() -> reconciler(generations, 0, 4, scheduler)
                            .reconcile(STREAM, policy, MaterializationTaskMutationGuard.noOp())
                            .join())
                    .hasRootCauseInstanceOf(NereusException.class)
                    .hasRootCauseMessage(
                            "materialization checkpoint is ahead of verified committed coverage");
        } finally {
            scheduler.shutdownNow();
            generations.close();
        }
    }

    @Test
    void activationFailurePreventsCheckpointCreationOrAdvance() {
        MaterializationPolicy policy = MaterializationPlannerTestSupport.policy();
        GenerationMetadataStore durable = GenerationMetadataStoreTestFactory.inMemory(CLOCK);
        GenerationMetadataStore generations = MaterializationPlannerTestSupport.generationStore(
                List.of(), List.of(), durable);
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            assertThatThrownBy(() -> reconciler(generations, 0, 0, scheduler)
                            .reconcile(STREAM, policy, () -> CompletableFuture.failedFuture(
                                    new NereusException(
                                            ErrorCode.METADATA_CONDITION_FAILED,
                                            true,
                                            "activation changed")))
                            .join())
                    .hasRootCauseMessage("activation changed");
            assertThat(durable.getMaterializationCheckpoint(
                            CLUSTER, STREAM, policy.policyId(), policy.policyVersion())
                    .join()).isEmpty();
        } finally {
            scheduler.shutdownNow();
            generations.close();
        }
    }

    private static DefaultMaterializationCheckpointReconciler reconciler(
            GenerationMetadataStore generations,
            long trim,
            long head,
            ScheduledExecutorService scheduler) {
        return new DefaultMaterializationCheckpointReconciler(
                CLUSTER,
                MaterializationPlannerTestSupport.l0Store(
                        MaterializationPlannerTestSupport.snapshot(trim, head)),
                generations,
                1,
                Duration.ofSeconds(5),
                scheduler,
                CLOCK);
    }

    private static GenerationMetadataStore loseFirstCheckpointCasResponse(
            GenerationMetadataStore delegate) {
        AtomicBoolean lose = new AtomicBoolean(true);
        return (GenerationMetadataStore) Proxy.newProxyInstance(
                GenerationMetadataStore.class.getClassLoader(),
                new Class<?>[] {GenerationMetadataStore.class},
                (proxy, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        return switch (method.getName()) {
                            case "toString" -> "lost-checkpoint-response-store";
                            case "hashCode" -> System.identityHashCode(proxy);
                            case "equals" -> proxy == args[0];
                            default -> throw new UnsupportedOperationException(method.getName());
                        };
                    }
                    Object result;
                    try {
                        result = method.invoke(delegate, args);
                    } catch (InvocationTargetException failure) {
                        throw failure.getCause();
                    }
                    if (method.getName().equals("compareAndSetMaterializationCheckpoint")
                            && lose.compareAndSet(true, false)) {
                        @SuppressWarnings("unchecked")
                        CompletableFuture<Object> committed = (CompletableFuture<Object>) result;
                        return committed.thenCompose(ignored -> CompletableFuture.failedFuture(
                                new NereusException(
                                        ErrorCode.METADATA_UNAVAILABLE,
                                        true,
                                        "lost checkpoint CAS response")));
                    }
                    return result;
                });
    }
}
