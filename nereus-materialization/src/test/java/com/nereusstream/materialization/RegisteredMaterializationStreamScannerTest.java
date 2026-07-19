/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import static com.nereusstream.materialization.MaterializationPlannerTestSupport.CLUSTER;
import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.ProjectionRef;
import com.nereusstream.api.ProjectionType;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamState;
import com.nereusstream.core.capability.GenerationActivationProof;
import com.nereusstream.core.capability.GenerationOperation;
import com.nereusstream.core.capability.GenerationProtocolActivationGuard;
import com.nereusstream.metadata.oxia.F4Keyspace;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationMetadataStoreTestFactory;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import com.nereusstream.metadata.oxia.records.CommittedEndOffsetRecord;
import com.nereusstream.metadata.oxia.records.MaterializationStreamRegistrationRecord;
import com.nereusstream.metadata.oxia.records.StreamMetadataRecord;
import com.nereusstream.metadata.oxia.records.TrimRecord;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
                durableTask -> CompletableFuture.completedFuture(
                        new MaterializationTaskProtections(
                                durableTask, List.of(), Optional.empty())),
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
        AtomicInteger sourceRepairs = new AtomicInteger();
        GenerationProtocolActivationGuard guard = countingGuard(readinessChecks, revalidations);
        RegisteredMaterializationStreamScanner scanner = new RegisteredMaterializationStreamScanner(
                CLUSTER,
                MaterializationPlannerTestSupport.l0Store(
                        MaterializationPlannerTestSupport.snapshot(0, 4)),
                generations,
                guard,
                streamId -> {
                    sourceRepairs.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                },
                (streamId, range, exactPolicy, maxTasks) -> {
                    assertThat(sourceRepairs.get()).isPositive();
                    return planner.plan(
                            streamId,
                            range,
                            exactPolicy,
                            maxTasks);
                },
                taskStore,
                recovery,
                recoveryScanner,
                streamId -> CompletableFuture.completedFuture(
                        com.nereusstream.materialization.recovery.RecoveryCheckpointRunResult.skipped(
                                com.nereusstream.materialization.recovery.RecoveryCheckpointBuildStatus
                                        .NO_LIVE_TAIL)),
                (streamId, exactPolicy, mutationGuard) -> mutationGuard.revalidate()
                        .thenCompose(ignored -> durable.getOrCreateMaterializationCheckpoint(
                                CLUSTER,
                                streamId,
                                exactPolicy.policyId(),
                                exactPolicy.policyVersion(),
                                exactPolicy.digestSha256())),
                (streamId, exactPolicy, trim, mutationGuard) ->
                        CompletableFuture.completedFuture(
                                TerminalWorkflowMetadataRetirementResult.empty()),
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
        assertThat(sourceRepairs).hasValue(2);
    }

    @Test
    void fullScanPaginatesEveryShardAndFreshProcessRecoversWithoutWatchHints() {
        Clock clock = Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC);
        GenerationMetadataStore durable = GenerationMetadataStoreTestFactory.inMemory(clock);
        F4Keyspace keyspace = new F4Keyspace(CLUSTER);
        Map<Integer, List<StreamId>> streamsByShard = twoStreamsPerShard(keyspace);
        streamsByShard.values().stream()
                .flatMap(List::stream)
                .map(RegisteredMaterializationStreamScannerTest::registration)
                .forEach(registration -> durable.createOrVerifyStreamRegistration(
                                CLUSTER, registration)
                        .join());

        OxiaMetadataStore l0Metadata = emptyL0Store();
        MaterializationPolicy policy = MaterializationPlannerTestSupport.policy();
        AtomicInteger readinessChecks = new AtomicInteger();
        AtomicInteger revalidations = new AtomicInteger();
        AtomicInteger plannerCalls = new AtomicInteger();

        RegisteredMaterializationScanResult first = emptyStreamScanner(
                        durable,
                        l0Metadata,
                        policy,
                        readinessChecks,
                        revalidations,
                        plannerCalls,
                        clock)
                .scanOnce()
                .join();
        // A new scanner has no watch state, queue contents, or process-local stream hints to reuse.
        RegisteredMaterializationScanResult afterWatchLossAndRestart = emptyStreamScanner(
                        durable,
                        l0Metadata,
                        policy,
                        readinessChecks,
                        revalidations,
                        plannerCalls,
                        clock)
                .scanOnce()
                .join();

        assertCompleteEmptyScan(first);
        assertCompleteEmptyScan(afterWatchLossAndRestart);
        assertThat(readinessChecks).hasValue(256);
        assertThat(revalidations).hasValue(256);
        assertThat(plannerCalls).hasValue(0);
    }

    private static RegisteredMaterializationStreamScanner emptyStreamScanner(
            GenerationMetadataStore durable,
            OxiaMetadataStore l0Metadata,
            MaterializationPolicy policy,
            AtomicInteger readinessChecks,
            AtomicInteger revalidations,
            AtomicInteger plannerCalls,
            Clock clock) {
        MaterializationTaskStore taskStore = new MaterializationTaskStore(CLUSTER, durable, clock);
        MaterializationTaskRecovery recovery = new MaterializationTaskRecovery(
                taskStore,
                durableTask -> CompletableFuture.completedFuture(
                        new MaterializationTaskProtections(
                                durableTask, List.of(), Optional.empty())),
                new GenerationPublicationReconciler((ignoredTask, ignoredOutput) ->
                        CompletableFuture.completedFuture(null)),
                (ignoredDurable, ignoredTask) -> CompletableFuture.completedFuture(null),
                clock,
                Duration.ZERO,
                Duration.ofSeconds(1));
        MaterializationPlanner planner = (streamId, requestedRange, exactPolicy, maxTasks) -> {
            plannerCalls.incrementAndGet();
            return CompletableFuture.failedFuture(
                    new AssertionError("empty committed coverage must not invoke the planner"));
        };
        return new RegisteredMaterializationStreamScanner(
                CLUSTER,
                l0Metadata,
                durable,
                countingGuard(readinessChecks, revalidations),
                planner,
                taskStore,
                recovery,
                new TaskRecoveryScanner(taskStore, recovery, 1),
                streamId -> CompletableFuture.completedFuture(
                        com.nereusstream.materialization.recovery.RecoveryCheckpointRunResult.skipped(
                                com.nereusstream.materialization.recovery.RecoveryCheckpointBuildStatus
                                        .NO_LIVE_TAIL)),
                (streamId, exactPolicy, mutationGuard) -> mutationGuard.revalidate()
                        .thenCompose(ignored -> durable.getOrCreateMaterializationCheckpoint(
                                CLUSTER,
                                streamId,
                                exactPolicy.policyId(),
                                exactPolicy.policyVersion(),
                                exactPolicy.digestSha256())),
                (streamId, exactPolicy, trim, mutationGuard) ->
                        CompletableFuture.completedFuture(
                                TerminalWorkflowMetadataRetirementResult.empty()),
                policy,
                1,
                1);
    }

    private static Map<Integer, List<StreamId>> twoStreamsPerShard(F4Keyspace keyspace) {
        Map<Integer, List<StreamId>> result = new LinkedHashMap<>();
        int discovered = 0;
        for (int candidate = 0; candidate < 100_000 && discovered < 128; candidate++) {
            StreamId streamId = new StreamId("paged-registry-stream-" + candidate);
            int shard = keyspace.materializationRegistryShard(streamId);
            List<StreamId> shardStreams = result.computeIfAbsent(shard, ignored -> new ArrayList<>());
            if (shardStreams.size() < 2) {
                shardStreams.add(streamId);
                discovered++;
            }
        }
        assertThat(result).hasSize(F4Keyspace.MATERIALIZATION_REGISTRY_SHARDS);
        assertThat(result.values()).allSatisfy(streams -> assertThat(streams).hasSize(2));
        return result;
    }

    private static MaterializationStreamRegistrationRecord registration(StreamId streamId) {
        ProjectionRef projection = new ProjectionRef(
                ProjectionType.VIRTUAL_LEDGER, "projection-" + streamId.value());
        return new MaterializationStreamRegistrationRecord(
                1,
                streamId.value(),
                MaterializationRecordMapper.projectionIdentity(Optional.of(projection)),
                MaterializationPlannerTestSupport.sha('e').value(),
                StorageProfile.OBJECT_WAL_SYNC_OBJECT.name(),
                100,
                0,
                100,
                0);
    }

    private static OxiaMetadataStore emptyL0Store() {
        return (OxiaMetadataStore) Proxy.newProxyInstance(
                OxiaMetadataStore.class.getClassLoader(),
                new Class<?>[] {OxiaMetadataStore.class},
                (proxy, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        return switch (method.getName()) {
                            case "toString" -> "empty-registry-scan-l0-store";
                            case "hashCode" -> System.identityHashCode(proxy);
                            case "equals" -> proxy == args[0];
                            default -> throw new UnsupportedOperationException(method.getName());
                        };
                    }
                    return switch (method.getName()) {
                        case "getStreamSnapshot" -> CompletableFuture.completedFuture(
                                emptySnapshot((StreamId) args[1]));
                        case "close" -> null;
                        default -> throw new UnsupportedOperationException(method.getName());
                    };
                });
    }

    private static StreamMetadataSnapshot emptySnapshot(StreamId streamId) {
        long metadataVersion = 5;
        return new StreamMetadataSnapshot(
                new StreamMetadataRecord(
                        streamId.value(),
                        "persistent://tenant/namespace/" + streamId.value(),
                        "name-hash-" + streamId.value(),
                        StreamState.ACTIVE.name(),
                        StorageProfile.OBJECT_WAL_SYNC_OBJECT.name(),
                        Map.of(),
                        1,
                        1,
                        metadataVersion),
                new CommittedEndOffsetRecord(streamId.value(), 0, 0, 0, metadataVersion),
                new TrimRecord(streamId.value(), 0, "", 1, metadataVersion));
    }

    private static void assertCompleteEmptyScan(RegisteredMaterializationScanResult result) {
        assertThat(result.shardsScanned()).isEqualTo(F4Keyspace.MATERIALIZATION_REGISTRY_SHARDS);
        assertThat(result.registrationsScanned()).isEqualTo(128);
        assertThat(result.registrationsAdmitted()).isEqualTo(128);
        assertThat(result.registrationsSkipped()).isZero();
        assertThat(result.existingTasksRecovered()).isZero();
        assertThat(result.plannedTasksConverged()).isZero();
        assertThat(result.workflowMetadataRetired()).isZero();
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
                assertThat(activateLiveProjectionIfAbsent).isTrue();
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
