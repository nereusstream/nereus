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
import com.nereusstream.metadata.oxia.F4ScanToken;
import com.nereusstream.metadata.oxia.F4Keyspace;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationMetadataStoreTestFactory;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.StreamRegistrationScanPage;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import com.nereusstream.metadata.oxia.records.CommittedEndOffsetRecord;
import com.nereusstream.metadata.oxia.records.MaterializationStreamRegistrationRecord;
import com.nereusstream.metadata.oxia.records.StreamMetadataRecord;
import com.nereusstream.metadata.oxia.records.TrimRecord;
import java.lang.reflect.InvocationTargetException;
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
    private static final int SCALE_STREAMS_PER_SHARD = 257;
    private static final int SCALE_REGISTRY_PAGE_SIZE = 256;
    private static final int SCALE_STREAM_COUNT =
            F4Keyspace.MATERIALIZATION_REGISTRY_SHARDS * SCALE_STREAMS_PER_SHARD;

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
                        clock,
                        1)
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
                        clock,
                        1)
                .scanOnce()
                .join();

        assertCompleteEmptyScan(first);
        assertCompleteEmptyScan(afterWatchLossAndRestart);
        assertThat(readinessChecks).hasValue(256);
        assertThat(revalidations).hasValue(256);
        assertThat(plannerCalls).hasValue(0);
    }

    @Test
    void scansSixteenThousandFourHundredFortyEightRegistrationsAcrossColdRestarts() {
        Clock clock = Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC);
        try (GenerationMetadataStore durable = GenerationMetadataStoreTestFactory.inMemory(clock)) {
            F4Keyspace keyspace = new F4Keyspace(CLUSTER);
            Map<Integer, List<StreamId>> streamsByShard = streamsPerShard(
                    keyspace, SCALE_STREAMS_PER_SHARD);
            Map<Integer, List<String>> expectedKeysByShard = new LinkedHashMap<>();
            streamsByShard.forEach((shard, streams) -> {
                List<String> keys = streams.stream()
                        .map(RegisteredMaterializationStreamScannerTest::registration)
                        .map(registration -> durable.createOrVerifyStreamRegistration(
                                        CLUSTER, registration)
                                .join()
                                .key())
                        .sorted()
                        .toList();
                expectedKeysByShard.put(shard, keys);
            });

            RegistryScanTrace firstTrace = new RegistryScanTrace();
            RegisteredMaterializationScanResult first = skippedStreamScanner(
                            tracingStore(durable, firstTrace), clock)
                    .scanOnce()
                    .join();

            // This scanner and trace have no process-local continuation or watch state from the
            // first pass. The durable registry is the only retained input.
            RegistryScanTrace restartedTrace = new RegistryScanTrace();
            RegisteredMaterializationScanResult restarted = skippedStreamScanner(
                            tracingStore(durable, restartedTrace), clock)
                    .scanOnce()
                    .join();

            assertCompleteSkippedScaleScan(first);
            assertCompleteSkippedScaleScan(restarted);
            firstTrace.assertExact(expectedKeysByShard);
            restartedTrace.assertExact(expectedKeysByShard);
        }
    }

    private static RegisteredMaterializationStreamScanner emptyStreamScanner(
            GenerationMetadataStore durable,
            OxiaMetadataStore l0Metadata,
            MaterializationPolicy policy,
            AtomicInteger readinessChecks,
            AtomicInteger revalidations,
            AtomicInteger plannerCalls,
            Clock clock,
            int registryPageSize) {
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
                registryPageSize,
                1);
    }

    private static RegisteredMaterializationStreamScanner skippedStreamScanner(
            GenerationMetadataStore generations,
            Clock clock) {
        return emptyStreamScanner(
                generations,
                skippedL0Store(),
                MaterializationPlannerTestSupport.policy(),
                new AtomicInteger(),
                new AtomicInteger(),
                new AtomicInteger(),
                clock,
                SCALE_REGISTRY_PAGE_SIZE);
    }

    private static Map<Integer, List<StreamId>> twoStreamsPerShard(F4Keyspace keyspace) {
        return streamsPerShard(keyspace, 2);
    }

    private static Map<Integer, List<StreamId>> streamsPerShard(
            F4Keyspace keyspace,
            int streamsPerShard) {
        Map<Integer, List<StreamId>> result = new LinkedHashMap<>();
        for (int shard = 0; shard < F4Keyspace.MATERIALIZATION_REGISTRY_SHARDS; shard++) {
            result.put(shard, new ArrayList<>(streamsPerShard));
        }
        int discovered = 0;
        int target = Math.multiplyExact(
                F4Keyspace.MATERIALIZATION_REGISTRY_SHARDS, streamsPerShard);
        for (int candidate = 0; candidate < 2_000_000 && discovered < target; candidate++) {
            StreamId streamId = new StreamId("paged-registry-stream-" + candidate);
            int shard = keyspace.materializationRegistryShard(streamId);
            List<StreamId> shardStreams = result.get(shard);
            if (shardStreams.size() < streamsPerShard) {
                shardStreams.add(streamId);
                discovered++;
            }
        }
        assertThat(result).hasSize(F4Keyspace.MATERIALIZATION_REGISTRY_SHARDS);
        assertThat(result.values())
                .allSatisfy(streams -> assertThat(streams).hasSize(streamsPerShard));
        assertThat(discovered).isEqualTo(target);
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
        return l0Store(StreamState.ACTIVE);
    }

    private static OxiaMetadataStore skippedL0Store() {
        return l0Store(StreamState.DELETED);
    }

    private static OxiaMetadataStore l0Store(StreamState state) {
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
                                emptySnapshot((StreamId) args[1], state));
                        case "close" -> null;
                        default -> throw new UnsupportedOperationException(method.getName());
                    };
                });
    }

    private static StreamMetadataSnapshot emptySnapshot(StreamId streamId, StreamState state) {
        long metadataVersion = 5;
        return new StreamMetadataSnapshot(
                new StreamMetadataRecord(
                        streamId.value(),
                        "persistent://tenant/namespace/" + streamId.value(),
                        "name-hash-" + streamId.value(),
                        state.name(),
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

    private static void assertCompleteSkippedScaleScan(RegisteredMaterializationScanResult result) {
        assertThat(result.shardsScanned()).isEqualTo(F4Keyspace.MATERIALIZATION_REGISTRY_SHARDS);
        assertThat(result.registrationsScanned()).isEqualTo(SCALE_STREAM_COUNT);
        assertThat(result.registrationsAdmitted()).isZero();
        assertThat(result.registrationsSkipped()).isEqualTo(SCALE_STREAM_COUNT);
        assertThat(result.existingTasksRecovered()).isZero();
        assertThat(result.plannedTasksConverged()).isZero();
        assertThat(result.workflowMetadataRetired()).isZero();
    }

    private static GenerationMetadataStore tracingStore(
            GenerationMetadataStore delegate,
            RegistryScanTrace trace) {
        return (GenerationMetadataStore) Proxy.newProxyInstance(
                GenerationMetadataStore.class.getClassLoader(),
                new Class<?>[] {GenerationMetadataStore.class},
                (proxy, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        return switch (method.getName()) {
                            case "toString" -> "traced-generation-metadata-store";
                            case "hashCode" -> System.identityHashCode(proxy);
                            case "equals" -> proxy == args[0];
                            default -> throw new UnsupportedOperationException(method.getName());
                        };
                    }
                    try {
                        Object result = method.invoke(delegate, args);
                        if (!method.getName().equals("scanStreamRegistrations")) {
                            return result;
                        }
                        @SuppressWarnings("unchecked")
                        CompletableFuture<StreamRegistrationScanPage> pageFuture =
                                (CompletableFuture<StreamRegistrationScanPage>) result;
                        int shard = (int) args[1];
                        @SuppressWarnings("unchecked")
                        Optional<F4ScanToken> continuation = (Optional<F4ScanToken>) args[2];
                        return pageFuture.thenApply(page -> {
                            trace.record(shard, continuation, page);
                            return page;
                        });
                    } catch (InvocationTargetException failure) {
                        throw failure.getCause();
                    }
                });
    }

    private static final class RegistryScanTrace {
        private final Map<Integer, List<Boolean>> emptyContinuationByShard = new LinkedHashMap<>();
        private final Map<Integer, List<Integer>> pageSizesByShard = new LinkedHashMap<>();
        private final Map<Integer, List<Boolean>> nextContinuationByShard = new LinkedHashMap<>();
        private final Map<Integer, List<String>> returnedKeysByShard = new LinkedHashMap<>();

        private void record(
                int shard,
                Optional<F4ScanToken> continuation,
                StreamRegistrationScanPage page) {
            emptyContinuationByShard
                    .computeIfAbsent(shard, ignored -> new ArrayList<>())
                    .add(continuation.isEmpty());
            pageSizesByShard
                    .computeIfAbsent(shard, ignored -> new ArrayList<>())
                    .add(page.values().size());
            nextContinuationByShard
                    .computeIfAbsent(shard, ignored -> new ArrayList<>())
                    .add(page.continuation().isPresent());
            returnedKeysByShard
                    .computeIfAbsent(shard, ignored -> new ArrayList<>())
                    .addAll(page.values().stream()
                            .map(value -> value.key())
                            .toList());
        }

        private void assertExact(Map<Integer, List<String>> expectedKeysByShard) {
            assertThat(emptyContinuationByShard)
                    .hasSize(F4Keyspace.MATERIALIZATION_REGISTRY_SHARDS);
            assertThat(pageSizesByShard)
                    .hasSize(F4Keyspace.MATERIALIZATION_REGISTRY_SHARDS);
            assertThat(nextContinuationByShard)
                    .hasSize(F4Keyspace.MATERIALIZATION_REGISTRY_SHARDS);
            assertThat(returnedKeysByShard)
                    .hasSize(F4Keyspace.MATERIALIZATION_REGISTRY_SHARDS);
            expectedKeysByShard.forEach((shard, expectedKeys) -> {
                assertThat(emptyContinuationByShard.get(shard)).containsExactly(true, false);
                assertThat(pageSizesByShard.get(shard))
                        .containsExactly(SCALE_REGISTRY_PAGE_SIZE, 1);
                assertThat(nextContinuationByShard.get(shard)).containsExactly(true, false);
                assertThat(returnedKeysByShard.get(shard))
                        .hasSize(SCALE_STREAMS_PER_SHARD)
                        .doesNotHaveDuplicates()
                        .containsExactlyElementsOf(expectedKeys);
            });
        }
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
