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

package com.nereusstream.metadata.oxia.v2.allocator.evidence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.registry.VirtualLedgerSliceAssignmentV1;
import com.nereusstream.domain.registry.allocator.AllocatorEvidenceAttachmentKindV1;
import com.nereusstream.domain.registry.allocator.AllocatorEvidenceCandidateV1;
import com.nereusstream.domain.registry.allocator.AllocatorEvidenceContextV1;
import com.nereusstream.domain.registry.allocator.AllocatorFaultCutV1;
import com.nereusstream.domain.registry.allocator.AllocatorHeadStateV1;
import com.nereusstream.domain.registry.allocator.AllocatorModeV1;
import com.nereusstream.domain.registry.allocator.AllocatorProtocolV1;
import com.nereusstream.domain.registry.allocator.AllocatorRawEvidenceEventV1;
import com.nereusstream.domain.registry.allocator.AllocatorRawEvidenceEventV1.EventKind;
import com.nereusstream.domain.registry.allocator.AllocatorRawEvidenceEventV1.EventOutcome;
import com.nereusstream.domain.registry.allocator.AllocatorRawEvidenceEventV1.OxiaOperationKind;
import com.nereusstream.domain.registry.allocator.CellAllocatorReservationV1;
import com.nereusstream.domain.registry.allocator.ChainPointerV1;
import com.nereusstream.domain.registry.allocator.ManagedLedgerAllocatorHeadV1;
import com.nereusstream.domain.registry.allocator.ManagedLedgerIncarnationIdV1;
import com.nereusstream.domain.registry.allocator.VirtualLedgerCandidateNodeV1;
import com.nereusstream.domain.registry.allocator.VirtualLedgerCellAllocatorStateV1;
import com.nereusstream.metadata.oxia.v2.allocator.OxiaVirtualLedgerAllocatorKeys;
import com.nereusstream.metadata.oxia.v2.mutation.AuthorityRecord;
import com.nereusstream.metadata.oxia.v2.mutation.MetadataVersionMapper;
import com.nereusstream.metadata.oxia.v2.mutation.MutationAcknowledgement;
import com.nereusstream.metadata.oxia.v2.mutation.OxiaConditionalClient;
import com.nereusstream.metadata.spi.allocator.VersionedAllocatorCellStateV1;
import com.nereusstream.metadata.spi.allocator.VersionedManagedLedgerAllocatorHeadV1;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionService;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class M3AllocatorEvidenceWiringTest {
    private static final Pattern SELF_SHA = Pattern.compile("\\\"selfSha256\\\":\\\"([0-9a-f]{64})\\\"");

    @Test
    void installsOnlyTheExpectedNativeHarnessCleanupWarningFilter() {
        assertThatCode(M3AllocatorEvidenceLoggingContract::requireInstalled).doesNotThrowAnyException();
    }

    @Test
    void controlledLatencySchedulerAdvancesFourDueCompletionsConcurrently() throws Exception {
        assertThat(M3RealOxiaActors.CONTROLLED_DELAY_SCHEDULER_THREADS_PER_ACTOR).isEqualTo(4);
        PendingReadConditionalClient delegate = new PendingReadConditionalClient();
        CountDownLatch enteredCallbacks = new CountDownLatch(4);
        CountDownLatch releaseCallbacks = new CountDownLatch(1);
        List<CompletableFuture<Void>> completions = new java.util.ArrayList<>();

        try (M3RealOxiaActors.InstrumentedClient client =
                new M3RealOxiaActors.InstrumentedClient(0, delegate)) {
            client.setControlledLatencyMillis(1);
            for (int index = 0; index < 4; index++) {
                completions.add(client.read("/nereus/v2/m3/delay-scheduler/" + index)
                        .thenRun(() -> {
                            enteredCallbacks.countDown();
                            try {
                                releaseCallbacks.await();
                            } catch (InterruptedException interrupted) {
                                Thread.currentThread().interrupt();
                                throw new IllegalStateException(interrupted);
                            }
                        })
                        .toCompletableFuture());
            }
            assertThat(delegate.reads).hasSize(4);
            delegate.reads.forEach(read -> read.complete(Optional.empty()));

            boolean allEntered = enteredCallbacks.await(5, TimeUnit.SECONDS);
            releaseCallbacks.countDown();
            CompletableFuture.allOf(completions.toArray(CompletableFuture[]::new)).join();
            assertThat(allEntered).isTrue();
        } finally {
            releaseCallbacks.countDown();
        }
    }

    @Test
    void isolatesRangePopulationExactCellAndRejectsInjectedConstructionLatency() throws Exception {
        ReentrantReadWriteLock cellLock = new ReentrantReadWriteLock(true);
        assertThatCode(() -> M3CandidateAllocatorPopulation.requireRangePopulationCellIsolation(
                        AllocatorEvidenceCandidateV1.strict(), cellLock))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> M3CandidateAllocatorPopulation.requireRangePopulationCellIsolation(
                        AllocatorEvidenceCandidateV1.range(16), cellLock))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("outside its proof lock");
        cellLock.writeLock().lock();
        try {
            assertThatCode(() -> M3CandidateAllocatorPopulation.requireRangePopulationCellIsolation(
                            AllocatorEvidenceCandidateV1.range(16), cellLock))
                    .doesNotThrowAnyException();
            Object capturedCell = new Object();
            assertThatCode(() -> M3CandidateAllocatorPopulation.requireRangePopulationCapturedCellUnchanged(
                            AllocatorEvidenceCandidateV1.range(16), cellLock, capturedCell, capturedCell))
                    .doesNotThrowAnyException();
            assertThatThrownBy(() -> M3CandidateAllocatorPopulation.requireRangePopulationCapturedCellUnchanged(
                            AllocatorEvidenceCandidateV1.range(16), cellLock, capturedCell, new Object()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("mutated its captured exact Cell");
        } finally {
            cellLock.writeLock().unlock();
        }

        PendingConditionalClient firstDelegate = new PendingConditionalClient();
        PendingConditionalClient secondDelegate = new PendingConditionalClient();
        try (M3RealOxiaActors.InstrumentedClient first =
                        new M3RealOxiaActors.InstrumentedClient(0, firstDelegate);
                M3RealOxiaActors.InstrumentedClient second =
                        new M3RealOxiaActors.InstrumentedClient(1, secondDelegate)) {
            assertThatCode(() -> M3RealOxiaActors.requirePopulationConstructionLatencyDisabled(
                            List.of(first, second)))
                    .doesNotThrowAnyException();
            second.setControlledLatencyMillis(25);
            assertThatThrownBy(() -> M3RealOxiaActors.requirePopulationConstructionLatencyDisabled(
                            List.of(first, second)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("inherited measured metadata latency");
        }
    }

    @Test
    void reconcilesCompletedV3WorkflowCellsMonotonicallyForFollowingFaultActions() {
        VersionedAllocatorCellStateV1 first = exactCell(16, 2, 1);
        VersionedAllocatorCellStateV1 second = exactCell(32, 3, 2);
        VersionedAllocatorCellStateV1 sameValueDifferentOpaqueVersion = exactCell(32, 3, 3);

        assertThat(M3CandidateAllocatorPopulation.newestCompletedWorkflowCell(first, second)).isSameAs(second);
        assertThat(M3CandidateAllocatorPopulation.newestCompletedWorkflowCell(second, first)).isSameAs(second);
        assertThat(M3CandidateAllocatorPopulation.newestCompletedWorkflowCell(
                        second, sameValueDifferentOpaqueVersion))
                .isSameAs(second);

        VersionedAllocatorCellStateV1 reservedSecond = reservedCell(32, 3, 2);
        assertThat(M3CandidateAllocatorPopulation.newestCompletedWorkflowCell(first, reservedSecond))
                .as("another in-flight RANGE reservation is not a terminal population proof")
                .isSameAs(first);
        assertThat(M3CandidateAllocatorPopulation.newestCompletedWorkflowCell(second, reservedSecond))
                .as("a late reserved snapshot cannot replace the matching cleared completion")
                .isSameAs(second);
        assertThatThrownBy(() -> M3CandidateAllocatorPopulation.newestCompletedWorkflowCell(
                        reservedSecond, second))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("population Cell proof retained a reservation");

        VersionedAllocatorCellStateV1 mismatchedGrant = exactCell(48, 3, 4);
        assertThatThrownBy(() -> M3CandidateAllocatorPopulation.newestCompletedWorkflowCell(
                        second, mismatchedGrant))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cursor/grant ordering diverged");
    }

    @Test
    void boundsPopulationConstructionAndReportsExactInterruptedProgress() throws Exception {
        ExecutorService workers = Executors.newFixedThreadPool(2);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        AtomicInteger createdHeads = new AtomicInteger(12_345);
        AtomicInteger installedGrants = new AtomicInteger(6_789);
        try {
            assertThatThrownBy(() -> M3BoundedPopulationConstruction.run(
                            workers,
                            Duration.ofMillis(50),
                            "allocator RANGE_LEASED(1024) population construction 10000->100000",
                            () -> "headCreates="
                                    + createdHeads.get()
                                    + "/90000, initialGrants="
                                    + installedGrants.get()
                                    + "/89424",
                            () -> {
                                entered.countDown();
                                try {
                                    new CountDownLatch(1).await();
                                } catch (InterruptedException failure) {
                                    interrupted.countDown();
                                    throw failure;
                                }
                            }))
                    .isInstanceOf(TimeoutException.class)
                    .hasMessage(
                            "allocator RANGE_LEASED(1024) population construction 10000->100000 did not finish "
                                    + "within 50 milliseconds; headCreates=12345/90000, "
                                    + "initialGrants=6789/89424");
            assertThat(entered.getCount()).isZero();
            assertThat(interrupted.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            workers.shutdownNow();
            assertThat(workers.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void bindsPopulationConstructionTimeoutToTheChargedFormalPathBudget() {
        assertThat(M3CandidateAllocatorPopulation.POPULATION_DRAIN_TIMEOUT_SECONDS)
                .isEqualTo(M3V3FormalActionExecutorAdapter.CONSTRUCTION_PATH_SECONDS)
                .isEqualTo(900);
    }

    @Test
    void recordsLateFreshOwnerAdmissionAsExactRecoveryProofAndTypedTimeout() {
        List<AllocatorRawEvidenceEventV1> events = new java.util.ArrayList<>();
        M3AllocatorRequestTelemetry telemetry = new M3AllocatorRequestTelemetry(events::add, System.nanoTime());
        AllocatorEvidenceContextV1 context = AllocatorEvidenceContextV1.candidateContext(
                AllocatorEvidenceCandidateV1.strict(), 10_000, 1, 200);
        M3AllocatorRequestTelemetry.RequestTrace trace = telemetry.trace(
                context,
                new M3AllocatorWorkloadPlan.PlannedRequest(
                        1_000_004,
                        1,
                        4,
                        M3AllocatorWorkloadPlan.Trigger.ENTRY,
                        M3AllocatorWorkloadPlan.Phase.MEASURED_STEADY,
                        0),
                AllocatorFaultCutV1.BROKER_SESSION_CRASH_MASS_TAKEOVER,
                2);

        trace.admitted();
        trace.appendAdmissionStart();
        trace.appendAdmissionRelease();
        trace.freshOwnerRecoveryComplete(false);

        assertThat(events)
                .extracting(AllocatorRawEvidenceEventV1::kind)
                .containsExactly(
                        EventKind.ADMITTED,
                        EventKind.APPEND_ADMISSION_START,
                        EventKind.APPEND_ADMISSION_RELEASE,
                        EventKind.FRESH_OWNER_APPEND_COMPLETE,
                        EventKind.TIMED_OUT);
        assertThat(events.get(3).ownerEpoch()).isEqualTo(2);
        assertThat(events.get(3).outcome()).isEqualTo(EventOutcome.SUCCESS);
        assertThat(events.get(4).outcome()).isEqualTo(EventOutcome.TIMED_OUT);
    }

    @Test
    void freezesTheExactFiveFileNaeaInventoryAndFailClosedSealEntrypoint() {
        assertThat(Arrays.stream(AllocatorEvidenceAttachmentKindV1.values())
                        .map(AllocatorEvidenceAttachmentKindV1::fileName))
                .containsExactly(
                        "test.naea",
                        "native.naea",
                        "fault.naea",
                        "scale-10000.naea",
                        "scale-100000.naea");
        assertThatThrownBy(() -> M3AllocatorEvidenceSealMain.main(new String[0]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("expected output");
    }

    @Test
    void keepsConcurrentSameKeyMutationAndRereadChainsBoundToTheirExactRequest() throws Exception {
        List<AllocatorRawEvidenceEventV1> events = Collections.synchronizedList(new java.util.ArrayList<>());
        M3AllocatorRequestTelemetry telemetry = new M3AllocatorRequestTelemetry(events::add, System.nanoTime());
        AllocatorEvidenceContextV1 context = AllocatorEvidenceContextV1.candidateContext(
                AllocatorEvidenceCandidateV1.strict(), 10_000, 1, 200);
        M3AllocatorRequestTelemetry.RequestTrace firstTrace = telemetry.trace(
                context,
                new M3AllocatorWorkloadPlan.PlannedRequest(
                        11, 1, 101, M3AllocatorWorkloadPlan.Trigger.ENTRY,
                        M3AllocatorWorkloadPlan.Phase.MEASURED_STEADY, 0),
                null,
                1);
        M3AllocatorRequestTelemetry.RequestTrace secondTrace = telemetry.trace(
                context,
                new M3AllocatorWorkloadPlan.PlannedRequest(
                        22, 1, 202, M3AllocatorWorkloadPlan.Trigger.BYTE,
                        M3AllocatorWorkloadPlan.Phase.MEASURED_STEADY, 0),
                null,
                1);
        PendingConditionalClient delegate = new PendingConditionalClient();
        String key = "/nereus/v2/m3/allocator/exact-cell";
        CanonicalBytes firstBytes = CanonicalBytes.copyOf(new byte[] {1, 2, 3});
        CanonicalBytes secondBytes = CanonicalBytes.copyOf(new byte[] {4, 5, 6, 7});

        try (M3RealOxiaActors.InstrumentedClient client =
                new M3RealOxiaActors.InstrumentedClient(1, delegate)) {
            OxiaConditionalClient first = client.bound(
                    client.binding(key, firstTrace, OxiaOperationKind.CELL_RESERVE_CAS));
            OxiaConditionalClient second = client.bound(
                    client.binding(key, secondTrace, OxiaOperationKind.CELL_RESERVE_CAS));

            CompletionStage<Void> firstChain = first.compareAndSet(key, firstBytes, 1)
                    .thenCompose(ignored -> first.read(key).thenApply(record -> null));
            CompletionStage<Void> secondChain = second.compareAndSet(key, secondBytes, 2)
                    .thenCompose(ignored -> second.read(key).thenApply(record -> null));

            assertThat(delegate.mutations).hasSize(2);
            delegate.mutations.get(1).complete(null);
            delegate.mutations.get(0).complete(null);
            CompletableFuture.allOf(
                            firstChain.toCompletableFuture(), secondChain.toCompletableFuture())
                    .join();

            assertThat(events.stream().filter(event -> event.requestOrdinal() == 11))
                    .extracting(AllocatorRawEvidenceEventV1::kind)
                    .containsExactly(
                            EventKind.OXIA_OPERATION_START,
                            EventKind.OXIA_OPERATION_END,
                            EventKind.OXIA_OPERATION_START,
                            EventKind.OXIA_OPERATION_END);
            assertThat(events.stream().filter(event -> event.requestOrdinal() == 11))
                    .extracting(AllocatorRawEvidenceEventV1::oxiaOperationKind)
                    .containsExactly(
                            OxiaOperationKind.CELL_RESERVE_CAS,
                            OxiaOperationKind.CELL_RESERVE_CAS,
                            OxiaOperationKind.EXACT_READ,
                            OxiaOperationKind.EXACT_READ);
            assertThat(events.stream().filter(event -> event.requestOrdinal() == 22))
                    .extracting(AllocatorRawEvidenceEventV1::kind)
                    .containsExactly(
                            EventKind.OXIA_OPERATION_START,
                            EventKind.OXIA_OPERATION_END,
                            EventKind.OXIA_OPERATION_START,
                            EventKind.OXIA_OPERATION_END);
            assertThat(events.stream().filter(event -> event.requestOrdinal() == 22))
                    .extracting(AllocatorRawEvidenceEventV1::oxiaOperationKind)
                    .containsExactly(
                            OxiaOperationKind.CELL_RESERVE_CAS,
                            OxiaOperationKind.CELL_RESERVE_CAS,
                            OxiaOperationKind.EXACT_READ,
                            OxiaOperationKind.EXACT_READ);
            assertThatThrownBy(() -> first.read(key + "/other"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("crossed an exact Oxia authority key");
        }
    }

    @Test
    void instrumentedClientPreservesAcknowledgedMutationResultsWithoutLegacyFallback() throws Exception {
        List<AllocatorRawEvidenceEventV1> events = Collections.synchronizedList(new java.util.ArrayList<>());
        M3AllocatorRequestTelemetry telemetry = new M3AllocatorRequestTelemetry(events::add, System.nanoTime());
        AllocatorEvidenceContextV1 context = AllocatorEvidenceContextV1.candidateContext(
                AllocatorEvidenceCandidateV1.range(1024), 10_000, 25, 1_000);
        M3AllocatorRequestTelemetry.RequestTrace trace = telemetry.trace(
                context,
                new M3AllocatorWorkloadPlan.PlannedRequest(
                        31,
                        1,
                        303,
                        M3AllocatorWorkloadPlan.Trigger.ENTRY,
                        M3AllocatorWorkloadPlan.Phase.MEASURED_STEADY,
                        0),
                null,
                1);
        AcknowledgedConditionalClient delegate = new AcknowledgedConditionalClient();
        String key = "/nereus/v2/m3/allocator/acknowledged";
        CanonicalBytes bytes = CanonicalBytes.copyOf(new byte[] {8, 9, 10});

        try (M3RealOxiaActors.InstrumentedClient client =
                new M3RealOxiaActors.InstrumentedClient(2, delegate)) {
            client.beginDiagnosticCapture();
            OxiaConditionalClient bound =
                    client.bound(client.binding(key, trace, OxiaOperationKind.HEAD_PUBLISH_CAS));

            MutationAcknowledgement created = client.createIfAbsentAcknowledged(key, bytes)
                    .toCompletableFuture()
                    .join()
                    .orElseThrow();
            MutationAcknowledgement applied = bound.compareAndSetAcknowledged(key, bytes, created.versionId())
                    .toCompletableFuture()
                    .join()
                    .orElseThrow();
            M3RealOxiaActors.InstrumentedClient.OperationDiagnosticSnapshot snapshot =
                    client.endDiagnosticCapture();

            assertThat(created).isEqualTo(new MutationAcknowledgement(key, 7));
            assertThat(applied).isEqualTo(new MutationAcknowledgement(key, 8));
            assertThat(delegate.acknowledgedCreates).isOne();
            assertThat(delegate.acknowledgedCas).isOne();
            assertThat(delegate.legacyMutations).isZero();
            assertThat(snapshot.samples())
                    .extracting(M3RealOxiaActors.InstrumentedClient.OperationSample::kind)
                    .containsExactly("CREATE_IF_ABSENT", "COMPARE_AND_SET");
            assertThat(events)
                    .extracting(AllocatorRawEvidenceEventV1::kind)
                    .containsExactly(EventKind.OXIA_OPERATION_START, EventKind.OXIA_OPERATION_END);
        }
    }

    @Test
    void evidenceStorePreservesInstalledRangeSpecializedMutationPaths() throws Exception {
        AcknowledgedConditionalClient delegate = new AcknowledgedConditionalClient();
        M3EvidenceAllocatorStore.TraceRegistry traces = new M3EvidenceAllocatorStore.TraceRegistry();
        String root = "/nereus/v2/m3/evidence-store-forwarding";
        Sha256Digest namespaceId = digest("evidence-store-namespace");
        Sha256Digest sliceAssignmentId = digest("evidence-store-assignment");
        ManagedLedgerIncarnationIdV1 incarnation =
                new ManagedLedgerIncarnationIdV1(digest("evidence-store-incarnation"));
        long rangeStart = VirtualLedgerSliceAssignmentV1.RESERVED_START_INCLUSIVE;
        ManagedLedgerAllocatorHeadV1 predecessor = new ManagedLedgerAllocatorHeadV1(
                VirtualLedgerCellAllocatorStateV1.PROTOCOL_VERSION,
                incarnation,
                1,
                ChainPointerV1.absent(),
                1,
                rangeStart,
                rangeStart + 1024,
                rangeStart);
        VirtualLedgerCandidateNodeV1 node =
                AllocatorProtocolV1.candidate(predecessor, digest("evidence-store-descriptor"));
        OxiaVirtualLedgerAllocatorKeys keys = new OxiaVirtualLedgerAllocatorKeys(root);
        VersionedManagedLedgerAllocatorHeadV1 exactPredecessor = new VersionedManagedLedgerAllocatorHeadV1(
                namespaceId,
                sliceAssignmentId,
                keys.headKey(namespaceId, sliceAssignmentId, incarnation),
                predecessor,
                MetadataVersionMapper.fromOxia(6));

        try (M3RealOxiaActors.InstrumentedClient client =
                new M3RealOxiaActors.InstrumentedClient(3, delegate)) {
            client.beginDiagnosticCapture();
            M3EvidenceAllocatorStore store = new M3EvidenceAllocatorStore(root, client, traces);

            var created = store.createNodeAfterStoreObservedRangeAuthorities(namespaceId, sliceAssignmentId, node)
                    .toCompletableFuture()
                    .join();
            ManagedLedgerAllocatorHeadV1 successor = AllocatorProtocolV1.publish(predecessor, node);
            var applied = store.compareAndSetHeadAfterStoreObservedRangeNode(
                            namespaceId, sliceAssignmentId, exactPredecessor, successor)
                    .toCompletableFuture()
                    .join();
            M3RealOxiaActors.InstrumentedClient.OperationDiagnosticSnapshot snapshot =
                    client.endDiagnosticCapture();

            assertThat(created.exactSnapshot().orElseThrow().value()).isEqualTo(node);
            assertThat(applied.exactSnapshot().orElseThrow().value()).isEqualTo(successor);
            assertThat(delegate.acknowledgedCreates).isOne();
            assertThat(delegate.acknowledgedCas).isOne();
            assertThat(delegate.legacyMutations).isZero();
            assertThat(snapshot.samples())
                    .extracting(M3RealOxiaActors.InstrumentedClient.OperationSample::kind)
                    .containsExactly("CREATE_IF_ABSENT", "COMPARE_AND_SET");
        }
    }

    @Test
    void drainsEverySubmittedCompletionBeforeRethrowingTheFirstFailure() throws Exception {
        ExecutorService workers = Executors.newFixedThreadPool(2);
        CompletionService<Void> completions = new ExecutorCompletionService<>(workers);
        CountDownLatch secondStarted = new CountDownLatch(1);
        CountDownLatch releaseSecond = new CountDownLatch(1);
        AtomicBoolean secondCompleted = new AtomicBoolean();
        AtomicReference<Throwable> observed = new AtomicReference<>();
        Thread waiter = new Thread(() -> {
            try {
                M3BoundedCompletionDrain.await(completions, 2, 5, "test completion batch");
            } catch (Throwable failure) {
                observed.set(failure);
            }
        });
        try {
            Future<Void> first = completions.submit(() -> {
                throw new IllegalStateException("first exact failure");
            });
            completions.submit(() -> {
                secondStarted.countDown();
                releaseSecond.await();
                secondCompleted.set(true);
                return null;
            });
            assertThatThrownBy(first::get)
                    .hasCauseInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("IllegalStateException");
            assertThat(secondStarted.await(5, TimeUnit.SECONDS)).isTrue();
            waiter.start();
            waiter.join(100);
            assertThat(waiter.isAlive()).isTrue();
            releaseSecond.countDown();
            waiter.join(TimeUnit.SECONDS.toMillis(5));
            assertThat(waiter.isAlive()).isFalse();
            assertThat(secondCompleted.get()).isTrue();
            assertThat(observed.get())
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("first exact failure");
        } finally {
            releaseSecond.countDown();
            workers.shutdownNow();
            assertThat(workers.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            if (waiter.isAlive()) {
                waiter.interrupt();
                waiter.join(TimeUnit.SECONDS.toMillis(5));
            }
        }
    }

    @Test
    void interruptsPendingOxiaStageWaitDuringBoundedCleanup() throws Exception {
        ExecutorService worker = Executors.newSingleThreadExecutor();
        CompletableFuture<String> never = new CompletableFuture<>();
        CountDownLatch entered = new CountDownLatch(1);
        AtomicReference<Throwable> observed = new AtomicReference<>();
        AtomicBoolean interruptRestored = new AtomicBoolean();
        try {
            Future<?> pending = worker.submit(() -> {
                entered.countDown();
                try {
                    M3CandidateAllocatorPopulation.awaitStage(never, "test Oxia mutation");
                } catch (Throwable failure) {
                    observed.set(failure);
                    interruptRestored.set(Thread.currentThread().isInterrupted());
                }
            });
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(pending.cancel(true)).isTrue();
            worker.shutdown();
            assertThat(worker.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            assertThat(observed.get())
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("interrupted during bounded cleanup");
            assertThat(observed.get().getCause()).isInstanceOf(InterruptedException.class);
            assertThat(interruptRestored.get()).isTrue();
        } finally {
            worker.shutdownNow();
            assertThat(worker.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void bindsConcurrentCellProofReadsToEachCallingRequest() throws Exception {
        M3EvidenceAllocatorStore.TraceRegistry registry = new M3EvidenceAllocatorStore.TraceRegistry();
        M3AllocatorRequestTelemetry telemetry =
                new M3AllocatorRequestTelemetry(event -> {}, System.nanoTime());
        AllocatorEvidenceContextV1 context = AllocatorEvidenceContextV1.candidateContext(
                AllocatorEvidenceCandidateV1.strict(), 10_000, 1, 200);
        M3AllocatorRequestTelemetry.RequestTrace first = telemetry.trace(
                context,
                new M3AllocatorWorkloadPlan.PlannedRequest(
                        31,
                        1,
                        101,
                        M3AllocatorWorkloadPlan.Trigger.ENTRY,
                        M3AllocatorWorkloadPlan.Phase.MEASURED_STEADY,
                        0),
                null,
                1);
        M3AllocatorRequestTelemetry.RequestTrace second = telemetry.trace(
                context,
                new M3AllocatorWorkloadPlan.PlannedRequest(
                        32,
                        2,
                        202,
                        M3AllocatorWorkloadPlan.Trigger.BYTE,
                        M3AllocatorWorkloadPlan.Phase.MEASURED_STEADY,
                        0),
                null,
                1);
        CyclicBarrier overlap = new CyclicBarrier(2);
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            Future<M3AllocatorRequestTelemetry.RequestTrace> firstObserved = workers.submit(() -> {
                registry.bindCellRead(first);
                try {
                    overlap.await(5, TimeUnit.SECONDS);
                    return registry.cellTrace();
                } finally {
                    registry.unbindCellRead(first);
                }
            });
            Future<M3AllocatorRequestTelemetry.RequestTrace> secondObserved = workers.submit(() -> {
                registry.bindCellRead(second);
                try {
                    overlap.await(5, TimeUnit.SECONDS);
                    return registry.cellTrace();
                } finally {
                    registry.unbindCellRead(second);
                }
            });
            assertThat(firstObserved.get(5, TimeUnit.SECONDS)).isSameAs(first);
            assertThat(secondObserved.get(5, TimeUnit.SECONDS)).isSameAs(second);
        } finally {
            workers.shutdownNow();
            assertThat(workers.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void sealsOnlyTheExactVerifierTestcaseAndUsesTheFrozenZeroedSelfHash(@TempDir Path temporary)
            throws Exception {
        Path raw = temporary.resolve("raw-verification-payload.json");
        Files.writeString(raw, rawVerificationJson());
        Path junit = temporary.resolve(M3AllocatorVerificationSealMain.TEST_XML);
        Files.writeString(junit, junitXml("0"));
        Path sealed = temporary.resolve("raw-verification.json");
        M3AllocatorVerificationSealMain.main(
                new String[] {raw.toString(), junit.toString(), sealed.toString()});

        byte[] exact = Files.readAllBytes(sealed);
        Matcher matcher = SELF_SHA.matcher(new String(exact, StandardCharsets.UTF_8));
        assertThat(matcher.find()).isTrue();
        String observed = matcher.group(1);
        byte[] zeroed = new String(exact, StandardCharsets.UTF_8)
                .replaceFirst(observed, "0".repeat(64))
                .getBytes(StandardCharsets.UTF_8);
        assertThat(HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(zeroed)))
                .isEqualTo(observed);

        Path skippedDirectory = temporary.resolve("skipped");
        Files.createDirectories(skippedDirectory);
        Path skipped = skippedDirectory.resolve(M3AllocatorVerificationSealMain.TEST_XML);
        Files.writeString(skipped, junitXml("1"));
        assertThatThrownBy(() -> M3AllocatorVerificationSealMain.main(new String[] {
                    raw.toString(), skipped.toString(), skippedDirectory.resolve("raw-verification.json").toString()
                }))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("1/0/0/0");

        Path tamperedDirectory = temporary.resolve("tampered");
        Files.createDirectories(tamperedDirectory);
        Path tamperedRaw = tamperedDirectory.resolve("raw-verification-payload.json");
        Files.writeString(tamperedRaw, Files.readString(raw).replace("\"note\":\"A\"", "\"note\":\"B\""));
        Path exactJUnit = tamperedDirectory.resolve(M3AllocatorVerificationSealMain.TEST_XML);
        Files.writeString(exactJUnit, junitXml("0"));
        assertThatThrownBy(() -> M3AllocatorVerificationSealMain.main(new String[] {
                    tamperedRaw.toString(),
                    exactJUnit.toString(),
                    tamperedDirectory.resolve("raw-verification.json").toString()
                }))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("self hash differs");
    }

    private static String rawVerificationJson() throws Exception {
        String zeroed = "{\"schema\":\"NEREUS_V2_M3_ALLOCATOR_RAW_RECOMPUTATION_V1\","
                + "\"selfSha256\":\""
                + "0".repeat(64)
                + "\",\"selfHashRule\":\""
                + M3AllocatorEvidenceVerifyMain.SELF_HASH_RULE
                + "\",\"status\":\"PASS_RAW_RECOMPUTED\",\"note\":\"A\"}\n";
        String digest = HexFormat.of()
                .formatHex(MessageDigest.getInstance("SHA-256").digest(zeroed.getBytes(StandardCharsets.UTF_8)));
        return zeroed.replaceFirst("0{64}", digest);
    }

    private static VersionedAllocatorCellStateV1 exactCell(long consumed, long nextGrantId, int version) {
        long start = VirtualLedgerSliceAssignmentV1.RESERVED_START_INCLUSIVE;
        return new VersionedAllocatorCellStateV1(
                new VirtualLedgerCellAllocatorStateV1(
                        AllocatorModeV1.RANGE_LEASED,
                        VirtualLedgerCellAllocatorStateV1.PROTOCOL_VERSION,
                        Sha256Digest.hash(CanonicalBytes.copyOf("namespace".getBytes(StandardCharsets.UTF_8))),
                        Sha256Digest.hash(CanonicalBytes.copyOf("assignment".getBytes(StandardCharsets.UTF_8))),
                        start,
                        Math.addExact(start, VirtualLedgerSliceAssignmentV1.SLICE_SIZE - 1),
                        Math.addExact(start, consumed),
                        nextGrantId,
                        Optional.empty()),
                new MetadataVersion(CanonicalBytes.copyOf(new byte[] {(byte) version})));
    }

    private static VersionedAllocatorCellStateV1 reservedCell(long consumed, long nextGrantId, int version) {
        VersionedAllocatorCellStateV1 exact = exactCell(consumed, nextGrantId, version);
        long rangeEnd = exact.value().nextSliceLedgerId();
        long rangeStart = rangeEnd - 16;
        CellAllocatorReservationV1 reservation = new CellAllocatorReservationV1(
                new ManagedLedgerIncarnationIdV1(
                        Sha256Digest.hash(CanonicalBytes.copyOf("incarnation".getBytes(StandardCharsets.UTF_8)))),
                nextGrantId - 1,
                rangeStart,
                rangeEnd,
                Sha256Digest.hash(CanonicalBytes.copyOf("request".getBytes(StandardCharsets.UTF_8))),
                new AllocatorHeadStateV1(ChainPointerV1.absent(), 0, 0, 0, rangeStart));
        return new VersionedAllocatorCellStateV1(
                new VirtualLedgerCellAllocatorStateV1(
                        exact.value().mode(),
                        exact.value().allocatorProtocolVersion(),
                        exact.value().ledgerIdCompatibilityNamespaceId(),
                        exact.value().sliceAssignmentId(),
                        exact.value().sliceStartInclusive(),
                        exact.value().sliceEndInclusive(),
                        exact.value().nextSliceLedgerId(),
                        exact.value().nextGrantId(),
                        Optional.of(reservation)),
                exact.metadataVersion());
    }

    private static String junitXml(String skipped) {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                + "<testsuite name=\"exact\" tests=\"1\" skipped=\""
                + skipped
                + "\" failures=\"0\" errors=\"0\">\n"
                + "  <testcase name=\""
                + M3AllocatorVerificationSealMain.TEST_CASE
                + "\" classname=\""
                + M3AllocatorVerificationSealMain.TEST_CLASS
                + "\"/>\n"
                + "</testsuite>\n";
    }

    private static Sha256Digest digest(String value) {
        return Sha256Digest.hash(CanonicalBytes.copyOf(value.getBytes(StandardCharsets.UTF_8)));
    }

    private static final class PendingConditionalClient implements OxiaConditionalClient {
        private final List<CompletableFuture<Void>> mutations = new CopyOnWriteArrayList<>();

        @Override
        public CompletionStage<Optional<AuthorityRecord>> read(String key) {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        @Override
        public CompletionStage<Void> createIfAbsent(String key, CanonicalBytes storedBytes) {
            return mutation();
        }

        @Override
        public CompletionStage<Void> compareAndSet(
                String key, CanonicalBytes storedBytes, long expectedVersionId) {
            return mutation();
        }

        private CompletionStage<Void> mutation() {
            CompletableFuture<Void> pending = new CompletableFuture<>();
            mutations.add(pending);
            return pending;
        }
    }

    private static final class PendingReadConditionalClient implements OxiaConditionalClient {
        private final List<CompletableFuture<Optional<AuthorityRecord>>> reads = new CopyOnWriteArrayList<>();

        @Override
        public CompletionStage<Optional<AuthorityRecord>> read(String key) {
            CompletableFuture<Optional<AuthorityRecord>> pending = new CompletableFuture<>();
            reads.add(pending);
            return pending;
        }

        @Override
        public CompletionStage<Void> createIfAbsent(String key, CanonicalBytes storedBytes) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> compareAndSet(
                String key, CanonicalBytes storedBytes, long expectedVersionId) {
            return CompletableFuture.completedFuture(null);
        }
    }

    private static final class AcknowledgedConditionalClient implements OxiaConditionalClient {
        private int acknowledgedCreates;
        private int acknowledgedCas;
        private int legacyMutations;

        @Override
        public CompletionStage<Optional<AuthorityRecord>> read(String key) {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        @Override
        public CompletionStage<Void> createIfAbsent(String key, CanonicalBytes storedBytes) {
            legacyMutations++;
            return CompletableFuture.failedFuture(new AssertionError("legacy create path was invoked"));
        }

        @Override
        public CompletionStage<Optional<MutationAcknowledgement>> createIfAbsentAcknowledged(
                String key, CanonicalBytes storedBytes) {
            acknowledgedCreates++;
            return CompletableFuture.completedFuture(Optional.of(new MutationAcknowledgement(key, 7)));
        }

        @Override
        public CompletionStage<Void> compareAndSet(
                String key, CanonicalBytes storedBytes, long expectedVersionId) {
            legacyMutations++;
            return CompletableFuture.failedFuture(new AssertionError("legacy CAS path was invoked"));
        }

        @Override
        public CompletionStage<Optional<MutationAcknowledgement>> compareAndSetAcknowledged(
                String key, CanonicalBytes storedBytes, long expectedVersionId) {
            acknowledgedCas++;
            return CompletableFuture.completedFuture(Optional.of(new MutationAcknowledgement(key, 8)));
        }
    }
}
