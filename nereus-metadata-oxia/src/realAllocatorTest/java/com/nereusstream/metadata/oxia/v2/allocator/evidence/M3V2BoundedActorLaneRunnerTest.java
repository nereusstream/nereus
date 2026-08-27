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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.DeploymentId;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.PulsarCellId;
import com.nereusstream.domain.identity.ReservationDomainId;
import com.nereusstream.domain.registry.VirtualLedgerSliceAssignmentV1;
import com.nereusstream.domain.registry.VirtualLedgerSliceLifecycleV1;
import com.nereusstream.domain.registry.VirtualLedgerSliceViewV1;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.Candidate;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.Cell;
import com.nereusstream.domain.registry.allocator.ManagedLedgerAllocatorHeadV1;
import com.nereusstream.domain.registry.allocator.ManagedLedgerIncarnationIdV1;
import com.nereusstream.metadata.spi.allocator.BoundedVirtualLedgerAllocatorWorkflowV2.Request;
import com.nereusstream.metadata.spi.allocator.VersionedManagedLedgerAllocatorHeadV1;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import com.nereusstream.metadata.spi.model.VersionedVirtualLedgerSliceViewV1;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import org.junit.jupiter.api.Test;

class M3V2BoundedActorLaneRunnerTest {
    private static final Duration NO_WARMUP = Duration.ZERO;
    private static final Duration MEASUREMENT = Duration.ofMillis(150);
    private static final Duration CLEANUP = Duration.ofMillis(150);

    @Test
    void fourActorsReachCellConcurrencyFourButEveryLaneRemainsOne() throws Exception {
        M3V2BoundedActorLaneRunner<String> runner = runner();
        List<CompletableFuture<Void>> gates = futures(4);
        CountDownLatch started = new CountDownLatch(4);
        Thread release = releaseWhenStarted(started, gates);

        var result = runner.run(2, fourActorSchedule(), (actorId, request) -> {
            started.countDown();
            return gates.get(actorId);
        });
        release.join();

        assertThat(result.queueCapacity()).isEqualTo(4);
        assertThat(result.laneQueueCapacities()).containsExactly(1, 1, 1, 1);
        assertThat(result.offered()).isEqualTo(4);
        assertThat(result.admitted()).isEqualTo(4);
        assertThat(result.completed()).isEqualTo(4);
        assertThat(result.inFlightMaximum()).isEqualTo(4);
        assertThat(result.perActorInFlightMaximum()).containsExactly(1, 1, 1, 1);
        assertConservation(result);
    }

    @Test
    void physicalQueueDropsOverflowAndCutoffBacklogWithoutSyntheticAdmission() throws Exception {
        M3V2BoundedActorLaneRunner<String> runner =
                new M3V2BoundedActorLaneRunner<>(NO_WARMUP, Duration.ofMillis(80), CLEANUP);
        CompletableFuture<Void> active = new CompletableFuture<>();
        AtomicInteger operationCalls = new AtomicInteger();
        Thread release = new Thread(() -> {
            try {
                Thread.sleep(110);
                active.complete(null);
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
            }
        });
        release.start();
        List<M3V2BoundedActorLaneRunner.ScheduledOffer<String>> schedule = new ArrayList<>();
        for (int ordinal = 0; ordinal < 5; ordinal++) {
            schedule.add(offer(ordinal, 0, 0, "actor-zero-" + ordinal));
        }

        var result = runner.run(2, schedule, (actorId, request) -> {
            operationCalls.incrementAndGet();
            return active;
        });
        release.join();

        assertThat(result.laneQueueCapacities()).containsExactly(1, 1, 1, 1);
        assertThat(result.offered()).isEqualTo(5);
        assertThat(result.admitted()).isEqualTo(1);
        assertThat(operationCalls).hasValue(1);
        assertThat(result.overloadDroppedBeforeAdmission()).isEqualTo(4);
        assertThat(result.completed()).isEqualTo(1);
        assertThat(result.measuredTerminals())
                .filteredOn(terminal -> terminal.outcome()
                        == M3V2BoundedActorLaneRunner.TerminalOutcome.OVERLOAD_DROPPED_BEFORE_ADMISSION)
                .hasSize(4);
        assertThat(result.backlogAtEnd()).isZero();
        assertThat(result.inFlightAtEnd()).isZero();
        assertThat(result.waiterAtEnd()).isZero();
        assertConservation(result);
    }

    @Test
    void admittedRequestsEndInExactlyOneRealTerminalPartition() throws Exception {
        M3V2BoundedActorLaneRunner<String> runner =
                new M3V2BoundedActorLaneRunner<>(NO_WARMUP, Duration.ofMillis(80), Duration.ofMillis(80));
        CompletableFuture<Void> timeout = new CompletableFuture<>();

        var result = runner.run(2, fourActorSchedule(), (actorId, request) -> switch (actorId) {
            case 0, 3 -> CompletableFuture.completedFuture(null);
            case 1 -> CompletableFuture.failedFuture(new IllegalStateException("real allocation failure"));
            case 2 -> timeout;
            default -> throw new IllegalArgumentException("unknown actor");
        });

        assertThat(result.admitted()).isEqualTo(4);
        assertThat(result.completed()).isEqualTo(2);
        assertThat(result.failedAfterAdmission()).isEqualTo(1);
        assertThat(result.timedOutAfterAdmission()).isEqualTo(1);
        assertThat(result.terminal()).isEqualTo(4);
        assertThat(result.measuredTerminals()).extracting(value -> value.outcome()).containsExactlyInAnyOrder(
                M3V2BoundedActorLaneRunner.TerminalOutcome.COMPLETED,
                M3V2BoundedActorLaneRunner.TerminalOutcome.COMPLETED,
                M3V2BoundedActorLaneRunner.TerminalOutcome.FAILED_AFTER_ADMISSION,
                M3V2BoundedActorLaneRunner.TerminalOutcome.TIMED_OUT_AFTER_ADMISSION);
        assertConservation(result);
    }

    @Test
    void warmupIsSeparatedFromMeasuredConservationAndUsesTheSamePhysicalLanes() throws Exception {
        M3V2BoundedActorLaneRunner<String> runner = new M3V2BoundedActorLaneRunner<>(
                Duration.ofMillis(50), Duration.ofMillis(100), Duration.ofMillis(100));
        List<M3V2BoundedActorLaneRunner.ScheduledOffer<String>> schedule = List.of(
                new M3V2BoundedActorLaneRunner.ScheduledOffer<>(0, 0, 0, false, "warmup"),
                new M3V2BoundedActorLaneRunner.ScheduledOffer<>(
                        1, 1, TimeUnit.MICROSECONDS.toNanos(49_875), true, "one"),
                new M3V2BoundedActorLaneRunner.ScheduledOffer<>(
                        2, 2, TimeUnit.MICROSECONDS.toNanos(149_875), true, "two"));

        var result = runner.run(2, schedule, (actorId, request) -> CompletableFuture.completedFuture(null));

        assertThat(result.warmupOffered()).isEqualTo(1);
        assertThat(result.warmupCompleted()).isEqualTo(1);
        assertThat(result.offered()).isEqualTo(2);
        assertThat(result.terminal() + result.overloadDroppedBeforeAdmission()).isEqualTo(2);
        assertThat(result.backlogAtEnd()).isZero();
        assertThat(result.inFlightAtEnd()).isZero();
        assertConservation(result);
    }

    @Test
    void allSixFrozenRatesHaveOneOrdinalAuthoritativeTransitionBeforeCutoff() {
        Map<Integer, Long> firstMeasuredMicros = Map.of(
                200, 10_000_000L,
                250, 9_999_750L,
                333, 9_999_875L,
                500, 10_000_000L,
                750, 9_999_750L,
                1000, 10_000_000L);

        for (int rate : M3AllocatorWorkloadPlan.OFFERED_RATES) {
            List<M3V2BoundedActorLaneRunner.ScheduledOffer<String>> schedule = frozenSchedule(rate);
            M3V2BoundedActorLaneRunner.<String>formal().validateSchedule(schedule);
            int warmupRequests = Math.multiplyExact(M3AllocatorWorkloadPlan.WARM_UP_SECONDS, rate);
            int measuredRequests = M3AllocatorWorkloadPlan.measuredRequestCount(rate);
            long transitions = 0;
            boolean priorMeasured = false;
            for (M3V2BoundedActorLaneRunner.ScheduledOffer<String> offer : schedule) {
                if (!priorMeasured && offer.measured()) {
                    transitions++;
                }
                priorMeasured = offer.measured();
            }

            assertThat(schedule).hasSize(Math.addExact(warmupRequests, measuredRequests));
            assertThat(schedule.subList(0, warmupRequests)).noneMatch(
                    M3V2BoundedActorLaneRunner.ScheduledOffer::measured);
            assertThat(schedule.subList(warmupRequests, schedule.size())).allMatch(
                    M3V2BoundedActorLaneRunner.ScheduledOffer::measured);
            assertThat(transitions).as("rate %s measurement transitions", rate).isEqualTo(1);
            assertThat(TimeUnit.NANOSECONDS.toMicros(schedule.get(warmupRequests).arrivalOffsetNanos()))
                    .isEqualTo(firstMeasuredMicros.get(rate));
            assertThat(TimeUnit.NANOSECONDS.toMicros(schedule.get(schedule.size() - 1).arrivalOffsetNanos()))
                    .isLessThan(TimeUnit.SECONDS.toMicros(40));
        }

        assertThat(firstMeasuredMicros)
                .containsEntry(250, 9_999_750L)
                .containsEntry(333, 9_999_875L)
                .containsEntry(750, 9_999_750L);
    }

    @Test
    void scheduleRejectsWarmupAfterTheMeasuredTransition() {
        M3V2BoundedActorLaneRunner<String> formal = M3V2BoundedActorLaneRunner.formal();
        List<M3V2BoundedActorLaneRunner.ScheduledOffer<String>> schedule = List.of(
                new M3V2BoundedActorLaneRunner.ScheduledOffer<>(0, 0, 0, false, "warmup"),
                new M3V2BoundedActorLaneRunner.ScheduledOffer<>(1, 1, 1, true, "measured"),
                new M3V2BoundedActorLaneRunner.ScheduledOffer<>(2, 2, 2, false, "repeated-warmup"));

        assertThatThrownBy(() -> formal.validateSchedule(schedule))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("returned to warmup");
    }

    @Test
    void boundedWarmupOverloadIsConservedWithoutBecomingInfrastructureFailure() throws Exception {
        M3V2BoundedActorLaneRunner<String> runner = new M3V2BoundedActorLaneRunner<>(
                Duration.ofMillis(60), Duration.ofMillis(60), Duration.ofMillis(100));
        CompletableFuture<Void> warmupGate = new CompletableFuture<>();
        CountDownLatch warmupStarted = new CountDownLatch(1);
        Thread release = new Thread(() -> {
            try {
                if (!warmupStarted.await(1, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("warmup request did not start");
                }
                Thread.sleep(20);
                warmupGate.complete(null);
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
            }
        });
        release.start();
        List<M3V2BoundedActorLaneRunner.ScheduledOffer<String>> schedule = new ArrayList<>();
        for (int ordinal = 0; ordinal < 5; ordinal++) {
            schedule.add(new M3V2BoundedActorLaneRunner.ScheduledOffer<>(
                    ordinal, 0, 0, false, "warmup-" + ordinal));
        }
        schedule.add(new M3V2BoundedActorLaneRunner.ScheduledOffer<>(
                5, 1, TimeUnit.MILLISECONDS.toNanos(60), true, "measured"));

        var result = runner.run(2, schedule, (actorId, request) -> {
            if (request.startsWith("warmup")) {
                warmupStarted.countDown();
                return warmupGate;
            }
            return CompletableFuture.completedFuture(null);
        });
        release.join();

        assertThat(result.warmupOffered()).isEqualTo(5);
        assertThat(result.warmupDroppedBeforeAdmission()).isGreaterThanOrEqualTo(3);
        assertThat(result.offered()).isEqualTo(1);
        assertThat(result.completed()).isEqualTo(1);
        assertThat(M3V2AllocatorFormalHarness.infrastructureValid(result)).isTrue();
        assertWarmupConservation(result);
        assertConservation(result);
    }

    @Test
    void admittedWarmupFailureRemainsInfrastructureInvalidWithExactDetail() throws Exception {
        M3V2BoundedActorLaneRunner<String> runner = new M3V2BoundedActorLaneRunner<>(
                Duration.ofMillis(20), Duration.ofMillis(30), Duration.ofMillis(50));
        List<M3V2BoundedActorLaneRunner.ScheduledOffer<String>> schedule = List.of(
                new M3V2BoundedActorLaneRunner.ScheduledOffer<>(0, 0, 0, false, "warmup"),
                new M3V2BoundedActorLaneRunner.ScheduledOffer<>(
                        1, 1, TimeUnit.MILLISECONDS.toNanos(20), true, "measured"));

        var result = runner.run(2, schedule, (actorId, request) -> request.equals("warmup")
                ? CompletableFuture.failedFuture(new IllegalStateException("warmup failure"))
                : CompletableFuture.completedFuture(null));

        assertThat(result.warmupFailedAfterAdmission()).isEqualTo(1);
        assertThat(M3V2AllocatorFormalHarness.infrastructureValid(result)).isFalse();
        assertThat(M3V2AllocatorFormalHarness.infrastructureDetail(result))
                .contains("actorLanesStoppedAtCleanupDeadline=true")
                .contains("warmupFailedAfterAdmission=1")
                .contains("warmupTimedOutAfterAdmission=0");
        assertWarmupConservation(result);
        assertConservation(result);
    }

    @Test
    void harnessRoutesEveryActorToAnIndependentCoordinatorAndRevalidatesConservation() throws Exception {
        List<AtomicInteger> calls = List.of(
                new AtomicInteger(), new AtomicInteger(), new AtomicInteger(), new AtomicInteger());
        List<M3V2AllocatorFormalHarness.ActorEndpoint> endpoints = new ArrayList<>();
        for (int actorId = 0; actorId < 4; actorId++) {
            int exactActor = actorId;
            Object identity = new Object();
            endpoints.add(new M3V2AllocatorFormalHarness.ActorEndpoint(exactActor, identity, request -> {
                calls.get(exactActor).incrementAndGet();
                return CompletableFuture.completedFuture(null);
            }));
        }
        M3V2AllocatorFormalHarness harness = M3V2AllocatorFormalHarness.forContractTest(
                NO_WARMUP, MEASUREMENT, CLEANUP, endpoints);
        Request request = request();
        List<M3V2BoundedActorLaneRunner.ScheduledOffer<M3V2AllocatorFormalHarness.CandidateRequest>> schedule = List.of(
                requestOffer(0, 0, request),
                requestOffer(1, 1, request),
                requestOffer(2, 2, request),
                requestOffer(3, 3, request));

        var result = harness.runCandidate(
                Cell.of(Candidate.STRICT, 10_000, 1, 200),
                schedule,
                M3V2AllocatorFormalHarness.SupplementaryMeasurements::empty);

        assertThat(calls).extracting(AtomicInteger::get).containsExactly(1, 1, 1, 1);
        assertThat(result.infrastructureValid()).isTrue();
        assertThat(result.evidence().offered()).isEqualTo(4);
        assertThat(result.evidence().admitted()).isEqualTo(4);
        assertThat(result.evidence().terminal()).isEqualTo(4);
        assertThat(result.runnerResult().perActorInFlightMaximum()).containsExactly(1, 1, 1, 1);
    }

    @Test
    void harnessRejectsSharedCoordinatorIdentityAndNativeCandidateRouting() {
        Object shared = new Object();
        List<M3V2AllocatorFormalHarness.ActorEndpoint> endpoints = new ArrayList<>();
        for (int actorId = 0; actorId < 4; actorId++) {
            endpoints.add(new M3V2AllocatorFormalHarness.ActorEndpoint(
                    actorId, shared, request -> CompletableFuture.completedFuture(null)));
        }

        assertThatThrownBy(() -> M3V2AllocatorFormalHarness.forContractTest(
                        NO_WARMUP, MEASUREMENT, CLEANUP, endpoints))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("identities");
    }

    @Test
    void runnerAndFormalHarnessContainNoSharedJavaCorrectnessLockOrWorkerPool() {
        List<Class<?>> forbidden = List.of(Lock.class, java.util.concurrent.ThreadPoolExecutor.class);

        assertThat(Arrays.stream(M3V2BoundedActorLaneRunner.class.getDeclaredFields()).map(Field::getType))
                .noneMatch(type -> forbidden.stream().anyMatch(forbiddenType -> forbiddenType.isAssignableFrom(type)));
        assertThat(Arrays.stream(M3V2AllocatorFormalHarness.class.getDeclaredFields()).map(Field::getType))
                .noneMatch(type -> forbidden.stream().anyMatch(forbiddenType -> forbiddenType.isAssignableFrom(type)));
    }

    private static M3V2BoundedActorLaneRunner<String> runner() {
        return new M3V2BoundedActorLaneRunner<>(NO_WARMUP, MEASUREMENT, CLEANUP);
    }

    private static List<M3V2BoundedActorLaneRunner.ScheduledOffer<String>> fourActorSchedule() {
        return List.of(
                offer(0, 0, 0, "zero"),
                offer(1, 1, 0, "one"),
                offer(2, 2, 0, "two"),
                offer(3, 3, 0, "three"));
    }

    private static List<M3V2BoundedActorLaneRunner.ScheduledOffer<String>> frozenSchedule(int rate) {
        List<M3V2BoundedActorLaneRunner.ScheduledOffer<String>> schedule =
                new ArrayList<>(M3AllocatorWorkloadPlan.requestCount(rate));
        for (M3AllocatorWorkloadPlan.PlannedRequest request : M3AllocatorWorkloadPlan.requests(10_000, rate)) {
            schedule.add(new M3V2BoundedActorLaneRunner.ScheduledOffer<>(
                    request.requestOrdinal(),
                    request.actorId(),
                    TimeUnit.MICROSECONDS.toNanos(request.arrivalOffsetMicros()),
                    request.phase() != M3AllocatorWorkloadPlan.Phase.WARM_UP,
                    "request-" + request.requestOrdinal()));
        }
        return List.copyOf(schedule);
    }

    private static M3V2BoundedActorLaneRunner.ScheduledOffer<String> offer(
            long ordinal, int actorId, long offsetNanos, String request) {
        return new M3V2BoundedActorLaneRunner.ScheduledOffer<>(ordinal, actorId, offsetNanos, true, request);
    }

    private static M3V2BoundedActorLaneRunner.ScheduledOffer<M3V2AllocatorFormalHarness.CandidateRequest> requestOffer(
            long ordinal, int actorId, Request request) {
        return new M3V2BoundedActorLaneRunner.ScheduledOffer<>(
                ordinal,
                actorId,
                0,
                true,
                new M3V2AllocatorFormalHarness.CandidateRequest(ordinal, 0, request));
    }

    private static List<CompletableFuture<Void>> futures(int size) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int index = 0; index < size; index++) {
            futures.add(new CompletableFuture<>());
        }
        return futures;
    }

    private static Thread releaseWhenStarted(CountDownLatch started, List<CompletableFuture<Void>> gates) {
        Thread release = new Thread(() -> {
            try {
                if (!started.await(1, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("four allocator actors did not start");
                }
                gates.forEach(future -> future.complete(null));
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
            }
        });
        release.start();
        return release;
    }

    private static void assertConservation(M3V2BoundedActorLaneRunner.IntervalResult result) {
        assertThat(result.offered())
                .isEqualTo(result.overloadDroppedBeforeAdmission()
                        + result.completed()
                        + result.failedAfterAdmission()
                        + result.timedOutAfterAdmission());
        assertThat(result.admitted())
                .isEqualTo(result.completed() + result.failedAfterAdmission() + result.timedOutAfterAdmission());
        assertThat(result.terminal()).isEqualTo(result.admitted());
        assertThat(result.measuredTerminals()).hasSize(Math.toIntExact(result.offered()));
    }

    private static void assertWarmupConservation(M3V2BoundedActorLaneRunner.IntervalResult result) {
        assertThat(result.warmupOffered())
                .isEqualTo(result.warmupDroppedBeforeAdmission()
                        + result.warmupCompleted()
                        + result.warmupFailedAfterAdmission()
                        + result.warmupTimedOutAfterAdmission());
    }

    private static Request request() {
        Sha256Digest namespace = digest("namespace");
        VirtualLedgerSliceAssignmentV1 assignment = VirtualLedgerSliceAssignmentV1.create(
                new DeploymentId(new Id128(1, 2)),
                new ReservationDomainId(new Id128(3, 4)),
                new PulsarCellId(new Id128(5, 6)),
                namespace,
                VirtualLedgerSliceAssignmentV1.RESERVED_START_INCLUSIVE,
                VirtualLedgerSliceLifecycleV1.ACTIVE);
        VersionedVirtualLedgerSliceViewV1 view = new VersionedVirtualLedgerSliceViewV1(
                new VirtualLedgerSliceViewV1(namespace, 1, assignment), version(1), digest("registry"));
        ManagedLedgerAllocatorHeadV1 head = ManagedLedgerAllocatorHeadV1.initial(
                new ManagedLedgerIncarnationIdV1(digest("ledger")),
                10,
                VirtualLedgerSliceAssignmentV1.RESERVED_START_INCLUSIVE);
        VersionedManagedLedgerAllocatorHeadV1 versionedHead = new VersionedManagedLedgerAllocatorHeadV1(
                namespace, assignment.sliceAssignmentId(), "/allocator/head", head, version(2));
        return new Request(digest("request"), digest("descriptor"), view, versionedHead);
    }

    private static Sha256Digest digest(String value) {
        return Sha256Digest.hash(CanonicalBytes.copyOf(value.getBytes(StandardCharsets.UTF_8)));
    }

    private static MetadataVersion version(long value) {
        return new MetadataVersion(CanonicalBytes.copyOf(ByteBuffer.allocate(8).putLong(value).array()));
    }
}
