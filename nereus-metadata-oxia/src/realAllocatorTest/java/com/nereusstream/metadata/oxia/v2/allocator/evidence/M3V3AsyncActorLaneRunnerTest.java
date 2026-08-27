/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.metadata.oxia.v2.allocator.evidence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.registry.allocator.AllocatorEvidenceAdmissionPolicyV3;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import org.junit.jupiter.api.Test;

class M3V3AsyncActorLaneRunnerTest {
    private static final Duration NO_WARMUP = Duration.ZERO;

    @Test
    void evidenceAdmissionCapIsDerivedFromFrozenRateLatencyAndActorCount() {
        assertThat(AllocatorEvidenceAdmissionPolicyV3.exactOutstandingPerActor(1000, 250, 4))
                .isEqualTo(63);
        assertThat(AllocatorEvidenceAdmissionPolicyV3.MAX_ASYNC_OUTSTANDING_PER_ACTOR)
                .isEqualTo(64);
        assertThat(AllocatorEvidenceAdmissionPolicyV3.MAX_GLOBAL_OUTSTANDING)
                .isEqualTo(256);
        assertThat(AllocatorEvidenceAdmissionPolicyV3.preAdmissionQueueCapacity(1000))
                .isEqualTo(2000);
    }

    @Test
    void dispatcherReachesEveryFrozenOutstandingLevelWithoutBlockingOnCompletion() throws Exception {
        for (int outstandingPerActor : List.of(1, 4, 16, 64)) {
            int total = Math.multiplyExact(4, outstandingPerActor);
            M3V3AsyncActorLaneRunner<String> runner = runner(Duration.ofMillis(40), Duration.ofMillis(400));
            List<CompletableFuture<Void>> completions = futures(total);
            CountDownLatch dispatched = new CountDownLatch(total);
            Thread release = releaseInReverseWhenStarted(dispatched, completions);

            var result = runner.run(Math.max(2, total / 2), uniqueSchedule(total), (actor, request, context) -> {
                dispatched.countDown();
                return completions.get(Integer.parseInt(request));
            });
            release.join();

            assertThat(result.completed()).isEqualTo(total);
            assertThat(result.globalOutstandingMaximum()).isEqualTo(total);
            assertThat(result.perActorOutstandingMaximum())
                    .containsExactly(
                            outstandingPerActor, outstandingPerActor, outstandingPerActor, outstandingPerActor);
            assertThat(result.measuredTerminals())
                    .extracting(M3V3AsyncActorLaneRunner.TerminalRecord::ordinal)
                    .containsExactlyElementsOf(longRange(total));
            assertConservation(result);
        }
    }

    @Test
    void controlledLatencyFuturesCoverFrozenAndDerivedRatesIncludingTwoHundredFiftyMillis() throws Exception {
        List<Integer> rates = List.of(1000, 800, 750, 600, 500, 400, 333, 267, 250, 200);
        List<Integer> latencies = List.of(1, 5, 10, 25, 250);
        for (int rate : rates) {
            for (int latency : latencies) {
                M3V3AsyncActorLaneRunner<String> runner =
                        runner(Duration.ofMillis(20), Duration.ofMillis(latency + 100L));
                var result = runner.run(rate, uniqueSchedule(4), (actor, request, context) -> {
                    CompletableFuture<Void> completion = new CompletableFuture<>();
                    CompletableFuture.delayedExecutor(latency, TimeUnit.MILLISECONDS).execute(() -> {
                        context.markOperationCompleted();
                        completion.complete(null);
                    });
                    return completion;
                });

                assertThat(result.completed()).as("rate=%s latency=%s", rate, latency).isEqualTo(4);
                assertThat(result.failedAfterAdmission()).isZero();
                assertThat(result.timedOutAfterAdmission()).isZero();
                assertConservation(result);
            }
        }
    }

    @Test
    void callbackReorderingStillProducesOneCanonicalTerminalPerOrdinal() throws Exception {
        int total = 32;
        List<CompletableFuture<Void>> completions = futures(total);
        CountDownLatch dispatched = new CountDownLatch(total);
        Thread release = releaseInReverseWhenStarted(dispatched, completions);

        var result = runner(Duration.ofMillis(40), Duration.ofMillis(400))
                .run(16, uniqueSchedule(total), (actor, request, context) -> {
                    dispatched.countDown();
                    return completions.get(Integer.parseInt(request));
                });
        release.join();

        assertThat(result.measuredTerminals())
                .extracting(M3V3AsyncActorLaneRunner.TerminalRecord::ordinal)
                .containsExactlyElementsOf(longRange(total));
        assertThat(result.measuredTerminals()).doesNotHaveDuplicates();
        assertThat(result.measuredTelemetry()).hasSize(total);
        assertConservation(result);
    }

    @Test
    void cutoffKeepsUndispatchedRequestsInThePreAdmissionDropPartition() throws Exception {
        M3V3AsyncActorLaneRunner<String> runner =
                runner(Duration.ofMillis(40), Duration.ofMillis(120));
        CompletableFuture<Void> first = new CompletableFuture<>();
        AtomicInteger calls = new AtomicInteger();
        Thread release = new Thread(() -> {
            try {
                Thread.sleep(80);
                first.complete(null);
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
            }
        });
        release.start();
        List<M3V3AsyncActorLaneRunner.ScheduledOffer<String>> schedule = new ArrayList<>();
        for (int ordinal = 0; ordinal < 5; ordinal++) {
            schedule.add(offer(ordinal, 0, 7, 0, "same-binding"));
        }

        var result = runner.run(2, schedule, (actor, request, context) -> {
            calls.incrementAndGet();
            return first;
        });
        release.join();

        assertThat(calls).hasValue(1);
        assertThat(result.admitted()).isEqualTo(1);
        assertThat(result.completed()).isEqualTo(1);
        assertThat(result.overloadDroppedBeforeAdmission()).isEqualTo(4);
        assertThat(result.queueDepthAtEnd()).isZero();
        assertThat(result.globalOutstandingAtEnd()).isZero();
        assertThat(result.bindingBusyAtEnd()).isZero();
        assertConservation(result);
    }

    @Test
    void cleanupTimeoutClosesTheWorkflowGuardAndLateCompletionCannotDispatchNextOperation() throws Exception {
        M3V3AsyncActorLaneRunner<String> runner =
                runner(Duration.ofMillis(20), Duration.ofMillis(30));
        CompletableFuture<Void> firstMetadataOperation = new CompletableFuture<>();
        AtomicInteger nextMetadataDispatches = new AtomicInteger();

        var result = runner.run(2, List.of(offer(0, 0, 0, 0, "timeout")), (actor, request, context) ->
                firstMetadataOperation.thenRun(() -> {
                    if (context.allowsNextMetadataOperation()) {
                        nextMetadataDispatches.incrementAndGet();
                    }
                }));

        assertThat(result.timedOutAfterAdmission()).isEqualTo(1);
        assertThat(result.completed()).isZero();
        firstMetadataOperation.complete(null);
        Thread.sleep(20);
        assertThat(nextMetadataDispatches).hasValue(0);
        assertConservation(result);
    }

    @Test
    void normalIntervalsSingleFlightBindingsWhileConflictProofRetainsSameKeyConcurrency() throws Exception {
        assertBindingConcurrency(M3V3AsyncActorLaneRunner.AdmissionMode.NORMAL_SINGLE_FLIGHT, 1);
        assertBindingConcurrency(M3V3AsyncActorLaneRunner.AdmissionMode.CONFLICT_PROOF, 4);
    }

    @Test
    void everyFrozenRateRetainsOneOrdinalAuthoritativeMeasurementTransition() {
        Map<Integer, Long> firstMeasuredMicros = Map.of(
                200, 10_000_000L,
                250, 9_999_750L,
                333, 9_999_875L,
                500, 10_000_000L,
                750, 9_999_750L,
                1000, 10_000_000L);

        for (int rate : M3AllocatorWorkloadPlan.OFFERED_RATES) {
            List<M3V3AsyncActorLaneRunner.ScheduledOffer<String>> schedule = frozenSchedule(rate);
            M3V3AsyncActorLaneRunner.<String>formal().validateSchedule(schedule);
            int warmupRequests = Math.multiplyExact(M3AllocatorWorkloadPlan.WARM_UP_SECONDS, rate);
            assertThat(schedule.subList(0, warmupRequests))
                    .noneMatch(M3V3AsyncActorLaneRunner.ScheduledOffer::measured);
            assertThat(schedule.subList(warmupRequests, schedule.size()))
                    .allMatch(M3V3AsyncActorLaneRunner.ScheduledOffer::measured);
            assertThat(TimeUnit.NANOSECONDS.toMicros(schedule.get(warmupRequests).arrivalOffsetNanos()))
                    .isEqualTo(firstMeasuredMicros.get(rate));
            assertThat(TimeUnit.NANOSECONDS.toMicros(schedule.get(schedule.size() - 1).arrivalOffsetNanos()))
                    .isLessThan(TimeUnit.SECONDS.toMicros(40));
        }
    }

    @Test
    void scheduleRejectsWarmupAfterMeasurementAndRunnerContainsNoCorrectnessLockOrWorkerPool() {
        M3V3AsyncActorLaneRunner<String> formal = M3V3AsyncActorLaneRunner.formal();
        List<M3V3AsyncActorLaneRunner.ScheduledOffer<String>> invalid = List.of(
                new M3V3AsyncActorLaneRunner.ScheduledOffer<>(0, 0, 0, 0, false, "warmup"),
                new M3V3AsyncActorLaneRunner.ScheduledOffer<>(1, 1, 1, 1, true, "measured"),
                new M3V3AsyncActorLaneRunner.ScheduledOffer<>(2, 2, 2, 2, false, "warmup-again"));

        assertThatThrownBy(() -> formal.validateSchedule(invalid))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("returned to warmup");

        List<Class<?>> forbidden = List.of(Lock.class, java.util.concurrent.ThreadPoolExecutor.class);
        assertThat(Arrays.stream(M3V3AsyncActorLaneRunner.class.getDeclaredFields()).map(Field::getType))
                .noneMatch(type -> forbidden.stream().anyMatch(forbiddenType -> forbiddenType.isAssignableFrom(type)));
    }

    private static void assertBindingConcurrency(
            M3V3AsyncActorLaneRunner.AdmissionMode mode, int expectedMaximum) throws Exception {
        M3V3AsyncActorLaneRunner<String> runner = runner(Duration.ofMillis(40), Duration.ofMillis(300));
        List<CompletableFuture<Void>> completions = futures(4);
        AtomicInteger active = new AtomicInteger();
        AtomicInteger maximum = new AtomicInteger();
        AtomicInteger calls = new AtomicInteger();
        Thread release = new Thread(() -> {
            try {
                while (calls.get() < expectedMaximum) {
                    Thread.sleep(1);
                }
                for (int index = 0; index < completions.size(); index++) {
                    completions.get(index).complete(null);
                    if (mode == M3V3AsyncActorLaneRunner.AdmissionMode.NORMAL_SINGLE_FLIGHT) {
                        while (calls.get() <= index && index + 1 < completions.size()) {
                            Thread.sleep(1);
                        }
                    }
                }
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
            }
        });
        release.start();

        var result = runner.run(2, sameBindingSchedule(), mode, (actor, request, context) -> {
            int index = calls.getAndIncrement();
            int now = active.incrementAndGet();
            maximum.accumulateAndGet(now, Math::max);
            return completions.get(index).whenComplete((ignored, failure) -> active.decrementAndGet());
        });
        release.join();

        assertThat(maximum).hasValue(expectedMaximum);
        assertThat(result.completed()).isEqualTo(4);
        assertConservation(result);
    }

    private static M3V3AsyncActorLaneRunner<String> runner(Duration measurement, Duration cleanup) {
        return new M3V3AsyncActorLaneRunner<>(NO_WARMUP, measurement, cleanup);
    }

    private static List<M3V3AsyncActorLaneRunner.ScheduledOffer<String>> uniqueSchedule(int total) {
        List<M3V3AsyncActorLaneRunner.ScheduledOffer<String>> schedule = new ArrayList<>(total);
        for (int ordinal = 0; ordinal < total; ordinal++) {
            schedule.add(offer(ordinal, ordinal % 4, ordinal, 0, Integer.toString(ordinal)));
        }
        return List.copyOf(schedule);
    }

    private static List<M3V3AsyncActorLaneRunner.ScheduledOffer<String>> sameBindingSchedule() {
        return List.of(
                offer(0, 0, 9, 0, "zero"),
                offer(1, 1, 9, 0, "one"),
                offer(2, 2, 9, 0, "two"),
                offer(3, 3, 9, 0, "three"));
    }

    private static List<M3V3AsyncActorLaneRunner.ScheduledOffer<String>> frozenSchedule(int rate) {
        List<M3V3AsyncActorLaneRunner.ScheduledOffer<String>> schedule =
                new ArrayList<>(M3AllocatorWorkloadPlan.requestCount(rate));
        for (M3AllocatorWorkloadPlan.PlannedRequest request : M3AllocatorWorkloadPlan.requests(10_000, rate)) {
            schedule.add(new M3V3AsyncActorLaneRunner.ScheduledOffer<>(
                    request.requestOrdinal(),
                    request.actorId(),
                    request.ledgerIndex(),
                    TimeUnit.MICROSECONDS.toNanos(request.arrivalOffsetMicros()),
                    request.phase() != M3AllocatorWorkloadPlan.Phase.WARM_UP,
                    "request-" + request.requestOrdinal()));
        }
        return List.copyOf(schedule);
    }

    private static M3V3AsyncActorLaneRunner.ScheduledOffer<String> offer(
            long ordinal, int actor, long binding, long offsetNanos, String request) {
        return new M3V3AsyncActorLaneRunner.ScheduledOffer<>(
                ordinal, actor, binding, offsetNanos, true, request);
    }

    private static List<CompletableFuture<Void>> futures(int size) {
        List<CompletableFuture<Void>> futures = new ArrayList<>(size);
        for (int index = 0; index < size; index++) {
            futures.add(new CompletableFuture<>());
        }
        return futures;
    }

    private static Thread releaseInReverseWhenStarted(
            CountDownLatch dispatched, List<CompletableFuture<Void>> completions) {
        Thread release = new Thread(() -> {
            try {
                if (!dispatched.await(2, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("allocator V3 dispatcher blocked before reaching its cap");
                }
                List<CompletableFuture<Void>> reverse = new ArrayList<>(completions);
                Collections.reverse(reverse);
                reverse.forEach(completion -> completion.complete(null));
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
            }
        });
        release.start();
        return release;
    }

    private static List<Long> longRange(int size) {
        List<Long> values = new ArrayList<>(size);
        for (long value = 0; value < size; value++) {
            values.add(value);
        }
        return values;
    }

    private static void assertConservation(M3V3AsyncActorLaneRunner.IntervalResult result) {
        assertThat(result.offered())
                .isEqualTo(result.overloadDroppedBeforeAdmission()
                        + result.completed()
                        + result.failedAfterAdmission()
                        + result.timedOutAfterAdmission());
        assertThat(result.admitted())
                .isEqualTo(result.completed() + result.failedAfterAdmission() + result.timedOutAfterAdmission());
        assertThat(result.terminal()).isEqualTo(result.admitted());
        assertThat(result.measuredTerminals()).hasSize(Math.toIntExact(result.offered()));
        assertThat(result.measuredTelemetry()).hasSize(Math.toIntExact(result.offered()));
    }
}
