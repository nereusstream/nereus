/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.metadata.oxia.v2.allocator.evidence;

import static org.assertj.core.api.Assertions.assertThat;
import com.nereusstream.domain.registry.allocator.AllocatorEvidenceAdmissionPolicyV5;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import org.junit.jupiter.api.Test;

class M3V5AsyncActorLaneRunnerTest {
    @Test
    void reachesTheBoundedStormAdmissionCapWithoutChangingPerBindingSingleFlight() throws Exception {
        int total = AllocatorEvidenceAdmissionPolicyV5.MAX_GLOBAL_OUTSTANDING;
        M3V3AsyncActorLaneRunner<String> runner = M3V3AsyncActorLaneRunner.v5(
                Duration.ZERO,
                Duration.ofMillis(200),
                Duration.ofSeconds(1),
                Duration.ofSeconds(1),
                AllocatorEvidenceAdmissionPolicyV5.MAX_ASYNC_OUTSTANDING_PER_ACTOR,
                AllocatorEvidenceAdmissionPolicyV5.MAX_GLOBAL_OUTSTANDING);
        List<CompletableFuture<Void>> completions = new ArrayList<>(total);
        for (int index = 0; index < total; index++) {
            completions.add(new CompletableFuture<>());
        }
        CountDownLatch dispatched = new CountDownLatch(total);
        AtomicInteger index = new AtomicInteger();
        Thread release = new Thread(() -> {
            try {
                if (!dispatched.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("allocator V5 dispatcher did not reach its bounded storm cap");
                }
                completions.forEach(completion -> completion.complete(null));
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
            }
        });
        release.start();

        var result = runner.run(2_000, uniqueSchedule(total), (actor, request, context) -> {
            int exact = index.getAndIncrement();
            dispatched.countDown();
            return completions.get(exact);
        });
        release.join();

        assertThat(result.perActorOutstandingMaximum()).containsExactly(128, 128, 128, 128);
        assertThat(result.globalOutstandingMaximum()).isEqualTo(512);
        assertThat(result.overloadDroppedBeforeAdmission()).isZero();
        assertThat(result.completed()).isEqualTo(512);
        assertThat(result.bindingBusyMaximum()).isEqualTo(512);
        assertThat(result.globalOutstandingAtEnd()).isZero();
        assertThat(result.bindingBusyAtEnd()).isZero();
        assertThat(result.pendingPermitAtEnd()).isZero();
        assertThat(result.offered()).isEqualTo(result.completed());
        assertThat(result.terminal()).isEqualTo(result.admitted());
    }

    @Test
    void frozenTargetMayArrivePhysicallyLateButStillEnterTheV5AdmissionDrain() throws Exception {
        Duration measurement = Duration.ofMillis(100);
        Duration drain = Duration.ofMillis(250);
        Duration cleanup = Duration.ofMillis(250);
        List<M3V3AsyncActorLaneRunner.ScheduledOffer<String>> schedule = List.of(
                new M3V3AsyncActorLaneRunner.ScheduledOffer<>(0, 0, 0, 0, true, "blocks-offerer"),
                new M3V3AsyncActorLaneRunner.ScheduledOffer<>(
                        1, 0, 1, TimeUnit.MILLISECONDS.toNanos(90), true, "late-delivery"));

        M3V3AsyncActorLaneRunner<String> v5 = M3V3AsyncActorLaneRunner.v5(
                Duration.ZERO,
                measurement,
                drain,
                cleanup,
                AllocatorEvidenceAdmissionPolicyV5.MAX_ASYNC_OUTSTANDING_PER_ACTOR,
                AllocatorEvidenceAdmissionPolicyV5.MAX_GLOBAL_OUTSTANDING);
        var v5Result = v5.run(200, schedule, blockingFirstDispatch());

        assertThat(v5Result.offered()).isEqualTo(2);
        assertThat(v5Result.admitted()).isEqualTo(2);
        assertThat(v5Result.completed()).isEqualTo(2);
        assertThat(v5Result.overloadDroppedBeforeAdmission()).isZero();
        assertThat(v5Result.measuredTelemetry().get(1).schedulerFiringLagMicros()).isGreaterThan(20_000);

        M3V3AsyncActorLaneRunner<String> v4 = new M3V3AsyncActorLaneRunner<>(
                Duration.ZERO,
                measurement,
                drain,
                cleanup,
                AllocatorEvidenceAdmissionPolicyV5.MAX_ASYNC_OUTSTANDING_PER_ACTOR,
                AllocatorEvidenceAdmissionPolicyV5.MAX_GLOBAL_OUTSTANDING);
        var v4Result = v4.run(200, schedule, blockingFirstDispatch());

        assertThat(v4Result.offered()).isEqualTo(2);
        assertThat(v4Result.admitted()).isEqualTo(1);
        assertThat(v4Result.completed()).isEqualTo(1);
        assertThat(v4Result.overloadDroppedBeforeAdmission()).isEqualTo(1);
        assertThat(v4Result.measuredTelemetry().get(1).failureSummary()).isEqualTo("PRE_ADMISSION_CUTOFF");
    }

    @Test
    void frozenTargetDeliveredAfterTheV5AdmissionDeadlineStillDrops() throws Exception {
        M3V3AsyncActorLaneRunner<String> runner = M3V3AsyncActorLaneRunner.v5(
                Duration.ZERO,
                Duration.ofMillis(50),
                Duration.ofMillis(50),
                Duration.ofMillis(250),
                AllocatorEvidenceAdmissionPolicyV5.MAX_ASYNC_OUTSTANDING_PER_ACTOR,
                AllocatorEvidenceAdmissionPolicyV5.MAX_GLOBAL_OUTSTANDING);
        List<M3V3AsyncActorLaneRunner.ScheduledOffer<String>> schedule = List.of(
                new M3V3AsyncActorLaneRunner.ScheduledOffer<>(0, 0, 0, 0, true, "blocks-offerer"),
                new M3V3AsyncActorLaneRunner.ScheduledOffer<>(
                        1, 0, 1, TimeUnit.MILLISECONDS.toNanos(40), true, "too-late"));
        AtomicInteger calls = new AtomicInteger();

        var result = runner.run(200, schedule, (actor, request, context) -> {
            if (calls.getAndIncrement() == 0) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(125));
            }
            return CompletableFuture.completedFuture(null);
        });

        assertThat(result.offered()).isEqualTo(2);
        assertThat(result.admitted()).isEqualTo(1);
        assertThat(result.completed()).isEqualTo(1);
        assertThat(result.overloadDroppedBeforeAdmission()).isEqualTo(1);
        assertThat(result.measuredTelemetry().get(1).schedulerFiringLagMicros()).isGreaterThan(50_000);
    }

    private static M3V3AsyncActorLaneRunner.ActorOperation<String> blockingFirstDispatch() {
        AtomicInteger calls = new AtomicInteger();
        return (actor, request, context) -> {
            if (calls.getAndIncrement() == 0) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(130));
            }
            return CompletableFuture.completedFuture(null);
        };
    }

    private static List<M3V3AsyncActorLaneRunner.ScheduledOffer<String>> uniqueSchedule(int total) {
        List<M3V3AsyncActorLaneRunner.ScheduledOffer<String>> schedule = new ArrayList<>(total);
        for (int ordinal = 0; ordinal < total; ordinal++) {
            schedule.add(new M3V3AsyncActorLaneRunner.ScheduledOffer<>(
                    ordinal, ordinal % 4, ordinal, 0, true, Integer.toString(ordinal)));
        }
        return List.copyOf(schedule);
    }
}
