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
import org.junit.jupiter.api.Test;

class M3V5AsyncActorLaneRunnerTest {
    @Test
    void reachesTheBoundedStormAdmissionCapWithoutChangingPerBindingSingleFlight() throws Exception {
        int total = AllocatorEvidenceAdmissionPolicyV5.MAX_GLOBAL_OUTSTANDING;
        M3V3AsyncActorLaneRunner<String> runner = new M3V3AsyncActorLaneRunner<>(
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

    private static List<M3V3AsyncActorLaneRunner.ScheduledOffer<String>> uniqueSchedule(int total) {
        List<M3V3AsyncActorLaneRunner.ScheduledOffer<String>> schedule = new ArrayList<>(total);
        for (int ordinal = 0; ordinal < total; ordinal++) {
            schedule.add(new M3V3AsyncActorLaneRunner.ScheduledOffer<>(
                    ordinal, ordinal % 4, ordinal, 0, true, Integer.toString(ordinal)));
        }
        return List.copyOf(schedule);
    }
}
