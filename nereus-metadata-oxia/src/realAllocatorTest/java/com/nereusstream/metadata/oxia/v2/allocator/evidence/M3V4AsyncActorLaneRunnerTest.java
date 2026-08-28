/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.metadata.oxia.v2.allocator.evidence;

import static org.assertj.core.api.Assertions.assertThat;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class M3V4AsyncActorLaneRunnerTest {
    @Test
    void admitsOnlyAlreadyOfferedWorkDuringTheTerminalDrain() throws Exception {
        M3V3AsyncActorLaneRunner<String> runner = new M3V3AsyncActorLaneRunner<>(
                Duration.ZERO,
                Duration.ofMillis(40),
                Duration.ofMillis(100),
                Duration.ofMillis(120));
        CompletableFuture<Void> first = new CompletableFuture<>();
        AtomicInteger calls = new AtomicInteger();
        Thread release = new Thread(() -> {
            try {
                Thread.sleep(70);
                first.complete(null);
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
            }
        });
        release.start();

        var result = runner.run(
                2,
                List.of(
                        offer(0, 0, 7, 0, "owner"),
                        offer(1, 0, 7, TimeUnit.MILLISECONDS.toNanos(30), "waiting")),
                (actor, request, context) -> {
                    calls.incrementAndGet();
                    return request.equals("owner") ? first : CompletableFuture.completedFuture(null);
                });
        release.join();

        assertThat(calls).hasValue(2);
        assertThat(result.offered()).isEqualTo(2);
        assertThat(result.admitted()).isEqualTo(2);
        assertThat(result.completed()).isEqualTo(2);
        assertThat(result.overloadDroppedBeforeAdmission()).isZero();
        assertThat(result.measuredTerminals())
                .extracting(M3V3AsyncActorLaneRunner.TerminalRecord::ordinal)
                .containsExactly(0L, 1L);
        assertConservation(result);
    }

    @Test
    void dropsAnOnTimeRequestStillBlockedAtTheFinalAdmissionDeadline() throws Exception {
        M3V3AsyncActorLaneRunner<String> runner = new M3V3AsyncActorLaneRunner<>(
                Duration.ZERO,
                Duration.ofMillis(30),
                Duration.ofMillis(40),
                Duration.ofMillis(120));
        CompletableFuture<Void> owner = new CompletableFuture<>();

        var result = runner.run(
                2,
                List.of(offer(0, 0, 9, 0, "owner"), offer(1, 0, 9, TimeUnit.MILLISECONDS.toNanos(20), "waiting")),
                (actor, request, context) -> owner);

        assertThat(result.admitted()).isEqualTo(1);
        assertThat(result.overloadDroppedBeforeAdmission()).isEqualTo(1);
        assertThat(result.timedOutAfterAdmission()).isEqualTo(1);
        assertConservation(result);
    }

    private static M3V3AsyncActorLaneRunner.ScheduledOffer<String> offer(
            long ordinal, int actor, long binding, long offsetNanos, String request) {
        return new M3V3AsyncActorLaneRunner.ScheduledOffer<>(
                ordinal, actor, binding, offsetNanos, true, request);
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
