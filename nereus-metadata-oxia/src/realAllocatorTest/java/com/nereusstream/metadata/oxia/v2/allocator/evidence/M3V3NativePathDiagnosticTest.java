/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.metadata.oxia.v2.allocator.evidence;

import static org.assertj.core.api.Assertions.assertThat;
import com.nereusstream.domain.registry.allocator.AllocatorEvidenceContextV1;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** Exact pinned Pulsar native-path diagnostic at the requested short rates and metadata latencies. */
class M3V3NativePathDiagnosticTest {
    private static final int POPULATION = 10_000;
    private static final Duration WARMUP = Duration.ofSeconds(2);
    private static final Duration MEASUREMENT = Duration.ofSeconds(8);
    private static final Duration CLEANUP = Duration.ofSeconds(5);

    @Test
    void nativeRowsCoverOneAndTwentyFiveMillisAtTwoHundredAndFiveHundred() throws Exception {
        ThreadPoolExecutor workers = M3RealAllocatorEvidenceTest.exactWorkers();
        workers.prestartAllCoreThreads();
        List<Row> rows = new ArrayList<>();
        try (M3NativePulsarPopulation population = new M3NativePulsarPopulation(workers)) {
            long constructionMicros = population.ensurePopulation(POPULATION);
            assertThat(constructionMicros).isPositive();
            for (int latency : List.of(1, 25)) {
                population.setMetadataLatencyMillis(latency);
                for (int rate : List.of(200, 500)) {
                    rows.add(runRow(population, workers, latency, rate));
                }
            }
            population.setMetadataLatencyMillis(0);
        } finally {
            workers.shutdownNow();
            assertThat(workers.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }
        M3V3DiagnosticOutput.writeNew("native-path-diagnostic.json", json(rows));
    }

    private static Row runRow(
            M3NativePulsarPopulation population, ThreadPoolExecutor workers, int latency, int rate)
            throws Exception {
        M3V3AsyncActorLaneRunner<M3AllocatorWorkloadPlan.PlannedRequest> runner =
                new M3V3AsyncActorLaneRunner<>(WARMUP, MEASUREMENT, CLEANUP);
        List<M3V3AsyncActorLaneRunner.ScheduledOffer<M3AllocatorWorkloadPlan.PlannedRequest>> schedule =
                schedule(rate);
        M3AllocatorRequestTelemetry telemetry = new M3AllocatorRequestTelemetry(ignored -> {}, System.nanoTime());
        AllocatorEvidenceContextV1 context = AllocatorEvidenceContextV1.nativeContext(POPULATION, latency, rate);

        var result = runner.run(rate, schedule, (actor, request, operationContext) ->
                CompletableFuture.runAsync(() -> {
                    M3AllocatorRequestTelemetry.RequestTrace trace = telemetry.trace(context, request, null, 1);
                    trace.offered();
                    trace.enqueued();
                    trace.dispatched();
                    try {
                        M3NativePulsarPopulation.NativeRollover rollover =
                                population.rollover(trace, request.ledgerIndex(), request.trigger());
                        if (rollover.successorLedgerId() <= 0) {
                            throw new AssertionError("native diagnostic produced a nonpositive successor ledger ID");
                        }
                        trace.completed();
                    } catch (Throwable failure) {
                        trace.completeFailureLifecycle();
                        trace.failed();
                        throw new RuntimeException(failure);
                    }
                }, workers));
        assertConservation(result);
        return new Row(
                rate,
                latency,
                result.offered(),
                result.admitted(),
                result.overloadDroppedBeforeAdmission(),
                result.completed(),
                result.failedAfterAdmission(),
                result.timedOutAfterAdmission(),
                result.perActorOutstandingMaximum().stream().mapToInt(Integer::intValue).max().orElse(0),
                result.globalOutstandingMaximum(),
                result.queueDepthMaximum(),
                result.pendingPermitMaximum(),
                result.schedulerFiringLagP99Micros(),
                result.callbackLagP99Micros(),
                result.rolloverP99Micros());
    }

    private static List<M3V3AsyncActorLaneRunner.ScheduledOffer<M3AllocatorWorkloadPlan.PlannedRequest>> schedule(
            int rate) {
        int warmupRequests = Math.multiplyExact(rate, Math.toIntExact(WARMUP.toSeconds()));
        int total = Math.multiplyExact(rate, Math.toIntExact(WARMUP.plus(MEASUREMENT).toSeconds()));
        List<M3V3AsyncActorLaneRunner.ScheduledOffer<M3AllocatorWorkloadPlan.PlannedRequest>> schedule =
                new ArrayList<>(total);
        M3AllocatorWorkloadPlan.Trigger[] triggers = M3AllocatorWorkloadPlan.Trigger.values();
        for (int ordinal = 0; ordinal < total; ordinal++) {
            int ledger = Math.floorMod(ordinal * 8191, POPULATION);
            M3AllocatorWorkloadPlan.PlannedRequest request = new M3AllocatorWorkloadPlan.PlannedRequest(
                    ordinal,
                    ordinal & 3,
                    ledger,
                    triggers[ordinal % triggers.length],
                    ordinal < warmupRequests
                            ? M3AllocatorWorkloadPlan.Phase.WARM_UP
                            : M3AllocatorWorkloadPlan.Phase.MEASURED_STEADY,
                    TimeUnit.NANOSECONDS.toMicros(
                            Math.floorDiv(Math.multiplyExact((long) ordinal, 1_000_000_000L), rate)));
            schedule.add(new M3V3AsyncActorLaneRunner.ScheduledOffer<>(
                    ordinal,
                    request.actorId(),
                    ledger,
                    TimeUnit.MICROSECONDS.toNanos(request.arrivalOffsetMicros()),
                    request.phase() != M3AllocatorWorkloadPlan.Phase.WARM_UP,
                    request));
        }
        return List.copyOf(schedule);
    }

    private static void assertConservation(M3V3AsyncActorLaneRunner.IntervalResult result) {
        assertThat(result.offered())
                .isEqualTo(result.overloadDroppedBeforeAdmission()
                        + result.completed()
                        + result.failedAfterAdmission()
                        + result.timedOutAfterAdmission());
        assertThat(result.admitted())
                .isEqualTo(result.completed() + result.failedAfterAdmission() + result.timedOutAfterAdmission());
    }

    private static String json(List<Row> rows) {
        StringBuilder json = new StringBuilder(2048);
        json.append("{\"schema\":\"NEREUS_V2_M3_ALLOCATOR_NATIVE_DIAGNOSTIC_V3\"")
                .append(",\"diagnosticOnly\":true,\"authority\":false,\"selectionEligible\":false")
                .append(",\"population\":10000,\"warmupSeconds\":2,\"measuredSeconds\":8,\"rows\":[");
        for (int index = 0; index < rows.size(); index++) {
            if (index > 0) {
                json.append(',');
            }
            rows.get(index).appendJson(json);
        }
        return json.append("]}\n").toString();
    }

    private record Row(
            int rate,
            int latencyMillis,
            long offered,
            long admitted,
            long dropped,
            long completed,
            long failed,
            long timedOut,
            long actorOutstandingMaximum,
            long globalOutstandingMaximum,
            long queueDepthMaximum,
            long pendingPermitMaximum,
            long schedulerLagP99Micros,
            long callbackLagP99Micros,
            long rolloverP99Micros) {
        private void appendJson(StringBuilder json) {
            json.append("{\"rate\":").append(rate)
                    .append(",\"latencyMillis\":").append(latencyMillis)
                    .append(",\"offered\":").append(offered)
                    .append(",\"admitted\":").append(admitted)
                    .append(",\"dropped\":").append(dropped)
                    .append(",\"completed\":").append(completed)
                    .append(",\"failed\":").append(failed)
                    .append(",\"timedOut\":").append(timedOut)
                    .append(",\"actorOutstandingMax\":").append(actorOutstandingMaximum)
                    .append(",\"globalOutstandingMax\":").append(globalOutstandingMaximum)
                    .append(",\"queueDepthMax\":").append(queueDepthMaximum)
                    .append(",\"pendingPermitMax\":").append(pendingPermitMaximum)
                    .append(",\"schedulerLagP99Micros\":").append(schedulerLagP99Micros)
                    .append(",\"callbackLagP99Micros\":").append(callbackLagP99Micros)
                    .append(",\"rolloverP99Micros\":").append(rolloverP99Micros)
                    .append('}');
        }
    }
}
