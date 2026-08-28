/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.metadata.oxia.v2.allocator.evidence;

import static org.assertj.core.api.Assertions.assertThat;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Candidate;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Cell;
import com.nereusstream.domain.registry.allocator.AllocatorEvidenceCandidateV1;
import com.nereusstream.domain.registry.allocator.AllocatorProtocolException;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3RealOxiaActors.InstrumentedClient.OperationDiagnosticSnapshot;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3RealOxiaActors.InstrumentedClient.OperationSample;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3RealOxiaActors.SharedOperationDiagnosticSnapshot;
import com.nereusstream.metadata.spi.allocator.BoundedVirtualLedgerAllocatorWorkflowV2.Result;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/** Diagnostic-only replay of the first V5 RANGE_1024 row that failed at ten milliseconds. */
class M3V5RangeLatencyDiagnosticTest {
    private static final int POPULATION = 10_000;

    @Test
    void exactRange1024TenMillisSequenceAttributesOperationAndSchedulerCapacity() throws Exception {
        runSequence(10, "v5-range1024-10ms-formal-sequence.json");
    }

    @Test
    void exactRange1024TwentyFiveMillisSequenceAttributesOperationAndSchedulerCapacity() throws Exception {
        runSequence(25, "v5-range1024-25ms-formal-sequence.json");
    }

    private static void runSequence(int latencyMillis, String outputFile) throws Exception {
        String serviceAddress =
                M3V3DiagnosticOutput.requiredProperty("nereus.m3.allocator.v3.oxiaServiceAddress");
        String sourceCommit = M3V3DiagnosticOutput.requiredProperty("nereus.m3.allocator.v3.nereusCommit");
        if (!sourceCommit.matches("[0-9a-f]{40}")) {
            throw new IllegalArgumentException("V5 RANGE latency diagnostic Nereus commit must be exact");
        }
        ThreadPoolExecutor workers = M3RealAllocatorEvidenceTest.exactWorkers();
        workers.prestartAllCoreThreads();
        Throwable executionFailure = null;
        try (M3RealOxiaActors actors = new M3RealOxiaActors(serviceAddress)) {
            M3CandidateAllocatorPopulation population = new M3CandidateAllocatorPopulation(
                    AllocatorEvidenceCandidateV1.range(1_024),
                    4,
                    "range1024-v5-latency-" + latencyMillis + "ms-" + sourceCommit.substring(0, 16),
                    actors,
                    workers);
            population.ensurePopulation(POPULATION);
            actors.setControlledLatencyMillis(latencyMillis);
            Row fixed = runRow(
                    actors,
                    population,
                    Cell.fixedRate(Candidate.RANGE_1024, POPULATION, latencyMillis, 1_000),
                    1_000,
                    latencyMillis);
            Row derived = runRow(
                    actors,
                    population,
                    Cell.derived(Candidate.RANGE_1024, POPULATION, latencyMillis),
                    800,
                    latencyMillis);
            M3V3DiagnosticOutput.writeNew(
                    outputFile,
                    "{\"schema\":\"NEREUS_V2_M3_ALLOCATOR_RANGE_LATENCY_DIAGNOSTIC_V5\""
                            + ",\"diagnosticOnly\":true,\"authority\":false,\"selectionEligible\":false"
                            + ",\"sourceCommit\":" + M3V3DiagnosticOutput.jsonString(sourceCommit)
                            + ",\"candidate\":\"RANGE_1024\",\"activePopulation\":10000"
                            + ",\"latencyMillis\":" + latencyMillis
                            + ",\"delaySchedulerThreadsPerActor\":"
                            + M3RealOxiaActors.CONTROLLED_DELAY_SCHEDULER_THREADS_PER_ACTOR
                            + ",\"fixed1000\":" + fixed.json()
                            + ",\"derived800\":" + derived.json() + "}\n");
            assertZeroDropFailureTimeout("fixed1000", fixed);
            assertZeroDropFailureTimeout("derived800", derived);
        } catch (Exception | Error failure) {
            executionFailure = failure;
            throw failure;
        } finally {
            workers.shutdownNow();
            if (!workers.awaitTermination(3, TimeUnit.MINUTES)) {
                IllegalStateException termination =
                        new IllegalStateException("V5 RANGE latency diagnostic executor did not terminate");
                if (executionFailure == null) {
                    throw termination;
                }
                executionFailure.addSuppressed(termination);
            }
        }
    }

    private static void assertZeroDropFailureTimeout(String rowName, Row row) {
        assertThat(row.dropped()).as(rowName + " dropped before admission").isZero();
        assertThat(row.failed()).as(rowName + " failed after admission").isZero();
        assertThat(row.timedOut()).as(rowName + " timed out after admission").isZero();
        assertThat(row.admitted()).as(rowName + " admitted").isEqualTo(row.offered());
        assertThat(row.completed()).as(rowName + " completed").isEqualTo(row.offered());
    }

    private static Row runRow(
            M3RealOxiaActors actors,
            M3CandidateAllocatorPopulation population,
            Cell cell,
            int offeredRate,
            int latencyMillis)
            throws InterruptedException {
        actors.beginSharedDiagnosticCapture();
        population.beginRetryDiagnosticCapture();
        AllocationObserver observer = new AllocationObserver();
        M3V3AllocatorFormalHarness.HarnessResult result;
        SharedOperationDiagnosticSnapshot sharedSnapshot;
        M3CandidateAllocatorPopulation.RetryDiagnosticSnapshot retrySnapshot;
        try {
            M3V3AllocatorFormalHarness harness = M3V3AllocatorFormalHarness.forContractTestV5(
                    Duration.ofSeconds(10),
                    Duration.ofSeconds(30),
                    Duration.ofSeconds(2),
                    Duration.ofSeconds(5),
                    population.formalActorEndpointsV3(cell, observer));
            result = harness.runCandidate(
                    cell,
                    offeredRate,
                    M3V3RealFormalActionRuntime.candidateSchedule(POPULATION, offeredRate),
                    M3V3AllocatorFormalHarness.SupplementaryMeasurements::empty);
        } finally {
            sharedSnapshot = actors.endSharedDiagnosticCapture();
            retrySnapshot = population.endRetryDiagnosticCapture();
        }
        assertConservationAndDrain(result);
        assertThat(retrySnapshot.total()).isEqualTo(observer.reconcileRetries.get());
        List<OperationDiagnosticSnapshot> snapshots = sharedSnapshot.actorSnapshots();
        List<OperationSample> samples = snapshots.stream()
                .flatMap(snapshot -> snapshot.samples().stream())
                .toList();
        assertThat(samples).isNotEmpty();
        assertThat(samples).allMatch(sample -> sample.injectedLatencyMillis() == latencyMillis);
        assertThat(sharedSnapshot.realOutstandingMaximum()).isGreaterThan(4);
        return Row.from(offeredRate, result, observer, sharedSnapshot, retrySnapshot, samples);
    }

    private static void assertConservationAndDrain(M3V3AllocatorFormalHarness.HarnessResult result) {
        var interval = result.runnerResult();
        assertThat(result.infrastructureValid()).isTrue();
        assertThat(interval.offered())
                .isEqualTo(interval.overloadDroppedBeforeAdmission()
                        + interval.completed()
                        + interval.failedAfterAdmission()
                        + interval.timedOutAfterAdmission());
        assertThat(interval.admitted())
                .isEqualTo(interval.completed() + interval.failedAfterAdmission() + interval.timedOutAfterAdmission());
        assertThat(interval.terminal()).isEqualTo(interval.admitted());
        assertThat(interval.queueDepthAtEnd()).isZero();
        assertThat(interval.globalOutstandingAtEnd()).isZero();
        assertThat(interval.bindingBusyAtEnd()).isZero();
        assertThat(interval.pendingPermitAtEnd()).isZero();
        assertThat(interval.actorLanesStoppedAtCleanupDeadline()).isTrue();
    }

    private static long percentile(List<Long> values, double quantile) {
        if (values.isEmpty()) {
            return 0;
        }
        List<Long> ordered = values.stream().sorted().toList();
        int index = Math.max(0, (int) Math.ceil(ordered.size() * quantile) - 1);
        return ordered.get(index);
    }

    private static String counts(Map<String, Long> values) {
        StringBuilder json = new StringBuilder("{");
        int index = 0;
        for (Map.Entry<String, Long> entry : new TreeMap<>(values).entrySet()) {
            if (index++ > 0) {
                json.append(',');
            }
            json.append(M3V3DiagnosticOutput.jsonString(entry.getKey()))
                    .append(':')
                    .append(entry.getValue());
        }
        return json.append('}').toString();
    }

    private static String summarizeFailure(Throwable failure) {
        Throwable exact = failure;
        while (exact instanceof CompletionException && exact.getCause() != null) {
            exact = exact.getCause();
        }
        String type = exact.getClass().getSimpleName();
        if (exact instanceof AllocatorProtocolException protocolFailure) {
            type += '[' + protocolFailure.code().name() + ']';
        }
        String message = exact.getMessage();
        String summary = message == null || message.isBlank() ? type : type + ':' + message;
        String printable = summary.replaceAll("[^\\x20-\\x7E]", "?");
        return printable.substring(0, Math.min(printable.length(), 512));
    }

    private static final class AllocationObserver implements M3CandidateAllocatorPopulation.FormalAllocationObserver {
        private final AtomicLong completed = new AtomicLong();
        private final AtomicLong failed = new AtomicLong();
        private final AtomicLong reconcileRetries = new AtomicLong();
        private final AtomicLong reconcileRetriesMaximum = new AtomicLong();
        private final List<Long> elapsedMicros = java.util.Collections.synchronizedList(new ArrayList<>());
        private final Map<String, Long> failureKinds = java.util.Collections.synchronizedMap(new TreeMap<>());
        private final AtomicReference<String> firstFailure = new AtomicReference<>("");

        @Override
        public void completed(
                int actorId, M3AllocatorWorkloadPlan.PlannedRequest request, Result result, long elapsed) {
            completed.incrementAndGet();
            elapsedMicros.add(elapsed);
            reconcileRetries.addAndGet(result.reconcileRetries());
            reconcileRetriesMaximum.accumulateAndGet(result.reconcileRetries(), Math::max);
        }

        @Override
        public void failed(
                int actorId, M3AllocatorWorkloadPlan.PlannedRequest request, Throwable failure, long elapsed) {
            failed.incrementAndGet();
            elapsedMicros.add(elapsed);
            String summary = summarizeFailure(failure);
            firstFailure.compareAndSet("", summary);
            synchronized (failureKinds) {
                failureKinds.merge(summary, 1L, Math::addExact);
            }
        }
    }

    private record Row(
            int offeredRate,
            long offered,
            long admitted,
            long dropped,
            long completed,
            long failed,
            long timedOut,
            long globalOutstandingMaximum,
            long queueDepthMaximum,
            long queueWaitP99Micros,
            long queueWaitMaximumMicros,
            long runnerSchedulerLagP99Micros,
            long runnerCallbackLagP99Micros,
            long rolloverP99Micros,
            long queueDepthAtEnd,
            long globalOutstandingAtEnd,
            long bindingBusyAtEnd,
            long pendingPermitAtEnd,
            boolean actorLanesStopped,
            long operationCount,
            long operationOutstandingMaximum,
            long realOperationOutstandingMaximum,
            long realRttP99Micros,
            long delaySchedulerLagP50Micros,
            long delaySchedulerLagP99Micros,
            long delaySchedulerLagMaximumMicros,
            long delayCallbackLagP99Micros,
            long workflowP99Micros,
            long observerCompleted,
            long observerFailed,
            long reconcileRetries,
            long reconcileRetriesMaximum,
            String firstFailure,
            Map<String, Long> retryReasons,
            Map<String, Long> operationKinds,
            Map<String, Long> failureKinds) {
        private Row {
            retryReasons = Map.copyOf(retryReasons);
            operationKinds = Map.copyOf(operationKinds);
            failureKinds = Map.copyOf(failureKinds);
        }

        private static Row from(
                int offeredRate,
                M3V3AllocatorFormalHarness.HarnessResult result,
                AllocationObserver observer,
                SharedOperationDiagnosticSnapshot sharedSnapshot,
                M3CandidateAllocatorPopulation.RetryDiagnosticSnapshot retrySnapshot,
                List<OperationSample> samples) {
            var interval = result.runnerResult();
            List<OperationDiagnosticSnapshot> snapshots = sharedSnapshot.actorSnapshots();
            Map<String, Long> operationKinds = new TreeMap<>();
            samples.forEach(sample -> operationKinds.merge(sample.kind(), 1L, Math::addExact));
            List<Long> realRtt = samples.stream().map(OperationSample::realRttMicros).toList();
            List<Long> schedulerLag = samples.stream().map(OperationSample::schedulerLagMicros).toList();
            List<Long> callbackLag = samples.stream().map(OperationSample::callbackLagMicros).toList();
            Map<String, Long> retryReasons = new TreeMap<>();
            retrySnapshot.reasons().forEach((reason, count) -> retryReasons.put(reason.name(), count));
            return new Row(
                    offeredRate,
                    interval.offered(),
                    interval.admitted(),
                    interval.overloadDroppedBeforeAdmission(),
                    interval.completed(),
                    interval.failedAfterAdmission(),
                    interval.timedOutAfterAdmission(),
                    interval.globalOutstandingMaximum(),
                    interval.queueDepthMaximum(),
                    interval.queueWaitP99Micros(),
                    interval.queueWaitMaximumMicros(),
                    interval.schedulerFiringLagP99Micros(),
                    interval.callbackLagP99Micros(),
                    interval.rolloverP99Micros(),
                    interval.queueDepthAtEnd(),
                    interval.globalOutstandingAtEnd(),
                    interval.bindingBusyAtEnd(),
                    interval.pendingPermitAtEnd(),
                    interval.actorLanesStoppedAtCleanupDeadline(),
                    samples.size(),
                    sharedSnapshot.outstandingMaximum(),
                    sharedSnapshot.realOutstandingMaximum(),
                    percentile(realRtt, 0.99),
                    percentile(schedulerLag, 0.50),
                    percentile(schedulerLag, 0.99),
                    schedulerLag.stream().mapToLong(Long::longValue).max().orElse(0),
                    percentile(callbackLag, 0.99),
                    percentile(observer.elapsedMicros, 0.99),
                    observer.completed.get(),
                    observer.failed.get(),
                    observer.reconcileRetries.get(),
                    observer.reconcileRetriesMaximum.get(),
                    observer.firstFailure.get(),
                    retryReasons,
                    operationKinds,
                    observer.failureKinds);
        }

        private String json() {
            return "{\"offeredRate\":" + offeredRate
                    + ",\"offered\":" + offered + ",\"admitted\":" + admitted
                    + ",\"dropped\":" + dropped + ",\"completed\":" + completed
                    + ",\"failed\":" + failed + ",\"timedOut\":" + timedOut
                    + ",\"globalOutstandingMaximum\":" + globalOutstandingMaximum
                    + ",\"queueDepthMaximum\":" + queueDepthMaximum
                    + ",\"queueWaitP99Micros\":" + queueWaitP99Micros
                    + ",\"queueWaitMaximumMicros\":" + queueWaitMaximumMicros
                    + ",\"runnerSchedulerLagP99Micros\":" + runnerSchedulerLagP99Micros
                    + ",\"runnerCallbackLagP99Micros\":" + runnerCallbackLagP99Micros
                    + ",\"rolloverP99Micros\":" + rolloverP99Micros
                    + ",\"queueDepthAtEnd\":" + queueDepthAtEnd
                    + ",\"globalOutstandingAtEnd\":" + globalOutstandingAtEnd
                    + ",\"bindingBusyAtEnd\":" + bindingBusyAtEnd
                    + ",\"pendingPermitAtEnd\":" + pendingPermitAtEnd
                    + ",\"actorLanesStopped\":" + actorLanesStopped
                    + ",\"metadataOperationCount\":" + operationCount
                    + ",\"metadataOperationsPerAdmittedNumerator\":" + operationCount
                    + ",\"metadataOperationsPerAdmittedDenominator\":" + admitted
                    + ",\"operationOutstandingMaximum\":" + operationOutstandingMaximum
                    + ",\"realOperationOutstandingMaximum\":" + realOperationOutstandingMaximum
                    + ",\"realRttP99Micros\":" + realRttP99Micros
                    + ",\"delaySchedulerLagP50Micros\":" + delaySchedulerLagP50Micros
                    + ",\"delaySchedulerLagP99Micros\":" + delaySchedulerLagP99Micros
                    + ",\"delaySchedulerLagMaximumMicros\":" + delaySchedulerLagMaximumMicros
                    + ",\"delayCallbackLagP99Micros\":" + delayCallbackLagP99Micros
                    + ",\"workflowP99Micros\":" + workflowP99Micros
                    + ",\"observerCompleted\":" + observerCompleted
                    + ",\"observerFailed\":" + observerFailed
                    + ",\"reconcileRetries\":" + reconcileRetries
                    + ",\"reconcileRetriesMaximum\":" + reconcileRetriesMaximum
                    + ",\"firstFailure\":" + M3V3DiagnosticOutput.jsonString(firstFailure)
                    + ",\"retryReasons\":" + counts(retryReasons)
                    + ",\"operationKinds\":" + counts(operationKinds)
                    + ",\"failureKinds\":" + counts(failureKinds) + '}';
        }
    }
}
