/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.metadata.oxia.v2.allocator.evidence;

import static org.assertj.core.api.Assertions.assertThat;
import com.nereusstream.domain.registry.allocator.AllocatorEvidenceCandidateV1;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3RealOxiaActors.InstrumentedClient.OperationDiagnosticSnapshot;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3RealOxiaActors.InstrumentedClient.OperationSample;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V2RealOxiaDiagnosticTest.Fixture;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V2RealOxiaDiagnosticTest.StageSnapshot;
import com.nereusstream.metadata.spi.allocator.BoundedVirtualLedgerAllocatorWorkflowV2.Result;
import com.nereusstream.metadata.spi.allocator.VersionedManagedLedgerAllocatorHeadV1;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

/** Short diagnostic-only async workflow rows; no threshold, evaluation, or selection Boolean is caller supplied. */
class M3V3AllocatorWorkflowDiagnosticTest {
    private static final int BINDINGS = 256;
    private static final Duration WARMUP = Duration.ofSeconds(2);
    private static final Duration MEASUREMENT = Duration.ofSeconds(8);
    private static final Duration CLEANUP = Duration.ofSeconds(5);
    private static final List<Row> ROWS = new ArrayList<>();

    @Test
    void strictAndRangeRowsUseAsyncAdmissionAtTwoHundredAndFiveHundred() throws Exception {
        for (CandidateCase candidate : List.of(
                new CandidateCase("STRICT", AllocatorEvidenceCandidateV1.strict()),
                new CandidateCase("RANGE_16", AllocatorEvidenceCandidateV1.range(16)),
                new CandidateCase("RANGE_64", AllocatorEvidenceCandidateV1.range(64)))) {
            try (Fixture fixture = Fixture.openV3("workflow-" + candidate.name().toLowerCase(), candidate.value())) {
                List<Binding> bindings = createBindings(fixture);
                fixture.setControlledLatencyMillis(1);
                for (int rate : List.of(200, 500)) {
                    ROWS.add(runRow(fixture, candidate.name(), rate, 1, bindings));
                }
            }
        }
    }

    @Test
    void fourActorSameCellConflictStormPreservesUniqueLedgerIds() throws Exception {
        try (Fixture fixture = Fixture.openV3ConflictStorm(
                "workflow-conflict-storm", AllocatorEvidenceCandidateV1.strict())) {
            fixture.setControlledLatencyMillis(1);
            List<VersionedManagedLedgerAllocatorHeadV1> heads = new ArrayList<>();
            for (int actor = 0; actor < 4; actor++) {
                heads.add(fixture.createHead(actor, "v3-conflict-" + actor));
            }
            fixture.beginOperationCapture();
            fixture.beginStageCapture();
            List<CompletableFuture<Result>> futures = new ArrayList<>();
            for (int actor = 0; actor < 4; actor++) {
                futures.add(fixture.allocateAsync(actor, heads.get(actor), "v3-conflict-request-" + actor));
            }
            CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).get(15, TimeUnit.SECONDS);
            List<Result> results = futures.stream().map(CompletableFuture::join).toList();
            List<OperationDiagnosticSnapshot> snapshots = fixture.endOperationCapture();
            StageSnapshot stages = fixture.endStageCapture();
            List<OperationSample> samples = flatten(snapshots);

            assertThat(results.stream().map(result -> result.exactNode().value().ledgerId()))
                    .doesNotHaveDuplicates();
            assertThat(results.stream().mapToInt(Result::reconcileRetries).sum()).isPositive();
            ROWS.add(Row.conflictStorm(
                    samples,
                    outstandingMaximum(snapshots),
                    results.stream().mapToInt(Result::reconcileRetries).sum(),
                    stages));
        }
    }

    @AfterAll
    static void writeDiagnostic() throws Exception {
        assertThat(ROWS).hasSize(7);
        M3V3DiagnosticOutput.writeNew("allocator-workflow-diagnostic.json", json(ROWS));
    }

    private static Row runRow(
            Fixture fixture, String candidate, int rate, int latencyMillis, List<Binding> bindings)
            throws Exception {
        AtomicLong reconcileRetries = new AtomicLong();
        AtomicLong installedGrantUses = new AtomicLong();
        AtomicLong renewals = new AtomicLong();
        fixture.beginOperationCapture();
        fixture.beginStageCapture();
        M3V3AsyncActorLaneRunner<Integer> runner =
                new M3V3AsyncActorLaneRunner<>(WARMUP, MEASUREMENT, CLEANUP);
        List<M3V3AsyncActorLaneRunner.ScheduledOffer<Integer>> schedule = schedule(rate, bindings.size());
        var result = runner.run(rate, schedule, (actor, bindingIndex, context) -> {
            Binding binding = bindings.get(bindingIndex);
            VersionedManagedLedgerAllocatorHeadV1 predecessor = binding.head;
            return fixture.allocateAsync(actor, predecessor, candidate + '-' + rate + '-' + binding.nextRequest())
                    .thenAccept(exact -> {
                        reconcileRetries.addAndGet(exact.reconcileRetries());
                        long previousGrant = predecessor.value().grantId();
                        long nextGrant = exact.exactHead().value().grantId();
                        if (previousGrant > 0 && previousGrant == nextGrant) {
                            installedGrantUses.incrementAndGet();
                        }
                        if (nextGrant > previousGrant) {
                            renewals.incrementAndGet();
                        }
                        binding.head = exact.exactHead();
                    });
        });
        List<OperationDiagnosticSnapshot> snapshots = fixture.endOperationCapture();
        StageSnapshot stages = fixture.endStageCapture();
        List<OperationSample> samples = flatten(snapshots);
        assertConservation(result);
        assertThat(samples).isNotEmpty();
        return Row.workflow(
                candidate,
                rate,
                latencyMillis,
                result,
                samples,
                outstandingMaximum(snapshots),
                reconcileRetries.get(),
                installedGrantUses.get(),
                renewals.get(),
                stages);
    }

    private static List<Binding> createBindings(Fixture fixture) {
        List<Binding> bindings = new ArrayList<>(BINDINGS);
        for (int index = 0; index < BINDINGS; index++) {
            bindings.add(new Binding(fixture.createHead(index & 3, "v3-binding-" + index)));
        }
        return bindings;
    }

    private static List<M3V3AsyncActorLaneRunner.ScheduledOffer<Integer>> schedule(int rate, int bindings) {
        int warmupRequests = Math.multiplyExact(rate, Math.toIntExact(WARMUP.toSeconds()));
        int total = Math.multiplyExact(rate, Math.toIntExact(WARMUP.plus(MEASUREMENT).toSeconds()));
        List<M3V3AsyncActorLaneRunner.ScheduledOffer<Integer>> schedule = new ArrayList<>(total);
        for (int ordinal = 0; ordinal < total; ordinal++) {
            int binding = ordinal % bindings;
            long offset = Math.floorDiv(Math.multiplyExact((long) ordinal, 1_000_000_000L), rate);
            schedule.add(new M3V3AsyncActorLaneRunner.ScheduledOffer<>(
                    ordinal, ordinal & 3, binding, offset, ordinal >= warmupRequests, binding));
        }
        return List.copyOf(schedule);
    }

    private static List<OperationSample> flatten(List<OperationDiagnosticSnapshot> snapshots) {
        return snapshots.stream().flatMap(snapshot -> snapshot.samples().stream()).toList();
    }

    private static long outstandingMaximum(List<OperationDiagnosticSnapshot> snapshots) {
        return snapshots.stream().mapToLong(OperationDiagnosticSnapshot::outstandingMaximum).sum();
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
    }

    private static String json(List<Row> rows) {
        StringBuilder json = new StringBuilder(4096);
        json.append("{\"schema\":\"NEREUS_V2_M3_ALLOCATOR_WORKFLOW_DIAGNOSTIC_V3\"")
                .append(",\"diagnosticOnly\":true,\"authority\":false,\"selectionEligible\":false")
                .append(",\"warmupSeconds\":2,\"measuredSeconds\":8,\"rows\":[");
        for (int index = 0; index < rows.size(); index++) {
            if (index > 0) {
                json.append(',');
            }
            rows.get(index).appendJson(json);
        }
        return json.append("]}\n").toString();
    }

    private record CandidateCase(String name, AllocatorEvidenceCandidateV1 value) {}

    private static final class Binding {
        private volatile VersionedManagedLedgerAllocatorHeadV1 head;
        private final AtomicLong requests = new AtomicLong();

        private Binding(VersionedManagedLedgerAllocatorHeadV1 head) {
            this.head = head;
        }

        private long nextRequest() {
            return requests.getAndIncrement();
        }
    }

    private record Row(
            String scenario,
            int rate,
            int latencyMillis,
            long offered,
            long admitted,
            long dropped,
            long completed,
            long failed,
            long timedOut,
            long metadataOperations,
            long casAttempts,
            long casConflicts,
            long reconcileRetries,
            long installedGrantUses,
            long renewals,
            long actorOutstandingMaximum,
            long globalOutstandingMaximum,
            long queueDepthMaximum,
            long bindingBusyMaximum,
            long pendingPermitMaximum,
            long operationOutstandingMaximum,
            long schedulerLagP99Micros,
            long callbackLagP99Micros,
            long realRttP99Micros,
            Map<String, Long> metadataOperationKinds,
            Map<String, Long> failureKinds,
            Map<String, Long> workflowStages,
            Map<String, Long> retryReasons) {
        private Row {
            metadataOperationKinds = Map.copyOf(metadataOperationKinds);
            failureKinds = Map.copyOf(failureKinds);
            workflowStages = Map.copyOf(workflowStages);
            retryReasons = Map.copyOf(retryReasons);
        }

        private static Row workflow(
                String scenario,
                int rate,
                int latency,
                M3V3AsyncActorLaneRunner.IntervalResult result,
                List<OperationSample> samples,
                long operationOutstanding,
                long retries,
                long installedGrantUses,
                long renewals,
                StageSnapshot stages) {
            return new Row(
                    scenario,
                    rate,
                    latency,
                    result.offered(),
                    result.admitted(),
                    result.overloadDroppedBeforeAdmission(),
                    result.completed(),
                    result.failedAfterAdmission(),
                    result.timedOutAfterAdmission(),
                    samples.size(),
                    samples.stream().filter(sample -> sample.kind().equals("COMPARE_AND_SET")).count(),
                    samples.stream()
                            .filter(sample -> sample.kind().equals("COMPARE_AND_SET") && sample.failed())
                            .count(),
                    retries,
                    installedGrantUses,
                    renewals,
                    result.perActorOutstandingMaximum().stream().mapToLong(Integer::longValue).max().orElse(0),
                    result.globalOutstandingMaximum(),
                    result.queueDepthMaximum(),
                    result.bindingBusyMaximum(),
                    result.pendingPermitMaximum(),
                    operationOutstanding,
                    result.schedulerFiringLagP99Micros(),
                    result.callbackLagP99Micros(),
                    percentile99(samples.stream().map(OperationSample::realRttMicros).toList()),
                    operationKinds(samples),
                    failureKinds(result),
                    stages.workflowStages(),
                    stages.retryReasons());
        }

        private static Row conflictStorm(
                List<OperationSample> samples,
                long operationOutstanding,
                long retries,
                StageSnapshot stages) {
            return new Row(
                    "CONFLICT_STORM",
                    0,
                    1,
                    4,
                    4,
                    0,
                    4,
                    0,
                    0,
                    samples.size(),
                    samples.stream().filter(sample -> sample.kind().equals("COMPARE_AND_SET")).count(),
                    samples.stream()
                            .filter(sample -> sample.kind().equals("COMPARE_AND_SET") && sample.failed())
                            .count(),
                    retries,
                    0,
                    0,
                    1,
                    4,
                    0,
                    1,
                    0,
                    operationOutstanding,
                    0,
                    0,
                    percentile99(samples.stream().map(OperationSample::realRttMicros).toList()),
                    operationKinds(samples),
                    Map.of(),
                    stages.workflowStages(),
                    stages.retryReasons());
        }

        private void appendJson(StringBuilder json) {
            json.append("{\"scenario\":").append(M3V3DiagnosticOutput.jsonString(scenario))
                    .append(",\"rate\":").append(rate)
                    .append(",\"latencyMillis\":").append(latencyMillis)
                    .append(",\"offered\":").append(offered)
                    .append(",\"admitted\":").append(admitted)
                    .append(",\"dropped\":").append(dropped)
                    .append(",\"completed\":").append(completed)
                    .append(",\"failed\":").append(failed)
                    .append(",\"timedOut\":").append(timedOut)
                    .append(",\"metadataOperations\":").append(metadataOperations)
                    .append(",\"metadataOperationsPerRequestMicros\":")
                    .append(admitted == 0 ? 0 : metadataOperations * 1_000_000L / admitted)
                    .append(",\"sequentialStagesPerRequestMicros\":")
                    .append(admitted == 0 ? 0 : metadataOperations * 1_000_000L / admitted)
                    .append(",\"metadataOperationKinds\":");
            appendCounts(json, metadataOperationKinds);
            json.append(",\"failureKinds\":");
            appendCounts(json, failureKinds);
            json.append(",\"workflowStages\":");
            appendCounts(json, workflowStages);
            json.append(",\"retryReasons\":");
            appendCounts(json, retryReasons);
            json
                    .append(",\"casAttempts\":").append(casAttempts)
                    .append(",\"casConflicts\":").append(casConflicts)
                    .append(",\"reconcileRetries\":").append(reconcileRetries)
                    .append(",\"installedGrantUses\":").append(installedGrantUses)
                    .append(",\"renewals\":").append(renewals)
                    .append(",\"actorOutstandingMax\":").append(actorOutstandingMaximum)
                    .append(",\"globalOutstandingMax\":").append(globalOutstandingMaximum)
                    .append(",\"queueDepthMax\":").append(queueDepthMaximum)
                    .append(",\"bindingBusyMax\":").append(bindingBusyMaximum)
                    .append(",\"pendingPermitMax\":").append(pendingPermitMaximum)
                    .append(",\"operationOutstandingMax\":").append(operationOutstandingMaximum)
                    .append(",\"schedulerLagP99Micros\":").append(schedulerLagP99Micros)
                    .append(",\"callbackLagP99Micros\":").append(callbackLagP99Micros)
                    .append(",\"realRttP99Micros\":").append(realRttP99Micros)
                    .append('}');
        }

        private static Map<String, Long> operationKinds(List<OperationSample> samples) {
            Map<String, Long> counts = new TreeMap<>();
            samples.forEach(sample -> counts.merge(sample.kind(), 1L, Math::addExact));
            return Map.copyOf(counts);
        }

        private static Map<String, Long> failureKinds(M3V3AsyncActorLaneRunner.IntervalResult result) {
            Map<String, Long> counts = new TreeMap<>();
            result.measuredTelemetry().stream()
                    .map(M3V3AsyncActorLaneRunner.RequestTelemetry::failureSummary)
                    .filter(value -> !value.isEmpty())
                    .map(Row::failureKind)
                    .forEach(kind -> counts.merge(kind, 1L, Math::addExact));
            return Map.copyOf(counts);
        }

        private static String failureKind(String summary) {
            int bracket = summary.indexOf('[');
            int bracketEnd = summary.indexOf(']', bracket + 1);
            if (bracket >= 0 && bracketEnd > bracket) {
                return summary.substring(0, bracketEnd + 1);
            }
            int colon = summary.indexOf(':');
            return colon < 0 ? summary : summary.substring(0, colon);
        }

        private static void appendCounts(StringBuilder json, Map<String, Long> counts) {
            json.append('{');
            int index = 0;
            for (Map.Entry<String, Long> entry : new TreeMap<>(counts).entrySet()) {
                if (index++ > 0) {
                    json.append(',');
                }
                json.append(M3V3DiagnosticOutput.jsonString(entry.getKey()))
                        .append(':')
                        .append(entry.getValue());
            }
            json.append('}');
        }

        private static long percentile99(List<Long> values) {
            if (values.isEmpty()) {
                return 0;
            }
            List<Long> sorted = values.stream().sorted().toList();
            return sorted.get(Math.max(0, (int) Math.ceil(sorted.size() * 0.99) - 1));
        }
    }
}
