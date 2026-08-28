/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.metadata.oxia.v2.allocator.evidence;

import static org.assertj.core.api.Assertions.assertThat;
import com.nereusstream.domain.registry.allocator.AllocatorNativeExecutionProfileV3;
import com.nereusstream.domain.registry.allocator.AllocatorNativeExecutionProfileV4;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** Diagnostic-only ADR-0109 canary. It never emits NACP3, NAEV3, NARS3, or formal attachments. */
class M3V3NativeBaselineCanaryTest {
    @Test
    void exactFormalScheduleClearsAllNativeBaselinesAndRepresentativeRows() throws Exception {
        boolean terminalDrainV4 = System.getProperty("nereus.m3.allocator.protocol", "V3").equals("V4");
        ThreadPoolExecutor constructionWorkers = M3RealAllocatorEvidenceTest.exactWorkers();
        constructionWorkers.prestartAllCoreThreads();
        List<Row> rows = new ArrayList<>();
        long runnerOutstandingMaximum = 0;
        long operationOutstandingMaximum = 0;
        try (M3NativePulsarPopulation population = new M3NativePulsarPopulation(constructionWorkers)) {
            M3V3NativeIntervalRuntime runtime = new M3V3NativeIntervalRuntime(population, terminalDrainV4);
            for (int activePopulation : M3AllocatorWorkloadPlan.ACTIVE_POPULATIONS) {
                for (int latencyMillis : M3AllocatorWorkloadPlan.METADATA_LATENCY_P99_MILLIS) {
                    Row row = runRow(runtime, activePopulation, latencyMillis, 200);
                    rows.add(row);
                    writeRow(rows.size() - 1, row);
                    assertBaseline(row);
                    runnerOutstandingMaximum = Math.max(runnerOutstandingMaximum, row.globalOutstandingMaximum());
                    operationOutstandingMaximum =
                            Math.max(operationOutstandingMaximum, row.managedLedgerOperationOutstandingMaximum());
                }
                if (activePopulation == 10_000) {
                    for (int latencyMillis : List.of(1, 25)) {
                        Row row = runRow(runtime, activePopulation, latencyMillis, 500);
                        rows.add(row);
                        writeRow(rows.size() - 1, row);
                        assertRepresentative(row);
                        runnerOutstandingMaximum =
                                Math.max(runnerOutstandingMaximum, row.globalOutstandingMaximum());
                        operationOutstandingMaximum = Math.max(
                                operationOutstandingMaximum, row.managedLedgerOperationOutstandingMaximum());
                    }
                }
            }
            assertThat(population.retainedPayloadBytes()).isZero();
        } finally {
            constructionWorkers.shutdownNow();
            assertThat(constructionWorkers.awaitTermination(5, TimeUnit.MINUTES)).isTrue();
        }
        assertThat(runnerOutstandingMaximum).isGreaterThan(4);
        assertThat(operationOutstandingMaximum).isGreaterThan(4);
        M3V3DiagnosticOutput.writeNew(
                "native-baseline-canary-summary.json",
                summaryJson(rows, runnerOutstandingMaximum, operationOutstandingMaximum, terminalDrainV4));
    }

    private static Row runRow(
            M3V3NativeIntervalRuntime runtime, int activePopulation, int latencyMillis, int offeredRate)
            throws Exception {
        M3V3NativeIntervalRuntime.Result result =
                runtime.run(activePopulation, latencyMillis, offeredRate, ignored -> {});
        M3V3AsyncActorLaneRunner.IntervalResult interval = result.interval();
        M3V3NativeIntervalRuntime.NativeTelemetry telemetry = result.telemetry();
        M3V3AsyncActorLaneRunner.RequestTelemetry firstDrop = interval.measuredTelemetry().stream()
                .filter(sample -> sample.outcome()
                        == M3V3AsyncActorLaneRunner.TerminalOutcome.OVERLOAD_DROPPED_BEFORE_ADMISSION)
                .findFirst()
                .orElse(null);
        return new Row(
                activePopulation,
                latencyMillis,
                offeredRate,
                interval.offered(),
                interval.admitted(),
                interval.overloadDroppedBeforeAdmission(),
                interval.completed(),
                interval.failedAfterAdmission(),
                interval.timedOutAfterAdmission(),
                interval.warmupDroppedBeforeAdmission(),
                interval.warmupFailedAfterAdmission(),
                interval.warmupTimedOutAfterAdmission(),
                firstDrop == null ? -1 : firstDrop.ordinal(),
                firstDrop == null ? 0 : firstDrop.schedulerFiringLagMicros(),
                firstDrop == null ? "" : firstDrop.failureSummary(),
                interval.globalOutstandingMaximum(),
                interval.queueDepthMaximum(),
                interval.queueWaitMaximumMicros(),
                interval.bindingBusyMaximum(),
                interval.pendingPermitMaximum(),
                interval.queueDepthAtEnd(),
                interval.globalOutstandingAtEnd(),
                interval.bindingBusyAtEnd(),
                interval.pendingPermitAtEnd(),
                interval.actorLanesStoppedAtCleanupDeadline(),
                telemetry.managedLedgerOperations(),
                telemetry.managedLedgerOperationOutstandingMaximum(),
                telemetry.managedLedgerOperationOutstandingAtEnd(),
                telemetry.admissionToFirstMetadataDispatchP99Micros(),
                telemetry.metadataOperationsPerRequestP99(),
                telemetry.triggerAppendCompletions(),
                telemetry.successorEstablishments(),
                telemetry.rolloverP99Micros().get(M3AllocatorWorkloadPlan.Trigger.ENTRY),
                telemetry.rolloverP99Micros().get(M3AllocatorWorkloadPlan.Trigger.BYTE),
                telemetry.rolloverP99Micros().get(M3AllocatorWorkloadPlan.Trigger.AGE),
                interval.schedulerFiringLagP99Micros(),
                interval.callbackLagP99Micros());
    }

    private static void assertBaseline(Row row) {
        assertCommon(row);
        assertThat(row.offeredRate()).isEqualTo(200);
        assertThat(row.dropped()).isZero();
        assertThat(row.failed()).isZero();
        assertThat(row.timedOut()).isZero();
        assertThat(row.warmupFailed()).isZero();
        assertThat(row.warmupTimedOut()).isZero();
    }

    private static void assertRepresentative(Row row) {
        assertCommon(row);
        assertThat(row.offeredRate()).isEqualTo(500);
        assertThat(row.dropped()).isZero();
        assertThat(row.failed()).isZero();
        assertThat(row.timedOut()).isZero();
        assertThat(row.warmupFailed()).isZero();
        assertThat(row.warmupTimedOut()).isZero();
    }

    private static void assertCommon(Row row) {
        assertThat(row.admitted()).isEqualTo(row.completed() + row.failed() + row.timedOut());
        assertThat(row.offered()).isEqualTo(row.dropped() + row.completed() + row.failed() + row.timedOut());
        assertThat(row.queueDepthAtEnd()).isZero();
        assertThat(row.globalOutstandingAtEnd()).isZero();
        assertThat(row.bindingBusyAtEnd()).isZero();
        assertThat(row.pendingPermitAtEnd()).isZero();
        assertThat(row.managedLedgerOperationOutstandingAtEnd()).isZero();
        assertThat(row.actorLanesStopped()).isTrue();
    }

    private static void writeRow(int ordinal, Row row) throws Exception {
        M3V3DiagnosticOutput.writeNew("native-baseline-row-%02d.json".formatted(ordinal), row.json());
    }

    private static String summaryJson(
            List<Row> rows, long runnerMaximum, long operationMaximum, boolean terminalDrainV4) {
        String version = terminalDrainV4 ? "V4" : "V3";
        String executionProfile = terminalDrainV4
                ? AllocatorNativeExecutionProfileV4.executionProfileDigest().toHex()
                : AllocatorNativeExecutionProfileV3.executionProfileDigest().toHex();
        return "{\"schema\":\"NEREUS_V2_M3_ALLOCATOR_NATIVE_BASELINE_CANARY_" + version + "\""
                + ",\"diagnosticOnly\":true,\"authority\":false,\"selectionEligible\":false"
                + ",\"nativeExecutionModel\":\"" + AllocatorNativeExecutionProfileV3.MODEL + "\""
                + ",\"nativeExecutionProfileSha256\":\""
                + executionProfile + "\""
                + ",\"workloadScheduleSha256\":\""
                + AllocatorNativeExecutionProfileV3.scheduleDigest().toHex() + "\""
                + ",\"nativeBridgeWorkers\":0,\"nativeBridgeQueueCapacity\":0,\"hiddenDispatchQueue\":0"
                + ",\"runnerOutstandingMaximum\":" + runnerMaximum
                + ",\"managedLedgerOperationOutstandingMaximum\":" + operationMaximum
                + ",\"rowCount\":" + rows.size() + "}\n";
    }

    private record Row(
            int activePopulation,
            int latencyMillis,
            int offeredRate,
            long offered,
            long admitted,
            long dropped,
            long completed,
            long failed,
            long timedOut,
            long warmupDropped,
            long warmupFailed,
            long warmupTimedOut,
            long firstDroppedOrdinal,
            long firstDroppedSchedulerLagMicros,
            String firstDroppedFailureSummary,
            long globalOutstandingMaximum,
            long queueDepthMaximum,
            long queueWaitMaximumMicros,
            long bindingBusyMaximum,
            long pendingPermitMaximum,
            long queueDepthAtEnd,
            long globalOutstandingAtEnd,
            long bindingBusyAtEnd,
            long pendingPermitAtEnd,
            boolean actorLanesStopped,
            long managedLedgerOperations,
            long managedLedgerOperationOutstandingMaximum,
            long managedLedgerOperationOutstandingAtEnd,
            long admissionToFirstMetadataDispatchP99Micros,
            long metadataOperationsPerRequestP99,
            long triggerAppendCompletions,
            long successorEstablishments,
            long entryP99Micros,
            long byteP99Micros,
            long ageP99Micros,
            long schedulerLagP99Micros,
            long callbackLagP99Micros) {
        private String json() {
            return "{\"schema\":\"NEREUS_V2_M3_ALLOCATOR_NATIVE_BASELINE_ROW_V3\""
                    + ",\"diagnosticOnly\":true,\"authority\":false,\"selectionEligible\":false"
                    + ",\"activePopulation\":" + activePopulation + ",\"latencyMillis\":" + latencyMillis
                    + ",\"offeredRate\":" + offeredRate + ",\"offered\":" + offered
                    + ",\"admitted\":" + admitted + ",\"dropped\":" + dropped
                    + ",\"completed\":" + completed + ",\"failed\":" + failed
                    + ",\"timedOut\":" + timedOut + ",\"warmupDropped\":" + warmupDropped
                    + ",\"warmupFailed\":" + warmupFailed + ",\"warmupTimedOut\":" + warmupTimedOut
                    + ",\"firstDroppedOrdinal\":" + firstDroppedOrdinal
                    + ",\"firstDroppedSchedulerLagMicros\":" + firstDroppedSchedulerLagMicros
                    + ",\"firstDroppedFailureSummary\":\"" + firstDroppedFailureSummary + "\""
                    + ",\"globalOutstandingMaximum\":" + globalOutstandingMaximum
                    + ",\"queueDepthMaximum\":" + queueDepthMaximum
                    + ",\"queueWaitMaximumMicros\":" + queueWaitMaximumMicros
                    + ",\"bindingBusyMaximum\":" + bindingBusyMaximum
                    + ",\"pendingPermitMaximum\":" + pendingPermitMaximum
                    + ",\"managedLedgerOperations\":" + managedLedgerOperations
                    + ",\"managedLedgerOperationOutstandingMaximum\":"
                    + managedLedgerOperationOutstandingMaximum
                    + ",\"managedLedgerOperationOutstandingAtEnd\":"
                    + managedLedgerOperationOutstandingAtEnd
                    + ",\"hiddenNativeQueueDepth\":0,\"bridgeActive\":0,\"bridgeQueueDepth\":0"
                    + ",\"admissionToFirstMetadataDispatchP99Micros\":"
                    + admissionToFirstMetadataDispatchP99Micros
                    + ",\"metadataOperationsPerRequestP99\":" + metadataOperationsPerRequestP99
                    + ",\"triggerAppendCompletions\":" + triggerAppendCompletions
                    + ",\"successorEstablishments\":" + successorEstablishments
                    + ",\"entryP99Micros\":" + entryP99Micros + ",\"byteP99Micros\":" + byteP99Micros
                    + ",\"ageP99Micros\":" + ageP99Micros
                    + ",\"schedulerLagP99Micros\":" + schedulerLagP99Micros
                    + ",\"callbackLagP99Micros\":" + callbackLagP99Micros
                    + ",\"queueDepthAtEnd\":" + queueDepthAtEnd
                    + ",\"globalOutstandingAtEnd\":" + globalOutstandingAtEnd
                    + ",\"bindingBusyAtEnd\":" + bindingBusyAtEnd
                    + ",\"pendingPermitAtEnd\":" + pendingPermitAtEnd
                    + ",\"actorLanesStopped\":" + actorLanesStopped + "}\n";
        }
    }
}
