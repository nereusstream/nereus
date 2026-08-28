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
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Candidate;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Cell;
import com.nereusstream.domain.registry.allocator.AllocatorEvidenceCandidateV1;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** Diagnostic-only exact-schedule attribution for the candidate cutoff boundary. */
class M3V3CandidateCutoffDiagnosticTest {
    @Test
    void range16FixedAndDerivedRowsAttributeEveryCutoffDrop() throws Exception {
        String serviceAddress =
                M3V3DiagnosticOutput.requiredProperty("nereus.m3.allocator.v3.oxiaServiceAddress");
        String sourceCommit = M3V3DiagnosticOutput.requiredProperty("nereus.m3.allocator.v3.nereusCommit");
        if (!sourceCommit.matches("[0-9a-f]{40}")) {
            throw new IllegalArgumentException("candidate cutoff diagnostic Nereus commit must be exact");
        }
        ThreadPoolExecutor workers = M3RealAllocatorEvidenceTest.exactWorkers();
        workers.prestartAllCoreThreads();
        Throwable executionFailure = null;
        try (M3RealOxiaActors actors = new M3RealOxiaActors(serviceAddress)) {
            M3CandidateAllocatorPopulation population = new M3CandidateAllocatorPopulation(
                    AllocatorEvidenceCandidateV1.range(16),
                    1,
                    "range16-cutoff-" + sourceCommit.substring(0, 16),
                    actors,
                    workers);
            population.ensurePopulation(10_000);
            actors.setControlledLatencyMillis(1);
            M3V3AsyncActorLaneRunner.IntervalResult fixed = runInterval(
                            population,
                            Cell.fixedRate(Candidate.RANGE_16, 10_000, 1, 1_000),
                            1_000)
                    .runnerResult();
            M3V3AsyncActorLaneRunner.IntervalResult derived = runInterval(
                            population,
                            Cell.derived(Candidate.RANGE_16, 10_000, 1),
                            800)
                    .runnerResult();
            assertDiagnosticInterval(fixed);
            assertDiagnosticInterval(derived);
            M3V3DiagnosticOutput.writeNew(
                    "candidate-cutoff-diagnostic.json",
                    "{\"schema\":\"NEREUS_V2_M3_ALLOCATOR_CANDIDATE_CUTOFF_DIAGNOSTIC_V1\""
                            + ",\"diagnosticOnly\":true,\"authority\":false,\"selectionEligible\":false"
                            + ",\"sourceCommit\":" + M3V3DiagnosticOutput.jsonString(sourceCommit)
                            + ",\"fixed1000\":" + intervalJson(fixed)
                            + ",\"derived800\":" + intervalJson(derived) + "}\n");
        } catch (Exception | Error failure) {
            executionFailure = failure;
            throw failure;
        } finally {
            workers.shutdownNow();
            if (!workers.awaitTermination(3, TimeUnit.MINUTES)) {
                IllegalStateException termination =
                        new IllegalStateException("candidate cutoff diagnostic executor did not terminate");
                if (executionFailure == null) {
                    throw termination;
                }
                executionFailure.addSuppressed(termination);
            }
        }
    }

    private static void assertDiagnosticInterval(M3V3AsyncActorLaneRunner.IntervalResult result) {
        assertThat(result.warmupUnexpectedFailedAfterAdmission())
                .withFailMessage(
                        "candidate cutoff unexpected warmup failure: %s", result.warmupFirstUnexpectedFailure())
                .isZero();
        assertThat(result.warmupTimedOutAfterAdmission()).isZero();
        assertThat(result.failedAfterAdmission()).isZero();
        assertThat(result.timedOutAfterAdmission()).isZero();
        assertThat(result.globalOutstandingMaximum()).isGreaterThan(4);
        assertThat(result.actorLanesStoppedAtCleanupDeadline()).isTrue();
        assertThat(result.offered())
                .isEqualTo(result.overloadDroppedBeforeAdmission() + result.completed());
    }

    private static String intervalJson(M3V3AsyncActorLaneRunner.IntervalResult result) {
        M3V3AsyncActorLaneRunner.RequestTelemetry firstDrop = result.measuredTelemetry().stream()
                .filter(telemetry -> telemetry.outcome()
                        == M3V3AsyncActorLaneRunner.TerminalOutcome.OVERLOAD_DROPPED_BEFORE_ADMISSION)
                .findFirst()
                .orElse(null);
        return "{\"offered\":" + result.offered()
                + ",\"admitted\":" + result.admitted()
                + ",\"dropped\":" + result.overloadDroppedBeforeAdmission()
                + ",\"completed\":" + result.completed()
                + ",\"globalOutstandingMaximum\":" + result.globalOutstandingMaximum()
                + ",\"queueDepthMaximum\":" + result.queueDepthMaximum()
                + ",\"queueWaitMaximumMicros\":" + result.queueWaitMaximumMicros()
                + ",\"pendingPermitMaximum\":" + result.pendingPermitMaximum()
                + ",\"schedulerFiringLagP99Micros\":" + result.schedulerFiringLagP99Micros()
                + ",\"firstDroppedOrdinal\":" + (firstDrop == null ? -1 : firstDrop.ordinal())
                + ",\"firstDroppedSchedulerLagMicros\":"
                + (firstDrop == null ? 0 : firstDrop.schedulerFiringLagMicros())
                + ",\"firstDroppedFailure\":"
                + M3V3DiagnosticOutput.jsonString(firstDrop == null ? "" : firstDrop.failureSummary())
                + '}';
    }

    private static M3V3AllocatorFormalHarness.HarnessResult runInterval(
            M3CandidateAllocatorPopulation population, Cell cell, int offeredRate) throws InterruptedException {
        M3V3AllocatorFormalHarness harness = M3V3AllocatorFormalHarness.forContractTest(
                Duration.ofSeconds(10),
                Duration.ofSeconds(30),
                Duration.ofSeconds(5),
                population.formalActorEndpointsV3(cell, new NoopObserver()));
        List<M3V3AsyncActorLaneRunner.ScheduledOffer<M3V3AllocatorFormalHarness.CandidateRequest>> schedule =
                M3V3RealFormalActionRuntime.candidateSchedule(10_000, offeredRate);
        return harness.runCandidate(
                cell,
                offeredRate,
                schedule,
                M3V3AllocatorFormalHarness.SupplementaryMeasurements::empty);
    }

    private static final class NoopObserver implements M3CandidateAllocatorPopulation.FormalAllocationObserver {
        @Override
        public void completed(
                int actorId,
                M3AllocatorWorkloadPlan.PlannedRequest request,
                com.nereusstream.metadata.spi.allocator.BoundedVirtualLedgerAllocatorWorkflowV2.Result result,
                long elapsedMicros) {}

        @Override
        public void failed(
                int actorId,
                M3AllocatorWorkloadPlan.PlannedRequest request,
                Throwable failure,
                long elapsedMicros) {}
    }
}
