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

/** Diagnostic-only replay of consecutive formal 10k/1ms STRICT intervals against real Oxia. */
class M3RealAllocatorStrictIntervalDiagnosticTest {
    @Test
    void replaysTheExactFormalSequenceWithoutUnexpectedWarmupFailure() throws Exception {
        String serviceAddress =
                M3V3DiagnosticOutput.requiredProperty("nereus.m3.allocator.v3.oxiaServiceAddress");
        String sourceCommit = M3V3DiagnosticOutput.requiredProperty("nereus.m3.allocator.v3.nereusCommit");
        if (!sourceCommit.matches("[0-9a-f]{40}")) {
            throw new IllegalArgumentException("diagnostic Nereus source commit must be exact");
        }
        ThreadPoolExecutor workers = M3RealAllocatorEvidenceTest.exactWorkers();
        workers.prestartAllCoreThreads();
        Throwable executionFailure = null;
        try (M3RealOxiaActors actors = new M3RealOxiaActors(serviceAddress)) {
            M3CandidateAllocatorPopulation population = new M3CandidateAllocatorPopulation(
                    AllocatorEvidenceCandidateV1.strict(),
                    0,
                    "strict-interval-" + sourceCommit.substring(0, 16),
                    actors,
                    workers);
            population.ensurePopulation(10_000);
            actors.setControlledLatencyMillis(1);
            M3V3AllocatorFormalHarness.HarnessResult fixed = runInterval(
                    population,
                    Cell.fixedRate(Candidate.STRICT, 10_000, 1, 1_000),
                    1_000);
            assertThat(fixed.runnerResult().warmupUnexpectedFailedAfterAdmission())
                    .withFailMessage(
                            "unexpected fixed-1000 warmup failure: %s",
                            fixed.runnerResult().warmupFirstUnexpectedFailure())
                    .isZero();
            assertThat(fixed.runnerResult().actorLanesStoppedAtCleanupDeadline()).isTrue();
            assertThat(fixed.runnerResult().warmupTimedOutAfterAdmission()).isZero();
            assertThat(fixed.runnerResult().globalOutstandingMaximum()).isGreaterThan(4);
            M3V3AllocatorFormalHarness.HarnessResult derived =
                    runInterval(population, Cell.derived(Candidate.STRICT, 10_000, 1), 800);
            assertThat(derived.runnerResult().warmupUnexpectedFailedAfterAdmission())
                    .withFailMessage(
                            "unexpected derived-800 warmup failure: %s",
                            derived.runnerResult().warmupFirstUnexpectedFailure())
                    .isZero();
            assertThat(derived.runnerResult().actorLanesStoppedAtCleanupDeadline()).isTrue();
            assertThat(derived.runnerResult().warmupTimedOutAfterAdmission()).isZero();
            assertThat(derived.runnerResult().globalOutstandingMaximum()).isGreaterThan(4);
            M3V3DiagnosticOutput.writeNew(
                    "strict-formal-sequence.json",
                    diagnosticJson(sourceCommit, fixed.runnerResult(), derived.runnerResult()));
        } catch (Exception | Error failure) {
            executionFailure = failure;
            throw failure;
        } finally {
            workers.shutdownNow();
            if (!workers.awaitTermination(3, TimeUnit.MINUTES)) {
                IllegalStateException termination =
                        new IllegalStateException("STRICT interval diagnostic executor did not terminate");
                if (executionFailure == null) {
                    throw termination;
                }
                executionFailure.addSuppressed(termination);
            }
        }
    }

    private static String diagnosticJson(
            String sourceCommit,
            M3V3AsyncActorLaneRunner.IntervalResult fixed,
            M3V3AsyncActorLaneRunner.IntervalResult derived) {
        return "{\"schema\":\"NEREUS_V2_M3_ALLOCATOR_STRICT_FORMAL_SEQUENCE_DIAGNOSTIC_V1\""
                + ",\"diagnosticOnly\":true,\"authority\":false,\"selectionEligible\":false"
                + ",\"sourceCommit\":" + M3V3DiagnosticOutput.jsonString(sourceCommit)
                + ",\"fixed1000\":" + intervalJson(fixed)
                + ",\"derived800\":" + intervalJson(derived) + "}\n";
    }

    private static String intervalJson(M3V3AsyncActorLaneRunner.IntervalResult result) {
        return "{\"warmupOffered\":" + result.warmupOffered()
                + ",\"warmupCompleted\":" + result.warmupCompleted()
                + ",\"warmupLoadRejectedAfterAdmission\":" + result.warmupLoadRejectedAfterAdmission()
                + ",\"warmupUnexpectedFailedAfterAdmission\":"
                + result.warmupUnexpectedFailedAfterAdmission()
                + ",\"warmupTimedOutAfterAdmission\":" + result.warmupTimedOutAfterAdmission()
                + ",\"measuredOffered\":" + result.offered()
                + ",\"measuredCompleted\":" + result.completed()
                + ",\"measuredFailedAfterAdmission\":" + result.failedAfterAdmission()
                + ",\"measuredTimedOutAfterAdmission\":" + result.timedOutAfterAdmission()
                + ",\"globalOutstandingMaximum\":" + result.globalOutstandingMaximum()
                + ",\"actorLanesStoppedAtCleanupDeadline\":"
                + result.actorLanesStoppedAtCleanupDeadline() + '}';
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
