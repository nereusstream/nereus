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
import com.nereusstream.domain.registry.allocator.AllocatorEvidenceContextV1;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/** Diagnostic-only real-Oxia execution of the 10k RANGE-16 fault batch; it emits no evidence. */
class M3RealAllocatorRangeFaultBatchDiagnosticTest {
    @Test
    void executesAllNineRangeCutsWithoutPublishingEvidence() throws Exception {
        String serviceAddress = M3AllocatorRawEvidenceFiles.requiredProperty("oxiaServiceAddress");
        String sourceCommit = M3AllocatorRawEvidenceFiles.requiredProperty("nereusSourceCommit");
        if (!sourceCommit.matches("[0-9a-f]{40}")) {
            throw new IllegalArgumentException("diagnostic Nereus source commit must be exact");
        }
        AllocatorEvidenceCandidateV1 candidate = AllocatorEvidenceCandidateV1.range(16);
        AtomicLong faultEvents = new AtomicLong();
        AtomicLong asyncCompletions = new AtomicLong();
        AtomicLong asyncFailures = new AtomicLong();
        ThreadPoolExecutor workers = M3RealAllocatorEvidenceTest.exactWorkers();
        workers.prestartAllCoreThreads();

        Throwable executionFailure = null;
        try (M3RealOxiaActors actors = new M3RealOxiaActors(serviceAddress)) {
            M3CandidateAllocatorPopulation population = new M3CandidateAllocatorPopulation(
                    candidate,
                    1,
                    "range-fault-" + sourceCommit.substring(0, 16),
                    actors,
                    workers);
            population.ensurePopulation(10_000);
            actors.setControlledLatencyMillis(1);
            Cell campaignCell = Cell.fixedRate(Candidate.RANGE_16, 10_000, 1, 200);
            M3CandidateAllocatorPopulation.FormalAllocationObserver observer =
                    new M3CandidateAllocatorPopulation.FormalAllocationObserver() {
                        @Override
                        public void completed(
                                int actorId,
                                M3AllocatorWorkloadPlan.PlannedRequest request,
                                com.nereusstream.metadata.spi.allocator.BoundedVirtualLedgerAllocatorWorkflowV2.Result
                                        result,
                                long elapsedMicros) {
                            asyncCompletions.incrementAndGet();
                        }

                        @Override
                        public void failed(
                                int actorId,
                                M3AllocatorWorkloadPlan.PlannedRequest request,
                                Throwable failure,
                                long elapsedMicros) {
                            asyncFailures.incrementAndGet();
                        }
                    };
            M3V3AllocatorFormalHarness harness = M3V3AllocatorFormalHarness.forContractTest(
                    Duration.ZERO,
                    Duration.ofSeconds(2),
                    Duration.ofSeconds(5),
                    population.formalActorEndpointsV3(campaignCell, observer));
            List<M3V3AsyncActorLaneRunner.ScheduledOffer<M3V3AllocatorFormalHarness.CandidateRequest>> schedule =
                    new ArrayList<>();
            for (int index = 0; index < 64; index++) {
                long ordinal = 4_000_000L + index;
                int ledgerIndex = 9_936 + index;
                M3AllocatorWorkloadPlan.PlannedRequest planned = new M3AllocatorWorkloadPlan.PlannedRequest(
                        ordinal,
                        index & 3,
                        ledgerIndex,
                        M3AllocatorWorkloadPlan.Trigger.ENTRY,
                        M3AllocatorWorkloadPlan.Phase.MEASURED_STEADY,
                        index);
                schedule.add(new M3V3AsyncActorLaneRunner.ScheduledOffer<>(
                        ordinal,
                        index & 3,
                        ledgerIndex,
                        TimeUnit.MICROSECONDS.toNanos(index),
                        true,
                        new M3V3AllocatorFormalHarness.CandidateRequest(ordinal, ledgerIndex, planned)));
            }
            M3V3AllocatorFormalHarness.HarnessResult handoff = harness.runCandidate(
                    campaignCell,
                    200,
                    List.copyOf(schedule),
                    M3V3AllocatorFormalHarness.SupplementaryMeasurements::empty);
            assertThat(handoff.runnerResult().completed()).isEqualTo(64);
            assertThat(handoff.runnerResult().failedAfterAdmission()).isZero();

            M3AllocatorRequestTelemetry telemetry =
                    new M3AllocatorRequestTelemetry(event -> faultEvents.incrementAndGet(), System.nanoTime());
            M3AllocatorFaultRunner faults = new M3AllocatorFaultRunner(workers, actors, telemetry);
            faults.runAll(
                    AllocatorEvidenceContextV1.candidateContext(candidate, 10_000, 1, 200),
                    population);
        } catch (Exception | Error failure) {
            executionFailure = failure;
            throw failure;
        } finally {
            workers.shutdownNow();
            if (!workers.awaitTermination(3, TimeUnit.MINUTES)) {
                IllegalStateException termination =
                        new IllegalStateException("RANGE fault diagnostic executor did not terminate");
                if (executionFailure == null) {
                    throw termination;
                }
                executionFailure.addSuppressed(termination);
            }
        }
        assertThat(asyncCompletions.get()).isEqualTo(64);
        assertThat(asyncFailures.get()).isZero();
        assertThat(faultEvents.get()).isPositive();
    }
}
