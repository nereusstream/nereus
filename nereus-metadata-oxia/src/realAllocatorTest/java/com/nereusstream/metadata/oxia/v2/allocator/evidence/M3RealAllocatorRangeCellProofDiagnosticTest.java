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
import com.nereusstream.domain.registry.allocator.AllocatorEvidenceCandidateV1;
import com.nereusstream.domain.registry.allocator.AllocatorEvidenceContextV1;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/** Diagnostic-only real-Oxia RANGE construction/renewal overlap; it emits no evidence. */
class M3RealAllocatorRangeCellProofDiagnosticTest {
    private static final int REQUEST_COUNT = 4_096;
    private static final int INSTALLED_LEDGER_COUNT = 64;
    private static final int RESERVED_FAULT_LEDGER_START = 9_936;

    @Test
    void constructsRangePopulationAndOverlapsInstalledReadsWithCellGrantRenewal() throws Exception {
        String serviceAddress = M3AllocatorRawEvidenceFiles.requiredProperty("oxiaServiceAddress");
        String sourceCommit = M3AllocatorRawEvidenceFiles.requiredProperty("nereusSourceCommit");
        if (!sourceCommit.matches("[0-9a-f]{40}")) {
            throw new IllegalArgumentException("diagnostic Nereus source commit must be exact");
        }
        AllocatorEvidenceCandidateV1 candidate = AllocatorEvidenceCandidateV1.range(16);
        AllocatorEvidenceContextV1 context =
                AllocatorEvidenceContextV1.candidateContext(candidate, 10_000, 1, 200);
        AtomicLong events = new AtomicLong();
        Set<Long> allocatedLedgerIds = ConcurrentHashMap.newKeySet();
        ThreadPoolExecutor workers = M3RealAllocatorEvidenceTest.exactWorkers();
        workers.prestartAllCoreThreads();

        Throwable executionFailure = null;
        try (M3RealOxiaActors actors = new M3RealOxiaActors(serviceAddress)) {
            M3CandidateAllocatorPopulation population = new M3CandidateAllocatorPopulation(
                    candidate,
                    1,
                    "range-proof-" + sourceCommit.substring(0, 16),
                    actors,
                    workers);
            population.ensurePopulation(10_000);
            actors.setControlledLatencyMillis(1);
            M3AllocatorRequestTelemetry telemetry =
                    new M3AllocatorRequestTelemetry(event -> events.incrementAndGet(), System.nanoTime());
            CompletionService<Void> completions = new ExecutorCompletionService<>(workers);
            for (int index = 0; index < REQUEST_COUNT; index++) {
                int exactIndex = index;
                completions.submit(() -> {
                    int slot = exactIndex % (2 * INSTALLED_LEDGER_COUNT);
                    int ledger = slot < INSTALLED_LEDGER_COUNT
                            ? slot
                            : RESERVED_FAULT_LEDGER_START + slot - INSTALLED_LEDGER_COUNT;
                    M3AllocatorRequestTelemetry.RequestTrace trace = telemetry.trace(
                            context,
                            new M3AllocatorWorkloadPlan.PlannedRequest(
                                    3_000_000L + exactIndex,
                                    exactIndex & 3,
                                    ledger,
                                    M3AllocatorWorkloadPlan.Trigger.ENTRY,
                                    M3AllocatorWorkloadPlan.Phase.MEASURED_STEADY,
                                    0),
                            null,
                            1);
                    trace.offered();
                    trace.enqueued();
                    trace.dispatched();
                    M3CandidateAllocatorPopulation.Allocation allocation = population.rollover(
                            trace, ledger, M3CandidateAllocatorPopulation.ResponseLossAt.NONE);
                    if (!allocatedLedgerIds.add(allocation.ledgerId())) {
                        throw new AssertionError("RANGE proof diagnostic reused one virtual ledger ID");
                    }
                    trace.completed();
                    return null;
                });
            }
            M3BoundedCompletionDrain.await(
                    completions, REQUEST_COUNT, 180, "RANGE Cell-proof diagnostic batch");
        } catch (Exception | Error failure) {
            executionFailure = failure;
            throw failure;
        } finally {
            workers.shutdownNow();
            if (!workers.awaitTermination(3, TimeUnit.MINUTES)) {
                IllegalStateException termination =
                        new IllegalStateException("RANGE proof diagnostic executor did not terminate");
                if (executionFailure == null) {
                    throw termination;
                }
                executionFailure.addSuppressed(termination);
            }
        }
        assertThat(allocatedLedgerIds).hasSize(REQUEST_COUNT);
        assertThat(events.get()).isPositive();
    }
}
