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
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/** Diagnostic-only real-Oxia execution of the 10k STRICT fault batch; it emits no evidence. */
class M3RealAllocatorFaultBatchDiagnosticTest {
    @Test
    void exposesTheFirstExactFaultBatchFailureWithoutPublishingEvidence() throws Exception {
        String serviceAddress = M3AllocatorRawEvidenceFiles.requiredProperty("oxiaServiceAddress");
        String sourceCommit = M3AllocatorRawEvidenceFiles.requiredProperty("nereusSourceCommit");
        if (!sourceCommit.matches("[0-9a-f]{40}")) {
            throw new IllegalArgumentException("diagnostic Nereus source commit must be exact");
        }
        AtomicLong faultEvents = new AtomicLong();
        ThreadPoolExecutor workers = M3RealAllocatorEvidenceTest.exactWorkers();
        workers.prestartAllCoreThreads();

        Throwable executionFailure = null;
        try (M3RealOxiaActors actors = new M3RealOxiaActors(serviceAddress)) {
            M3CandidateAllocatorPopulation population = new M3CandidateAllocatorPopulation(
                    AllocatorEvidenceCandidateV1.strict(),
                    0,
                    "diagnostic-" + sourceCommit.substring(0, 16),
                    actors,
                    workers);
            population.ensurePopulation(10_000);
            actors.setControlledLatencyMillis(1);
            M3AllocatorRequestTelemetry telemetry =
                    new M3AllocatorRequestTelemetry(event -> faultEvents.incrementAndGet(), System.nanoTime());
            M3AllocatorFaultRunner faults = new M3AllocatorFaultRunner(workers, actors, telemetry);
            faults.runAll(
                    AllocatorEvidenceContextV1.candidateContext(
                            AllocatorEvidenceCandidateV1.strict(), 10_000, 1, 200),
                    population);
        } catch (Exception | Error failure) {
            executionFailure = failure;
            throw failure;
        } finally {
            workers.shutdownNow();
            if (!workers.awaitTermination(3, TimeUnit.MINUTES)) {
                IllegalStateException termination =
                        new IllegalStateException("diagnostic allocator executor did not terminate");
                if (executionFailure == null) {
                    throw termination;
                }
                executionFailure.addSuppressed(termination);
            }
        }
        assertThat(faultEvents.get()).isPositive();
    }
}
