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
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** Diagnostic-only real-Oxia RANGE-1024 10k-to-100k construction; it emits no evidence. */
class M3RealAllocatorRange100kConstructionDiagnosticTest {
    @Test
    void constructsRange1024PopulationThroughTheExact100kBoundary() throws Exception {
        String serviceAddress = M3AllocatorRawEvidenceFiles.requiredProperty("oxiaServiceAddress");
        String sourceCommit = M3AllocatorRawEvidenceFiles.requiredProperty("nereusSourceCommit");
        if (!sourceCommit.matches("[0-9a-f]{40}")) {
            throw new IllegalArgumentException("diagnostic Nereus source commit must be exact");
        }
        ThreadPoolExecutor workers = M3RealAllocatorEvidenceTest.exactWorkers();
        workers.prestartAllCoreThreads();

        long construction10kMicros;
        long construction100kMicros;
        Throwable executionFailure = null;
        try (M3RealOxiaActors actors = new M3RealOxiaActors(serviceAddress)) {
            M3CandidateAllocatorPopulation population = new M3CandidateAllocatorPopulation(
                    AllocatorEvidenceCandidateV1.range(1024),
                    4,
                    "range-100k-" + sourceCommit.substring(0, 16),
                    actors,
                    workers);
            construction10kMicros = population.ensurePopulation(10_000);
            construction100kMicros = population.ensurePopulation(100_000);
        } catch (Exception | Error failure) {
            executionFailure = failure;
            throw failure;
        } finally {
            workers.shutdownNow();
            if (!workers.awaitTermination(5, TimeUnit.MINUTES)) {
                IllegalStateException termination =
                        new IllegalStateException("RANGE-1024 100k construction executor did not terminate");
                if (executionFailure == null) {
                    throw termination;
                }
                executionFailure.addSuppressed(termination);
            }
        }
        assertThat(construction10kMicros).isPositive();
        assertThat(construction100kMicros).isPositive();
    }
}
