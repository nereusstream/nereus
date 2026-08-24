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
import com.nereusstream.metadata.oxia.v2.allocator.OxiaVirtualLedgerAllocatorStore;
import com.nereusstream.metadata.spi.allocator.ProductionVirtualLedgerAllocator;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** Full current-source ADR-0094 real Oxia and exact native Pulsar evidence execution. */
class M3RealAllocatorEvidenceTest {
    @Test
    void executesCompleteRawMatrixAndAllNineCutsWithoutCallerSelectionInputs() throws Exception {
        assertFormalClassOrigins();
        Path outputDirectory = Path.of(M3AllocatorRawEvidenceFiles.requiredProperty("outputDirectory"));
        String serviceAddress = M3AllocatorRawEvidenceFiles.requiredProperty("oxiaServiceAddress");
        String executionDiscriminator =
                M3AllocatorRawEvidenceFiles.requiredProperty("nereusSourceCommit").substring(0, 16);
        Files.createDirectories(outputDirectory);
        long executionStartNanos = System.nanoTime();
        List<ConstructionTiming> construction = new ArrayList<>();
        ThreadPoolExecutor workers = exactWorkers();
        workers.prestartAllCoreThreads();

        M3AllocatorRawEvidenceFiles.EvidenceCounts counts;
        try (M3AllocatorRawEvidenceFiles evidence = new M3AllocatorRawEvidenceFiles(
                        outputDirectory, M3AllocatorRawEvidenceFiles.sourceTupleFromSystemProperties());
                M3RealOxiaActors actors = new M3RealOxiaActors(serviceAddress)) {
            M3AllocatorRequestTelemetry nativeTelemetry =
                    new M3AllocatorRequestTelemetry(evidence.nativeSink(), executionStartNanos);
            M3AllocatorRequestTelemetry candidateTelemetry =
                    new M3AllocatorRequestTelemetry(evidence.candidateSink(), executionStartNanos);
            M3AllocatorRequestTelemetry faultTelemetry =
                    new M3AllocatorRequestTelemetry(evidence.faultSink(), executionStartNanos);
            M3AllocatorIntervalRunner intervals = new M3AllocatorIntervalRunner(workers);

            try (M3NativePulsarPopulation nativePopulation = new M3NativePulsarPopulation(workers)) {
                for (int activePopulation : M3AllocatorWorkloadPlan.ACTIVE_POPULATIONS) {
                    construction.add(new ConstructionTiming(
                            "NATIVE", 0, activePopulation, nativePopulation.ensurePopulation(activePopulation)));
                    for (int latency : M3AllocatorWorkloadPlan.METADATA_LATENCY_P99_MILLIS) {
                        nativePopulation.setMetadataLatencyMillis(latency);
                        for (int rate : M3AllocatorWorkloadPlan.OFFERED_RATES) {
                            intervals.runNative(
                                    AllocatorEvidenceContextV1.nativeContext(activePopulation, latency, rate),
                                    nativePopulation,
                                    nativeTelemetry);
                        }
                    }
                    nativePopulation.setMetadataLatencyMillis(0);
                }
            }

            List<AllocatorEvidenceCandidateV1> candidates = candidates();
            for (int candidateIndex = 0; candidateIndex < candidates.size(); candidateIndex++) {
                AllocatorEvidenceCandidateV1 candidate = candidates.get(candidateIndex);
                M3CandidateAllocatorPopulation population = new M3CandidateAllocatorPopulation(
                        candidate, candidateIndex, executionDiscriminator, actors, workers);
                M3AllocatorFaultRunner faults = new M3AllocatorFaultRunner(workers, actors, faultTelemetry);
                for (int activePopulation : M3AllocatorWorkloadPlan.ACTIVE_POPULATIONS) {
                    construction.add(new ConstructionTiming(
                            candidate.mode().name(),
                            candidate.rangeSize(),
                            activePopulation,
                            population.ensurePopulation(activePopulation)));
                    for (int latency : M3AllocatorWorkloadPlan.METADATA_LATENCY_P99_MILLIS) {
                        actors.setControlledLatencyMillis(latency);
                        AllocatorEvidenceContextV1 faultContext = AllocatorEvidenceContextV1.candidateContext(
                                candidate, activePopulation, latency, 200);
                        faults.runAll(faultContext, population);
                        for (int rate : M3AllocatorWorkloadPlan.OFFERED_RATES) {
                            intervals.runCandidate(
                                    AllocatorEvidenceContextV1.candidateContext(
                                            candidate, activePopulation, latency, rate),
                                    population,
                                    candidateTelemetry);
                        }
                    }
                    actors.setControlledLatencyMillis(0);
                }
            }
            counts = evidence.counts();
        } finally {
            workers.shutdownNow();
            if (!workers.awaitTermination(5, TimeUnit.MINUTES)) {
                throw new IllegalStateException("allocator 96-worker executor did not terminate");
            }
        }

        assertThat(counts.nativeEvents()).isPositive();
        assertThat(counts.faultEvents()).isPositive();
        assertThat(counts.scale10kEvents()).isPositive();
        assertThat(counts.scale100kEvents()).isPositive();
        writeConstructionSummary(outputDirectory.resolve("construction-summary.json"), construction, counts);
    }

    private static void assertFormalClassOrigins() throws Exception {
        assertJarOrigin(M3RealAllocatorEvidenceTest.class, "nereus-v2-m3-real-allocator-evidence");
        assertJarOrigin(AllocatorEvidenceCandidateV1.class, "nereus-domain");
        assertJarOrigin(ProductionVirtualLedgerAllocator.class, "nereus-metadata-spi");
        assertJarOrigin(OxiaVirtualLedgerAllocatorStore.class, "nereus-metadata-oxia");
    }

    private static void assertJarOrigin(Class<?> type, String artifactName) throws Exception {
        Path source = Path.of(type.getProtectionDomain().getCodeSource().getLocation().toURI());
        assertThat(source.getFileName().toString())
                .as("formal class origin for %s", type.getName())
                .startsWith(artifactName)
                .endsWith(".jar");
    }

    private static List<AllocatorEvidenceCandidateV1> candidates() {
        List<AllocatorEvidenceCandidateV1> candidates = new ArrayList<>(5);
        candidates.add(AllocatorEvidenceCandidateV1.strict());
        AllocatorEvidenceCandidateV1.RANGE_SIZES.forEach(
                size -> candidates.add(AllocatorEvidenceCandidateV1.range(size)));
        return List.copyOf(candidates);
    }

    private static ThreadPoolExecutor exactWorkers() {
        AtomicInteger threadId = new AtomicInteger();
        ThreadFactory factory = runnable -> {
            Thread thread = new Thread(runnable, "m3-allocator-worker-" + threadId.getAndIncrement());
            thread.setDaemon(false);
            return thread;
        };
        return new ThreadPoolExecutor(
                M3AllocatorWorkloadPlan.WORKER_THREADS,
                M3AllocatorWorkloadPlan.WORKER_THREADS,
                0,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                factory,
                new ThreadPoolExecutor.AbortPolicy());
    }

    private static void writeConstructionSummary(
            Path target,
            List<ConstructionTiming> construction,
            M3AllocatorRawEvidenceFiles.EvidenceCounts counts) throws Exception {
        StringBuilder json = new StringBuilder(2_048);
        json.append("{\"schema\":\"NEREUS_V2_M3_ALLOCATOR_CONSTRUCTION_V1\",\"authority\":false,")
                .append("\"nativeEvents\":")
                .append(counts.nativeEvents())
                .append(",\"faultEvents\":")
                .append(counts.faultEvents())
                .append(",\"scale10kEvents\":")
                .append(counts.scale10kEvents())
                .append(",\"scale100kEvents\":")
                .append(counts.scale100kEvents())
                .append(",\"construction\":[");
        for (int index = 0; index < construction.size(); index++) {
            if (index > 0) {
                json.append(',');
            }
            ConstructionTiming timing = construction.get(index);
            json.append("{\"mode\":\"")
                    .append(timing.mode())
                    .append("\",\"rangeSize\":")
                    .append(timing.rangeSize())
                    .append(",\"activeManagedLedgers\":")
                    .append(timing.activeManagedLedgers())
                    .append(",\"elapsedMicros\":")
                    .append(timing.elapsedMicros())
                    .append('}');
        }
        Files.writeString(target, json.append("]}\n").toString(), StandardCharsets.UTF_8);
    }

    private record ConstructionTiming(
            String mode, long rangeSize, int activeManagedLedgers, long elapsedMicros) {}
}
