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
import com.nereusstream.domain.registry.allocator.AllocatorEvidenceContextV1;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import org.junit.jupiter.api.Test;

class M3AllocatorWorkloadPlanTest {
    @Test
    void loadsEvidenceTestsFromTheReproducibleThinJar() throws Exception {
        java.nio.file.Path origin = java.nio.file.Path.of(getClass()
                .getProtectionDomain()
                .getCodeSource()
                .getLocation()
                .toURI());
        assertThat(origin.getFileName().toString())
                .startsWith("nereus-v2-m3-real-allocator-evidence")
                .endsWith(".jar");
    }

    @Test
    void freezesAllCandidatesAndMatrixDimensionsBeforeExecution() {
        assertThat(M3AllocatorWorkloadPlan.candidates())
                .extracting(M3AllocatorWorkloadPlan.Candidate::toString)
                .containsExactly(
                        "Candidate[mode=STRICT_SERIALIZED, rangeSize=1]",
                        "Candidate[mode=RANGE_LEASED, rangeSize=16]",
                        "Candidate[mode=RANGE_LEASED, rangeSize=64]",
                        "Candidate[mode=RANGE_LEASED, rangeSize=256]",
                        "Candidate[mode=RANGE_LEASED, rangeSize=1024]");
        assertThat(M3AllocatorWorkloadPlan.ACTIVE_POPULATIONS).containsExactly(10_000, 100_000);
        assertThat(M3AllocatorWorkloadPlan.METADATA_LATENCY_P99_MILLIS).containsExactly(1, 5, 10, 25);
        assertThat(M3AllocatorWorkloadPlan.OFFERED_RATES).containsExactly(200, 250, 333, 500, 750, 1000);
        assertThat(M3AllocatorWorkloadPlan.BROKER_ACTORS).isEqualTo(4);
        assertThat(M3AllocatorWorkloadPlan.WORKER_THREADS).isEqualTo(96);
        assertThat(M3AllocatorWorkloadPlan.BYTE_PAYLOAD_BYTES).isEqualTo(65_536);
        assertThat(M3AllocatorWorkloadPlan.AGE_ADVANCE_NANOS).isEqualTo(1_000_000_000L);
        assertThat(AllocatorEvidenceContextV1.massTakeoverRecoveryBoundMicros(10_000))
                .isEqualTo(30_000_000L);
        assertThat(AllocatorEvidenceContextV1.massTakeoverRecoveryBoundMicros(100_000))
                .isEqualTo(60_000_000L);
    }

    @Test
    void producesExactDeterministicDistributionJitterAndStormShape() {
        List<M3AllocatorWorkloadPlan.PlannedRequest> first = collect(10_000, 333);
        List<M3AllocatorWorkloadPlan.PlannedRequest> second = collect(10_000, 333);
        assertThat(first).isEqualTo(second).hasSize(13_320);
        assertThat(first).allSatisfy(request -> {
            assertThat(request.actorId()).isEqualTo((int) (request.requestOrdinal() % 4));
            assertThat(request.ledgerIndex()).isBetween(0, 9_999);
        });

        EnumMap<M3AllocatorWorkloadPlan.Phase, Integer> phases = new EnumMap<>(M3AllocatorWorkloadPlan.Phase.class);
        EnumMap<M3AllocatorWorkloadPlan.Trigger, Integer> triggers =
                new EnumMap<>(M3AllocatorWorkloadPlan.Trigger.class);
        first.forEach(request -> {
            phases.merge(request.phase(), 1, Math::addExact);
            triggers.merge(request.trigger(), 1, Math::addExact);
        });
        assertThat(phases)
                .containsEntry(M3AllocatorWorkloadPlan.Phase.WARM_UP, 3_330)
                .containsEntry(M3AllocatorWorkloadPlan.Phase.MEASURED_STEADY, 3_330)
                .containsEntry(M3AllocatorWorkloadPlan.Phase.MEASURED_STORM, 6_660);
        assertThat(triggers)
                .containsEntry(M3AllocatorWorkloadPlan.Trigger.ENTRY, 6_660)
                .containsEntry(M3AllocatorWorkloadPlan.Trigger.BYTE, 3_996)
                .containsEntry(M3AllocatorWorkloadPlan.Trigger.AGE, 2_664);

        long previous = Long.MIN_VALUE;
        for (M3AllocatorWorkloadPlan.PlannedRequest request : first) {
            assertThat(request.arrivalOffsetMicros()).isGreaterThanOrEqualTo(previous);
            previous = request.arrivalOffsetMicros();
        }
        // ADR-0094 applies the signed jitter to the phase boundary as well. Phase authority is the
        // ordinal, not a wall-clock comparison against an unjittered boundary.
        assertThat(first.get(3_330).phase()).isEqualTo(M3AllocatorWorkloadPlan.Phase.MEASURED_STEADY);
        assertThat(first.get(3_330).arrivalOffsetMicros()).isEqualTo(9_999_875L);
        assertThat(first.get(6_660).phase()).isEqualTo(M3AllocatorWorkloadPlan.Phase.MEASURED_STORM);
        assertThat(first.get(6_660).arrivalOffsetMicros()).isEqualTo(29_999_750L);
        assertThat(first.get(first.size() - 1).arrivalOffsetMicros()).isLessThan(40_000_000L);
        assertThat(new HashSet<>(first.stream()
                        .map(M3AllocatorWorkloadPlan.PlannedRequest::ledgerIndex)
                        .toList()))
                .hasSizeGreaterThan(5_000);
    }

    private static List<M3AllocatorWorkloadPlan.PlannedRequest> collect(int population, int rate) {
        List<M3AllocatorWorkloadPlan.PlannedRequest> requests = new ArrayList<>();
        M3AllocatorWorkloadPlan.requests(population, rate).forEach(requests::add);
        return List.copyOf(requests);
    }
}
