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
import com.nereusstream.domain.registry.allocator.AllocatorRawEvidenceEventV1;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class M3NativePulsarPathTest {
    @Test
    void usesExactManagedLedgerEntryByteAndAgeRolloverDecisionsWithoutRetainingPayloadBodies()
            throws Exception {
        ExecutorService workers = Executors.newFixedThreadPool(4);
        List<com.nereusstream.domain.registry.allocator.AllocatorRawEvidenceEventV1> events =
                new ArrayList<>();
        M3AllocatorRequestTelemetry telemetry =
                new M3AllocatorRequestTelemetry(events::add, System.nanoTime());
        try (M3NativePulsarPopulation population = new M3NativePulsarPopulation(workers)) {
            population.ensureOneForComponentTest();
            long predecessor = -1;
            for (M3AllocatorWorkloadPlan.Trigger trigger : M3AllocatorWorkloadPlan.Trigger.values()) {
                long ordinal = switch (trigger) {
                    case ENTRY -> 0;
                    case BYTE -> 5;
                    case AGE -> 8;
                };
                M3AllocatorRequestTelemetry.RequestTrace trace = telemetry.trace(
                        AllocatorEvidenceContextV1.nativeContext(10_000, 1, 200),
                        new M3AllocatorWorkloadPlan.PlannedRequest(
                                ordinal,
                                Math.toIntExact(ordinal % 4),
                                0,
                                trigger,
                                M3AllocatorWorkloadPlan.Phase.MEASURED_STEADY,
                                0),
                        null,
                        1);
                M3NativePulsarPopulation.NativeRollover result = population.rollover(trace, 0, trigger);
                trace.completed();
                assertThat(result.predecessorLedgerId()).isNotEqualTo(result.successorLedgerId());
                assertThat(result.triggerLedgerId()).isEqualTo(result.predecessorLedgerId());
                assertThat(result.successorEntryId()).isZero();
                assertThat(result.triggerEntryId())
                        .isEqualTo(trigger == M3AllocatorWorkloadPlan.Trigger.BYTE ? 16 : 1);
                assertThat(result.predecessorEntriesBeforeTrigger())
                        .isEqualTo(trigger == M3AllocatorWorkloadPlan.Trigger.BYTE ? 16 : 1);
                assertThat(result.predecessorBytesBeforeTrigger())
                        .isEqualTo(trigger == M3AllocatorWorkloadPlan.Trigger.BYTE
                                ? 1024L * 1024L
                                : M3AllocatorWorkloadPlan.BYTE_PAYLOAD_BYTES);
                assertThat(result.successorLedgerId()).isNotEqualTo(predecessor);
                predecessor = result.successorLedgerId();
                assertThat(population.retainedPayloadBytes()).isZero();
            }
        } finally {
            workers.shutdownNow();
            assertThat(workers.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }
        assertThat(events).isNotEmpty();
        assertThat(events.stream().map(AllocatorRawEvidenceEventV1::kind).toList())
                .containsSubsequence(
                        AllocatorRawEvidenceEventV1.EventKind.ADMITTED,
                        AllocatorRawEvidenceEventV1.EventKind.APPEND_ADMISSION_START,
                        AllocatorRawEvidenceEventV1.EventKind.APPEND_ADMISSION_RELEASE,
                        AllocatorRawEvidenceEventV1.EventKind.ALLOCATED_LEDGER_ID,
                        AllocatorRawEvidenceEventV1.EventKind.COMPLETED);
    }

    @Test
    void rejectsArmingTheOneMibThresholdBeforeTheExactUnlimitedPrefill() throws Exception {
        ExecutorService workers = Executors.newFixedThreadPool(4);
        try (M3NativePulsarPopulation population = new M3NativePulsarPopulation(workers)) {
            M3NativePulsarPopulation.PrematureByteThresholdProbe probe =
                    population.probePrematureByteThreshold();
            assertThat(probe.thresholdTriggerLedgerId()).isEqualTo(probe.predecessorLedgerId());
            assertThat(probe.thresholdTriggerEntryId()).isEqualTo(15);
            assertThat(probe.successorLedgerId()).isNotEqualTo(probe.predecessorLedgerId());
            assertThat(probe.successorEntryId()).isZero();
        } finally {
            workers.shutdownNow();
            assertThat(workers.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }
    }
}
