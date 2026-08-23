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

package com.nereusstream.domain.registry.allocator;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Deterministic entry point for an injected real/native runner. This class schedules the frozen matrix but supplies no
 * fake measurements and never creates a selection receipt.
 */
public final class AllocatorDeterministicEvidenceHarnessV1 {
    private AllocatorDeterministicEvidenceHarnessV1() {}

    public static List<Scenario> completePlan(AllocatorModeV1 mode, long selectedRangeSize, int brokerCount) {
        List<Scenario> scenarios = new ArrayList<>();
        for (AllocatorEvidenceWorkloadV1 workload :
                AllocatorEvidenceWorkloadV1.completeMatrix(mode, selectedRangeSize, brokerCount)) {
            for (FaultCut cut : FaultCut.values()) {
                scenarios.add(new Scenario(workload, cut, brokerCount, cut == FaultCut.SYNCHRONIZED_ROLLOVER_STORM));
            }
        }
        return List.copyOf(scenarios);
    }

    public static List<AllocatorNativeRelativeMetricsV1> execute(
            List<Scenario> exactPlan, ScenarioRunner sourceQualifiedRunner) {
        Objects.requireNonNull(exactPlan, "exactPlan");
        Objects.requireNonNull(sourceQualifiedRunner, "sourceQualifiedRunner");
        if (exactPlan.isEmpty()) {
            throw new IllegalArgumentException("allocator evidence plan must not be empty");
        }
        List<AllocatorNativeRelativeMetricsV1> measurements = new ArrayList<>(exactPlan.size());
        for (Scenario scenario : exactPlan) {
            AllocatorNativeRelativeMetricsV1 measured =
                    Objects.requireNonNull(sourceQualifiedRunner.run(scenario), "allocator measurement");
            if (!measured.workload().equals(scenario.workload())) {
                throw new IllegalArgumentException("allocator runner returned metrics for a different workload");
            }
            measurements.add(measured);
        }
        return List.copyOf(measurements);
    }

    public enum FaultCut {
        CELL_RESERVE_RESPONSE_LOSS,
        MODE_SPECIFIC_GRANT_READY_CUT,
        NODE_CREATE_RESPONSE_LOSS,
        HEAD_PUBLISH_RESPONSE_LOSS,
        CELL_CLEAR_RESPONSE_LOSS,
        OWNER_TAKEOVER,
        LATE_OLD_OWNER_WRITE,
        BROKER_WIDE_MASS_TAKEOVER,
        SYNCHRONIZED_ROLLOVER_STORM
    }

    public record Scenario(
            AllocatorEvidenceWorkloadV1 workload,
            FaultCut faultCut,
            int concurrentBrokerCount,
            boolean synchronizedStorm) {
        public Scenario {
            Objects.requireNonNull(workload, "workload");
            Objects.requireNonNull(faultCut, "faultCut");
            if (concurrentBrokerCount != workload.brokerCount()
                    || synchronizedStorm != (faultCut == FaultCut.SYNCHRONIZED_ROLLOVER_STORM)) {
                throw new IllegalArgumentException("allocator scenario concurrency/storm shape differs");
            }
        }
    }

    @FunctionalInterface
    public interface ScenarioRunner {
        AllocatorNativeRelativeMetricsV1 run(Scenario scenario);
    }
}
