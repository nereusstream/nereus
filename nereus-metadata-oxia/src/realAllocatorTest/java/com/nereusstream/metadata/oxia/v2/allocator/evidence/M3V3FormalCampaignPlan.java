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

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Candidate;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Cell;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.ExecuteCell;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.ExecuteFaultRow;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.IntervalEvidence;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Observation;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.RequiredAction;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Row;
import com.nereusstream.domain.registry.allocator.AllocatorFaultCutV1;
import com.nereusstream.domain.registry.allocator.AllocatorNativeExecutionProfileV3;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Frozen physical-action projection for the ADR-0108 bounded adaptive formal entry. */
final class M3V3FormalCampaignPlan {
    static final String SCHEMA = "NEREUS_V2_M3_ALLOCATOR_CAMPAIGN_PLAN_V3";
    static final int LOGICAL_INTERVAL_CELLS = 328;
    static final int MINIMUM_VALID_EVALUATION_CELLS = 13;
    static final int MINIMUM_PROMOTABLE_CELLS = 17;
    static final int MAXIMUM_EXECUTED_INTERVAL_CELLS = 328;
    static final int MAXIMUM_EXECUTED_FAULT_ACTIONS = 360;
    static final int MAXIMUM_EXECUTED_SCALE_ACTIONS = 32;
    static final int MAXIMUM_TOTAL_EXECUTED_ACTIONS = 720;
    static final long CAMPAIGN_WALL_CLOCK_CAP_SECONDS = 48_000;

    private M3V3FormalCampaignPlan() {}

    static List<PlannedActionV3> zeroDecisionActions() {
        List<PlannedActionV3> actions = new ArrayList<>(MAXIMUM_TOTAL_EXECUTED_ACTIONS);
        for (Cell cell : AllocatorCampaignV3.logicalCells()) {
            actions.add(interval(cell));
        }
        for (Candidate candidate : Candidate.values()) {
            if (candidate.nativePath()) {
                continue;
            }
            for (int population : AllocatorCampaignV3.POPULATIONS) {
                for (int latency : AllocatorCampaignV3.LATENCIES_MILLIS) {
                    Row row = new Row(candidate, population, latency);
                    for (AllocatorFaultCutV1 cut : AllocatorFaultCutV1.values()) {
                        actions.add(PlannedActionV3.fault(row, cut));
                    }
                }
            }
        }
        for (Candidate candidate : Candidate.values()) {
            if (!candidate.range()) {
                continue;
            }
            for (int population : AllocatorCampaignV3.POPULATIONS) {
                for (int latency : AllocatorCampaignV3.LATENCIES_MILLIS) {
                    actions.add(PlannedActionV3.scale(new Row(candidate, population, latency)));
                }
            }
        }
        validateInventory(actions);
        return List.copyOf(actions);
    }

    static List<PlannedActionV3> actionsFor(RequiredAction required, List<Observation> validatedPrefix) {
        Objects.requireNonNull(required, "required");
        List<Observation> prefix = List.copyOf(Objects.requireNonNull(validatedPrefix, "validatedPrefix"));
        if (required instanceof ExecuteCell executeCell) {
            Cell cell = executeCell.cell();
            List<PlannedActionV3> actions = new ArrayList<>(2);
            if (cell.candidate().range() && firstExecutedCellForRow(cell.row(), prefix)) {
                actions.add(PlannedActionV3.scale(cell.row()));
            }
            actions.add(interval(cell, executeCell.offeredRate()));
            return List.copyOf(actions);
        }
        if (required instanceof ExecuteFaultRow executeFaultRow) {
            List<PlannedActionV3> actions = new ArrayList<>(AllocatorFaultCutV1.values().length);
            for (AllocatorFaultCutV1 cut : AllocatorFaultCutV1.values()) {
                actions.add(PlannedActionV3.fault(executeFaultRow.row(), cut));
            }
            return List.copyOf(actions);
        }
        throw new IllegalArgumentException("allocator V3 planner produced an unknown required action");
    }

    static Sha256Digest zeroDecisionPlanDigest() {
        StringBuilder canonical = new StringBuilder(64 * MAXIMUM_TOTAL_EXECUTED_ACTIONS);
        canonical.append(SCHEMA).append('\n')
                .append("plannerVersion=").append(AllocatorCampaignV3.PLANNER_VERSION).append('\n')
                .append("logicalIntervalCells=").append(LOGICAL_INTERVAL_CELLS).append('\n')
                .append("minimumValidEvaluationCells=").append(MINIMUM_VALID_EVALUATION_CELLS).append('\n')
                .append("minimumPromotableCells=").append(MINIMUM_PROMOTABLE_CELLS).append('\n')
                .append("maximumExecutedIntervalCells=").append(MAXIMUM_EXECUTED_INTERVAL_CELLS).append('\n')
                .append("maximumExecutedFaultActions=").append(MAXIMUM_EXECUTED_FAULT_ACTIONS).append('\n')
                .append("maximumExecutedScaleActions=").append(MAXIMUM_EXECUTED_SCALE_ACTIONS).append('\n')
                .append("maximumTotalExecutedActions=").append(MAXIMUM_TOTAL_EXECUTED_ACTIONS).append('\n')
                .append("campaignWallClockCapSeconds=").append(CAMPAIGN_WALL_CLOCK_CAP_SECONDS).append('\n')
                .append("nativeExecutionModel=").append(AllocatorNativeExecutionProfileV3.MODEL).append('\n')
                .append("nativeBridgeWorkers=")
                .append(AllocatorNativeExecutionProfileV3.NATIVE_BRIDGE_WORKERS)
                .append('\n')
                .append("nativeBridgeQueueCapacity=")
                .append(AllocatorNativeExecutionProfileV3.NATIVE_BRIDGE_QUEUE_CAPACITY)
                .append('\n')
                .append("hiddenDispatchQueue=")
                .append(AllocatorNativeExecutionProfileV3.HIDDEN_DISPATCH_QUEUE)
                .append('\n')
                .append("nativeExecutionProfileSha256=")
                .append(AllocatorNativeExecutionProfileV3.executionProfileDigest().toHex())
                .append('\n')
                .append("workloadScheduleSha256=")
                .append(AllocatorNativeExecutionProfileV3.scheduleDigest().toHex())
                .append('\n');
        for (PlannedActionV3 action : zeroDecisionActions()) {
            canonical.append(action.identity()).append('\n');
        }
        return Sha256Digest.hash(CanonicalBytes.copyOf(canonical.toString().getBytes(StandardCharsets.UTF_8)));
    }

    static int completedPhysicalActions(List<Observation> observations) {
        List<Observation> exact = List.copyOf(Objects.requireNonNull(observations, "observations"));
        int intervals = 0;
        int faultActions = 0;
        List<Row> scaledRows = new ArrayList<>();
        for (Observation observation : exact) {
            if (observation instanceof IntervalEvidence interval) {
                intervals++;
                if (interval.cell().candidate().range() && !scaledRows.contains(interval.cell().row())) {
                    scaledRows.add(interval.cell().row());
                }
            } else {
                faultActions = Math.addExact(faultActions, AllocatorFaultCutV1.values().length);
            }
        }
        return Math.addExact(Math.addExact(intervals, faultActions), scaledRows.size());
    }

    private static PlannedActionV3 interval(Cell cell) {
        int offeredRate = cell.rateSlot().derivedFloor() ? 0 : cell.rateSlot().fixedRate();
        return interval(cell, offeredRate);
    }

    private static PlannedActionV3 interval(Cell cell, int offeredRate) {
        return cell.candidate().nativePath()
                ? PlannedActionV3.nativeInterval(cell, offeredRate)
                : PlannedActionV3.candidateInterval(cell, offeredRate);
    }

    private static boolean firstExecutedCellForRow(Row row, List<Observation> observations) {
        return observations.stream()
                .filter(IntervalEvidence.class::isInstance)
                .map(IntervalEvidence.class::cast)
                .noneMatch(interval -> interval.cell().row().equals(row));
    }

    private static void validateInventory(List<PlannedActionV3> actions) {
        long intervals = actions.stream().filter(action -> action.kind().interval()).count();
        long faults = actions.stream().filter(action -> action.kind() == ActionKind.FAULT_ACTION).count();
        long scales = actions.stream().filter(action -> action.kind() == ActionKind.SCALE_ACTION).count();
        if (AllocatorCampaignV3.LOGICAL_PERFORMANCE_CELLS != LOGICAL_INTERVAL_CELLS
                || AllocatorCampaignV3.EXECUTED_PERFORMANCE_CELLS_MIN != MINIMUM_VALID_EVALUATION_CELLS
                || AllocatorCampaignV3.EXECUTED_PERFORMANCE_CELLS_MIN_PROMOTABLE != MINIMUM_PROMOTABLE_CELLS
                || AllocatorCampaignV3.EXECUTED_PERFORMANCE_CELLS_MAX != MAXIMUM_EXECUTED_INTERVAL_CELLS
                || intervals != MAXIMUM_EXECUTED_INTERVAL_CELLS
                || faults != MAXIMUM_EXECUTED_FAULT_ACTIONS
                || scales != MAXIMUM_EXECUTED_SCALE_ACTIONS
                || actions.size() != MAXIMUM_TOTAL_EXECUTED_ACTIONS
                || actions.stream().map(PlannedActionV3::identity).distinct().count() != actions.size()) {
            throw new IllegalStateException("allocator V3 frozen physical action inventory differs");
        }
    }

    enum ActionKind {
        NATIVE_INTERVAL,
        CANDIDATE_INTERVAL,
        FAULT_ACTION,
        SCALE_ACTION;

        boolean interval() {
            return this == NATIVE_INTERVAL || this == CANDIDATE_INTERVAL;
        }
    }

    record PlannedActionV3(
            ActionKind kind, Cell cell, Row row, AllocatorFaultCutV1 faultCut, int offeredRate) {
        PlannedActionV3 {
            Objects.requireNonNull(kind, "kind");
            switch (kind) {
                case NATIVE_INTERVAL -> {
                    if (cell == null
                            || !cell.candidate().nativePath()
                            || row != null
                            || faultCut != null
                            || offeredRate != cell.rateSlot().fixedRate()) {
                        throw new IllegalArgumentException("allocator native interval action dimensions differ");
                    }
                }
                case CANDIDATE_INTERVAL -> {
                    if (cell == null
                            || cell.candidate().nativePath()
                            || row != null
                            || faultCut != null
                            || (cell.rateSlot().derivedFloor() && offeredRate != 0
                                    && (offeredRate < AllocatorCampaignV3.ABSOLUTE_FLOOR
                                            || offeredRate > AllocatorCampaignV3.DESCENDING_FIXED_RATES.get(0)))
                            || (!cell.rateSlot().derivedFloor() && offeredRate != cell.rateSlot().fixedRate())) {
                        throw new IllegalArgumentException("allocator candidate interval action dimensions differ");
                    }
                }
                case FAULT_ACTION -> {
                    if (cell != null || row == null || row.candidate().nativePath() || faultCut == null
                            || offeredRate != 0) {
                        throw new IllegalArgumentException("allocator fault action dimensions differ");
                    }
                }
                case SCALE_ACTION -> {
                    if (cell != null || row == null || !row.candidate().range() || faultCut != null
                            || offeredRate != 0) {
                        throw new IllegalArgumentException("allocator scale action dimensions differ");
                    }
                }
            }
        }

        static PlannedActionV3 nativeInterval(Cell cell, int offeredRate) {
            return new PlannedActionV3(ActionKind.NATIVE_INTERVAL, cell, null, null, offeredRate);
        }

        static PlannedActionV3 candidateInterval(Cell cell, int offeredRate) {
            return new PlannedActionV3(ActionKind.CANDIDATE_INTERVAL, cell, null, null, offeredRate);
        }

        static PlannedActionV3 fault(Row row, AllocatorFaultCutV1 cut) {
            return new PlannedActionV3(ActionKind.FAULT_ACTION, null, row, cut, 0);
        }

        static PlannedActionV3 scale(Row row) {
            return new PlannedActionV3(ActionKind.SCALE_ACTION, null, row, null, 0);
        }

        String identity() {
            return switch (kind) {
                case NATIVE_INTERVAL, CANDIDATE_INTERVAL -> kind + ":" + cell.contextId();
                case FAULT_ACTION -> kind + ":" + row.candidate() + ":" + row.activeManagedLedgers() + ":"
                        + row.metadataLatencyP99Millis() + ":" + faultCut;
                case SCALE_ACTION -> kind + ":" + row.candidate() + ":" + row.activeManagedLedgers() + ":"
                        + row.metadataLatencyP99Millis();
            };
        }
    }
}
