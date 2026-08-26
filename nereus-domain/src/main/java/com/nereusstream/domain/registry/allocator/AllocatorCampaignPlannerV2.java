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

import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.Candidate;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.Cell;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.Disposition;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.DispositionKind;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.ExecuteCell;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.ExecuteFaultRow;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.FaultEvidence;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.IntervalEvidence;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.Observation;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.Plan;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.RequiredAction;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.Row;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Deterministic ADR-0104 adaptive planner. It derives dispositions and never calls a metadata store. */
public final class AllocatorCampaignPlannerV2 {
    private AllocatorCampaignPlannerV2() {}

    public static Plan plan(List<Observation> observations) {
        State state = new State();
        for (Observation observation : List.copyOf(observations)) {
            RequiredAction expected = state.nextAction()
                    .orElseThrow(() -> invalid("allocator V2 observation exists after the campaign completed"));
            if (expected instanceof ExecuteCell executeCell) {
                if (!(observation instanceof IntervalEvidence interval)
                        || !interval.cell().equals(executeCell.cell())) {
                    throw invalid("allocator V2 interval observation is absent, reordered, or for another cell");
                }
                state.accept(interval);
            } else if (expected instanceof ExecuteFaultRow executeFaultRow) {
                if (!(observation instanceof FaultEvidence fault)
                        || !fault.row().equals(executeFaultRow.row())) {
                    throw invalid("allocator V2 fault observation is absent, reordered, or for another row");
                }
                state.accept(fault);
            } else {
                throw new IllegalStateException("allocator V2 planner produced an unknown required action");
            }
        }
        Optional<RequiredAction> next = state.nextAction();
        boolean completed = next.isEmpty();
        if (completed
                && state.executedPerformanceCells + state.dispositions.size()
                        != AllocatorCampaignV2.LOGICAL_PERFORMANCE_CELLS) {
            throw new IllegalStateException("allocator V2 terminal plan does not account for all logical cells");
        }
        return new Plan(
                AllocatorCampaignV2.PLANNER_VERSION,
                AllocatorCampaignV2.logicalCells(),
                List.copyOf(state.dispositions.values()),
                next,
                state.executedPerformanceCells,
                state.qualifiedCandidates,
                completed);
    }

    static boolean completeZeroFailure(IntervalEvidence interval) {
        long expectedOffered =
                (long) interval.cell().offeredRolloverRequestsPerSecond() * AllocatorCampaignV2.MEASURED_SECONDS;
        return interval.offered() == expectedOffered
                && interval.admitted() == expectedOffered
                && interval.overloadDroppedBeforeAdmission() == 0
                && interval.completed() == expectedOffered
                && interval.failedAfterAdmission() == 0
                && interval.timedOutAfterAdmission() == 0
                && interval.failedAssertions() == 0
                && interval.unexpectedErrors() == 0
                && interval.skipped() == 0
                && interval.duplicateLedgerIds() == 0
                && interval.reusedLedgerIds() == 0
                && interval.backlogAtEnd() == 0
                && interval.inFlightAtEnd() == 0
                && interval.waiterCountAtEnd() == 0;
    }

    static boolean absoluteCandidateBoundsPass(IntervalEvidence interval) {
        long rate = interval.cell().offeredRolloverRequestsPerSecond();
        return completeZeroFailure(interval)
                && interval.rolloverP99Micros() <= 250_000
                && interval.oxiaOperationP99Micros() <= 250_000
                && interval.queueAgeP99Micros() <= 1_000_000
                && interval.queueDepthMaximum() <= 2 * rate
                && interval.starvationMaximumMicros() <= 2_000_000
                && interval.appendStallP99Micros() <= 2_000_000;
    }

    static boolean faultBoundsPass(FaultEvidence fault) {
        long recoveryBound = fault.row().activeManagedLedgers() == 10_000 ? 30_000_000 : 60_000_000;
        return fault.cuts().equals(EnumSet.allOf(AllocatorFaultCutV1.class))
                && fault.failed() == 0
                && fault.timedOut() == 0
                && fault.unexpectedErrors() == 0
                && fault.failedAssertions() == 0
                && fault.skipped() == 0
                && fault.duplicateLedgerIds() == 0
                && fault.reusedLedgerIds() == 0
                && fault.permanentOrphans() == 0
                && fault.staleCandidateBurnMaximum() <= 1
                && fault.massTakeoverRecoveryMicros() <= recoveryBound;
    }

    static void validateConservation(IntervalEvidence interval) {
        long admittedTerminal =
                exactAdd(interval.completed(), interval.failedAfterAdmission(), interval.timedOutAfterAdmission());
        long offeredTerminal = exactAdd(interval.overloadDroppedBeforeAdmission(), admittedTerminal);
        if (interval.admitted() != admittedTerminal
                || interval.terminal() != admittedTerminal
                || interval.offered() != offeredTerminal) {
            throw invalid("allocator V2 interval violates offered/admitted terminal conservation");
        }
    }

    private static long exactAdd(long first, long... rest) {
        long result = first;
        try {
            for (long value : rest) {
                result = Math.addExact(result, value);
            }
            return result;
        } catch (ArithmeticException error) {
            throw invalid("allocator V2 interval terminal conservation overflows", error);
        }
    }

    private static IllegalArgumentException invalid(String message) {
        return new IllegalArgumentException(message);
    }

    private static IllegalArgumentException invalid(String message, Throwable cause) {
        return new IllegalArgumentException(message, cause);
    }

    private enum Phase {
        NATIVE,
        CANDIDATE,
        COMPLETE
    }

    private record NativeResult(int sustainableRate, long appendStallP99Micros, List<Integer> executedContextIds) {
        private boolean available() {
            return sustainableRate > 0;
        }
    }

    private static final class State {
        private final List<Row> nativeRows = rows(Candidate.NATIVE);
        private final Map<Row, NativeResult> nativeResults = new LinkedHashMap<>();
        private final LinkedHashMap<Cell, Disposition> dispositions = new LinkedHashMap<>();
        private final List<Candidate> qualifiedCandidates = new ArrayList<>();
        private final List<Integer> currentRowExecutedContextIds = new ArrayList<>();
        private final List<Integer> currentCandidateQualificationContextIds = new ArrayList<>();
        private Phase phase = Phase.NATIVE;
        private int nativeRowIndex;
        private int nativeRateIndex;
        private int candidateIndex = 1;
        private int candidateRowIndex;
        private int candidateEligibleRateIndex;
        private boolean candidateEliminated;
        private Integer eliminationDependencyContextId;
        private IntervalEvidence pendingSustainableInterval;
        private int executedPerformanceCells;

        private Optional<RequiredAction> nextAction() {
            while (true) {
                if (phase == Phase.COMPLETE) {
                    return Optional.empty();
                }
                if (phase == Phase.NATIVE) {
                    if (nativeRowIndex == nativeRows.size()) {
                        phase = Phase.CANDIDATE;
                        continue;
                    }
                    Row row = nativeRows.get(nativeRowIndex);
                    return Optional.of(new ExecuteCell(Cell.of(
                            Candidate.NATIVE,
                            row.activeManagedLedgers(),
                            row.metadataLatencyP99Millis(),
                            AllocatorCampaignV2.DESCENDING_RATES.get(nativeRateIndex))));
                }
                if (candidateIndex >= Candidate.values().length) {
                    phase = Phase.COMPLETE;
                    continue;
                }
                Candidate candidate = Candidate.values()[candidateIndex];
                List<Row> rows = rows(candidate);
                if (candidateEliminated) {
                    dispositionRemainingCandidate(candidate);
                    advanceCandidate();
                    continue;
                }
                if (candidateRowIndex == rows.size()) {
                    qualifiedCandidates.add(candidate);
                    if (candidate.range()) {
                        dispositionLargerRanges(candidate);
                        phase = Phase.COMPLETE;
                    } else {
                        advanceCandidate();
                    }
                    continue;
                }
                Row row = rows.get(candidateRowIndex);
                NativeResult nativeResult = nativeResults.get(nativeRow(row));
                if (nativeResult == null) {
                    throw new IllegalStateException("allocator V2 candidate row precedes its native baseline");
                }
                if (!nativeResult.available()) {
                    for (int rate : AllocatorCampaignV2.DESCENDING_RATES) {
                        disposition(
                                Cell.of(candidate, row.activeManagedLedgers(), row.metadataLatencyP99Millis(), rate),
                                DispositionKind.NATIVE_BASELINE_UNAVAILABLE,
                                nativeResult.executedContextIds());
                    }
                    candidateEliminated = true;
                    eliminationDependencyContextId = nativeResult
                            .executedContextIds()
                            .get(nativeResult.executedContextIds().size() - 1);
                    candidateRowIndex++;
                    continue;
                }
                if (pendingSustainableInterval != null) {
                    return Optional.of(new ExecuteFaultRow(row));
                }
                List<Integer> eligibleRates = eligibleRates(nativeResult.sustainableRate());
                dispositionRatesBelowFloor(candidate, row, nativeResult, eligibleRates);
                return Optional.of(new ExecuteCell(Cell.of(
                        candidate,
                        row.activeManagedLedgers(),
                        row.metadataLatencyP99Millis(),
                        eligibleRates.get(candidateEligibleRateIndex))));
            }
        }

        private void accept(IntervalEvidence interval) {
            validateConservation(interval);
            executedPerformanceCells++;
            currentRowExecutedContextIds.add(interval.cell().contextId());
            if (phase == Phase.NATIVE) {
                acceptNative(interval);
            } else if (phase == Phase.CANDIDATE) {
                acceptCandidate(interval);
            } else {
                throw new IllegalStateException("allocator V2 interval cannot be accepted after completion");
            }
        }

        private void acceptNative(IntervalEvidence interval) {
            if (completeZeroFailure(interval)) {
                for (int lowerIndex = nativeRateIndex + 1;
                        lowerIndex < AllocatorCampaignV2.DESCENDING_RATES.size();
                        lowerIndex++) {
                    disposition(
                            Cell.of(
                                    Candidate.NATIVE,
                                    interval.cell().activeManagedLedgers(),
                                    interval.cell().metadataLatencyP99Millis(),
                                    AllocatorCampaignV2.DESCENDING_RATES.get(lowerIndex)),
                            DispositionKind.NATIVE_SUSTAINABLE_RATE_FOUND,
                            List.of(interval.cell().contextId()));
                }
                nativeResults.put(
                        interval.cell().row(),
                        new NativeResult(
                                interval.cell().offeredRolloverRequestsPerSecond(),
                                interval.appendStallP99Micros(),
                                List.copyOf(currentRowExecutedContextIds)));
                advanceNativeRow();
            } else if (nativeRateIndex + 1 == AllocatorCampaignV2.DESCENDING_RATES.size()) {
                nativeResults.put(
                        interval.cell().row(), new NativeResult(0, 0, List.copyOf(currentRowExecutedContextIds)));
                advanceNativeRow();
            } else {
                nativeRateIndex++;
            }
        }

        private void acceptCandidate(IntervalEvidence interval) {
            Row row = interval.cell().row();
            NativeResult nativeResult = nativeResults.get(nativeRow(row));
            List<Integer> eligibleRates = eligibleRates(nativeResult.sustainableRate());
            if (!absoluteCandidateBoundsPass(interval)) {
                if (candidateEligibleRateIndex + 1 < eligibleRates.size()) {
                    candidateEligibleRateIndex++;
                    return;
                }
                eliminateCandidate(interval.cell().contextId());
                return;
            }
            for (int lowerIndex = candidateEligibleRateIndex + 1; lowerIndex < eligibleRates.size(); lowerIndex++) {
                disposition(
                        Cell.of(
                                interval.cell().candidate(),
                                row.activeManagedLedgers(),
                                row.metadataLatencyP99Millis(),
                                eligibleRates.get(lowerIndex)),
                        DispositionKind.ROW_TERMINAL_AT_SUSTAINABLE_RATE,
                        List.of(interval.cell().contextId()));
            }
            if (interval.appendStallP99Micros() > nativeResult.appendStallP99Micros() + 250_000) {
                eliminateCandidate(interval.cell().contextId());
                return;
            }
            pendingSustainableInterval = interval;
        }

        private void accept(FaultEvidence fault) {
            if (pendingSustainableInterval == null) {
                throw new IllegalStateException("allocator V2 fault row has no sustainable interval dependency");
            }
            int dependency = pendingSustainableInterval.cell().contextId();
            if (!faultBoundsPass(fault)) {
                eliminateCandidate(dependency);
                return;
            }
            currentCandidateQualificationContextIds.add(dependency);
            candidateRowIndex++;
            candidateEligibleRateIndex = 0;
            pendingSustainableInterval = null;
            currentRowExecutedContextIds.clear();
        }

        private void eliminateCandidate(int dependencyContextId) {
            candidateEliminated = true;
            eliminationDependencyContextId = dependencyContextId;
            pendingSustainableInterval = null;
            candidateRowIndex++;
            candidateEligibleRateIndex = 0;
            currentRowExecutedContextIds.clear();
        }

        private void advanceNativeRow() {
            nativeRowIndex++;
            nativeRateIndex = 0;
            currentRowExecutedContextIds.clear();
        }

        private void advanceCandidate() {
            candidateIndex++;
            candidateRowIndex = 0;
            candidateEligibleRateIndex = 0;
            candidateEliminated = false;
            eliminationDependencyContextId = null;
            pendingSustainableInterval = null;
            currentRowExecutedContextIds.clear();
            currentCandidateQualificationContextIds.clear();
        }

        private void dispositionRatesBelowFloor(
                Candidate candidate, Row row, NativeResult nativeResult, List<Integer> eligibleRates) {
            for (int rate : AllocatorCampaignV2.DESCENDING_RATES) {
                if (!eligibleRates.contains(rate)) {
                    Cell cell = Cell.of(candidate, row.activeManagedLedgers(), row.metadataLatencyP99Millis(), rate);
                    if (!dispositions.containsKey(cell)) {
                        disposition(
                                cell,
                                DispositionKind.BELOW_NATIVE_RELATIVE_FLOOR,
                                List.of(nativeResult
                                        .executedContextIds()
                                        .get(nativeResult.executedContextIds().size() - 1)));
                    }
                }
            }
        }

        private void dispositionRemainingCandidate(Candidate candidate) {
            if (eliminationDependencyContextId == null) {
                throw new IllegalStateException("allocator V2 eliminated candidate has no executed dependency");
            }
            List<Row> rows = rows(candidate);
            for (int rowIndex = candidateRowIndex; rowIndex < rows.size(); rowIndex++) {
                Row row = rows.get(rowIndex);
                for (int rate : AllocatorCampaignV2.DESCENDING_RATES) {
                    Cell cell = Cell.of(candidate, row.activeManagedLedgers(), row.metadataLatencyP99Millis(), rate);
                    if (!dispositions.containsKey(cell)) {
                        disposition(
                                cell, DispositionKind.CANDIDATE_ELIMINATED, List.of(eliminationDependencyContextId));
                    }
                }
            }
        }

        private void dispositionLargerRanges(Candidate qualifiedRange) {
            if (currentCandidateQualificationContextIds.size() != 8) {
                throw new IllegalStateException("allocator V2 qualified RANGE does not bind all eight row intervals");
            }
            for (int index = candidateIndex + 1; index < Candidate.values().length; index++) {
                Candidate candidate = Candidate.values()[index];
                if (!candidate.range() || candidate.rangeSize() <= qualifiedRange.rangeSize()) {
                    throw new IllegalStateException("allocator V2 RANGE candidate order differs from ADR 0104");
                }
                for (Row row : rows(candidate)) {
                    for (int rate : AllocatorCampaignV2.DESCENDING_RATES) {
                        disposition(
                                Cell.of(candidate, row.activeManagedLedgers(), row.metadataLatencyP99Millis(), rate),
                                DispositionKind.SMALLER_RANGE_QUALIFIED,
                                currentCandidateQualificationContextIds);
                    }
                }
            }
        }

        private void disposition(Cell cell, DispositionKind kind, List<Integer> dependencies) {
            if (dispositions.putIfAbsent(cell, new Disposition(cell, kind, dependencies)) != null) {
                throw new IllegalStateException("allocator V2 logical cell received duplicate dispositions");
            }
        }
    }

    private static List<Row> rows(Candidate candidate) {
        List<Row> rows = new ArrayList<>(8);
        for (int population : AllocatorCampaignV2.POPULATIONS) {
            for (int latency : AllocatorCampaignV2.LATENCIES_MILLIS) {
                rows.add(new Row(candidate, population, latency));
            }
        }
        return List.copyOf(rows);
    }

    private static Row nativeRow(Row candidateRow) {
        return new Row(Candidate.NATIVE, candidateRow.activeManagedLedgers(), candidateRow.metadataLatencyP99Millis());
    }

    private static List<Integer> eligibleRates(int nativeSustainableRate) {
        List<Integer> eligible = AllocatorCampaignV2.DESCENDING_RATES.stream()
                .filter(rate -> rate * 100L >= nativeSustainableRate * 80L)
                .toList();
        if (eligible.isEmpty()) {
            throw new IllegalStateException("allocator V2 native sustainable rate has no relative candidate rate");
        }
        return eligible;
    }
}
