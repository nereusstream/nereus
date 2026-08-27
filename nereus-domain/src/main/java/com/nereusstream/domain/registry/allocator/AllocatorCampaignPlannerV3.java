/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.domain.registry.allocator;

import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.BaselineStatus;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Candidate;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Cell;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Disposition;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.DispositionKind;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.ExecuteCell;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.ExecuteFaultRow;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.FaultEvidence;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.IntervalEvidence;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Observation;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Plan;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.RequiredAction;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Row;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Deterministic ADR-0108 adaptive planner. It derives exact floors and never calls a metadata store. */
public final class AllocatorCampaignPlannerV3 {
    private AllocatorCampaignPlannerV3() {}

    public static Plan plan(List<Observation> observations) {
        State state = new State();
        for (Observation observation : List.copyOf(observations)) {
            RequiredAction expected = state.nextAction()
                    .orElseThrow(() -> invalid("allocator V3 observation exists after the campaign completed"));
            if (expected instanceof ExecuteCell executeCell) {
                if (!(observation instanceof IntervalEvidence interval)
                        || !interval.cell().equals(executeCell.cell())
                        || interval.offeredRate() != executeCell.offeredRate()) {
                    throw invalid("allocator V3 interval is absent, reordered, or differs from its logical slot");
                }
                state.accept(interval);
            } else if (expected instanceof ExecuteFaultRow executeFaultRow) {
                if (!(observation instanceof FaultEvidence fault)
                        || !fault.row().equals(executeFaultRow.row())) {
                    throw invalid("allocator V3 fault observation is absent, reordered, or for another row");
                }
                state.accept(fault);
            } else {
                throw new IllegalStateException("allocator V3 planner produced an unknown required action");
            }
        }
        Optional<RequiredAction> next = state.nextAction();
        boolean completed = next.isEmpty();
        if (completed
                && state.executedPerformanceCells + state.dispositions.size()
                        != AllocatorCampaignV3.LOGICAL_PERFORMANCE_CELLS) {
            throw new IllegalStateException("allocator V3 terminal plan does not account for all logical cells");
        }
        return new Plan(
                AllocatorCampaignV3.PLANNER_VERSION,
                AllocatorCampaignV3.logicalCells(),
                List.copyOf(state.dispositions.values()),
                next,
                state.executedPerformanceCells,
                state.qualifiedCandidates,
                state.baselineStatus(),
                completed);
    }

    static boolean completeZeroFailure(IntervalEvidence interval) {
        long expectedOffered = Math.multiplyExact((long) interval.offeredRate(), AllocatorCampaignV3.MEASURED_SECONDS);
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
        long rate = interval.offeredRate();
        return completeZeroFailure(interval)
                && interval.rolloverP99Micros() <= 250_000
                && interval.oxiaOperationP99Micros() <= 250_000
                && interval.queueAgeP99Micros() <= 1_000_000
                && interval.queueDepthMaximum() <= Math.multiplyExact(2, rate)
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
            throw invalid("allocator V3 interval violates offered/admitted terminal conservation");
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
            throw invalid("allocator V3 interval terminal conservation overflows", error);
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

        private int dependencyContextId() {
            return executedContextIds.get(executedContextIds.size() - 1);
        }
    }

    private record SlotExecution(Cell cell, int offeredRate) {}

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
        private int candidateExecutionIndex;
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
                        if (nativeResults.values().stream().anyMatch(result -> !result.available())) {
                            dispositionComparisonUnavailable();
                            phase = Phase.COMPLETE;
                        } else {
                            phase = Phase.CANDIDATE;
                        }
                        continue;
                    }
                    Row row = nativeRows.get(nativeRowIndex);
                    int rate = AllocatorCampaignV3.DESCENDING_FIXED_RATES.get(nativeRateIndex);
                    return Optional.of(new ExecuteCell(
                            Cell.fixed(
                                    Candidate.NATIVE,
                                    row.activeManagedLedgers(),
                                    row.metadataLatencyP99Millis(),
                                    nativeRateIndex),
                            rate));
                }
                if (candidateIndex >= Candidate.values().length) {
                    phase = Phase.COMPLETE;
                    continue;
                }
                Candidate candidate = Candidate.values()[candidateIndex];
                List<Row> candidateRows = rows(candidate);
                if (candidateEliminated) {
                    dispositionRemainingCandidate(candidate);
                    advanceCandidate();
                    continue;
                }
                if (candidateRowIndex == candidateRows.size()) {
                    qualifiedCandidates.add(candidate);
                    if (candidate.range()) {
                        dispositionLargerRanges(candidate);
                        phase = Phase.COMPLETE;
                    } else {
                        advanceCandidate();
                    }
                    continue;
                }
                Row row = candidateRows.get(candidateRowIndex);
                NativeResult nativeResult = nativeResults.get(nativeRow(row));
                if (nativeResult == null || !nativeResult.available()) {
                    throw new IllegalStateException("allocator V3 candidate row lacks a comparable native baseline");
                }
                if (pendingSustainableInterval != null) {
                    return Optional.of(new ExecuteFaultRow(row));
                }
                List<SlotExecution> executions = executions(candidate, row, nativeResult.sustainableRate());
                dispositionNonExecutedRates(candidate, row, nativeResult);
                SlotExecution execution = executions.get(candidateExecutionIndex);
                return Optional.of(new ExecuteCell(execution.cell(), execution.offeredRate()));
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
                throw new IllegalStateException("allocator V3 interval cannot be accepted after completion");
            }
        }

        private void acceptNative(IntervalEvidence interval) {
            if (completeZeroFailure(interval)) {
                for (int lowerIndex = nativeRateIndex + 1;
                        lowerIndex < AllocatorCampaignV3.DESCENDING_FIXED_RATES.size();
                        lowerIndex++) {
                    disposition(
                            Cell.fixed(
                                    Candidate.NATIVE,
                                    interval.cell().activeManagedLedgers(),
                                    interval.cell().metadataLatencyP99Millis(),
                                    lowerIndex),
                            DispositionKind.NATIVE_SUSTAINABLE_RATE_FOUND,
                            List.of(interval.cell().contextId()));
                }
                nativeResults.put(
                        interval.cell().row(),
                        new NativeResult(
                                interval.offeredRate(),
                                interval.appendStallP99Micros(),
                                List.copyOf(currentRowExecutedContextIds)));
                advanceNativeRow();
            } else if (nativeRateIndex + 1 == AllocatorCampaignV3.DESCENDING_FIXED_RATES.size()) {
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
            List<SlotExecution> executions =
                    executions(interval.cell().candidate(), row, nativeResult.sustainableRate());
            if (!absoluteCandidateBoundsPass(interval)) {
                if (candidateExecutionIndex + 1 < executions.size()) {
                    candidateExecutionIndex++;
                    return;
                }
                eliminateCandidate(interval.cell().contextId());
                return;
            }
            for (int index = candidateExecutionIndex + 1; index < executions.size(); index++) {
                disposition(
                        executions.get(index).cell(),
                        DispositionKind.ROW_TERMINAL_AT_SUSTAINABLE_RATE,
                        List.of(interval.cell().contextId()));
            }
            long relativeAppendBound = exactAdd(nativeResult.appendStallP99Micros(), 250_000);
            if (interval.appendStallP99Micros() > relativeAppendBound) {
                eliminateCandidate(interval.cell().contextId());
                return;
            }
            pendingSustainableInterval = interval;
        }

        private void accept(FaultEvidence fault) {
            if (pendingSustainableInterval == null) {
                throw new IllegalStateException("allocator V3 fault row has no sustainable interval dependency");
            }
            int dependency = pendingSustainableInterval.cell().contextId();
            if (!faultBoundsPass(fault)) {
                eliminateCandidate(dependency);
                return;
            }
            currentCandidateQualificationContextIds.add(dependency);
            candidateRowIndex++;
            candidateExecutionIndex = 0;
            pendingSustainableInterval = null;
            currentRowExecutedContextIds.clear();
        }

        private BaselineStatus baselineStatus() {
            if (nativeResults.size() < nativeRows.size()) {
                return BaselineStatus.INCOMPLETE;
            }
            return nativeResults.values().stream().allMatch(NativeResult::available)
                    ? BaselineStatus.AVAILABLE
                    : BaselineStatus.UNAVAILABLE;
        }

        private void dispositionComparisonUnavailable() {
            List<Integer> dependencies = nativeResults.values().stream()
                    .filter(result -> !result.available())
                    .map(NativeResult::dependencyContextId)
                    .toList();
            if (dependencies.isEmpty()) {
                throw new IllegalStateException("allocator V3 missing-baseline disposition lacks dependencies");
            }
            for (Cell cell : AllocatorCampaignV3.logicalCells()) {
                if (!cell.candidate().nativePath()) {
                    disposition(cell, DispositionKind.COMPARISON_UNAVAILABLE, dependencies);
                }
            }
        }

        private void dispositionNonExecutedRates(Candidate candidate, Row row, NativeResult nativeResult) {
            int floor = AllocatorCampaignV3.derivedRate(nativeResult.sustainableRate());
            int dependency = nativeResult.dependencyContextId();
            for (int ordinal = 0; ordinal < AllocatorCampaignV3.DESCENDING_FIXED_RATES.size(); ordinal++) {
                int rate = AllocatorCampaignV3.DESCENDING_FIXED_RATES.get(ordinal);
                if (rate < floor) {
                    dispositionIfAbsent(
                            Cell.fixed(candidate, row.activeManagedLedgers(), row.metadataLatencyP99Millis(), ordinal),
                            DispositionKind.BELOW_DERIVED_FLOOR,
                            List.of(dependency));
                } else if (rate == floor) {
                    dispositionIfAbsent(
                            Cell.fixed(candidate, row.activeManagedLedgers(), row.metadataLatencyP99Millis(), ordinal),
                            DispositionKind.DUPLICATE_DERIVED_FLOOR,
                            List.of(dependency));
                }
            }
        }

        private void eliminateCandidate(int dependencyContextId) {
            candidateEliminated = true;
            eliminationDependencyContextId = dependencyContextId;
            pendingSustainableInterval = null;
            candidateRowIndex++;
            candidateExecutionIndex = 0;
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
            candidateExecutionIndex = 0;
            candidateEliminated = false;
            eliminationDependencyContextId = null;
            pendingSustainableInterval = null;
            currentRowExecutedContextIds.clear();
            currentCandidateQualificationContextIds.clear();
        }

        private void dispositionRemainingCandidate(Candidate candidate) {
            if (eliminationDependencyContextId == null) {
                throw new IllegalStateException("allocator V3 eliminated candidate has no executed dependency");
            }
            for (int rowIndex = candidateRowIndex; rowIndex < rows(candidate).size(); rowIndex++) {
                Row row = rows(candidate).get(rowIndex);
                for (Cell cell : cells(row)) {
                    dispositionIfAbsent(
                            cell, DispositionKind.CANDIDATE_ELIMINATED, List.of(eliminationDependencyContextId));
                }
            }
        }

        private void dispositionLargerRanges(Candidate qualifiedRange) {
            if (currentCandidateQualificationContextIds.size() != 8) {
                throw new IllegalStateException("allocator V3 qualified RANGE does not bind all eight row intervals");
            }
            for (int index = candidateIndex + 1; index < Candidate.values().length; index++) {
                Candidate candidate = Candidate.values()[index];
                if (!candidate.range() || candidate.rangeSize() <= qualifiedRange.rangeSize()) {
                    throw new IllegalStateException("allocator V3 RANGE candidate order differs from ADR 0108");
                }
                for (Row row : rows(candidate)) {
                    for (Cell cell : cells(row)) {
                        disposition(
                                cell, DispositionKind.SMALLER_RANGE_QUALIFIED, currentCandidateQualificationContextIds);
                    }
                }
            }
        }

        private void disposition(Cell cell, DispositionKind kind, List<Integer> dependencies) {
            if (dispositions.putIfAbsent(cell, new Disposition(cell, kind, dependencies)) != null) {
                throw new IllegalStateException("allocator V3 logical cell received duplicate dispositions");
            }
        }

        private void dispositionIfAbsent(Cell cell, DispositionKind kind, List<Integer> dependencies) {
            dispositions.putIfAbsent(cell, new Disposition(cell, kind, dependencies));
        }
    }

    private static List<Row> rows(Candidate candidate) {
        List<Row> rows = new ArrayList<>(8);
        for (int population : AllocatorCampaignV3.POPULATIONS) {
            for (int latency : AllocatorCampaignV3.LATENCIES_MILLIS) {
                rows.add(new Row(candidate, population, latency));
            }
        }
        return List.copyOf(rows);
    }

    private static Row nativeRow(Row candidateRow) {
        return new Row(Candidate.NATIVE, candidateRow.activeManagedLedgers(), candidateRow.metadataLatencyP99Millis());
    }

    private static List<Cell> cells(Row row) {
        List<Cell> cells = new ArrayList<>(row.candidate().nativePath() ? 6 : 7);
        for (int ordinal = 0; ordinal < AllocatorCampaignV3.DESCENDING_FIXED_RATES.size(); ordinal++) {
            cells.add(Cell.fixed(row.candidate(), row.activeManagedLedgers(), row.metadataLatencyP99Millis(), ordinal));
        }
        if (!row.candidate().nativePath()) {
            cells.add(Cell.derived(row.candidate(), row.activeManagedLedgers(), row.metadataLatencyP99Millis()));
        }
        return List.copyOf(cells);
    }

    private static List<SlotExecution> executions(Candidate candidate, Row row, int nativeSustainableRate) {
        int floor = AllocatorCampaignV3.derivedRate(nativeSustainableRate);
        List<SlotExecution> executions = new ArrayList<>(AllocatorCampaignV3.DESCENDING_FIXED_RATES.size() + 1);
        for (int ordinal = 0; ordinal < AllocatorCampaignV3.DESCENDING_FIXED_RATES.size(); ordinal++) {
            int rate = AllocatorCampaignV3.DESCENDING_FIXED_RATES.get(ordinal);
            if (rate > floor) {
                Cell cell = Cell.fixed(candidate, row.activeManagedLedgers(), row.metadataLatencyP99Millis(), ordinal);
                executions.add(new SlotExecution(cell, rate));
            }
        }
        executions.add(new SlotExecution(
                Cell.derived(candidate, row.activeManagedLedgers(), row.metadataLatencyP99Millis()), floor));
        return List.copyOf(executions);
    }
}
