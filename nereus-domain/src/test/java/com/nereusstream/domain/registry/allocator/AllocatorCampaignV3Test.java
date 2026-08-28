/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.domain.registry.allocator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignEvaluationV3.Status;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.BaselineStatus;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Campaign;
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
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Row;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

class AllocatorCampaignV3Test {
    @Test
    void logicalInventoryHasIndependentFixedAndDerivedIdentities() {
        List<Cell> cells = AllocatorCampaignV3.logicalCells();

        assertThat(cells).hasSize(328);
        assertThat(cells.stream().map(Cell::contextId)).doesNotHaveDuplicates();
        assertThat(new HashSet<>(cells)).hasSize(328);

        Cell fixed = Cell.fixedRate(Candidate.STRICT, 10_000, 1, 200);
        assertThatThrownBy(() -> Cell.fixedRate(Candidate.STRICT, 10_000, 1, 400))
                .isInstanceOf(IllegalArgumentException.class);
        Cell derived = Cell.derived(Candidate.STRICT, 10_000, 1);
        assertThat(fixed).isNotEqualTo(derived);
        assertThat(fixed.contextId()).isNotEqualTo(derived.contextId());
    }

    @Test
    void exactDerivedFloorsDoNotQuantizeToFixedRates() {
        assertThat(AllocatorCampaignV3.derivedRate(1000)).isEqualTo(800);
        assertThat(AllocatorCampaignV3.derivedRate(750)).isEqualTo(600);
        assertThat(AllocatorCampaignV3.derivedRate(500)).isEqualTo(400);
        assertThat(AllocatorCampaignV3.derivedRate(333)).isEqualTo(267);
        assertThat(AllocatorCampaignV3.derivedRate(250)).isEqualTo(200);
        assertThat(AllocatorCampaignV3.derivedRate(200)).isEqualTo(200);
    }

    @Test
    void v3ArrivalEntryAcceptsOnlyFixedOrExactlyDerivedRates() {
        assertThatThrownBy(() -> AllocatorEvidenceScheduleV1.arrivalCursor(800))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not frozen");
        for (int rate : List.of(800, 600, 400, 267)) {
            assertThat(AllocatorEvidenceScheduleV1.arrivalCursorV3(rate).nextOfferedTimestampMicros())
                    .isZero();
        }
        assertThatThrownBy(() -> AllocatorEvidenceScheduleV1.arrivalCursorV3(801))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not frozen or exact-derived");
    }

    @Test
    void noneQualifiedIsValidAndDerivesMinimumValidCount() {
        Completed completed = drive(cell ->
                cell.candidate().nativePath() ? passing(cell, cell.rateSlot().fixedRate()) : relativeFailure(cell));

        assertThat(completed.plan().executedPerformanceCells()).isEqualTo(13);
        assertThat(completed.plan().dispositions()).hasSize(315);
        assertThat(completed.plan().baselineStatus()).isEqualTo(BaselineStatus.AVAILABLE);
        assertThat(evaluate(completed).status()).isEqualTo(Status.NONE_QUALIFIED);
    }

    @Test
    void strictSelectionRequiresAllRangeCandidatesToBeEliminated() {
        Completed completed = drive(cell -> {
            if (cell.candidate().nativePath() || cell.candidate() == Candidate.STRICT) {
                return passing(cell, actionRate(cell));
            }
            return relativeFailure(cell);
        });

        assertThat(completed.plan().executedPerformanceCells()).isEqualTo(20);
        assertThat(evaluate(completed).status()).isEqualTo(Status.STRICT_SELECTED);
        assertThat(evaluate(completed).selectedCandidate()).contains(Candidate.STRICT);
    }

    @Test
    void smallestQualifiedRangeIsSelectedAndLargerRangesAreDispositioned() {
        for (Candidate selected :
                List.of(Candidate.RANGE_16, Candidate.RANGE_64, Candidate.RANGE_256, Candidate.RANGE_1024)) {
            Completed completed = drive(cell -> {
                if (cell.candidate().nativePath() || cell.candidate() == selected) {
                    return passing(cell, actionRate(cell));
                }
                return relativeFailure(cell);
            });

            assertThat(evaluate(completed).status()).isEqualTo(Status.RANGE_SELECTED);
            assertThat(evaluate(completed).selectedCandidate()).contains(selected);
            assertThat(completed.plan().qualifiedCandidates()).containsExactly(selected);
            if (selected == Candidate.RANGE_16) {
                assertThat(completed.plan().executedPerformanceCells()).isEqualTo(17);
            }
        }
    }

    @Test
    void bothQualifiedIsValidButNonPromotable() {
        Completed completed = drive(cell -> {
            if (cell.candidate().nativePath()
                    || cell.candidate() == Candidate.STRICT
                    || cell.candidate() == Candidate.RANGE_16) {
                return passing(cell, actionRate(cell));
            }
            return relativeFailure(cell);
        });

        assertThat(evaluate(completed).status()).isEqualTo(Status.BOTH_QUALIFIED);
        assertThat(evaluate(completed).selectionEligible()).isFalse();
    }

    @Test
    void missingNativeBaselineIsAnIndependentWholeEvaluationState() {
        Completed completed = drive(cell -> {
            if (cell.candidate().nativePath()
                    && cell.activeManagedLedgers() == 10_000
                    && cell.metadataLatencyP99Millis() == 1) {
                return zeroFailureMiss(cell, cell.rateSlot().fixedRate());
            }
            return passing(cell, actionRate(cell));
        });

        assertThat(completed.plan().baselineStatus()).isEqualTo(BaselineStatus.UNAVAILABLE);
        assertThat(completed.plan().executedPerformanceCells()).isEqualTo(13);
        assertThat(completed.plan().dispositions())
                .filteredOn(disposition -> disposition.kind() == DispositionKind.COMPARISON_UNAVAILABLE)
                .hasSize(280);
        assertThat(evaluate(completed).status()).isEqualTo(Status.NATIVE_BASELINE_UNAVAILABLE);
        assertThat(evaluate(completed).qualifiedCandidates()).isEmpty();
    }

    @Test
    void duplicateDerivedFloorHasItsOwnReconstructableDisposition() {
        Completed completed = drive(cell -> {
            if (cell.candidate().nativePath()) {
                return nativeAtRate(cell, 250);
            }
            return relativeFailure(cell);
        });

        assertThat(completed.plan().dispositions())
                .filteredOn(disposition -> disposition.kind() == DispositionKind.DUPLICATE_DERIVED_FLOOR)
                .hasSize(5);
        assertThat(completed.plan().dispositions())
                .filteredOn(disposition -> disposition.kind() == DispositionKind.DUPLICATE_DERIVED_FLOOR)
                .allSatisfy(disposition ->
                        assertThat(disposition.cell().rateSlot().fixedRate()).isEqualTo(200));
    }

    @Test
    void nativeFiveHundredProducesAnExactCandidateFourHundredAction() {
        List<Observation> observations = new ArrayList<>();
        for (int guard = 0; guard < 200; guard++) {
            Plan plan = AllocatorCampaignPlannerV3.plan(observations);
            ExecuteCell action = (ExecuteCell) plan.nextAction().orElseThrow();
            if (!action.cell().candidate().nativePath()) {
                while (action.offeredRate() != 400) {
                    observations.add(absoluteFailure(action.cell(), action.offeredRate()));
                    action = (ExecuteCell) AllocatorCampaignPlannerV3.plan(observations)
                            .nextAction()
                            .orElseThrow();
                }
                assertThat(action.cell().rateSlot().derivedFloor()).isTrue();
                assertThat(action.offeredRate()).isEqualTo(400);
                return;
            }
            observations.add(nativeAtRate(action.cell(), 500));
        }
        throw new AssertionError("allocator V3 planner did not reach an exact derived 400 slot");
    }

    @Test
    void forgedDispositionAndConservationAreRejected() {
        Completed completed =
                drive(cell -> cell.candidate().nativePath() ? passing(cell, actionRate(cell)) : relativeFailure(cell));
        List<Disposition> forged = new ArrayList<>(completed.plan().dispositions());
        Disposition first = forged.get(0);
        forged.set(
                0, new Disposition(first.cell(), DispositionKind.CANDIDATE_ELIMINATED, first.dependencyContextIds()));

        assertThatThrownBy(() -> AllocatorCampaignValidatorV3.validate(new Campaign(completed.observations(), forged)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("caller dispositions differ");

        IntervalEvidence passing = passing(Cell.fixedRate(Candidate.NATIVE, 10_000, 1, 1000), 1000);
        IntervalEvidence corrupt = new IntervalEvidence(
                passing.cell(),
                passing.offeredRate(),
                passing.offered(),
                passing.admitted(),
                0,
                passing.completed() - 1,
                0,
                0,
                passing.terminal(),
                0,
                0,
                0,
                0,
                0,
                100_000,
                100_000,
                100_000,
                1000,
                100_000,
                100_000,
                0,
                0,
                0);
        assertThatThrownBy(() -> AllocatorCampaignValidatorV3.validateIntervalConservation(corrupt))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("conservation");
    }

    private static AllocatorCampaignEvaluationV3 evaluate(Completed completed) {
        return AllocatorCampaignSelectorV3.evaluate(
                new Campaign(completed.observations(), completed.plan().dispositions()));
    }

    private static Completed drive(Function<Cell, IntervalEvidence> intervals) {
        List<Observation> observations = new ArrayList<>();
        for (int guard = 0; guard < 800; guard++) {
            Plan plan = AllocatorCampaignPlannerV3.plan(observations);
            if (plan.completed()) {
                return new Completed(List.copyOf(observations), plan);
            }
            Observation observation;
            if (plan.nextAction().orElseThrow() instanceof ExecuteCell action) {
                observation = intervals.apply(action.cell());
                if (((IntervalEvidence) observation).offeredRate() != action.offeredRate()) {
                    observation = copyAtRate((IntervalEvidence) observation, action.offeredRate());
                }
            } else if (plan.nextAction().orElseThrow() instanceof ExecuteFaultRow action) {
                observation = passingFault(action.row());
            } else {
                throw new AssertionError("unknown allocator V3 action");
            }
            observations.add(observation);
        }
        throw new AssertionError("allocator V3 test campaign did not terminate");
    }

    private static int actionRate(Cell cell) {
        return cell.rateSlot().derivedFloor() ? 800 : cell.rateSlot().fixedRate();
    }

    private static IntervalEvidence nativeAtRate(Cell cell, int sustainableRate) {
        int rate = cell.rateSlot().fixedRate();
        return rate > sustainableRate ? zeroFailureMiss(cell, rate) : passing(cell, rate);
    }

    private static IntervalEvidence passing(Cell cell, int rate) {
        long offered = (long) rate * AllocatorCampaignV3.MEASURED_SECONDS;
        return new IntervalEvidence(
                cell, rate, offered, offered, 0, offered, 0, 0, offered, 0, 0, 0, 0, 0, 100_000, 100_000, 100_000, rate,
                100_000, 100_000, 0, 0, 0);
    }

    private static IntervalEvidence zeroFailureMiss(Cell cell, int rate) {
        long offered = (long) rate * AllocatorCampaignV3.MEASURED_SECONDS;
        return new IntervalEvidence(
                cell,
                rate,
                offered,
                offered,
                0,
                offered - 1,
                1,
                0,
                offered,
                0,
                0,
                0,
                0,
                0,
                100_000,
                100_000,
                100_000,
                rate,
                100_000,
                100_000,
                0,
                0,
                0);
    }

    private static IntervalEvidence absoluteFailure(Cell cell, int rate) {
        IntervalEvidence passing = passing(cell, rate);
        return new IntervalEvidence(
                cell,
                rate,
                passing.offered(),
                passing.admitted(),
                0,
                passing.completed(),
                0,
                0,
                passing.terminal(),
                0,
                0,
                0,
                0,
                0,
                250_001,
                passing.oxiaOperationP99Micros(),
                passing.queueAgeP99Micros(),
                passing.queueDepthMaximum(),
                passing.starvationMaximumMicros(),
                passing.appendStallP99Micros(),
                0,
                0,
                0);
    }

    private static IntervalEvidence relativeFailure(Cell cell) {
        int rate = actionRate(cell);
        IntervalEvidence passing = passing(cell, rate);
        return new IntervalEvidence(
                cell,
                rate,
                passing.offered(),
                passing.admitted(),
                0,
                passing.completed(),
                0,
                0,
                passing.terminal(),
                0,
                0,
                0,
                0,
                0,
                passing.rolloverP99Micros(),
                passing.oxiaOperationP99Micros(),
                passing.queueAgeP99Micros(),
                passing.queueDepthMaximum(),
                passing.starvationMaximumMicros(),
                400_001,
                0,
                0,
                0);
    }

    private static IntervalEvidence copyAtRate(IntervalEvidence interval, int rate) {
        if (interval.appendStallP99Micros() == 400_001) {
            return relativeFailureAtRate(interval.cell(), rate);
        }
        return passing(interval.cell(), rate);
    }

    private static IntervalEvidence relativeFailureAtRate(Cell cell, int rate) {
        IntervalEvidence passing = passing(cell, rate);
        return new IntervalEvidence(
                cell,
                rate,
                passing.offered(),
                passing.admitted(),
                0,
                passing.completed(),
                0,
                0,
                passing.terminal(),
                0,
                0,
                0,
                0,
                0,
                passing.rolloverP99Micros(),
                passing.oxiaOperationP99Micros(),
                passing.queueAgeP99Micros(),
                passing.queueDepthMaximum(),
                passing.starvationMaximumMicros(),
                400_001,
                0,
                0,
                0);
    }

    private static FaultEvidence passingFault(Row row) {
        return new FaultEvidence(row, EnumSet.allOf(AllocatorFaultCutV1.class), 0, 0, 0, 0, 0, 0, 0, 0, 1, 1_000_000);
    }

    private record Completed(List<Observation> observations, Plan plan) {}
}
