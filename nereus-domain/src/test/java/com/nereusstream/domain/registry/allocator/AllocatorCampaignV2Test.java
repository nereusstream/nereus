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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignEvaluationV2.Status;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.Campaign;
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
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.Row;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

class AllocatorCampaignV2Test {
    @Test
    void inventoryKeepsAll288LogicalCellsAndStableV1ContextIdentities() {
        List<Cell> cells = AllocatorCampaignV2.logicalCells();

        assertThat(cells).hasSize(288);
        assertThat(new HashSet<>(cells)).hasSize(288);
        assertThat(cells.stream().map(Cell::contextId)).doesNotHaveDuplicates();
        assertThat(cells.stream().filter(cell -> cell.candidate() == Candidate.NATIVE))
                .hasSize(48);
        assertThat(cells.stream().filter(cell -> !cell.candidate().nativePath()))
                .hasSize(240);
        assertThat(cells.get(0)).isEqualTo(Cell.of(Candidate.NATIVE, 10_000, 1, 1000));
        assertThat(cells.get(48)).isEqualTo(Cell.of(Candidate.STRICT, 10_000, 1, 1000));
        assertThat(cells.get(287)).isEqualTo(Cell.of(Candidate.RANGE_1024, 100_000, 25, 200));
    }

    @Test
    void minimumCompletedCampaignProducesValidNonPromotableNoneEvaluation() {
        CompletedCampaign completed = drive(
                cell -> cell.candidate().nativePath() ? passing(cell, 100_000) : relativeAppendFailure(cell),
                AllocatorCampaignV2Test::passingFault);

        AllocatorCampaignEvaluationV2 evaluation = AllocatorCampaignSelectorV2.evaluate(completed.campaign());
        assertThat(evaluation.status()).isEqualTo(Status.NONE_QUALIFIED);
        assertThat(evaluation.selectionEligible()).isFalse();
        assertThat(evaluation.selectedCandidate()).isEmpty();
        assertThat(evaluation.executedPerformanceCells()).isEqualTo(13);
        assertThat(evaluation.dispositionCells()).isEqualTo(275);
    }

    @Test
    void unavailableNativeBaselineDispositionsDependentCandidateRowsWithoutInventingADenominator() {
        CompletedCampaign completed = drive(
                cell -> cell.candidate().nativePath()
                                && cell.activeManagedLedgers() == 10_000
                                && cell.metadataLatencyP99Millis() == 1
                        ? failedAfterAdmission(cell)
                        : passing(cell, 100_000),
                AllocatorCampaignV2Test::passingFault);

        AllocatorCampaignEvaluationV2 evaluation = AllocatorCampaignSelectorV2.evaluate(completed.campaign());
        assertThat(evaluation.status()).isEqualTo(Status.NONE_QUALIFIED);
        assertThat(evaluation.executedPerformanceCells()).isEqualTo(13);
        assertThat(completed.plan().dispositions())
                .filteredOn(disposition -> disposition.kind() == DispositionKind.NATIVE_BASELINE_UNAVAILABLE)
                .hasSize(30);
    }

    @Test
    void minimumPromotableCampaignSelectsRangeAndDispositionsLargerRanges() {
        CompletedCampaign completed = drive(
                cell -> {
                    if (cell.candidate().nativePath()) {
                        return passing(cell, 100_000);
                    }
                    return cell.candidate() == Candidate.STRICT ? relativeAppendFailure(cell) : passing(cell, 100_000);
                },
                AllocatorCampaignV2Test::passingFault);

        AllocatorCampaignEvaluationV2 evaluation = AllocatorCampaignSelectorV2.evaluate(completed.campaign());
        assertThat(evaluation.status()).isEqualTo(Status.RANGE_SELECTED);
        assertThat(evaluation.selectionEligible()).isTrue();
        assertThat(evaluation.selectedCandidate()).contains(Candidate.RANGE_16);
        assertThat(evaluation.executedPerformanceCells()).isEqualTo(17);
        assertThat(evaluation.dispositionCells()).isEqualTo(271);
        assertThat(completed.plan().dispositions())
                .filteredOn(disposition -> disposition.kind() == DispositionKind.SMALLER_RANGE_QUALIFIED)
                .hasSize(144);
    }

    @Test
    void strictQualificationStillSearchesForRangeAndBothIsValidNonPromotableEvaluation() {
        CompletedCampaign completed = drive(
                cell -> {
                    if (cell.candidate().nativePath()
                            || cell.candidate() == Candidate.STRICT
                            || cell.candidate() == Candidate.RANGE_16) {
                        return passing(cell, 100_000);
                    }
                    throw new AssertionError("larger RANGE candidate must be dispositioned");
                },
                AllocatorCampaignV2Test::passingFault);

        AllocatorCampaignEvaluationV2 evaluation = AllocatorCampaignSelectorV2.evaluate(completed.campaign());
        assertThat(evaluation.status()).isEqualTo(Status.BOTH_QUALIFIED);
        assertThat(evaluation.selectionEligible()).isFalse();
        assertThat(evaluation.qualifiedCandidates()).containsExactly(Candidate.STRICT, Candidate.RANGE_16);
        assertThat(evaluation.executedPerformanceCells()).isEqualTo(24);
    }

    @Test
    void strictSelectionRequiresEliminatingEveryRangeCandidate() {
        CompletedCampaign completed = drive(
                cell -> cell.candidate().nativePath() || cell.candidate() == Candidate.STRICT
                        ? passing(cell, 100_000)
                        : relativeAppendFailure(cell),
                AllocatorCampaignV2Test::passingFault);

        AllocatorCampaignEvaluationV2 evaluation = AllocatorCampaignSelectorV2.evaluate(completed.campaign());
        assertThat(evaluation.status()).isEqualTo(Status.STRICT_SELECTED);
        assertThat(evaluation.selectedCandidate()).contains(Candidate.STRICT);
        assertThat(evaluation.executedPerformanceCells()).isEqualTo(20);
    }

    @Test
    void worstCaseCampaignCanExecuteAll288LogicalPerformanceCells() {
        CompletedCampaign completed = drive(
                cell -> {
                    boolean lastCandidateRow = !cell.candidate().nativePath()
                            && cell.activeManagedLedgers() == 100_000
                            && cell.metadataLatencyP99Millis() == 25;
                    return cell.offeredRolloverRequestsPerSecond() == 200 && !lastCandidateRow
                            ? passing(cell, 100_000)
                            : failedAfterAdmission(cell);
                },
                AllocatorCampaignV2Test::passingFault);

        AllocatorCampaignEvaluationV2 evaluation = AllocatorCampaignSelectorV2.evaluate(completed.campaign());
        assertThat(evaluation.status()).isEqualTo(Status.NONE_QUALIFIED);
        assertThat(evaluation.executedPerformanceCells()).isEqualTo(288);
        assertThat(evaluation.dispositionCells()).isZero();
    }

    @Test
    void nativeAndCandidateRatesDescendAndRelativeFloorDispositionsAreReproved() {
        List<Observation> observations = new ArrayList<>();
        Plan plan = AllocatorCampaignPlannerV2.plan(observations);
        Cell native1000 = ((ExecuteCell) plan.nextAction().orElseThrow()).cell();
        observations.add(failedAfterAdmission(native1000));
        plan = AllocatorCampaignPlannerV2.plan(observations);
        Cell native750 = ((ExecuteCell) plan.nextAction().orElseThrow()).cell();
        observations.add(passing(native750, 100_000));

        while (observations.stream().filter(IntervalEvidence.class::isInstance).count() < 9) {
            plan = AllocatorCampaignPlannerV2.plan(observations);
            Cell next = ((ExecuteCell) plan.nextAction().orElseThrow()).cell();
            observations.add(passing(next, 100_000));
        }
        plan = AllocatorCampaignPlannerV2.plan(observations);
        Cell firstCandidate = ((ExecuteCell) plan.nextAction().orElseThrow()).cell();

        assertThat(native1000.offeredRolloverRequestsPerSecond()).isEqualTo(1000);
        assertThat(native750.offeredRolloverRequestsPerSecond()).isEqualTo(750);
        assertThat(firstCandidate.candidate()).isEqualTo(Candidate.STRICT);
        assertThat(firstCandidate.offeredRolloverRequestsPerSecond()).isEqualTo(1000);
        assertThat(plan.dispositions())
                .filteredOn(disposition -> disposition.cell().candidate() == Candidate.STRICT)
                .extracting(disposition -> disposition.cell().offeredRolloverRequestsPerSecond())
                .containsExactlyInAnyOrder(500, 333, 250, 200);

        observations.add(failedAfterAdmission(firstCandidate));
        plan = AllocatorCampaignPlannerV2.plan(observations);
        Cell secondCandidate = ((ExecuteCell) plan.nextAction().orElseThrow()).cell();
        assertThat(secondCandidate.offeredRolloverRequestsPerSecond()).isEqualTo(750);
    }

    @Test
    void boundedAdmissionConservationRejectsSyntheticOrMissingTerminalLifecycle() {
        Cell cell = Cell.of(Candidate.NATIVE, 10_000, 1, 1000);
        IntervalEvidence invalid = new IntervalEvidence(
                cell, 30_000, 29_999, 1, 29_998, 0, 0, 29_998, 0, 0, 0, 0, 0, 100_000, 0, 0, 0, 0, 100_000, 0, 1, 0);

        assertThatThrownBy(() -> AllocatorCampaignPlannerV2.plan(List.of(invalid)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("terminal conservation");
    }

    @Test
    void overloadDropIsConservedButCannotEstablishASustainableRate() {
        List<Observation> observations = new ArrayList<>();
        Cell first = ((ExecuteCell) AllocatorCampaignPlannerV2.plan(observations)
                        .nextAction()
                        .orElseThrow())
                .cell();
        observations.add(overloadDrop(first));

        Cell next = ((ExecuteCell) AllocatorCampaignPlannerV2.plan(observations)
                        .nextAction()
                        .orElseThrow())
                .cell();
        assertThat(next.offeredRolloverRequestsPerSecond()).isEqualTo(750);
    }

    @Test
    void validatorRejectsCallerDispositionClaimsAndObservationReordering() {
        CompletedCampaign completed = drive(
                cell -> cell.candidate().nativePath() ? passing(cell, 100_000) : relativeAppendFailure(cell),
                AllocatorCampaignV2Test::passingFault);
        List<Disposition> missing = completed
                .plan()
                .dispositions()
                .subList(1, completed.plan().dispositions().size());

        assertThatThrownBy(() -> AllocatorCampaignValidatorV2.validate(
                        new Campaign(completed.campaign().observations(), missing)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("differ from deterministic validator");

        List<Observation> reordered = new ArrayList<>(completed.campaign().observations());
        Observation first = reordered.get(0);
        reordered.set(0, reordered.get(1));
        reordered.set(1, first);
        assertThatThrownBy(() -> AllocatorCampaignPlannerV2.plan(reordered))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reordered");
    }

    @Test
    void incompleteCampaignCannotProduceFormalEvaluation() {
        Plan plan = AllocatorCampaignPlannerV2.plan(List.of());
        Campaign campaign = new Campaign(List.of(), plan.dispositions());

        assertThatThrownBy(() -> AllocatorCampaignSelectorV2.evaluate(campaign))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("incomplete");
    }

    @Test
    void failedFaultRowEliminatesCandidateButStillProducesAValidRangeEvaluation() {
        CompletedCampaign completed = drive(
                cell -> passing(cell, 100_000),
                row -> row.candidate() == Candidate.STRICT ? failingFault(row) : passingFault(row));

        AllocatorCampaignEvaluationV2 evaluation = AllocatorCampaignSelectorV2.evaluate(completed.campaign());
        assertThat(evaluation.status()).isEqualTo(Status.RANGE_SELECTED);
        assertThat(evaluation.qualifiedCandidates()).containsExactly(Candidate.RANGE_16);
    }

    private static CompletedCampaign drive(
            Function<Cell, IntervalEvidence> intervals, Function<Row, FaultEvidence> faults) {
        List<Observation> observations = new ArrayList<>();
        for (int guard = 0; guard < 400; guard++) {
            Plan plan = AllocatorCampaignPlannerV2.plan(observations);
            if (plan.completed()) {
                Campaign campaign = new Campaign(observations, plan.dispositions());
                AllocatorCampaignValidatorV2.validate(campaign);
                return new CompletedCampaign(campaign, plan);
            }
            if (plan.nextAction().orElseThrow() instanceof ExecuteCell executeCell) {
                observations.add(intervals.apply(executeCell.cell()));
            } else if (plan.nextAction().orElseThrow() instanceof ExecuteFaultRow executeFaultRow) {
                observations.add(faults.apply(executeFaultRow.row()));
            }
        }
        throw new AssertionError("allocator V2 test campaign did not terminate inside the logical-cell bound");
    }

    private static IntervalEvidence passing(Cell cell, long appendStallP99Micros) {
        long offered = (long) cell.offeredRolloverRequestsPerSecond() * AllocatorCampaignV2.MEASURED_SECONDS;
        return new IntervalEvidence(
                cell,
                offered,
                offered,
                0,
                offered,
                0,
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
                cell.offeredRolloverRequestsPerSecond(),
                100_000,
                appendStallP99Micros,
                0,
                0,
                0);
    }

    private static IntervalEvidence relativeAppendFailure(Cell cell) {
        return passing(cell, 400_001);
    }

    private static IntervalEvidence failedAfterAdmission(Cell cell) {
        long offered = (long) cell.offeredRolloverRequestsPerSecond() * AllocatorCampaignV2.MEASURED_SECONDS;
        return new IntervalEvidence(
                cell,
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
                cell.offeredRolloverRequestsPerSecond(),
                100_000,
                100_000,
                0,
                0,
                0);
    }

    private static IntervalEvidence overloadDrop(Cell cell) {
        long offered = (long) cell.offeredRolloverRequestsPerSecond() * AllocatorCampaignV2.MEASURED_SECONDS;
        return new IntervalEvidence(
                cell,
                offered,
                offered - 1,
                1,
                offered - 1,
                0,
                0,
                offered - 1,
                0,
                0,
                0,
                0,
                0,
                100_000,
                100_000,
                100_000,
                cell.offeredRolloverRequestsPerSecond(),
                100_000,
                100_000,
                0,
                0,
                0);
    }

    private static FaultEvidence passingFault(Row row) {
        return new FaultEvidence(row, EnumSet.allOf(AllocatorFaultCutV1.class), 0, 0, 0, 0, 0, 0, 0, 0, 1, 1_000_000);
    }

    private static FaultEvidence failingFault(Row row) {
        return new FaultEvidence(row, EnumSet.allOf(AllocatorFaultCutV1.class), 0, 0, 1, 0, 0, 0, 0, 0, 1, 1_000_000);
    }

    private record CompletedCampaign(Campaign campaign, Plan plan) {}
}
