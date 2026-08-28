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
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV3.ExecutionRecord;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV3.RemainingBudgets;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV3.SourceBinding;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV3.Status;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPromotionGateV4.DecisionStatus;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPromotionGateV4.DiagnosticAttestation;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPromotionGateV4.DiagnosticScenario;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPromotionGateV4.JUnitSummary;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Candidate;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Cell;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.ExecuteCell;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.ExecuteFaultRow;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.FaultEvidence;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.IntervalEvidence;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Observation;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Plan;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Row;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

class AllocatorCampaignProtocolV4EvaluationTest {
    @Test
    void syntheticStrictAndEveryMinimumRangeSelectionSealCanonically() {
        assertSelection(
                "1",
                Candidate.STRICT,
                cell -> cell.candidate().nativePath() || cell.candidate() == Candidate.STRICT
                        ? passing(cell)
                        : relativeFailure(cell));
        for (Candidate selected :
                List.of(Candidate.RANGE_16, Candidate.RANGE_64, Candidate.RANGE_256, Candidate.RANGE_1024)) {
            assertSelection(
                    Integer.toString(selected.ordinal() + 2),
                    selected,
                    cell -> cell.candidate().nativePath() || cell.candidate() == selected
                            ? passing(cell)
                            : relativeFailure(cell));
        }
    }

    @Test
    void syntheticNoneBothAndNativeBaselineUnavailableStayCanonicalAndNonPromotable() {
        assertNonPromotable(
                "7",
                cell -> cell.candidate().nativePath() ? passing(cell) : relativeFailure(cell),
                AllocatorCampaignEvaluationV3.Status.NONE_QUALIFIED);
        assertNonPromotable(
                "8",
                cell -> cell.candidate().nativePath()
                                || cell.candidate() == Candidate.STRICT
                                || cell.candidate() == Candidate.RANGE_16
                        ? passing(cell)
                        : relativeFailure(cell),
                AllocatorCampaignEvaluationV3.Status.BOTH_QUALIFIED);
        assertNonPromotable(
                "9",
                cell -> cell.candidate().nativePath()
                                && cell.activeManagedLedgers() == 10_000
                                && cell.metadataLatencyP99Millis() == 1
                        ? zeroFailureMiss(cell)
                        : passing(cell),
                AllocatorCampaignEvaluationV3.Status.NATIVE_BASELINE_UNAVAILABLE);
    }

    @Test
    void v4DerivedFloorsRetainExactNonQuantizedRatesAndDuplicateDispositionIdentity() {
        assertThat(List.of(500, 750, 1_000, 333).stream().map(AllocatorCampaignV3::derivedRate))
                .containsExactly(400, 600, 800, 267);

        Synthetic duplicate =
                drive("b", cell -> cell.candidate().nativePath() ? nativeAtRate(cell, 250) : relativeFailure(cell));
        assertThat(duplicate.checkpoint().dispositions())
                .filteredOn(disposition ->
                        disposition.kind() == AllocatorCampaignV3.DispositionKind.DUPLICATE_DERIVED_FLOOR)
                .hasSize(5)
                .allSatisfy(disposition ->
                        assertThat(disposition.cell().rateSlot().fixedRate()).isEqualTo(200));
    }

    @Test
    void v3EvaluationDiagnosticAndSelectionWiresCannotEnterV4() {
        Synthetic completed = drive("a", cell -> cell.candidate().nativePath() ? passing(cell) : relativeFailure(cell));
        CanonicalBytes evaluation = AllocatorCampaignEvaluationSealV4.seal(completed.checkpointBytes());

        assertThatThrownBy(() -> AllocatorCampaignEvaluationSealV3.decode(evaluation))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> AllocatorCampaignEvaluationSealV4.decode(
                        AllocatorCampaignEvaluationSealV3.seal(completed.logicalCheckpointBytes())))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> AllocatorCampaignPromotionGateV4.decodeDiagnostic(
                        AllocatorCampaignPromotionGateV3.encodeDiagnostic(
                                new AllocatorCampaignPromotionGateV3.DiagnosticAttestation(
                                        completed.source(),
                                        EnumSet.allOf(AllocatorCampaignPromotionGateV3.DiagnosticScenario.class),
                                        digest("old-diagnostic")))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static void assertSelection(String digit, Candidate expected, Function<Cell, IntervalEvidence> intervals) {
        Synthetic completed = drive(digit, intervals);
        CanonicalBytes evaluationBytes = AllocatorCampaignEvaluationSealV4.seal(completed.checkpointBytes());
        var evaluation = AllocatorCampaignEvaluationSealV4.decode(evaluationBytes);
        DiagnosticAttestation diagnostic = diagnostic(completed.source());
        JUnitSummary junit = new JUnitSummary(9, 0, 0, 0);

        assertThat(evaluation.selectedCandidate()).contains(expected);
        assertThat(AllocatorCampaignPromotionGateV4.evaluate(
                                evaluationBytes,
                                completed.checkpointBytes(),
                                completed.source(),
                                completed.checkpoint().attachmentDigests(),
                                diagnostic,
                                diagnostic.receiptDigest(),
                                junit)
                        .status())
                .isEqualTo(DecisionStatus.PROMOTABLE);
        CanonicalBytes selectionBytes = AllocatorCampaignSelectionV4.seal(
                evaluationBytes,
                completed.checkpointBytes(),
                completed.source(),
                completed.checkpoint().attachmentDigests(),
                diagnostic,
                diagnostic.receiptDigest(),
                junit);
        assertThat(AllocatorCampaignSelectionV4.decode(selectionBytes).selectedCandidate())
                .isEqualTo(expected);
    }

    private static void assertNonPromotable(
            String digit, Function<Cell, IntervalEvidence> intervals, AllocatorCampaignEvaluationV3.Status expected) {
        Synthetic completed = drive(digit, intervals);
        CanonicalBytes evaluationBytes = AllocatorCampaignEvaluationSealV4.seal(completed.checkpointBytes());
        DiagnosticAttestation diagnostic = diagnostic(completed.source());

        assertThat(AllocatorCampaignEvaluationSealV4.decode(evaluationBytes).status())
                .isEqualTo(expected);
        assertThat(AllocatorCampaignPromotionGateV4.evaluate(
                                evaluationBytes,
                                completed.checkpointBytes(),
                                completed.source(),
                                completed.checkpoint().attachmentDigests(),
                                diagnostic,
                                diagnostic.receiptDigest(),
                                new JUnitSummary(9, 0, 0, 0))
                        .status())
                .isEqualTo(DecisionStatus.NON_PROMOTABLE_EVALUATION);
        assertThatThrownBy(() -> AllocatorCampaignSelectionV4.seal(
                        evaluationBytes,
                        completed.checkpointBytes(),
                        completed.source(),
                        completed.checkpoint().attachmentDigests(),
                        diagnostic,
                        diagnostic.receiptDigest(),
                        new JUnitSummary(9, 0, 0, 0)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-promotable");
    }

    private static Synthetic drive(String digit, Function<Cell, IntervalEvidence> intervals) {
        SourceBinding source = source(digit);
        List<Observation> observations = new ArrayList<>();
        List<ExecutionRecord> records = new ArrayList<>();
        Plan plan = AllocatorCampaignPlannerV3.plan(observations);
        AllocatorCampaignCheckpointV3 inner = AllocatorCampaignCheckpointV3.initial(
                source, remaining(records), records, plan.dispositions(), Status.RUNNING);
        CanonicalBytes innerBytes = AllocatorCampaignCheckpointV3.encode(inner);
        AllocatorCampaignCheckpointV4 outer = AllocatorCampaignCheckpointV4.initial(innerBytes);
        CanonicalBytes outerBytes = AllocatorCampaignCheckpointV4.encode(outer);
        for (int guard = 0; guard < 400; guard++) {
            plan = AllocatorCampaignPlannerV3.plan(observations);
            if (plan.completed()) {
                return new Synthetic(source, outerBytes, outer, innerBytes);
            }
            Observation observation;
            if (plan.nextAction().orElseThrow() instanceof ExecuteCell executeCell) {
                observation = intervals.apply(executeCell.cell());
            } else if (plan.nextAction().orElseThrow() instanceof ExecuteFaultRow executeFaultRow) {
                observation = passingFault(executeFaultRow.row());
            } else {
                throw new AssertionError("unknown allocator V4 logical action");
            }
            observations.add(observation);
            records.add(new ExecutionRecord(observation, digest("attachment-" + digit + '-' + records.size())));
            Plan nextPlan = AllocatorCampaignPlannerV3.plan(observations);
            Status nextStatus = nextPlan.completed() ? Status.COMPLETED : Status.RUNNING;
            inner = AllocatorCampaignCheckpointV3.resume(
                    innerBytes, source, remaining(records), records, nextPlan.dispositions(), nextStatus);
            innerBytes = AllocatorCampaignCheckpointV3.encode(inner);
            outer = AllocatorCampaignCheckpointV4.resume(outerBytes, innerBytes);
            outerBytes = AllocatorCampaignCheckpointV4.encode(outer);
        }
        throw new AssertionError("allocator V4 synthetic campaign did not terminate");
    }

    private static RemainingBudgets remaining(List<ExecutionRecord> records) {
        long intervals = records.stream()
                .map(ExecutionRecord::observation)
                .filter(IntervalEvidence.class::isInstance)
                .count();
        long faults = records.stream()
                .map(ExecutionRecord::observation)
                .filter(FaultEvidence.class::isInstance)
                .count();
        Set<Candidate> tenThousand = populations(records, 10_000);
        Set<Candidate> hundredThousand = populations(records, 100_000);
        return new RemainingBudgets(
                records.isEmpty() ? 900 : 0,
                5_400 - 900L * tenThousand.size(),
                7_200 - 180L * faults,
                5_400 - 900L * hundredThousand.size(),
                13_120 - 40L * intervals,
                1_640 - 5L * intervals,
                600);
    }

    private static Set<Candidate> populations(List<ExecutionRecord> records, int population) {
        Set<Candidate> candidates = new HashSet<>();
        records.stream().map(ExecutionRecord::observation).forEach(observation -> {
            Row row = observation instanceof IntervalEvidence interval
                    ? interval.cell().row()
                    : ((FaultEvidence) observation).row();
            if (row.activeManagedLedgers() == population) {
                candidates.add(row.candidate());
            }
        });
        return candidates;
    }

    private static IntervalEvidence passing(Cell cell) {
        int rate = cell.rateSlot().derivedFloor() ? 800 : cell.rateSlot().fixedRate();
        long offered = (long) rate * AllocatorCampaignV3.MEASURED_SECONDS;
        return new IntervalEvidence(
                cell, rate, offered, offered, 0, offered, 0, 0, offered, 0, 0, 0, 0, 0, 100_000, 100_000, 100_000, rate,
                100_000, 100_000, 0, 0, 0);
    }

    private static IntervalEvidence nativeAtRate(Cell cell, int sustainableRate) {
        IntervalEvidence passing = passing(cell);
        long offered = (long) passing.offeredRate() * AllocatorCampaignV3.MEASURED_SECONDS;
        long completed = Math.min(offered, (long) sustainableRate * AllocatorCampaignV3.MEASURED_SECONDS);
        return new IntervalEvidence(
                cell,
                passing.offeredRate(),
                offered,
                completed,
                offered - completed,
                completed,
                0,
                0,
                completed,
                0,
                0,
                0,
                0,
                0,
                100_000,
                100_000,
                100_000,
                passing.offeredRate(),
                100_000,
                100_000,
                0,
                0,
                0);
    }

    private static IntervalEvidence relativeFailure(Cell cell) {
        IntervalEvidence passing = passing(cell);
        return new IntervalEvidence(
                cell,
                passing.offeredRate(),
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

    private static IntervalEvidence zeroFailureMiss(Cell cell) {
        IntervalEvidence passing = passing(cell);
        return new IntervalEvidence(
                cell,
                passing.offeredRate(),
                passing.offered(),
                passing.admitted(),
                0,
                passing.completed() - 1,
                1,
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
                passing.appendStallP99Micros(),
                0,
                0,
                0);
    }

    private static FaultEvidence passingFault(Row row) {
        return new FaultEvidence(row, EnumSet.allOf(AllocatorFaultCutV1.class), 0, 0, 0, 0, 0, 0, 0, 0, 1, 1_000_000);
    }

    private static DiagnosticAttestation diagnostic(SourceBinding source) {
        return new DiagnosticAttestation(
                source,
                AllocatorNativeExecutionProfileV4.executionProfileDigest(),
                AllocatorCampaignPlanProfileV4.zeroDecisionPlanDigest(),
                EnumSet.allOf(DiagnosticScenario.class),
                digest("diagnostic-receipt"));
    }

    private static SourceBinding source(String digit) {
        return new SourceBinding(
                digit.repeat(40),
                digest("oxia-" + digit),
                digest("dependency-" + digit),
                digest("executor-" + digit),
                AllocatorCampaignPlanProfileV4.zeroDecisionPlanDigest());
    }

    private static Sha256Digest digest(String value) {
        return Sha256Digest.hash(CanonicalBytes.copyOf(value.getBytes(StandardCharsets.UTF_8)));
    }

    private record Synthetic(
            SourceBinding source,
            CanonicalBytes checkpointBytes,
            AllocatorCampaignCheckpointV4 checkpoint,
            CanonicalBytes logicalCheckpointBytes) {}
}
