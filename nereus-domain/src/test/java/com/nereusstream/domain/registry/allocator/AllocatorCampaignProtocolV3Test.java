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
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV3.ExecutionRecord;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV3.RemainingBudgets;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV3.SourceBinding;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV3.Status;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPromotionGateV3.DecisionStatus;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPromotionGateV3.DiagnosticAttestation;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPromotionGateV3.DiagnosticScenario;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPromotionGateV3.JUnitSummary;
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
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

class AllocatorCampaignProtocolV3Test {
    @Test
    void canonicalCheckpointCarriesTheFullInventorySourceBudgetsAndAttachmentDigests() {
        Plan plan = AllocatorCampaignPlannerV3.plan(List.of());
        AllocatorCampaignCheckpointV3 checkpoint = AllocatorCampaignCheckpointV3.initial(
                source("1"), budgets(), List.of(), plan.dispositions(), Status.RUNNING);

        CanonicalBytes encoded = AllocatorCampaignCheckpointV3.encode(checkpoint);
        AllocatorCampaignCheckpointV3 decoded = AllocatorCampaignCheckpointV3.decode(encoded);

        assertThat(encoded.length()).isLessThan(AllocatorCampaignCheckpointV3.MAX_ENCODED_BYTES);
        assertThat(decoded.status()).isEqualTo(Status.RUNNING);
        assertThat(decoded.checkpointSequence()).isZero();
        assertThat(decoded.predecessorCheckpointDigest().isZero()).isTrue();
        assertThat(decoded.source()).isEqualTo(source("1"));
        assertThat(decoded.remainingBudgets()).isEqualTo(budgets());
        assertThat(decoded.campaign().observations()).isEmpty();
        assertThat(AllocatorCampaignCheckpointV3.encode(decoded)).isEqualTo(encoded);
    }

    @Test
    void resumeRequiresExactSourceOrderedPrefixAndNonIncreasingIndependentBudgets() {
        Plan initialPlan = AllocatorCampaignPlannerV3.plan(List.of());
        AllocatorCampaignCheckpointV3 initial = AllocatorCampaignCheckpointV3.initial(
                source("2"), budgets(), List.of(), initialPlan.dispositions(), Status.RUNNING);
        CanonicalBytes initialBytes = AllocatorCampaignCheckpointV3.encode(initial);
        Cell firstCell = ((ExecuteCell) initialPlan.nextAction().orElseThrow()).cell();
        List<ExecutionRecord> records = List.of(record(passing(firstCell), 0));
        Plan nextPlan = AllocatorCampaignPlannerV3.plan(List.of(passing(firstCell)));
        RemainingBudgets reduced = new RemainingBudgets(899, 5_399, 7_199, 5_399, 13_080, 1_635, 590);

        AllocatorCampaignCheckpointV3 resumed = AllocatorCampaignCheckpointV3.resume(
                initialBytes, source("2"), reduced, records, nextPlan.dispositions(), Status.RUNNING);

        assertThat(resumed.checkpointSequence()).isEqualTo(1);
        assertThat(resumed.predecessorCheckpointDigest()).isEqualTo(AllocatorCampaignCheckpointV3.digest(initialBytes));
        assertThat(resumed.executionRecords()).isEqualTo(records);
        assertThatThrownBy(() -> AllocatorCampaignCheckpointV3.resume(
                        initialBytes, source("3"), reduced, records, nextPlan.dispositions(), Status.RUNNING))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("source/executor");
        assertThatThrownBy(() -> AllocatorCampaignCheckpointV3.resume(
                        AllocatorCampaignCheckpointV3.encode(resumed),
                        source("2"),
                        budgets(),
                        records,
                        nextPlan.dispositions(),
                        Status.RUNNING))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("budget");
    }

    @Test
    void canonicalParserRejectsTrailingTamperedOrAliasedCheckpointBytes() {
        Plan plan = AllocatorCampaignPlannerV3.plan(List.of());
        CanonicalBytes encoded = AllocatorCampaignCheckpointV3.encode(AllocatorCampaignCheckpointV3.initial(
                source("4"), budgets(), List.of(), plan.dispositions(), Status.RUNNING));
        byte[] trailing = Arrays.copyOf(encoded.toByteArray(), encoded.length() + 1);
        byte[] magic = encoded.toByteArray();
        magic[0] ^= 1;

        assertThatThrownBy(() -> AllocatorCampaignCheckpointV3.decode(CanonicalBytes.copyOf(trailing)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("trailing");
        assertThatThrownBy(() -> AllocatorCampaignCheckpointV3.decode(CanonicalBytes.copyOf(magic)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("magic");
        assertThatThrownBy(() -> new ExecutionRecord(
                        passing(Cell.fixedRate(Candidate.NATIVE, 10_000, 1, 1000)), Sha256Digest.copyOf(new byte[32])))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("zero");
    }

    @Test
    void evaluationAndDiagnosticWiresKeepTheirFixedLengthsAndStrictHeaders() {
        Completed completed = drive(cell -> cell.candidate().nativePath() ? passing(cell) : relativeFailure(cell));
        CanonicalBytes checkpointBytes = AllocatorCampaignCheckpointV3.encode(complete(source("c"), completed));
        CanonicalBytes evaluationBytes = AllocatorCampaignEvaluationSealV3.seal(checkpointBytes);
        CanonicalBytes diagnosticBytes = AllocatorCampaignPromotionGateV3.encodeDiagnostic(diagnostic(source("c")));
        byte[] evaluationMagic = evaluationBytes.toByteArray();
        evaluationMagic[0] ^= 1;
        byte[] diagnosticReserved = diagnosticBytes.toByteArray();
        diagnosticReserved[11] = 1;

        assertThat(evaluationBytes.length()).isEqualTo(284);
        assertThat(diagnosticBytes.length()).isEqualTo(212);
        assertThatThrownBy(() -> AllocatorCampaignEvaluationSealV3.decode(CanonicalBytes.copyOf(evaluationMagic)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("magic");
        assertThatThrownBy(() ->
                        AllocatorCampaignPromotionGateV3.decodeDiagnostic(CanonicalBytes.copyOf(diagnosticReserved)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reserved");
        var sealed = AllocatorCampaignEvaluationSealV3.decode(evaluationBytes);
        assertThatThrownBy(() -> new AllocatorCampaignEvaluationSealV3.SealedEvaluation(
                        sealed.source(),
                        sealed.campaignId(),
                        sealed.checkpointDigest(),
                        sealed.attachmentRootDigest(),
                        AllocatorCampaignEvaluationV3.Status.STRICT_SELECTED,
                        java.util.Optional.of(Candidate.RANGE_16),
                        sealed.executedPerformanceCells(),
                        sealed.dispositionCells()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("selection");
    }

    @Test
    void completedStrictCampaignSealsAndPromotesOnlyWithExactFreshInputs() {
        Completed completed = drive(cell -> cell.candidate().nativePath() || cell.candidate() == Candidate.STRICT
                ? passing(cell)
                : relativeFailure(cell));
        AllocatorCampaignCheckpointV3 checkpoint = complete(source("5"), completed);
        CanonicalBytes checkpointBytes = AllocatorCampaignCheckpointV3.encode(checkpoint);
        CanonicalBytes evaluationBytes = AllocatorCampaignEvaluationSealV3.seal(checkpointBytes);
        var sealed = AllocatorCampaignEvaluationSealV3.decode(evaluationBytes);
        DiagnosticAttestation diagnostic = diagnostic(source("5"));
        assertThat(AllocatorCampaignPromotionGateV3.decodeDiagnostic(
                        AllocatorCampaignPromotionGateV3.encodeDiagnostic(diagnostic)))
                .isEqualTo(diagnostic);

        assertThat(sealed.status()).isEqualTo(AllocatorCampaignEvaluationV3.Status.STRICT_SELECTED);
        assertThat(sealed.selectedCandidate()).contains(Candidate.STRICT);
        assertThat(sealed.checkpointDigest()).isEqualTo(AllocatorCampaignCheckpointV3.digest(checkpointBytes));
        assertThat(AllocatorCampaignPromotionGateV3.evaluate(
                                evaluationBytes,
                                checkpointBytes,
                                source("5"),
                                checkpoint.attachmentDigests(),
                                diagnostic,
                                diagnostic.receiptDigest(),
                                new JUnitSummary(7, 0, 0, 0))
                        .status())
                .isEqualTo(DecisionStatus.PROMOTABLE);
        assertThat(AllocatorCampaignPromotionGateV3.evaluate(
                                evaluationBytes,
                                checkpointBytes,
                                source("6"),
                                checkpoint.attachmentDigests(),
                                diagnostic,
                                diagnostic.receiptDigest(),
                                new JUnitSummary(7, 0, 0, 0))
                        .status())
                .isEqualTo(DecisionStatus.SOURCE_MISMATCH);
        assertThat(AllocatorCampaignPromotionGateV3.evaluate(
                                evaluationBytes,
                                checkpointBytes,
                                source("5"),
                                EnumSet.noneOf(DiagnosticScenario.class).stream()
                                        .map(value -> digest(value.name()))
                                        .collect(java.util.stream.Collectors.toSet()),
                                diagnostic,
                                diagnostic.receiptDigest(),
                                new JUnitSummary(7, 0, 0, 0))
                        .status())
                .isEqualTo(DecisionStatus.ATTACHMENT_MISMATCH);
    }

    @Test
    void promotableEvaluationProducesStrictCanonicalNars3OnlyAfterTheGate() {
        Completed completed = drive(cell -> cell.candidate().nativePath() || cell.candidate() == Candidate.STRICT
                ? passing(cell)
                : relativeFailure(cell));
        SourceBinding source = source("d");
        AllocatorCampaignCheckpointV3 checkpoint = complete(source, completed);
        CanonicalBytes checkpointBytes = AllocatorCampaignCheckpointV3.encode(checkpoint);
        CanonicalBytes evaluationBytes = AllocatorCampaignEvaluationSealV3.seal(checkpointBytes);
        DiagnosticAttestation diagnostic = diagnostic(source);

        CanonicalBytes selectionBytes = AllocatorCampaignSelectionV3.seal(
                evaluationBytes,
                checkpointBytes,
                source,
                checkpoint.attachmentDigests(),
                diagnostic,
                diagnostic.receiptDigest(),
                new JUnitSummary(7, 0, 0, 0));
        AllocatorCampaignSelectionV3.Selection selection = AllocatorCampaignSelectionV3.decode(selectionBytes);

        assertThat(selection.selectedCandidate()).isEqualTo(Candidate.STRICT);
        assertThat(selection.source()).isEqualTo(source);
        assertThat(selection.evaluationDigest()).isEqualTo(Sha256Digest.hash(evaluationBytes));
        assertThat(AllocatorCampaignSelectionV3.encode(selection)).isEqualTo(selectionBytes);

        byte[] forgedMagic = selectionBytes.toByteArray();
        forgedMagic[0] ^= 1;
        assertThatThrownBy(() -> AllocatorCampaignSelectionV3.decode(CanonicalBytes.copyOf(forgedMagic)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("magic");
    }

    @Test
    void noneAndBothSealAsValidNonPromotableEvaluationsWithoutFailingTheGate() {
        Completed none = drive(cell -> cell.candidate().nativePath() ? passing(cell) : relativeFailure(cell));
        Completed both = drive(cell -> cell.candidate().nativePath()
                        || cell.candidate() == Candidate.STRICT
                        || cell.candidate() == Candidate.RANGE_16
                ? passing(cell)
                : relativeFailure(cell));

        assertNonPromotable(source("7"), none, AllocatorCampaignEvaluationV3.Status.NONE_QUALIFIED);
        assertNonPromotable(source("8"), both, AllocatorCampaignEvaluationV3.Status.BOTH_QUALIFIED);
    }

    @Test
    void nativeBaselineUnavailableSealsButCannotProduceSelection() {
        Completed unavailable = drive(cell -> {
            if (cell.candidate().nativePath()
                    && cell.activeManagedLedgers() == 10_000
                    && cell.metadataLatencyP99Millis() == 1) {
                return zeroFailureMiss(cell);
            }
            return passing(cell);
        });
        SourceBinding source = source("e");
        AllocatorCampaignCheckpointV3 checkpoint = complete(source, unavailable);
        CanonicalBytes checkpointBytes = AllocatorCampaignCheckpointV3.encode(checkpoint);
        CanonicalBytes evaluationBytes = AllocatorCampaignEvaluationSealV3.seal(checkpointBytes);

        assertThat(AllocatorCampaignEvaluationSealV3.decode(evaluationBytes).status())
                .isEqualTo(AllocatorCampaignEvaluationV3.Status.NATIVE_BASELINE_UNAVAILABLE);
        assertThatThrownBy(() -> AllocatorCampaignSelectionV3.seal(
                        evaluationBytes,
                        checkpointBytes,
                        source,
                        checkpoint.attachmentDigests(),
                        diagnostic(source),
                        diagnostic(source).receiptDigest(),
                        new JUnitSummary(7, 0, 0, 0)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-promotable");
    }

    @Test
    void interruptedAndInfrastructureFailedCampaignsCannotProduceFormalEvaluationOrResumeAsSuccess() {
        Plan plan = AllocatorCampaignPlannerV3.plan(List.of());
        AllocatorCampaignCheckpointV3 interrupted = AllocatorCampaignCheckpointV3.initial(
                source("9"), budgets(), List.of(), plan.dispositions(), Status.INTERRUPTED);
        AllocatorCampaignCheckpointV3 failed = AllocatorCampaignCheckpointV3.initial(
                source("a"), budgets(), List.of(), plan.dispositions(), Status.INFRASTRUCTURE_FAILED);

        assertThatThrownBy(
                        () -> AllocatorCampaignEvaluationSealV3.seal(AllocatorCampaignCheckpointV3.encode(interrupted)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot produce");
        assertThatThrownBy(() -> AllocatorCampaignEvaluationSealV3.seal(AllocatorCampaignCheckpointV3.encode(failed)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot produce");
        assertThatThrownBy(() -> AllocatorCampaignCheckpointV3.resume(
                        AllocatorCampaignCheckpointV3.encode(failed),
                        source("a"),
                        budgets(),
                        List.of(),
                        plan.dispositions(),
                        Status.RUNNING))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot resume");
    }

    @Test
    void promotionGateRequiresAllFourDiagnosticsZeroSkipJUnitAndExactAttachments() {
        Completed completed = drive(cell -> cell.candidate().nativePath() || cell.candidate() == Candidate.STRICT
                ? passing(cell)
                : relativeFailure(cell));
        AllocatorCampaignCheckpointV3 checkpoint = complete(source("b"), completed);
        CanonicalBytes checkpointBytes = AllocatorCampaignCheckpointV3.encode(checkpoint);
        CanonicalBytes evaluationBytes = AllocatorCampaignEvaluationSealV3.seal(checkpointBytes);
        DiagnosticAttestation missing =
                new DiagnosticAttestation(source("b"), EnumSet.of(DiagnosticScenario.STRICT), digest("diagnostic"));

        assertThat(AllocatorCampaignPromotionGateV3.evaluate(
                                evaluationBytes,
                                checkpointBytes,
                                source("b"),
                                checkpoint.attachmentDigests(),
                                missing,
                                missing.receiptDigest(),
                                new JUnitSummary(7, 0, 0, 0))
                        .status())
                .isEqualTo(DecisionStatus.DIAGNOSTIC_INCOMPLETE);
        assertThat(AllocatorCampaignPromotionGateV3.evaluate(
                                evaluationBytes,
                                checkpointBytes,
                                source("b"),
                                checkpoint.attachmentDigests(),
                                diagnostic(source("b")),
                                diagnostic(source("b")).receiptDigest(),
                                new JUnitSummary(7, 0, 0, 1))
                        .status())
                .isEqualTo(DecisionStatus.JUNIT_INVALID);
        assertThat(AllocatorCampaignPromotionGateV3.evaluate(
                                evaluationBytes,
                                checkpointBytes,
                                source("b"),
                                checkpoint.attachmentDigests(),
                                diagnostic(source("b")),
                                digest("wrong-diagnostic-junit"),
                                new JUnitSummary(7, 0, 0, 0))
                        .status())
                .isEqualTo(DecisionStatus.DIAGNOSTIC_INCOMPLETE);

        var exactEvaluation = AllocatorCampaignEvaluationSealV3.decode(evaluationBytes);
        CanonicalBytes forgedEvaluation =
                AllocatorCampaignEvaluationSealV3.encode(new AllocatorCampaignEvaluationSealV3.SealedEvaluation(
                        exactEvaluation.source(),
                        exactEvaluation.campaignId(),
                        exactEvaluation.checkpointDigest(),
                        exactEvaluation.attachmentRootDigest(),
                        AllocatorCampaignEvaluationV3.Status.RANGE_SELECTED,
                        java.util.Optional.of(Candidate.RANGE_16),
                        exactEvaluation.executedPerformanceCells(),
                        exactEvaluation.dispositionCells()));
        assertThat(AllocatorCampaignPromotionGateV3.evaluate(
                                forgedEvaluation,
                                checkpointBytes,
                                source("b"),
                                checkpoint.attachmentDigests(),
                                diagnostic(source("b")),
                                diagnostic(source("b")).receiptDigest(),
                                new JUnitSummary(7, 0, 0, 0))
                        .status())
                .isEqualTo(DecisionStatus.CHECKPOINT_LINK_INVALID);
    }

    private static void assertNonPromotable(
            SourceBinding source, Completed completed, AllocatorCampaignEvaluationV3.Status expected) {
        AllocatorCampaignCheckpointV3 checkpoint = complete(source, completed);
        CanonicalBytes checkpointBytes = AllocatorCampaignCheckpointV3.encode(checkpoint);
        CanonicalBytes evaluationBytes = AllocatorCampaignEvaluationSealV3.seal(checkpointBytes);

        assertThat(AllocatorCampaignEvaluationSealV3.decode(evaluationBytes).status())
                .isEqualTo(expected);
        assertThat(AllocatorCampaignPromotionGateV3.evaluate(
                                evaluationBytes,
                                checkpointBytes,
                                source,
                                checkpoint.attachmentDigests(),
                                diagnostic(source),
                                diagnostic(source).receiptDigest(),
                                new JUnitSummary(7, 0, 0, 0))
                        .status())
                .isEqualTo(DecisionStatus.NON_PROMOTABLE_EVALUATION);
    }

    private static AllocatorCampaignCheckpointV3 complete(SourceBinding source, Completed completed) {
        return AllocatorCampaignCheckpointV3.initial(
                source, budgets(), completed.records(), completed.plan().dispositions(), Status.COMPLETED);
    }

    private static Completed drive(Function<Cell, IntervalEvidence> intervals) {
        List<Observation> observations = new ArrayList<>();
        List<ExecutionRecord> records = new ArrayList<>();
        for (int guard = 0; guard < 400; guard++) {
            Plan plan = AllocatorCampaignPlannerV3.plan(observations);
            if (plan.completed()) {
                return new Completed(List.copyOf(records), plan);
            }
            Observation observation;
            if (plan.nextAction().orElseThrow() instanceof ExecuteCell executeCell) {
                observation = intervals.apply(executeCell.cell());
            } else if (plan.nextAction().orElseThrow() instanceof ExecuteFaultRow executeFaultRow) {
                observation = passingFault(executeFaultRow.row());
            } else {
                throw new AssertionError("unknown allocator V3 action");
            }
            observations.add(observation);
            records.add(record(observation, records.size()));
        }
        throw new AssertionError("allocator V3 protocol test campaign did not terminate");
    }

    private static ExecutionRecord record(Observation observation, int index) {
        return new ExecutionRecord(observation, digest("attachment-" + index));
    }

    private static IntervalEvidence passing(Cell cell) {
        int rate = cell.rateSlot().derivedFloor() ? 800 : cell.rateSlot().fixedRate();
        long offered = (long) rate * AllocatorCampaignV3.MEASURED_SECONDS;
        return new IntervalEvidence(
                cell, rate, offered, offered, 0, offered, 0, 0, offered, 0, 0, 0, 0, 0, 100_000, 100_000, 100_000, rate,
                100_000, 100_000, 0, 0, 0);
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
        return new DiagnosticAttestation(source, EnumSet.allOf(DiagnosticScenario.class), digest("diagnostic-receipt"));
    }

    private static SourceBinding source(String digit) {
        return new SourceBinding(
                digit.repeat(40),
                digest("oxia-" + digit),
                digest("dependency-" + digit),
                digest("executor-" + digit),
                digest("workload-" + digit));
    }

    private static RemainingBudgets budgets() {
        return new RemainingBudgets(900, 5_400, 7_200, 5_400, 13_120, 1_640, 600);
    }

    private static Sha256Digest digest(String value) {
        return Sha256Digest.hash(CanonicalBytes.copyOf(value.getBytes(StandardCharsets.UTF_8)));
    }

    private record Completed(List<ExecutionRecord> records, Plan plan) {}
}
