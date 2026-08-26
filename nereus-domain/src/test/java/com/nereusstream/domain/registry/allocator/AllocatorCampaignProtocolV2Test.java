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
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV2.ExecutionRecord;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV2.RemainingBudgets;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV2.SourceBinding;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV2.Status;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPromotionGateV2.DecisionStatus;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPromotionGateV2.DiagnosticAttestation;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPromotionGateV2.DiagnosticScenario;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPromotionGateV2.JUnitSummary;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.Candidate;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.Cell;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.ExecuteCell;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.ExecuteFaultRow;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.FaultEvidence;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.IntervalEvidence;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.Observation;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.Plan;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.Row;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

class AllocatorCampaignProtocolV2Test {
    @Test
    void canonicalCheckpointCarriesTheFullInventorySourceBudgetsAndAttachmentDigests() {
        Plan plan = AllocatorCampaignPlannerV2.plan(List.of());
        AllocatorCampaignCheckpointV2 checkpoint = AllocatorCampaignCheckpointV2.initial(
                source("1"), budgets(), List.of(), plan.dispositions(), Status.RUNNING);

        CanonicalBytes encoded = AllocatorCampaignCheckpointV2.encode(checkpoint);
        AllocatorCampaignCheckpointV2 decoded = AllocatorCampaignCheckpointV2.decode(encoded);

        assertThat(encoded.length()).isLessThan(AllocatorCampaignCheckpointV2.MAX_ENCODED_BYTES);
        assertThat(decoded.status()).isEqualTo(Status.RUNNING);
        assertThat(decoded.checkpointSequence()).isZero();
        assertThat(decoded.predecessorCheckpointDigest().isZero()).isTrue();
        assertThat(decoded.source()).isEqualTo(source("1"));
        assertThat(decoded.remainingBudgets()).isEqualTo(budgets());
        assertThat(decoded.campaign().observations()).isEmpty();
        assertThat(AllocatorCampaignCheckpointV2.encode(decoded)).isEqualTo(encoded);
    }

    @Test
    void resumeRequiresExactSourceOrderedPrefixAndNonIncreasingIndependentBudgets() {
        Plan initialPlan = AllocatorCampaignPlannerV2.plan(List.of());
        AllocatorCampaignCheckpointV2 initial = AllocatorCampaignCheckpointV2.initial(
                source("2"), budgets(), List.of(), initialPlan.dispositions(), Status.RUNNING);
        CanonicalBytes initialBytes = AllocatorCampaignCheckpointV2.encode(initial);
        Cell firstCell = ((ExecuteCell) initialPlan.nextAction().orElseThrow()).cell();
        List<ExecutionRecord> records = List.of(record(passing(firstCell), 0));
        Plan nextPlan = AllocatorCampaignPlannerV2.plan(List.of(passing(firstCell)));
        RemainingBudgets reduced = new RemainingBudgets(899, 5_399, 7_199, 5_399, 11_480, 1_435, 590);

        AllocatorCampaignCheckpointV2 resumed = AllocatorCampaignCheckpointV2.resume(
                initialBytes, source("2"), reduced, records, nextPlan.dispositions(), Status.RUNNING);

        assertThat(resumed.checkpointSequence()).isEqualTo(1);
        assertThat(resumed.predecessorCheckpointDigest()).isEqualTo(AllocatorCampaignCheckpointV2.digest(initialBytes));
        assertThat(resumed.executionRecords()).isEqualTo(records);
        assertThatThrownBy(() -> AllocatorCampaignCheckpointV2.resume(
                        initialBytes, source("3"), reduced, records, nextPlan.dispositions(), Status.RUNNING))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("source/executor");
        assertThatThrownBy(() -> AllocatorCampaignCheckpointV2.resume(
                        AllocatorCampaignCheckpointV2.encode(resumed),
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
        Plan plan = AllocatorCampaignPlannerV2.plan(List.of());
        CanonicalBytes encoded = AllocatorCampaignCheckpointV2.encode(AllocatorCampaignCheckpointV2.initial(
                source("4"), budgets(), List.of(), plan.dispositions(), Status.RUNNING));
        byte[] trailing = Arrays.copyOf(encoded.toByteArray(), encoded.length() + 1);
        byte[] magic = encoded.toByteArray();
        magic[0] ^= 1;

        assertThatThrownBy(() -> AllocatorCampaignCheckpointV2.decode(CanonicalBytes.copyOf(trailing)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("trailing");
        assertThatThrownBy(() -> AllocatorCampaignCheckpointV2.decode(CanonicalBytes.copyOf(magic)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("magic");
        assertThatThrownBy(() -> new ExecutionRecord(
                        passing(Cell.of(Candidate.NATIVE, 10_000, 1, 1000)), Sha256Digest.copyOf(new byte[32])))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("zero");
    }

    @Test
    void evaluationAndDiagnosticWiresKeepTheirFixedLengthsAndStrictHeaders() {
        Completed completed = drive(cell -> cell.candidate().nativePath() ? passing(cell) : relativeFailure(cell));
        CanonicalBytes checkpointBytes = AllocatorCampaignCheckpointV2.encode(complete(source("c"), completed));
        CanonicalBytes evaluationBytes = AllocatorCampaignEvaluationSealV2.seal(checkpointBytes);
        CanonicalBytes diagnosticBytes = AllocatorCampaignPromotionGateV2.encodeDiagnostic(diagnostic(source("c")));
        byte[] evaluationMagic = evaluationBytes.toByteArray();
        evaluationMagic[0] ^= 1;
        byte[] diagnosticReserved = diagnosticBytes.toByteArray();
        diagnosticReserved[11] = 1;

        assertThat(evaluationBytes.length()).isEqualTo(284);
        assertThat(diagnosticBytes.length()).isEqualTo(212);
        assertThatThrownBy(() -> AllocatorCampaignEvaluationSealV2.decode(CanonicalBytes.copyOf(evaluationMagic)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("magic");
        assertThatThrownBy(() ->
                        AllocatorCampaignPromotionGateV2.decodeDiagnostic(CanonicalBytes.copyOf(diagnosticReserved)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reserved");
        var sealed = AllocatorCampaignEvaluationSealV2.decode(evaluationBytes);
        assertThatThrownBy(() -> new AllocatorCampaignEvaluationSealV2.SealedEvaluation(
                        sealed.source(),
                        sealed.campaignId(),
                        sealed.checkpointDigest(),
                        sealed.attachmentRootDigest(),
                        AllocatorCampaignEvaluationV2.Status.STRICT_SELECTED,
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
        AllocatorCampaignCheckpointV2 checkpoint = complete(source("5"), completed);
        CanonicalBytes checkpointBytes = AllocatorCampaignCheckpointV2.encode(checkpoint);
        CanonicalBytes evaluationBytes = AllocatorCampaignEvaluationSealV2.seal(checkpointBytes);
        var sealed = AllocatorCampaignEvaluationSealV2.decode(evaluationBytes);
        DiagnosticAttestation diagnostic = diagnostic(source("5"));
        assertThat(AllocatorCampaignPromotionGateV2.decodeDiagnostic(
                        AllocatorCampaignPromotionGateV2.encodeDiagnostic(diagnostic)))
                .isEqualTo(diagnostic);

        assertThat(sealed.status()).isEqualTo(AllocatorCampaignEvaluationV2.Status.STRICT_SELECTED);
        assertThat(sealed.selectedCandidate()).contains(Candidate.STRICT);
        assertThat(sealed.checkpointDigest()).isEqualTo(AllocatorCampaignCheckpointV2.digest(checkpointBytes));
        assertThat(AllocatorCampaignPromotionGateV2.evaluate(
                                evaluationBytes,
                                checkpointBytes,
                                source("5"),
                                checkpoint.attachmentDigests(),
                                diagnostic,
                                diagnostic.receiptDigest(),
                                new JUnitSummary(7, 0, 0, 0))
                        .status())
                .isEqualTo(DecisionStatus.PROMOTABLE);
        assertThat(AllocatorCampaignPromotionGateV2.evaluate(
                                evaluationBytes,
                                checkpointBytes,
                                source("6"),
                                checkpoint.attachmentDigests(),
                                diagnostic,
                                diagnostic.receiptDigest(),
                                new JUnitSummary(7, 0, 0, 0))
                        .status())
                .isEqualTo(DecisionStatus.SOURCE_MISMATCH);
        assertThat(AllocatorCampaignPromotionGateV2.evaluate(
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
    void noneAndBothSealAsValidNonPromotableEvaluationsWithoutFailingTheGate() {
        Completed none = drive(cell -> cell.candidate().nativePath() ? passing(cell) : relativeFailure(cell));
        Completed both = drive(cell -> cell.candidate().nativePath()
                        || cell.candidate() == Candidate.STRICT
                        || cell.candidate() == Candidate.RANGE_16
                ? passing(cell)
                : relativeFailure(cell));

        assertNonPromotable(source("7"), none, AllocatorCampaignEvaluationV2.Status.NONE_QUALIFIED);
        assertNonPromotable(source("8"), both, AllocatorCampaignEvaluationV2.Status.BOTH_QUALIFIED);
    }

    @Test
    void interruptedAndInfrastructureFailedCampaignsCannotProduceFormalEvaluationOrResumeAsSuccess() {
        Plan plan = AllocatorCampaignPlannerV2.plan(List.of());
        AllocatorCampaignCheckpointV2 interrupted = AllocatorCampaignCheckpointV2.initial(
                source("9"), budgets(), List.of(), plan.dispositions(), Status.INTERRUPTED);
        AllocatorCampaignCheckpointV2 failed = AllocatorCampaignCheckpointV2.initial(
                source("a"), budgets(), List.of(), plan.dispositions(), Status.INFRASTRUCTURE_FAILED);

        assertThatThrownBy(
                        () -> AllocatorCampaignEvaluationSealV2.seal(AllocatorCampaignCheckpointV2.encode(interrupted)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot produce");
        assertThatThrownBy(() -> AllocatorCampaignEvaluationSealV2.seal(AllocatorCampaignCheckpointV2.encode(failed)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot produce");
        assertThatThrownBy(() -> AllocatorCampaignCheckpointV2.resume(
                        AllocatorCampaignCheckpointV2.encode(failed),
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
        AllocatorCampaignCheckpointV2 checkpoint = complete(source("b"), completed);
        CanonicalBytes checkpointBytes = AllocatorCampaignCheckpointV2.encode(checkpoint);
        CanonicalBytes evaluationBytes = AllocatorCampaignEvaluationSealV2.seal(checkpointBytes);
        DiagnosticAttestation missing =
                new DiagnosticAttestation(source("b"), EnumSet.of(DiagnosticScenario.STRICT), digest("diagnostic"));

        assertThat(AllocatorCampaignPromotionGateV2.evaluate(
                                evaluationBytes,
                                checkpointBytes,
                                source("b"),
                                checkpoint.attachmentDigests(),
                                missing,
                                missing.receiptDigest(),
                                new JUnitSummary(7, 0, 0, 0))
                        .status())
                .isEqualTo(DecisionStatus.DIAGNOSTIC_INCOMPLETE);
        assertThat(AllocatorCampaignPromotionGateV2.evaluate(
                                evaluationBytes,
                                checkpointBytes,
                                source("b"),
                                checkpoint.attachmentDigests(),
                                diagnostic(source("b")),
                                diagnostic(source("b")).receiptDigest(),
                                new JUnitSummary(7, 0, 0, 1))
                        .status())
                .isEqualTo(DecisionStatus.JUNIT_INVALID);
        assertThat(AllocatorCampaignPromotionGateV2.evaluate(
                                evaluationBytes,
                                checkpointBytes,
                                source("b"),
                                checkpoint.attachmentDigests(),
                                diagnostic(source("b")),
                                digest("wrong-diagnostic-junit"),
                                new JUnitSummary(7, 0, 0, 0))
                        .status())
                .isEqualTo(DecisionStatus.DIAGNOSTIC_INCOMPLETE);

        var exactEvaluation = AllocatorCampaignEvaluationSealV2.decode(evaluationBytes);
        CanonicalBytes forgedEvaluation =
                AllocatorCampaignEvaluationSealV2.encode(new AllocatorCampaignEvaluationSealV2.SealedEvaluation(
                        exactEvaluation.source(),
                        exactEvaluation.campaignId(),
                        exactEvaluation.checkpointDigest(),
                        exactEvaluation.attachmentRootDigest(),
                        AllocatorCampaignEvaluationV2.Status.RANGE_SELECTED,
                        java.util.Optional.of(Candidate.RANGE_16),
                        exactEvaluation.executedPerformanceCells(),
                        exactEvaluation.dispositionCells()));
        assertThat(AllocatorCampaignPromotionGateV2.evaluate(
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
            SourceBinding source, Completed completed, AllocatorCampaignEvaluationV2.Status expected) {
        AllocatorCampaignCheckpointV2 checkpoint = complete(source, completed);
        CanonicalBytes checkpointBytes = AllocatorCampaignCheckpointV2.encode(checkpoint);
        CanonicalBytes evaluationBytes = AllocatorCampaignEvaluationSealV2.seal(checkpointBytes);

        assertThat(AllocatorCampaignEvaluationSealV2.decode(evaluationBytes).status())
                .isEqualTo(expected);
        assertThat(AllocatorCampaignPromotionGateV2.evaluate(
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

    private static AllocatorCampaignCheckpointV2 complete(SourceBinding source, Completed completed) {
        return AllocatorCampaignCheckpointV2.initial(
                source, budgets(), completed.records(), completed.plan().dispositions(), Status.COMPLETED);
    }

    private static Completed drive(Function<Cell, IntervalEvidence> intervals) {
        List<Observation> observations = new ArrayList<>();
        List<ExecutionRecord> records = new ArrayList<>();
        for (int guard = 0; guard < 400; guard++) {
            Plan plan = AllocatorCampaignPlannerV2.plan(observations);
            if (plan.completed()) {
                return new Completed(List.copyOf(records), plan);
            }
            Observation observation;
            if (plan.nextAction().orElseThrow() instanceof ExecuteCell executeCell) {
                observation = intervals.apply(executeCell.cell());
            } else if (plan.nextAction().orElseThrow() instanceof ExecuteFaultRow executeFaultRow) {
                observation = passingFault(executeFaultRow.row());
            } else {
                throw new AssertionError("unknown allocator V2 action");
            }
            observations.add(observation);
            records.add(record(observation, records.size()));
        }
        throw new AssertionError("allocator V2 protocol test campaign did not terminate");
    }

    private static ExecutionRecord record(Observation observation, int index) {
        return new ExecutionRecord(observation, digest("attachment-" + index));
    }

    private static IntervalEvidence passing(Cell cell) {
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
                100_000,
                0,
                0,
                0);
    }

    private static IntervalEvidence relativeFailure(Cell cell) {
        IntervalEvidence passing = passing(cell);
        return new IntervalEvidence(
                cell,
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
        return new RemainingBudgets(900, 5_400, 7_200, 5_400, 11_520, 1_440, 600);
    }

    private static Sha256Digest digest(String value) {
        return Sha256Digest.hash(CanonicalBytes.copyOf(value.getBytes(StandardCharsets.UTF_8)));
    }

    private record Completed(List<ExecutionRecord> records, Plan plan) {}
}
