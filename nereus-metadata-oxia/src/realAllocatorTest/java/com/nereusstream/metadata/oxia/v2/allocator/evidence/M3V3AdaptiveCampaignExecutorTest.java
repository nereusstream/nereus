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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV3;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV3.RemainingBudgets;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV3.SourceBinding;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV3.Status;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignEvaluationSealV3;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Candidate;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Cell;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.ExecuteCell;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.ExecuteFaultRow;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.FaultEvidence;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.IntervalEvidence;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Observation;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.RequiredAction;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Row;
import com.nereusstream.domain.registry.allocator.AllocatorFaultCutV1;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3AdaptiveCampaignExecutor.ActionExecutor;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3AdaptiveCampaignExecutor.ActionResult;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3AdaptiveCampaignExecutor.BudgetCharge;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3AdaptiveCampaignExecutor.CampaignResult;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3AdaptiveCampaignExecutor.CheckpointSink;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3AdaptiveCampaignExecutor.PhysicalActionResult;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3AdaptiveCampaignExecutor.TerminalReason;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3FormalActionExecutorAdapter.FaultActionResult;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3FormalActionExecutorAdapter.IntervalActionResult;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3FormalActionExecutorAdapter.RealActionRuntime;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3FormalActionExecutorAdapter.ScaleActionResult;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3FormalCampaignPlan.PlannedActionV3;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class M3V3AdaptiveCampaignExecutorTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void frozenPhysicalPlanHasExact720ActionInventoryAndStableDigest() {
        List<PlannedActionV3> actions = M3V3FormalCampaignPlan.zeroDecisionActions();

        assertThat(actions).hasSize(720);
        assertThat(actions.stream().collect(Collectors.groupingBy(
                        PlannedActionV3::kind, Collectors.counting())))
                .containsEntry(M3V3FormalCampaignPlan.ActionKind.NATIVE_INTERVAL, 48L)
                .containsEntry(M3V3FormalCampaignPlan.ActionKind.CANDIDATE_INTERVAL, 280L)
                .containsEntry(M3V3FormalCampaignPlan.ActionKind.FAULT_ACTION, 360L)
                .containsEntry(M3V3FormalCampaignPlan.ActionKind.SCALE_ACTION, 32L);
        assertThat(M3V3FormalCampaignPlan.zeroDecisionPlanDigest().toHex())
                .isEqualTo("5f94079eb0d41739e4da32c0d4170a837ca2a63b33a6a8ad71b25a87ca49b283");
    }

    @Test
    void rangeRowExpandsToOneScaleThenOneIntervalAndFaultRowToNineCuts() {
        Cell range = Cell.fixedRate(Candidate.RANGE_16, 10_000, 1, 1000);
        List<PlannedActionV3> first = M3V3FormalCampaignPlan.actionsFor(
                new ExecuteCell(range, 1000), List.of());
        List<PlannedActionV3> later = M3V3FormalCampaignPlan.actionsFor(
                new ExecuteCell(Cell.fixedRate(Candidate.RANGE_16, 10_000, 1, 750), 750),
                List.of(passing(range)));
        List<PlannedActionV3> faults = M3V3FormalCampaignPlan.actionsFor(
                new ExecuteFaultRow(range.row()), List.of(passing(range)));

        assertThat(first).extracting(PlannedActionV3::kind)
                .containsExactly(
                        M3V3FormalCampaignPlan.ActionKind.SCALE_ACTION,
                        M3V3FormalCampaignPlan.ActionKind.CANDIDATE_INTERVAL);
        assertThat(later).extracting(PlannedActionV3::kind)
                .containsExactly(M3V3FormalCampaignPlan.ActionKind.CANDIDATE_INTERVAL);
        assertThat(faults).extracting(PlannedActionV3::faultCut)
                .containsExactly(AllocatorFaultCutV1.values());
    }

    @Test
    void strictCampaignPersistsEveryValidatorDerivedCheckpointWithoutSealingEvaluation() throws Exception {
        CollectingSink sink = new CollectingSink();
        FakeActions actions = new FakeActions();

        CampaignResult result = executor(source("a"), budgets(), actions, sink, () -> false).start();
        AllocatorCampaignCheckpointV3 finalCheckpoint = checkpoint(result);

        assertThat(result.completed()).isTrue();
        assertThat(result.reason()).isEqualTo(TerminalReason.COMPLETED);
        assertThat(finalCheckpoint.status()).isEqualTo(Status.COMPLETED);
        assertThat(finalCheckpoint.executionRecords()).hasSize(28);
        assertThat(finalCheckpoint.campaign().observations().stream()
                        .filter(IntervalEvidence.class::isInstance))
                .hasSize(20);
        assertThat(finalCheckpoint.dispositions()).hasSize(308);
        assertThat(finalCheckpoint.checkpointSequence()).isEqualTo(28);
        assertThat(sink.values).hasSize(29);
        assertThat(actions.calls()).isEqualTo(96);
        assertThat(com.nereusstream.domain.registry.allocator.AllocatorCampaignValidatorV3
                        .validate(finalCheckpoint.campaign())
                        .qualifiedCandidates())
                .containsExactly(Candidate.STRICT);
        assertExactLineage(sink.values);
    }

    @Test
    void budgetExhaustionInterruptsBeforeExecutingOrForgingDispositions() throws Exception {
        CollectingSink sink = new CollectingSink();
        FakeActions actions = new FakeActions();
        RemainingBudgets tooSmall = new RemainingBudgets(900, 5_400, 7_200, 5_400, 39, 1_440, 600);

        CampaignResult result = executor(source("b"), tooSmall, actions, sink, () -> false).start();
        AllocatorCampaignCheckpointV3 interrupted = checkpoint(result);

        assertThat(result.reason()).isEqualTo(TerminalReason.BUDGET_EXHAUSTED);
        assertThat(interrupted.status()).isEqualTo(Status.INTERRUPTED);
        assertThat(interrupted.checkpointSequence()).isEqualTo(1);
        assertThat(interrupted.executionRecords()).isEmpty();
        assertThat(interrupted.dispositions()).isEmpty();
        assertThat(interrupted.remainingBudgets()).isEqualTo(tooSmall);
        assertThat(actions.calls()).isZero();
        assertThat(sink.values).hasSize(2);
        assertThatThrownBy(() -> AllocatorCampaignEvaluationSealV3.seal(result.checkpointBytes()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void stopAndResumePreserveExactPrefixSourceAndNonIncreasingBudgets() throws Exception {
        CollectingSink sink = new CollectingSink();
        FakeActions firstActions = new FakeActions();
        SourceBinding source = source("c");

        CampaignResult stopped = executor(source, budgets(), firstActions, sink, () -> firstActions.calls() >= 3)
                .start();
        AllocatorCampaignCheckpointV3 interrupted = checkpoint(stopped);

        assertThat(stopped.reason()).isEqualTo(TerminalReason.STOP_REQUESTED);
        assertThat(interrupted.status()).isEqualTo(Status.INTERRUPTED);
        assertThat(interrupted.executionRecords()).hasSize(3);
        assertThat(interrupted.checkpointSequence()).isEqualTo(4);
        assertThatThrownBy(() -> executor(source("d"), budgets(), new FakeActions(), sink, () -> false)
                        .resume(stopped.checkpointBytes()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("source/executor tuple");

        CampaignResult resumed = executor(source, budgets(), new FakeActions(), sink, () -> false)
                .resume(stopped.checkpointBytes());
        AllocatorCampaignCheckpointV3 completed = checkpoint(resumed);

        assertThat(resumed.completed()).isTrue();
        assertThat(completed.executionRecords()).hasSize(28);
        assertThat(completed.executionRecords().subList(0, interrupted.executionRecords().size()))
                .isEqualTo(interrupted.executionRecords());
        assertThat(completed.checkpointSequence()).isEqualTo(29);
        assertThat(completed.remainingBudgets().intervalSeconds())
                .isLessThanOrEqualTo(interrupted.remainingBudgets().intervalSeconds());
        assertThat(completed.remainingBudgets().faultSeconds())
                .isLessThanOrEqualTo(interrupted.remainingBudgets().faultSeconds());
        assertThat(sink.values).hasSize(30);
        assertExactLineage(sink.values);
    }

    @Test
    void reorderedActionResultFailsInfrastructureWithoutBindingForgedObservation() throws Exception {
        CollectingSink sink = new CollectingSink();
        ActionExecutor reordered = new M3V3FormalActionExecutorAdapter(new FakeRuntime() {
            @Override
            public IntervalActionResult executeNativeInterval(Cell cell, int offeredRate) {
                Cell wrong = Cell.fixedRate(Candidate.NATIVE, 10_000, 1, 750);
                return intervalResult(passing(wrong), true);
            }
        });

        CampaignResult result = executor(source("e"), budgets(), reordered, sink, () -> false).start();
        AllocatorCampaignCheckpointV3 failed = checkpoint(result);

        assertThat(result.reason()).isEqualTo(TerminalReason.INVALID_ACTION_RESULT);
        assertThat(failed.status()).isEqualTo(Status.INFRASTRUCTURE_FAILED);
        assertThat(failed.executionRecords()).isEmpty();
        assertThat(failed.remainingBudgets().intervalSeconds()).isEqualTo(11_480);
        assertThat(sink.values).hasSize(2);
        assertThatThrownBy(() -> AllocatorCampaignEvaluationSealV3.seal(result.checkpointBytes()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void infrastructureInvalidActionBindsRawObservationButCannotEvaluateOrResume() throws Exception {
        CollectingSink sink = new CollectingSink();
        ActionExecutor invalidInfrastructure = new M3V3FormalActionExecutorAdapter(new FakeRuntime() {
            @Override
            public IntervalActionResult executeNativeInterval(Cell cell, int offeredRate) {
                return intervalResult(passing(cell, offeredRate), false);
            }
        });

        CampaignResult result = executor(source("f"), budgets(), invalidInfrastructure, sink, () -> false).start();
        AllocatorCampaignCheckpointV3 failed = checkpoint(result);

        assertThat(result.reason()).isEqualTo(TerminalReason.INFRASTRUCTURE_FAILED);
        assertThat(result.detail()).contains("fake interval infrastructure invalid");
        assertThat(failed.status()).isEqualTo(Status.INFRASTRUCTURE_FAILED);
        assertThat(failed.executionRecords()).hasSize(1);
        assertThatThrownBy(() -> AllocatorCampaignEvaluationSealV3.seal(result.checkpointBytes()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> executor(source("f"), budgets(), new FakeActions(), sink, () -> false)
                        .resume(result.checkpointBytes()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot resume");
    }

    @Test
    void actionExceptionConsumesOnlyItsDeclaredBudgetAndPersistsNoObservation() throws Exception {
        CollectingSink sink = new CollectingSink();
        ActionExecutor failing = new M3V3FormalActionExecutorAdapter(new FakeRuntime() {
            @Override
            public IntervalActionResult executeNativeInterval(Cell cell, int offeredRate) throws Exception {
                throw new IOException("diagnostic action failure");
            }
        });

        CampaignResult result = executor(source("1"), budgets(), failing, sink, () -> false).start();
        AllocatorCampaignCheckpointV3 failed = checkpoint(result);

        assertThat(result.reason()).isEqualTo(TerminalReason.INFRASTRUCTURE_FAILED);
        assertThat(failed.executionRecords()).isEmpty();
        assertThat(failed.remainingBudgets().intervalSeconds()).isEqualTo(11_480);
        assertThat(result.detail()).contains("IOException").contains("diagnostic action failure");
        assertThat(sink.values).hasSize(2);
    }

    @Test
    void failedCampaignResultPreservesAndEscapesDiagnosticDetail() throws Exception {
        CollectingSink sink = new CollectingSink();
        ActionExecutor failing = new M3V3FormalActionExecutorAdapter(new FakeRuntime() {
            @Override
            public IntervalActionResult executeNativeInterval(Cell cell, int offeredRate) throws Exception {
                throw new IOException("diagnostic \"quoted\" \\ path\nnext");
            }
        });
        CampaignResult result = executor(source("4"), budgets(), failing, sink, () -> false).start();
        Path output = temporaryDirectory.resolve("campaign-result.json");

        M3V3BoundedAdaptiveFormalCampaignTest.writeResult(output, result, checkpoint(result));

        String json = Files.readString(output, StandardCharsets.UTF_8);
        assertThat(result.status()).isEqualTo(Status.INFRASTRUCTURE_FAILED);
        assertThat(json).contains(
                "\"terminalDetail\":" + M3V3BoundedAdaptiveFormalCampaignTest.jsonString(result.detail()));
        assertThat(json).contains("diagnostic \\\"quoted\\\" \\\\ path\\nnext");
        assertThat(json.lines()).hasSize(1);
    }

    @Test
    void budgetAccountingFailureConsumesNothingAndFailsClosed() throws Exception {
        CollectingSink sink = new CollectingSink();
        ActionExecutor failing = new ActionExecutor() {
            @Override
            public BudgetCharge budgetFor(PlannedActionV3 action) {
                throw new IllegalStateException("missing phase budget");
            }

            @Override
            public PhysicalActionResult execute(PlannedActionV3 action) {
                throw new AssertionError("action must not execute without a validated budget charge");
            }

            @Override
            public ActionResult complete(RequiredAction required, List<PhysicalActionResult> physicalResults) {
                throw new AssertionError("an unexecuted action cannot complete");
            }
        };

        CampaignResult result = executor(source("3"), budgets(), failing, sink, () -> false).start();
        AllocatorCampaignCheckpointV3 failed = checkpoint(result);

        assertThat(result.reason()).isEqualTo(TerminalReason.BUDGET_ACCOUNTING_FAILED);
        assertThat(failed.status()).isEqualTo(Status.INFRASTRUCTURE_FAILED);
        assertThat(failed.executionRecords()).isEmpty();
        assertThat(failed.remainingBudgets()).isEqualTo(budgets());
        assertThat(result.detail()).contains("IllegalStateException").contains("missing phase budget");
        assertThat(sink.values).hasSize(2);
    }

    @Test
    void createNewCheckpointSinkNeverOverwrites() throws Exception {
        Path directory = Files.createDirectory(temporaryDirectory.resolve("checkpoints"));
        CheckpointSink sink = CheckpointSink.createNewDirectory(directory);
        var initial = AllocatorCampaignCheckpointV3.initial(
                source("2"), budgets(), List.of(), List.of(), Status.RUNNING);
        CanonicalBytes bytes = AllocatorCampaignCheckpointV3.encode(initial);

        sink.persist(0, bytes);

        assertThatThrownBy(() -> sink.persist(0, bytes))
                .isInstanceOf(java.nio.file.FileAlreadyExistsException.class);
        try (var files = Files.list(directory)) {
            assertThat(files).hasSize(1);
        }
    }

    private static M3V3AdaptiveCampaignExecutor executor(
            SourceBinding source,
            RemainingBudgets budgets,
            ActionExecutor actions,
            CheckpointSink sink,
            M3V3AdaptiveCampaignExecutor.StopSignal stopSignal) {
        return new M3V3AdaptiveCampaignExecutor(source, budgets, actions, sink, stopSignal);
    }

    private static AllocatorCampaignCheckpointV3 checkpoint(CampaignResult result) {
        return AllocatorCampaignCheckpointV3.decode(result.checkpointBytes());
    }

    private static void assertExactLineage(List<CanonicalBytes> checkpoints) {
        for (int index = 0; index < checkpoints.size(); index++) {
            AllocatorCampaignCheckpointV3 current = AllocatorCampaignCheckpointV3.decode(checkpoints.get(index));
            assertThat(current.checkpointSequence()).isEqualTo(index);
            if (index > 0) {
                assertThat(current.predecessorCheckpointDigest())
                        .isEqualTo(AllocatorCampaignCheckpointV3.digest(checkpoints.get(index - 1)));
            }
        }
    }

    private static RemainingBudgets budgets() {
        return new RemainingBudgets(900, 5_400, 7_200, 5_400, 11_520, 1_440, 600);
    }

    private static BudgetCharge intervalCharge() {
        return new BudgetCharge(0, 0, 0, 0, 40, 0, 0);
    }

    private static String identity(Observation observation) {
        if (observation instanceof IntervalEvidence interval) {
            return "cell-" + interval.cell().contextId();
        }
        FaultEvidence fault = (FaultEvidence) observation;
        return "fault-" + fault.row().candidate() + '-' + fault.row().activeManagedLedgers() + '-'
                + fault.row().metadataLatencyP99Millis();
    }

    private static IntervalEvidence passing(Cell cell) {
        return passing(cell, cell.rateSlot().fixedRate());
    }

    private static IntervalEvidence passing(Cell cell, int offeredRate) {
        long offered = (long) offeredRate * AllocatorCampaignV3.MEASURED_SECONDS;
        return new IntervalEvidence(
                cell,
                offeredRate,
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
                offeredRate,
                100_000,
                100_000,
                0,
                0,
                0);
    }

    private static IntervalEvidence relativeFailure(Cell cell) {
        return relativeFailure(passing(cell));
    }

    private static IntervalEvidence relativeFailure(IntervalEvidence value) {
        Cell cell = value.cell();
        return new IntervalEvidence(
                cell,
                value.offeredRate(),
                value.offered(),
                value.admitted(),
                value.overloadDroppedBeforeAdmission(),
                value.completed(),
                value.failedAfterAdmission(),
                value.timedOutAfterAdmission(),
                value.terminal(),
                value.failedAssertions(),
                value.unexpectedErrors(),
                value.skipped(),
                value.duplicateLedgerIds(),
                value.reusedLedgerIds(),
                value.rolloverP99Micros(),
                value.oxiaOperationP99Micros(),
                value.queueAgeP99Micros(),
                value.queueDepthMaximum(),
                value.starvationMaximumMicros(),
                400_001,
                value.backlogAtEnd(),
                value.inFlightAtEnd(),
                value.waiterCountAtEnd());
    }

    private static FaultEvidence passingFault(Row row) {
        return new FaultEvidence(row, EnumSet.allOf(AllocatorFaultCutV1.class), 0, 0, 0, 0, 0, 0, 0, 0, 1, 1_000_000);
    }

    private static SourceBinding source(String commitCharacter) {
        return new SourceBinding(
                commitCharacter.repeat(40),
                digest("oxia"),
                digest("dependency"),
                digest("executor"),
                digest("workload"));
    }

    private static Sha256Digest digest(String value) {
        return Sha256Digest.hash(CanonicalBytes.copyOf(value.getBytes(StandardCharsets.UTF_8)));
    }

    private static final class CollectingSink implements CheckpointSink {
        private final List<CanonicalBytes> values = new ArrayList<>();

        @Override
        public void persist(long sequence, CanonicalBytes checkpointBytes) {
            if (sequence != values.size()) {
                throw new AssertionError("unexpected checkpoint sequence " + sequence);
            }
            values.add(checkpointBytes);
        }
    }

    private static final class FakeActions implements ActionExecutor {
        private final AtomicInteger calls = new AtomicInteger();
        private final M3V3FormalActionExecutorAdapter delegate =
                new M3V3FormalActionExecutorAdapter(new FakeRuntime(calls));

        @Override
        public BudgetCharge budgetFor(PlannedActionV3 action) {
            return delegate.budgetFor(action);
        }

        @Override
        public PhysicalActionResult execute(PlannedActionV3 action) throws Exception {
            return delegate.execute(action);
        }

        @Override
        public ActionResult complete(RequiredAction required, List<PhysicalActionResult> physicalResults) {
            return delegate.complete(required, physicalResults);
        }

        int calls() {
            return calls.get();
        }
    }

    private static class FakeRuntime implements RealActionRuntime {
        private final AtomicInteger calls;

        private FakeRuntime() {
            this(new AtomicInteger());
        }

        private FakeRuntime(AtomicInteger calls) {
            this.calls = calls;
        }

        @Override
        public IntervalActionResult executeNativeInterval(Cell cell, int offeredRate) throws Exception {
            calls.incrementAndGet();
            return intervalResult(passing(cell, offeredRate), true);
        }

        @Override
        public IntervalActionResult executeCandidateInterval(Cell cell, int offeredRate) throws Exception {
            calls.incrementAndGet();
            IntervalEvidence evidence = cell.candidate().strict()
                    ? passing(cell, offeredRate)
                    : relativeFailure(passing(cell, offeredRate));
            return intervalResult(evidence, true);
        }

        @Override
        public FaultActionResult executeFaultAction(Row row, AllocatorFaultCutV1 cut) throws Exception {
            calls.incrementAndGet();
            return new FaultActionResult(
                    row,
                    cut,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    1,
                    cut == AllocatorFaultCutV1.BROKER_SESSION_CRASH_MASS_TAKEOVER ? 1_000_000 : 0,
                    digest("fault-" + row + '-' + cut),
                    true);
        }

        @Override
        public ScaleActionResult executeScaleAction(Row row) throws Exception {
            calls.incrementAndGet();
            return new ScaleActionResult(row, digest("scale-" + row), true);
        }
    }

    private static IntervalActionResult intervalResult(IntervalEvidence evidence, boolean infrastructureValid) {
        return new IntervalActionResult(
                evidence,
                digest("attachment-" + identity(evidence)),
                infrastructureValid,
                infrastructureValid ? "" : "fake interval infrastructure invalid");
    }
}
