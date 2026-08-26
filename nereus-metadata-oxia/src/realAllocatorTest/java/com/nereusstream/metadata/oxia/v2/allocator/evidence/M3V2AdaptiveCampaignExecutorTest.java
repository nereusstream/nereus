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
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV2;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV2.RemainingBudgets;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV2.SourceBinding;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV2.Status;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignEvaluationSealV2;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.Candidate;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.Cell;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.ExecuteCell;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.ExecuteFaultRow;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.FaultEvidence;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.IntervalEvidence;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.Observation;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.RequiredAction;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.Row;
import com.nereusstream.domain.registry.allocator.AllocatorFaultCutV1;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V2AdaptiveCampaignExecutor.ActionExecutor;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V2AdaptiveCampaignExecutor.ActionResult;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V2AdaptiveCampaignExecutor.BudgetCharge;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V2AdaptiveCampaignExecutor.CampaignResult;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V2AdaptiveCampaignExecutor.CheckpointSink;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V2AdaptiveCampaignExecutor.TerminalReason;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class M3V2AdaptiveCampaignExecutorTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void strictCampaignPersistsEveryValidatorDerivedCheckpointWithoutSealingEvaluation() throws Exception {
        CollectingSink sink = new CollectingSink();
        FakeActions actions = new FakeActions();

        CampaignResult result = executor(source("a"), budgets(), actions, sink, () -> false).start();
        AllocatorCampaignCheckpointV2 finalCheckpoint = checkpoint(result);

        assertThat(result.completed()).isTrue();
        assertThat(result.reason()).isEqualTo(TerminalReason.COMPLETED);
        assertThat(finalCheckpoint.status()).isEqualTo(Status.COMPLETED);
        assertThat(finalCheckpoint.executionRecords()).hasSize(28);
        assertThat(finalCheckpoint.campaign().observations().stream()
                        .filter(IntervalEvidence.class::isInstance))
                .hasSize(20);
        assertThat(finalCheckpoint.dispositions()).hasSize(268);
        assertThat(finalCheckpoint.checkpointSequence()).isEqualTo(28);
        assertThat(sink.values).hasSize(29);
        assertThat(actions.calls()).isEqualTo(28);
        assertThat(com.nereusstream.domain.registry.allocator.AllocatorCampaignValidatorV2
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
        AllocatorCampaignCheckpointV2 interrupted = checkpoint(result);

        assertThat(result.reason()).isEqualTo(TerminalReason.BUDGET_EXHAUSTED);
        assertThat(interrupted.status()).isEqualTo(Status.INTERRUPTED);
        assertThat(interrupted.checkpointSequence()).isEqualTo(1);
        assertThat(interrupted.executionRecords()).isEmpty();
        assertThat(interrupted.dispositions()).isEmpty();
        assertThat(interrupted.remainingBudgets()).isEqualTo(tooSmall);
        assertThat(actions.calls()).isZero();
        assertThat(sink.values).hasSize(2);
        assertThatThrownBy(() -> AllocatorCampaignEvaluationSealV2.seal(result.checkpointBytes()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void stopAndResumePreserveExactPrefixSourceAndNonIncreasingBudgets() throws Exception {
        CollectingSink sink = new CollectingSink();
        FakeActions firstActions = new FakeActions();
        SourceBinding source = source("c");

        CampaignResult stopped = executor(source, budgets(), firstActions, sink, () -> firstActions.calls() >= 3)
                .start();
        AllocatorCampaignCheckpointV2 interrupted = checkpoint(stopped);

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
        AllocatorCampaignCheckpointV2 completed = checkpoint(resumed);

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
        ActionExecutor reordered = new ActionExecutor() {
            @Override
            public BudgetCharge budgetFor(RequiredAction action) {
                return intervalCharge();
            }

            @Override
            public ActionResult execute(RequiredAction action) {
                Cell wrong = Cell.of(Candidate.NATIVE, 10_000, 1, 750);
                return result(passing(wrong), true);
            }
        };

        CampaignResult result = executor(source("e"), budgets(), reordered, sink, () -> false).start();
        AllocatorCampaignCheckpointV2 failed = checkpoint(result);

        assertThat(result.reason()).isEqualTo(TerminalReason.INVALID_ACTION_RESULT);
        assertThat(failed.status()).isEqualTo(Status.INFRASTRUCTURE_FAILED);
        assertThat(failed.executionRecords()).isEmpty();
        assertThat(failed.remainingBudgets().intervalSeconds()).isEqualTo(11_480);
        assertThat(sink.values).hasSize(2);
        assertThatThrownBy(() -> AllocatorCampaignEvaluationSealV2.seal(result.checkpointBytes()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void infrastructureInvalidActionBindsRawObservationButCannotEvaluateOrResume() throws Exception {
        CollectingSink sink = new CollectingSink();
        ActionExecutor invalidInfrastructure = new ActionExecutor() {
            @Override
            public BudgetCharge budgetFor(RequiredAction action) {
                return intervalCharge();
            }

            @Override
            public ActionResult execute(RequiredAction action) {
                return result(passing(((ExecuteCell) action).cell()), false);
            }
        };

        CampaignResult result = executor(source("f"), budgets(), invalidInfrastructure, sink, () -> false).start();
        AllocatorCampaignCheckpointV2 failed = checkpoint(result);

        assertThat(result.reason()).isEqualTo(TerminalReason.INFRASTRUCTURE_FAILED);
        assertThat(failed.status()).isEqualTo(Status.INFRASTRUCTURE_FAILED);
        assertThat(failed.executionRecords()).hasSize(1);
        assertThatThrownBy(() -> AllocatorCampaignEvaluationSealV2.seal(result.checkpointBytes()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> executor(source("f"), budgets(), new FakeActions(), sink, () -> false)
                        .resume(result.checkpointBytes()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot resume");
    }

    @Test
    void actionExceptionConsumesOnlyItsDeclaredBudgetAndPersistsNoObservation() throws Exception {
        CollectingSink sink = new CollectingSink();
        ActionExecutor failing = new ActionExecutor() {
            @Override
            public BudgetCharge budgetFor(RequiredAction action) {
                return intervalCharge();
            }

            @Override
            public ActionResult execute(RequiredAction action) throws Exception {
                throw new IOException("diagnostic action failure");
            }
        };

        CampaignResult result = executor(source("1"), budgets(), failing, sink, () -> false).start();
        AllocatorCampaignCheckpointV2 failed = checkpoint(result);

        assertThat(result.reason()).isEqualTo(TerminalReason.INFRASTRUCTURE_FAILED);
        assertThat(failed.executionRecords()).isEmpty();
        assertThat(failed.remainingBudgets().intervalSeconds()).isEqualTo(11_480);
        assertThat(result.detail()).contains("IOException").contains("diagnostic action failure");
        assertThat(sink.values).hasSize(2);
    }

    @Test
    void budgetAccountingFailureConsumesNothingAndFailsClosed() throws Exception {
        CollectingSink sink = new CollectingSink();
        ActionExecutor failing = new ActionExecutor() {
            @Override
            public BudgetCharge budgetFor(RequiredAction action) {
                throw new IllegalStateException("missing phase budget");
            }

            @Override
            public ActionResult execute(RequiredAction action) {
                throw new AssertionError("action must not execute without a validated budget charge");
            }
        };

        CampaignResult result = executor(source("3"), budgets(), failing, sink, () -> false).start();
        AllocatorCampaignCheckpointV2 failed = checkpoint(result);

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
        var initial = AllocatorCampaignCheckpointV2.initial(
                source("2"), budgets(), List.of(), List.of(), Status.RUNNING);
        CanonicalBytes bytes = AllocatorCampaignCheckpointV2.encode(initial);

        sink.persist(0, bytes);

        assertThatThrownBy(() -> sink.persist(0, bytes))
                .isInstanceOf(java.nio.file.FileAlreadyExistsException.class);
        try (var files = Files.list(directory)) {
            assertThat(files).hasSize(1);
        }
    }

    private static M3V2AdaptiveCampaignExecutor executor(
            SourceBinding source,
            RemainingBudgets budgets,
            ActionExecutor actions,
            CheckpointSink sink,
            M3V2AdaptiveCampaignExecutor.StopSignal stopSignal) {
        return new M3V2AdaptiveCampaignExecutor(source, budgets, actions, sink, stopSignal);
    }

    private static AllocatorCampaignCheckpointV2 checkpoint(CampaignResult result) {
        return AllocatorCampaignCheckpointV2.decode(result.checkpointBytes());
    }

    private static void assertExactLineage(List<CanonicalBytes> checkpoints) {
        for (int index = 0; index < checkpoints.size(); index++) {
            AllocatorCampaignCheckpointV2 current = AllocatorCampaignCheckpointV2.decode(checkpoints.get(index));
            assertThat(current.checkpointSequence()).isEqualTo(index);
            if (index > 0) {
                assertThat(current.predecessorCheckpointDigest())
                        .isEqualTo(AllocatorCampaignCheckpointV2.digest(checkpoints.get(index - 1)));
            }
        }
    }

    private static RemainingBudgets budgets() {
        return new RemainingBudgets(900, 5_400, 7_200, 5_400, 11_520, 1_440, 600);
    }

    private static BudgetCharge intervalCharge() {
        return new BudgetCharge(0, 0, 0, 0, 40, 0, 0);
    }

    private static ActionResult result(Observation observation, boolean infrastructureValid) {
        return new ActionResult(
                observation,
                digest("attachment-" + identity(observation)),
                infrastructureValid);
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
        IntervalEvidence value = passing(cell);
        return new IntervalEvidence(
                cell,
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

        @Override
        public BudgetCharge budgetFor(RequiredAction action) {
            return action instanceof ExecuteCell
                    ? intervalCharge()
                    : new BudgetCharge(0, 0, 1, 0, 0, 0, 0);
        }

        @Override
        public ActionResult execute(RequiredAction action) {
            calls.incrementAndGet();
            Observation observation;
            if (action instanceof ExecuteCell executeCell) {
                Cell cell = executeCell.cell();
                observation = cell.candidate().nativePath() || cell.candidate().strict()
                        ? passing(cell)
                        : relativeFailure(cell);
            } else {
                observation = passingFault(((ExecuteFaultRow) action).row());
            }
            return result(observation, true);
        }

        int calls() {
            return calls.get();
        }
    }
}
