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
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV3;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV3.ExecutionRecord;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV3.RemainingBudgets;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV3.SourceBinding;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV3.Status;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPlannerV3;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Observation;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.Plan;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV3.RequiredAction;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignValidatorV3;
import com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3FormalCampaignPlan.PlannedActionV3;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * Bounded ADR-0108 campaign orchestration. The deterministic planner is the only source of required actions and
 * dispositions. This class persists a canonical checkpoint before execution and after every action or terminal stop;
 * it never seals an evaluation or makes a promotion decision.
 */
final class M3V3AdaptiveCampaignExecutor {
    private final SourceBinding source;
    private final RemainingBudgets initialBudgets;
    private final ActionExecutor actions;
    private final CheckpointSink checkpoints;
    private final StopSignal stopSignal;
    private final HardDeadline hardDeadline;

    M3V3AdaptiveCampaignExecutor(
            SourceBinding source,
            RemainingBudgets initialBudgets,
            ActionExecutor actions,
            CheckpointSink checkpoints,
            StopSignal stopSignal) {
        this(source, initialBudgets, actions, checkpoints, stopSignal, HardDeadline.never());
    }

    M3V3AdaptiveCampaignExecutor(
            SourceBinding source,
            RemainingBudgets initialBudgets,
            ActionExecutor actions,
            CheckpointSink checkpoints,
            StopSignal stopSignal,
            HardDeadline hardDeadline) {
        this.source = Objects.requireNonNull(source, "source");
        this.initialBudgets = Objects.requireNonNull(initialBudgets, "initialBudgets");
        this.actions = Objects.requireNonNull(actions, "actions");
        this.checkpoints = Objects.requireNonNull(checkpoints, "checkpoints");
        this.stopSignal = Objects.requireNonNull(stopSignal, "stopSignal");
        this.hardDeadline = Objects.requireNonNull(hardDeadline, "hardDeadline");
    }

    CampaignResult start() throws IOException {
        Plan plan = AllocatorCampaignPlannerV3.plan(List.of());
        AllocatorCampaignCheckpointV3 initial = AllocatorCampaignCheckpointV3.initial(
                source, initialBudgets, List.of(), plan.dispositions(), Status.RUNNING);
        CanonicalBytes initialBytes = AllocatorCampaignCheckpointV3.encode(initial);
        persist(initialBytes);
        return drive(initialBytes);
    }

    CampaignResult resume(CanonicalBytes predecessorBytes) throws IOException {
        AllocatorCampaignCheckpointV3 predecessor =
                AllocatorCampaignCheckpointV3.decode(Objects.requireNonNull(predecessorBytes, "predecessorBytes"));
        if (!predecessor.source().equals(source)) {
            throw new IllegalArgumentException("allocator V3 resume source/executor tuple differs");
        }
        if (predecessor.status() != Status.RUNNING && predecessor.status() != Status.INTERRUPTED) {
            throw new IllegalArgumentException("allocator V3 checkpoint status cannot resume");
        }
        return drive(predecessorBytes);
    }

    private CampaignResult drive(CanonicalBytes startingBytes) throws IOException {
        CanonicalBytes currentBytes = startingBytes;
        AllocatorCampaignCheckpointV3 current = AllocatorCampaignCheckpointV3.decode(currentBytes);
        int executedPhysicalActions = M3V3FormalCampaignPlan.completedPhysicalActions(
                current.campaign().observations());
        while (true) {
            Plan currentPlan = AllocatorCampaignValidatorV3.validate(current.campaign());
            if (currentPlan.completed()) {
                throw new IllegalStateException("allocator V3 resumable checkpoint is already complete");
            }
            if (executedPhysicalActions >= M3V3FormalCampaignPlan.MAXIMUM_TOTAL_EXECUTED_ACTIONS) {
                return transition(
                        currentBytes,
                        current,
                        current.remainingBudgets(),
                        Status.INFRASTRUCTURE_FAILED,
                        TerminalReason.ACTION_CAP_EXCEEDED,
                        "campaign reached the frozen 720-action cap before validator completion");
            }
            if (hardDeadline.exceeded()) {
                return transition(
                        currentBytes,
                        current,
                        current.remainingBudgets(),
                        Status.INFRASTRUCTURE_FAILED,
                        TerminalReason.WALL_CLOCK_CAP_EXCEEDED,
                        "campaign exceeded the frozen 48000-second wall-clock cap");
            }
            if (stopSignal.stopRequested()) {
                if (current.status() == Status.INTERRUPTED) {
                    return result(currentBytes, current, TerminalReason.STOP_REQUESTED, "stop already checkpointed");
                }
                return transition(
                        currentBytes,
                        current,
                        current.remainingBudgets(),
                        Status.INTERRUPTED,
                        TerminalReason.STOP_REQUESTED,
                        "stop requested before next action");
            }
            RequiredAction action = currentPlan.nextAction().orElseThrow();
            List<PlannedActionV3> plannedActions = M3V3FormalCampaignPlan.actionsFor(
                    action, current.campaign().observations());
            List<PhysicalActionResult> physicalResults = new ArrayList<>(plannedActions.size());
            RemainingBudgets afterCharge = current.remainingBudgets();
            for (PlannedActionV3 plannedAction : plannedActions) {
                if (executedPhysicalActions >= M3V3FormalCampaignPlan.MAXIMUM_TOTAL_EXECUTED_ACTIONS) {
                    return transition(
                            currentBytes,
                            current,
                            afterCharge,
                            Status.INFRASTRUCTURE_FAILED,
                            TerminalReason.ACTION_CAP_EXCEEDED,
                            "campaign attempted a physical action outside the frozen 720-action inventory");
                }
                BudgetCharge charge;
                try {
                    charge = Objects.requireNonNull(
                            actions.budgetFor(plannedAction), "allocator V3 physical action budget");
                } catch (RuntimeException failure) {
                    return transition(
                            currentBytes,
                            current,
                            afterCharge,
                            Status.INFRASTRUCTURE_FAILED,
                            TerminalReason.BUDGET_ACCOUNTING_FAILED,
                            failure.getClass().getSimpleName() + ": "
                                    + Objects.toString(failure.getMessage(), ""));
                }
                if (!charge.fits(afterCharge)) {
                    if (current.status() == Status.INTERRUPTED) {
                        return result(
                                currentBytes,
                                current,
                                TerminalReason.BUDGET_EXHAUSTED,
                                "remaining independent phase budget cannot admit the next physical action");
                    }
                    return transition(
                            currentBytes,
                            current,
                            afterCharge,
                            Status.INTERRUPTED,
                            TerminalReason.BUDGET_EXHAUSTED,
                            "remaining independent phase budget cannot admit the next physical action");
                }
                afterCharge = charge.subtractFrom(afterCharge);
                try {
                    PhysicalActionResult result = Objects.requireNonNull(
                            actions.execute(plannedAction), "allocator V3 physical action result");
                    if (!result.action().equals(plannedAction)) {
                        throw new IllegalArgumentException("allocator V3 physical result belongs to another action");
                    }
                    physicalResults.add(result);
                    executedPhysicalActions++;
                } catch (InterruptedException failure) {
                    CampaignResult interrupted = transition(
                            currentBytes,
                            current,
                            afterCharge,
                            Status.INTERRUPTED,
                            TerminalReason.ACTION_INTERRUPTED,
                            failure.getClass().getSimpleName());
                    Thread.currentThread().interrupt();
                    return interrupted;
                } catch (IllegalArgumentException failure) {
                    return transition(
                            currentBytes,
                            current,
                            afterCharge,
                            Status.INFRASTRUCTURE_FAILED,
                            TerminalReason.INVALID_ACTION_RESULT,
                            failure.getClass().getSimpleName() + ": "
                                    + Objects.toString(failure.getMessage(), ""));
                } catch (Exception failure) {
                    return transition(
                            currentBytes,
                            current,
                            afterCharge,
                            Status.INFRASTRUCTURE_FAILED,
                            TerminalReason.INFRASTRUCTURE_FAILED,
                            failure.getClass().getSimpleName() + ": "
                                    + Objects.toString(failure.getMessage(), ""));
                }
            }

            ActionResult actionResult;
            try {
                actionResult = Objects.requireNonNull(
                        actions.complete(action, physicalResults), "allocator V3 completed action result");
            } catch (RuntimeException failure) {
                return transition(
                        currentBytes,
                        current,
                        afterCharge,
                        Status.INFRASTRUCTURE_FAILED,
                        TerminalReason.INVALID_ACTION_RESULT,
                        failure.getClass().getSimpleName() + ": " + Objects.toString(failure.getMessage(), ""));
            }

            ExecutionRecord record;
            Plan nextPlan;
            List<ExecutionRecord> nextRecords = new ArrayList<>(current.executionRecords());
            try {
                record = new ExecutionRecord(actionResult.observation(), actionResult.attachmentDigest());
                nextRecords.add(record);
                List<Observation> observations =
                        nextRecords.stream().map(ExecutionRecord::observation).toList();
                nextPlan = AllocatorCampaignPlannerV3.plan(observations);
            } catch (RuntimeException failure) {
                return transition(
                        currentBytes,
                        current,
                        afterCharge,
                        Status.INFRASTRUCTURE_FAILED,
                        TerminalReason.INVALID_ACTION_RESULT,
                        failure.getClass().getSimpleName() + ": " + Objects.toString(failure.getMessage(), ""));
            }

            Status nextStatus = actionResult.infrastructureValid()
                    ? (nextPlan.completed() ? Status.COMPLETED : Status.RUNNING)
                    : Status.INFRASTRUCTURE_FAILED;
            AllocatorCampaignCheckpointV3 next;
            CanonicalBytes nextBytes;
            try {
                next = AllocatorCampaignCheckpointV3.resume(
                        currentBytes,
                        source,
                        afterCharge,
                        nextRecords,
                        nextPlan.dispositions(),
                        nextStatus);
                nextBytes = AllocatorCampaignCheckpointV3.encode(next);
            } catch (RuntimeException failure) {
                return transition(
                        currentBytes,
                        current,
                        afterCharge,
                        Status.INFRASTRUCTURE_FAILED,
                        TerminalReason.INVALID_ACTION_RESULT,
                        failure.getClass().getSimpleName() + ": " + Objects.toString(failure.getMessage(), ""));
            }
            persist(nextBytes);
            if (!actionResult.infrastructureValid()) {
                return result(
                        nextBytes,
                        next,
                        TerminalReason.INFRASTRUCTURE_FAILED,
                        actionResult.infrastructureDetail());
            }
            if (nextStatus == Status.COMPLETED) {
                return result(nextBytes, next, TerminalReason.COMPLETED, "validator-complete campaign");
            }
            current = next;
            currentBytes = nextBytes;
        }
    }

    private CampaignResult transition(
            CanonicalBytes predecessorBytes,
            AllocatorCampaignCheckpointV3 predecessor,
            RemainingBudgets remainingBudgets,
            Status nextStatus,
            TerminalReason reason,
            String detail)
            throws IOException {
        Plan plan = AllocatorCampaignValidatorV3.validate(predecessor.campaign());
        AllocatorCampaignCheckpointV3 terminal = AllocatorCampaignCheckpointV3.resume(
                predecessorBytes,
                source,
                remainingBudgets,
                predecessor.executionRecords(),
                plan.dispositions(),
                nextStatus);
        CanonicalBytes terminalBytes = AllocatorCampaignCheckpointV3.encode(terminal);
        persist(terminalBytes);
        return result(terminalBytes, terminal, reason, detail);
    }

    private CampaignResult result(
            CanonicalBytes checkpointBytes,
            AllocatorCampaignCheckpointV3 checkpoint,
            TerminalReason reason,
            String detail) {
        return new CampaignResult(checkpointBytes, checkpoint.status(), reason, detail);
    }

    private void persist(CanonicalBytes checkpointBytes) throws IOException {
        AllocatorCampaignCheckpointV3 checkpoint = AllocatorCampaignCheckpointV3.decode(checkpointBytes);
        checkpoints.persist(checkpoint.checkpointSequence(), checkpointBytes);
    }

    @FunctionalInterface
    interface StopSignal {
        boolean stopRequested();

        static StopSignal never() {
            return () -> false;
        }
    }

    @FunctionalInterface
    interface HardDeadline {
        boolean exceeded();

        static HardDeadline never() {
            return () -> false;
        }
    }

    interface ActionExecutor {
        BudgetCharge budgetFor(PlannedActionV3 action);

        PhysicalActionResult execute(PlannedActionV3 action) throws Exception;

        ActionResult complete(RequiredAction required, List<PhysicalActionResult> physicalResults);
    }

    @FunctionalInterface
    interface CheckpointSink {
        void persist(long sequence, CanonicalBytes checkpointBytes) throws IOException;

        static CheckpointSink createNewDirectory(Path directory) {
            Path exactDirectory = Objects.requireNonNull(directory, "directory").toAbsolutePath().normalize();
            return (sequence, checkpointBytes) -> {
                if (!Files.isDirectory(exactDirectory, LinkOption.NOFOLLOW_LINKS)) {
                    throw new IllegalArgumentException("allocator V3 checkpoint directory is absent or a link");
                }
                String digest = AllocatorCampaignCheckpointV3.digest(checkpointBytes).toHex();
                Path output = exactDirectory.resolve(String.format(
                        Locale.ROOT, "checkpoint-%020d-%s.nacp", sequence, digest));
                Files.write(
                        output,
                        checkpointBytes.toByteArray(),
                        StandardOpenOption.CREATE_NEW,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.DSYNC);
            };
        }
    }

    record ActionResult(
            Observation observation,
            Sha256Digest attachmentDigest,
            boolean infrastructureValid,
            String infrastructureDetail) {
        ActionResult {
            Objects.requireNonNull(observation, "observation");
            Objects.requireNonNull(attachmentDigest, "attachmentDigest");
            Objects.requireNonNull(infrastructureDetail, "infrastructureDetail");
            if (!infrastructureValid && infrastructureDetail.isBlank()) {
                throw new IllegalArgumentException("allocator V3 infrastructure failure detail is absent");
            }
        }
    }

    record PhysicalActionResult(
            PlannedActionV3 action, Object rawEvidence, Sha256Digest attachmentDigest, boolean infrastructureValid) {
        PhysicalActionResult {
            Objects.requireNonNull(action, "action");
            Objects.requireNonNull(rawEvidence, "rawEvidence");
            Objects.requireNonNull(attachmentDigest, "attachmentDigest");
        }
    }

    record BudgetCharge(
            long setupSeconds,
            long populationSeconds,
            long faultSeconds,
            long scaleSeconds,
            long intervalSeconds,
            long cleanupSeconds,
            long checkpointAndSealSeconds) {
        BudgetCharge {
            if (setupSeconds < 0
                    || populationSeconds < 0
                    || faultSeconds < 0
                    || scaleSeconds < 0
                    || intervalSeconds < 0
                    || cleanupSeconds < 0
                    || checkpointAndSealSeconds < 0) {
                throw new IllegalArgumentException("allocator V3 action budget charge cannot be negative");
            }
        }

        static BudgetCharge none() {
            return new BudgetCharge(0, 0, 0, 0, 0, 0, 0);
        }

        boolean fits(RemainingBudgets remaining) {
            Objects.requireNonNull(remaining, "remaining");
            return setupSeconds <= remaining.setupSeconds()
                    && populationSeconds <= remaining.populationSeconds()
                    && faultSeconds <= remaining.faultSeconds()
                    && scaleSeconds <= remaining.scaleSeconds()
                    && intervalSeconds <= remaining.intervalSeconds()
                    && cleanupSeconds <= remaining.cleanupSeconds()
                    && checkpointAndSealSeconds <= remaining.checkpointAndSealSeconds();
        }

        RemainingBudgets subtractFrom(RemainingBudgets remaining) {
            if (!fits(remaining)) {
                throw new IllegalArgumentException("allocator V3 action exceeds an independent remaining phase budget");
            }
            return new RemainingBudgets(
                    Math.subtractExact(remaining.setupSeconds(), setupSeconds),
                    Math.subtractExact(remaining.populationSeconds(), populationSeconds),
                    Math.subtractExact(remaining.faultSeconds(), faultSeconds),
                    Math.subtractExact(remaining.scaleSeconds(), scaleSeconds),
                    Math.subtractExact(remaining.intervalSeconds(), intervalSeconds),
                    Math.subtractExact(remaining.cleanupSeconds(), cleanupSeconds),
                    Math.subtractExact(remaining.checkpointAndSealSeconds(), checkpointAndSealSeconds));
        }
    }

    enum TerminalReason {
        COMPLETED,
        STOP_REQUESTED,
        BUDGET_EXHAUSTED,
        BUDGET_ACCOUNTING_FAILED,
        ACTION_INTERRUPTED,
        ACTION_CAP_EXCEEDED,
        WALL_CLOCK_CAP_EXCEEDED,
        INFRASTRUCTURE_FAILED,
        INVALID_ACTION_RESULT
    }

    record CampaignResult(
            CanonicalBytes checkpointBytes, Status status, TerminalReason reason, String detail) {
        CampaignResult {
            Objects.requireNonNull(checkpointBytes, "checkpointBytes");
            Objects.requireNonNull(status, "status");
            Objects.requireNonNull(reason, "reason");
            Objects.requireNonNull(detail, "detail");
            if (AllocatorCampaignCheckpointV3.decode(checkpointBytes).status() != status) {
                throw new IllegalArgumentException("allocator V3 campaign result status differs from checkpoint");
            }
        }

        boolean completed() {
            return status == Status.COMPLETED && reason == TerminalReason.COMPLETED;
        }
    }
}
