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
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV2;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV2.ExecutionRecord;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV2.RemainingBudgets;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV2.SourceBinding;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV2.Status;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPlannerV2;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.Campaign;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.Observation;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.Plan;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.RequiredAction;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignValidatorV2;
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
 * Bounded ADR-0104 campaign orchestration. The deterministic planner is the only source of required actions and
 * dispositions. This class persists a canonical checkpoint before execution and after every action or terminal stop;
 * it never seals an evaluation or makes a promotion decision.
 */
final class M3V2AdaptiveCampaignExecutor {
    private static final int MAX_ACTIONS = 328;

    private final SourceBinding source;
    private final RemainingBudgets initialBudgets;
    private final ActionExecutor actions;
    private final CheckpointSink checkpoints;
    private final StopSignal stopSignal;

    M3V2AdaptiveCampaignExecutor(
            SourceBinding source,
            RemainingBudgets initialBudgets,
            ActionExecutor actions,
            CheckpointSink checkpoints,
            StopSignal stopSignal) {
        this.source = Objects.requireNonNull(source, "source");
        this.initialBudgets = Objects.requireNonNull(initialBudgets, "initialBudgets");
        this.actions = Objects.requireNonNull(actions, "actions");
        this.checkpoints = Objects.requireNonNull(checkpoints, "checkpoints");
        this.stopSignal = Objects.requireNonNull(stopSignal, "stopSignal");
    }

    CampaignResult start() throws IOException {
        Plan plan = AllocatorCampaignPlannerV2.plan(List.of());
        AllocatorCampaignCheckpointV2 initial = AllocatorCampaignCheckpointV2.initial(
                source, initialBudgets, List.of(), plan.dispositions(), Status.RUNNING);
        CanonicalBytes initialBytes = AllocatorCampaignCheckpointV2.encode(initial);
        persist(initialBytes);
        return drive(initialBytes);
    }

    CampaignResult resume(CanonicalBytes predecessorBytes) throws IOException {
        AllocatorCampaignCheckpointV2 predecessor =
                AllocatorCampaignCheckpointV2.decode(Objects.requireNonNull(predecessorBytes, "predecessorBytes"));
        if (!predecessor.source().equals(source)) {
            throw new IllegalArgumentException("allocator V2 resume source/executor tuple differs");
        }
        if (predecessor.status() != Status.RUNNING && predecessor.status() != Status.INTERRUPTED) {
            throw new IllegalArgumentException("allocator V2 checkpoint status cannot resume");
        }
        return drive(predecessorBytes);
    }

    private CampaignResult drive(CanonicalBytes startingBytes) throws IOException {
        CanonicalBytes currentBytes = startingBytes;
        AllocatorCampaignCheckpointV2 current = AllocatorCampaignCheckpointV2.decode(currentBytes);
        while (true) {
            Plan currentPlan = AllocatorCampaignValidatorV2.validate(current.campaign());
            if (currentPlan.completed()) {
                throw new IllegalStateException("allocator V2 resumable checkpoint is already complete");
            }
            if (current.executionRecords().size() >= MAX_ACTIONS) {
                return transition(
                        currentBytes,
                        current,
                        current.remainingBudgets(),
                        Status.INFRASTRUCTURE_FAILED,
                        TerminalReason.ACTION_CAP_EXCEEDED,
                        "campaign reached the frozen 328-action observation cap before validator completion");
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
            BudgetCharge charge;
            try {
                charge = Objects.requireNonNull(actions.budgetFor(action), "allocator V2 action budget");
            } catch (RuntimeException failure) {
                return transition(
                        currentBytes,
                        current,
                        current.remainingBudgets(),
                        Status.INFRASTRUCTURE_FAILED,
                        TerminalReason.BUDGET_ACCOUNTING_FAILED,
                        failure.getClass().getSimpleName() + ": " + Objects.toString(failure.getMessage(), ""));
            }
            if (!charge.fits(current.remainingBudgets())) {
                if (current.status() == Status.INTERRUPTED) {
                    return result(
                            currentBytes,
                            current,
                            TerminalReason.BUDGET_EXHAUSTED,
                            "remaining independent phase budget cannot admit the next action");
                }
                return transition(
                        currentBytes,
                        current,
                        current.remainingBudgets(),
                        Status.INTERRUPTED,
                        TerminalReason.BUDGET_EXHAUSTED,
                        "remaining independent phase budget cannot admit the next action");
            }
            RemainingBudgets afterCharge = charge.subtractFrom(current.remainingBudgets());
            ActionResult actionResult;
            try {
                actionResult = Objects.requireNonNull(actions.execute(action), "allocator V2 action result");
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
            } catch (Exception failure) {
                return transition(
                        currentBytes,
                        current,
                        afterCharge,
                        Status.INFRASTRUCTURE_FAILED,
                        TerminalReason.INFRASTRUCTURE_FAILED,
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
                nextPlan = AllocatorCampaignPlannerV2.plan(observations);
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
            AllocatorCampaignCheckpointV2 next;
            CanonicalBytes nextBytes;
            try {
                next = AllocatorCampaignCheckpointV2.resume(
                        currentBytes,
                        source,
                        afterCharge,
                        nextRecords,
                        nextPlan.dispositions(),
                        nextStatus);
                nextBytes = AllocatorCampaignCheckpointV2.encode(next);
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
                        "action returned infrastructure-invalid raw evidence");
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
            AllocatorCampaignCheckpointV2 predecessor,
            RemainingBudgets remainingBudgets,
            Status nextStatus,
            TerminalReason reason,
            String detail)
            throws IOException {
        Plan plan = AllocatorCampaignValidatorV2.validate(predecessor.campaign());
        AllocatorCampaignCheckpointV2 terminal = AllocatorCampaignCheckpointV2.resume(
                predecessorBytes,
                source,
                remainingBudgets,
                predecessor.executionRecords(),
                plan.dispositions(),
                nextStatus);
        CanonicalBytes terminalBytes = AllocatorCampaignCheckpointV2.encode(terminal);
        persist(terminalBytes);
        return result(terminalBytes, terminal, reason, detail);
    }

    private CampaignResult result(
            CanonicalBytes checkpointBytes,
            AllocatorCampaignCheckpointV2 checkpoint,
            TerminalReason reason,
            String detail) {
        return new CampaignResult(checkpointBytes, checkpoint.status(), reason, detail);
    }

    private void persist(CanonicalBytes checkpointBytes) throws IOException {
        AllocatorCampaignCheckpointV2 checkpoint = AllocatorCampaignCheckpointV2.decode(checkpointBytes);
        checkpoints.persist(checkpoint.checkpointSequence(), checkpointBytes);
    }

    @FunctionalInterface
    interface StopSignal {
        boolean stopRequested();

        static StopSignal never() {
            return () -> false;
        }
    }

    interface ActionExecutor {
        BudgetCharge budgetFor(RequiredAction action);

        ActionResult execute(RequiredAction action) throws Exception;
    }

    @FunctionalInterface
    interface CheckpointSink {
        void persist(long sequence, CanonicalBytes checkpointBytes) throws IOException;

        static CheckpointSink createNewDirectory(Path directory) {
            Path exactDirectory = Objects.requireNonNull(directory, "directory").toAbsolutePath().normalize();
            return (sequence, checkpointBytes) -> {
                if (!Files.isDirectory(exactDirectory, LinkOption.NOFOLLOW_LINKS)) {
                    throw new IllegalArgumentException("allocator V2 checkpoint directory is absent or a link");
                }
                String digest = AllocatorCampaignCheckpointV2.digest(checkpointBytes).toHex();
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

    record ActionResult(Observation observation, Sha256Digest attachmentDigest, boolean infrastructureValid) {
        ActionResult {
            Objects.requireNonNull(observation, "observation");
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
                throw new IllegalArgumentException("allocator V2 action budget charge cannot be negative");
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
                throw new IllegalArgumentException("allocator V2 action exceeds an independent remaining phase budget");
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
            if (AllocatorCampaignCheckpointV2.decode(checkpointBytes).status() != status) {
                throw new IllegalArgumentException("allocator V2 campaign result status differs from checkpoint");
            }
        }

        boolean completed() {
            return status == Status.COMPLETED && reason == TerminalReason.COMPLETED;
        }
    }
}
