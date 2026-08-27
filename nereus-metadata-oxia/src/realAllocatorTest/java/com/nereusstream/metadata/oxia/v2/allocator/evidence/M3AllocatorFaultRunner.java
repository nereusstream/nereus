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

import com.nereusstream.domain.registry.allocator.AllocatorEvidenceContextV1;
import com.nereusstream.domain.registry.allocator.AllocatorFaultCutV1;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionService;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReferenceArray;

/** Executes every ADR-0094 cut against a full candidate population and four real Oxia sessions. */
final class M3AllocatorFaultRunner {
    private static final int CRASHED_ACTOR = 0;
    private static final int STORM_CONCURRENCY = M3AllocatorWorkloadPlan.WORKER_THREADS;
    private static final long FAULT_BATCH_DRAIN_TIMEOUT_SECONDS = 120;
    private static final long MASS_TAKEOVER_POST_DEADLINE_DRAIN_SECONDS = 120;

    private final ExecutorService workers;
    private final M3RealOxiaActors actors;
    private final M3AllocatorRequestTelemetry telemetry;

    M3AllocatorFaultRunner(
            ExecutorService workers, M3RealOxiaActors actors, M3AllocatorRequestTelemetry telemetry) {
        this.workers = workers;
        this.actors = actors;
        this.telemetry = telemetry;
    }

    void runAll(
            AllocatorEvidenceContextV1 context,
            M3CandidateAllocatorPopulation population) throws Exception {
        if (context.nativePath()
                || context.offeredRolloverRequestsPerSecond() != 200
                || context.activeManagedLedgers() < 10_000) {
            throw new IllegalArgumentException("allocator fault context is not the exact 200-RPS candidate row");
        }
        for (AllocatorFaultCutV1 cut : AllocatorFaultCutV1.values()) {
            runOne(context, population, cut);
        }
    }

    void runOne(
            AllocatorEvidenceContextV1 context,
            M3CandidateAllocatorPopulation population,
            AllocatorFaultCutV1 cut) throws Exception {
        if (context.nativePath()
                || context.offeredRolloverRequestsPerSecond() != 200
                || context.activeManagedLedgers() < 10_000) {
            throw new IllegalArgumentException("allocator fault context is not the exact 200-RPS candidate row");
        }
        switch (Objects.requireNonNull(cut, "cut")) {
            case RESERVE_RESPONSE_LOSS -> responseLoss(
                    context, population, cut, M3CandidateAllocatorPopulation.ResponseLossAt.RESERVE);
            case MODE_GRANT_READY_RESPONSE_LOSS_OR_STRICT_NO_INSTALL -> responseLoss(
                    context,
                    population,
                    cut,
                    population.candidate().mode()
                                    == com.nereusstream.domain.registry.allocator.AllocatorModeV1.RANGE_LEASED
                            ? M3CandidateAllocatorPopulation.ResponseLossAt.RANGE_GRANT_INSTALL
                            : M3CandidateAllocatorPopulation.ResponseLossAt.NONE);
            case NODE_CREATE_RESPONSE_LOSS -> responseLoss(
                    context, population, cut, M3CandidateAllocatorPopulation.ResponseLossAt.NODE_CREATE);
            case HEAD_PUBLISH_RESPONSE_LOSS -> responseLoss(
                    context, population, cut, M3CandidateAllocatorPopulation.ResponseLossAt.HEAD_PUBLISH);
            case CELL_CLEAR_RESPONSE_LOSS -> responseLoss(
                    context, population, cut, M3CandidateAllocatorPopulation.ResponseLossAt.CELL_CLEAR);
            case SINGLE_OWNER_TAKEOVER -> singleOwnerTakeover(context, population, cut);
            case LATE_OLD_OWNER_WRITE -> lateOldOwner(context, population, cut);
            case BROKER_SESSION_CRASH_MASS_TAKEOVER -> brokerCrashMassTakeover(context, population, cut);
            case SYNCHRONIZED_STORM -> synchronizedStorm(context, population, cut);
        }
    }

    private void responseLoss(
            AllocatorEvidenceContextV1 context,
            M3CandidateAllocatorPopulation population,
            AllocatorFaultCutV1 cut,
            M3CandidateAllocatorPopulation.ResponseLossAt lossAt) {
        M3AllocatorRequestTelemetry.RequestTrace trace = trace(context, cut, faultLedger(context, cut));
        begin(trace);
        population.rollover(trace, faultLedger(context, cut), lossAt);
        trace.completed();
        trace.cutEnd();
    }

    private void singleOwnerTakeover(
            AllocatorEvidenceContextV1 context,
            M3CandidateAllocatorPopulation population,
            AllocatorFaultCutV1 cut) {
        int ledger = faultLedger(context, cut);
        M3AllocatorRequestTelemetry.RequestTrace trace = trace(context, cut, ledger);
        begin(trace);
        population.rolloverWithSingleOwnerTakeover(trace, ledger);
        trace.completed();
        trace.cutEnd();
    }

    private void lateOldOwner(
            AllocatorEvidenceContextV1 context,
            M3CandidateAllocatorPopulation population,
            AllocatorFaultCutV1 cut) {
        int ledger = faultLedger(context, cut);
        M3AllocatorRequestTelemetry.RequestTrace trace = trace(context, cut, ledger);
        begin(trace);
        population.lateOldOwnerWriteAndRecover(trace, ledger);
        trace.cutEnd();
    }

    private void brokerCrashMassTakeover(
            AllocatorEvidenceContextV1 context,
            M3CandidateAllocatorPopulation population,
            AllocatorFaultCutV1 cut) throws Exception {
        int inFlightLedger = context.activeManagedLedgers() - 64;
        M3AllocatorRequestTelemetry.RequestTrace cutTrace = trace(context, cut, inFlightLedger);
        begin(cutTrace);
        M3RealOxiaActors.Actor actor = actors.actor(CRASHED_ACTOR);
        M3RealOxiaActors.CrashBarrier barrier = actor.armCrashAfterNextMutationApplied();
        Future<?> inFlight = workers.submit(() -> population.rollover(
                cutTrace,
                inFlightLedger,
                M3CandidateAllocatorPopulation.ResponseLossAt.NONE));
        barrier.awaitApplied();
        cutTrace.ownerLossDetected();
        long recoveryDeadlineNanos = System.nanoTime()
                + TimeUnit.MICROSECONDS.toNanos(
                        AllocatorEvidenceContextV1.massTakeoverRecoveryBoundMicros(
                                context.activeManagedLedgers()));
        long cleanupDeadlineNanos = recoveryDeadlineNanos
                + TimeUnit.SECONDS.toNanos(MASS_TAKEOVER_POST_DEADLINE_DRAIN_SECONDS);
        actor.closeSessionWithWorkInFlight();
        actor.reopenFreshSession();
        barrier.releaseResponse();
        inFlight.get(30, TimeUnit.SECONDS);
        cutTrace.completed();

        List<Integer> affected = population.ledgersOwnedByActor(
                CRASHED_ACTOR, context.activeManagedLedgers());
        AtomicReferenceArray<M3AllocatorRequestTelemetry.RequestTrace> recoveryTraces =
                new AtomicReferenceArray<>(context.activeManagedLedgers());
        AtomicLongArray recoveryOwnerEpochs = new AtomicLongArray(context.activeManagedLedgers());
        for (int ledgerIndex : affected) {
            M3AllocatorRequestTelemetry.RequestTrace trace = massTrace(context, cut, ledgerIndex);
            trace.offered();
            trace.enqueued();
            trace.dispatched();
            recoveryTraces.set(ledgerIndex, trace);
            recoveryOwnerEpochs.set(ledgerIndex, population.nextOwnerEpoch(ledgerIndex));
        }
        parallelUntil(
                affected,
                ledgerIndex -> population.takeover(
                        recoveryTraces.get(ledgerIndex),
                        ledgerIndex,
                        recoveryOwnerEpochs.get(ledgerIndex)),
                cleanupDeadlineNanos,
                "allocator mass-takeover Head phase");
        parallelUntil(
                affected,
                ledgerIndex -> {
                    M3AllocatorRequestTelemetry.RequestTrace trace = recoveryTraces.get(ledgerIndex);
                    population.admitAppendUnderFreshOwner(
                            trace, ledgerIndex, recoveryOwnerEpochs.get(ledgerIndex));
                    trace.freshOwnerRecoveryComplete(System.nanoTime() <= recoveryDeadlineNanos);
                },
                cleanupDeadlineNanos,
                "allocator mass-takeover append-admission phase");
        cutTrace.cutEnd();
    }

    private void synchronizedStorm(
            AllocatorEvidenceContextV1 context,
            M3CandidateAllocatorPopulation population,
            AllocatorFaultCutV1 cut) throws Exception {
        CyclicBarrier start = new CyclicBarrier(STORM_CONCURRENCY);
        CompletionService<Void> completions = new ExecutorCompletionService<>(workers);
        M3AllocatorRequestTelemetry.RequestTrace cutTrace = trace(context, cut, faultLedger(context, cut));
        cutTrace.cutBegin();
        for (int index = 0; index < STORM_CONCURRENCY; index++) {
            int exactIndex = index;
            completions.submit(() -> {
                int ledger = Math.floorMod(exactIndex * 997, context.activeManagedLedgers());
                M3AllocatorRequestTelemetry.RequestTrace trace = stormTrace(context, cut, exactIndex, ledger);
                trace.offered();
                trace.enqueued();
                start.await(30, TimeUnit.SECONDS);
                trace.dispatched();
                population.rollover(trace, ledger, M3CandidateAllocatorPopulation.ResponseLossAt.NONE);
                trace.completed();
                return null;
            });
        }
        await(completions, STORM_CONCURRENCY);
        cutTrace.cutEnd();
    }

    private void parallel(List<Integer> indices, IndexedOperation operation) throws Exception {
        parallel(indices, operation, FAULT_BATCH_DRAIN_TIMEOUT_SECONDS, "allocator fault batch");
    }

    private void parallelUntil(
            List<Integer> indices,
            IndexedOperation operation,
            long absoluteDeadlineNanos,
            String label) throws Exception {
        long remainingNanos = absoluteDeadlineNanos - System.nanoTime();
        long remainingSeconds = Math.max(1, TimeUnit.NANOSECONDS.toSeconds(remainingNanos) + 1);
        parallel(indices, operation, remainingSeconds, label);
    }

    private void parallel(
            List<Integer> indices,
            IndexedOperation operation,
            long timeoutSeconds,
            String label) throws Exception {
        CompletionService<Void> completions = new ExecutorCompletionService<>(workers);
        for (int index : indices) {
            completions.submit(() -> {
                operation.run(index);
                return null;
            });
        }
        await(completions, indices.size(), timeoutSeconds, label);
    }

    private static void await(CompletionService<Void> completions, int count) throws Exception {
        await(completions, count, FAULT_BATCH_DRAIN_TIMEOUT_SECONDS, "allocator fault batch");
    }

    private static void await(
            CompletionService<Void> completions,
            int count,
            long timeoutSeconds,
            String label) throws Exception {
        M3BoundedCompletionDrain.await(
                completions,
                count,
                timeoutSeconds,
                label);
    }

    private M3AllocatorRequestTelemetry.RequestTrace trace(
            AllocatorEvidenceContextV1 context, AllocatorFaultCutV1 cut, int ledger) {
        return telemetry.trace(
                context,
                planned((long) cut.code() * 4, CRASHED_ACTOR, ledger),
                cut,
                1);
    }

    private M3AllocatorRequestTelemetry.RequestTrace massTrace(
            AllocatorEvidenceContextV1 context, AllocatorFaultCutV1 cut, int ledger) {
        int freshActor = 1 + Math.floorMod(ledger / 4, M3AllocatorWorkloadPlan.BROKER_ACTORS - 1);
        return telemetry.trace(
                context,
                planned(1_000_000L + ledger * 4L, freshActor, ledger),
                cut,
                1);
    }

    private M3AllocatorRequestTelemetry.RequestTrace stormTrace(
            AllocatorEvidenceContextV1 context, AllocatorFaultCutV1 cut, int ordinal, int ledger) {
        return telemetry.trace(
                context,
                planned(2_000_000L + ordinal, ordinal & 3, ledger),
                cut,
                1);
    }

    private static M3AllocatorWorkloadPlan.PlannedRequest planned(
            long ordinal, int actorId, int ledger) {
        M3AllocatorWorkloadPlan.Trigger trigger = switch ((int) (ordinal % 10)) {
            case 0, 1, 2, 3, 4 -> M3AllocatorWorkloadPlan.Trigger.ENTRY;
            case 5, 6, 7 -> M3AllocatorWorkloadPlan.Trigger.BYTE;
            default -> M3AllocatorWorkloadPlan.Trigger.AGE;
        };
        return new M3AllocatorWorkloadPlan.PlannedRequest(
                ordinal,
                actorId,
                ledger,
                trigger,
                M3AllocatorWorkloadPlan.Phase.MEASURED_STEADY,
                0);
    }

    private static int faultLedger(
            AllocatorEvidenceContextV1 context, AllocatorFaultCutV1 cut) {
        return context.activeManagedLedgers() - 64 + cut.code();
    }

    private static void begin(M3AllocatorRequestTelemetry.RequestTrace trace) {
        trace.cutBegin();
        trace.offered();
        trace.enqueued();
        trace.dispatched();
    }

    @FunctionalInterface
    private interface IndexedOperation {
        void run(int index) throws Exception;
    }
}
