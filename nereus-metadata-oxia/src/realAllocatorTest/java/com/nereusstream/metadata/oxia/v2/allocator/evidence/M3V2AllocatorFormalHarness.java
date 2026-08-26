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

import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.Cell;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignV2.IntervalEvidence;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignValidatorV2;
import com.nereusstream.metadata.spi.allocator.BoundedVirtualLedgerAllocatorWorkflowV2;
import com.nereusstream.metadata.spi.allocator.BoundedVirtualLedgerAllocatorWorkflowV2.Request;
import java.time.Duration;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

/**
 * V2 performance harness boundary. Candidate actor endpoints are adapters over four distinct instances of the exact
 * production-neutral workflow; the harness adds only bounded local admission and measurement.
 */
final class M3V2AllocatorFormalHarness {
    private final M3V2BoundedActorLaneRunner<Request> candidateRunner;
    private final List<ActorEndpoint> candidateActors;

    private M3V2AllocatorFormalHarness(
            M3V2BoundedActorLaneRunner<Request> candidateRunner, List<ActorEndpoint> candidateActors) {
        this.candidateRunner = Objects.requireNonNull(candidateRunner, "candidateRunner");
        this.candidateActors = requireIndependentActors(candidateActors);
    }

    static M3V2AllocatorFormalHarness formal(List<BoundedVirtualLedgerAllocatorWorkflowV2> workflows) {
        Objects.requireNonNull(workflows, "workflows");
        List<ActorEndpoint> endpoints = new ArrayList<>(workflows.size());
        for (int actorId = 0; actorId < workflows.size(); actorId++) {
            BoundedVirtualLedgerAllocatorWorkflowV2 workflow = Objects.requireNonNull(
                    workflows.get(actorId), "candidate workflow");
            endpoints.add(new ActorEndpoint(actorId, workflow, workflow::allocate));
        }
        return new M3V2AllocatorFormalHarness(M3V2BoundedActorLaneRunner.formal(), endpoints);
    }

    static M3V2AllocatorFormalHarness forContractTest(
            Duration warmup, Duration measurement, Duration cleanupGrace, List<ActorEndpoint> actors) {
        return new M3V2AllocatorFormalHarness(
                new M3V2BoundedActorLaneRunner<>(warmup, measurement, cleanupGrace), actors);
    }

    HarnessResult runCandidate(
            Cell cell,
            List<M3V2BoundedActorLaneRunner.ScheduledOffer<Request>> schedule,
            SupplementaryMeasurements supplementary)
            throws InterruptedException {
        Objects.requireNonNull(cell, "cell");
        if (cell.candidate().nativePath()) {
            throw new IllegalArgumentException("allocator candidate harness cannot execute the native baseline");
        }
        Objects.requireNonNull(supplementary, "supplementary");
        M3V2BoundedActorLaneRunner.IntervalResult interval = candidateRunner.run(
                cell.offeredRolloverRequestsPerSecond(),
                schedule,
                (actorId, request) -> candidateActors.get(actorId).operation().allocate(request));
        IntervalEvidence evidence = new IntervalEvidence(
                cell,
                interval.offered(),
                interval.admitted(),
                interval.overloadDroppedBeforeAdmission(),
                interval.completed(),
                interval.failedAfterAdmission(),
                interval.timedOutAfterAdmission(),
                interval.terminal(),
                supplementary.failedAssertions(),
                supplementary.unexpectedErrors(),
                supplementary.skipped(),
                supplementary.duplicateLedgerIds(),
                supplementary.reusedLedgerIds(),
                interval.rolloverP99Micros(),
                supplementary.oxiaOperationP99Micros(),
                interval.queueAgeP99Micros(),
                interval.backlogMaximum(),
                interval.starvationMaximumMicros(),
                supplementary.appendStallP99Micros(),
                interval.backlogAtEnd(),
                interval.inFlightAtEnd(),
                interval.waiterAtEnd());
        AllocatorCampaignValidatorV2.validateIntervalConservation(evidence);
        boolean infrastructureValid = interval.actorLanesStoppedAtCleanupDeadline()
                && interval.warmupDroppedBeforeAdmission() == 0
                && interval.warmupFailedAfterAdmission() == 0
                && interval.warmupTimedOutAfterAdmission() == 0;
        return new HarnessResult(evidence, interval, infrastructureValid);
    }

    private static List<ActorEndpoint> requireIndependentActors(List<ActorEndpoint> actors) {
        List<ActorEndpoint> exact = List.copyOf(Objects.requireNonNull(actors, "actors"));
        if (exact.size() != M3V2BoundedActorLaneRunner.ACTOR_COUNT) {
            throw new IllegalArgumentException("allocator V2 harness requires exactly four actor coordinators");
        }
        Map<Object, Boolean> identities = new IdentityHashMap<>();
        for (int actorId = 0; actorId < exact.size(); actorId++) {
            ActorEndpoint endpoint = Objects.requireNonNull(exact.get(actorId), "actor endpoint");
            if (endpoint.actorId() != actorId || identities.put(endpoint.coordinatorIdentity(), Boolean.TRUE) != null) {
                throw new IllegalArgumentException(
                        "allocator V2 harness actor IDs or coordinator identities are not independent");
            }
        }
        return exact;
    }

    record ActorEndpoint(int actorId, Object coordinatorIdentity, CandidateOperation operation) {
        ActorEndpoint {
            Objects.requireNonNull(coordinatorIdentity, "coordinatorIdentity");
            Objects.requireNonNull(operation, "operation");
        }
    }

    @FunctionalInterface
    interface CandidateOperation {
        CompletionStage<?> allocate(Request request);
    }

    record SupplementaryMeasurements(
            long failedAssertions,
            long unexpectedErrors,
            long skipped,
            long duplicateLedgerIds,
            long reusedLedgerIds,
            long oxiaOperationP99Micros,
            long appendStallP99Micros) {
        SupplementaryMeasurements {
            if (failedAssertions < 0
                    || unexpectedErrors < 0
                    || skipped < 0
                    || duplicateLedgerIds < 0
                    || reusedLedgerIds < 0
                    || oxiaOperationP99Micros < 0
                    || appendStallP99Micros < 0) {
                throw new IllegalArgumentException("allocator V2 supplementary measurement cannot be negative");
            }
        }

        static SupplementaryMeasurements empty() {
            return new SupplementaryMeasurements(0, 0, 0, 0, 0, 0, 0);
        }
    }

    record HarnessResult(
            IntervalEvidence evidence,
            M3V2BoundedActorLaneRunner.IntervalResult runnerResult,
            boolean infrastructureValid) {
        HarnessResult {
            Objects.requireNonNull(evidence, "evidence");
            Objects.requireNonNull(runnerResult, "runnerResult");
        }
    }
}
