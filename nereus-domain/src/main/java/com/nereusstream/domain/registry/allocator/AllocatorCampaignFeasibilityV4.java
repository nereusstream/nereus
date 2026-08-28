/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.domain.registry.allocator;

import java.util.List;
import java.util.Objects;

/** Pure offline ADR-0125 feasibility gate. It does not promise allocator throughput. */
public final class AllocatorCampaignFeasibilityV4 {
    public static final String PROTOCOL_VERSION = "ADR-0125-NACP4";
    public static final TerminalBoundary FORMAL_BOUNDARY = new TerminalBoundary(
            AllocatorEvidenceAdmissionPolicyV4.OFFER_HORIZON_SECONDS,
            AllocatorEvidenceAdmissionPolicyV4.TERMINAL_ADMISSION_DRAIN_SECONDS,
            AllocatorEvidenceAdmissionPolicyV4.CLEANUP_GRACE_SECONDS);
    public static final AllocatorCampaignFeasibilityV3.PhaseBudgets PHASE_BUDGETS =
            new AllocatorCampaignFeasibilityV3.PhaseBudgets(900, 5_400, 7_200, 5_400, 13_776, 1_640, 600);
    public static final long HARD_CAP_SECONDS = 48_000;
    public static final List<TailCollision> REQUIRED_TAIL_COLLISIONS = List.of(
            new TailCollision(800, 31_960, 9_730, 23_875, 25_000),
            new TailCollision(1_000, 39_943, 1_269, 9_750, 28_500));

    private AllocatorCampaignFeasibilityV4() {}

    public static Result evaluate(TerminalBoundary boundary) {
        Objects.requireNonNull(boundary, "boundary");
        AllocatorCampaignFeasibilityV3.Result structural = AllocatorCampaignFeasibilityV3.requireFormalFeasible();
        Status status = boundary.offerHorizonSeconds() == FORMAL_BOUNDARY.offerHorizonSeconds()
                        && boundary.terminalAdmissionDrainSeconds() == FORMAL_BOUNDARY.terminalAdmissionDrainSeconds()
                        && boundary.cleanupGraceSeconds() == FORMAL_BOUNDARY.cleanupGraceSeconds()
                ? Status.PLAN_FEASIBLE
                : Status.TERMINAL_CENSORING_INFEASIBLE;
        return new Result(
                PROTOCOL_VERSION,
                boundary,
                structural,
                AllocatorNativeExecutionProfileV4.scheduleDigest().toHex(),
                AllocatorNativeExecutionProfileV4.executionProfileDigest().toHex(),
                REQUIRED_TAIL_COLLISIONS,
                PHASE_BUDGETS,
                HARD_CAP_SECONDS,
                status);
    }

    public static Result requireFormalFeasible() {
        Result result = evaluate(FORMAL_BOUNDARY);
        if (result.status() != Status.PLAN_FEASIBLE) {
            throw new IllegalStateException("allocator V4 terminal admission boundary is structurally infeasible");
        }
        return result;
    }

    public enum Status {
        PLAN_FEASIBLE,
        TERMINAL_CENSORING_INFEASIBLE
    }

    public record TerminalBoundary(
            int offerHorizonSeconds, int terminalAdmissionDrainSeconds, int cleanupGraceSeconds) {
        public TerminalBoundary {
            if (offerHorizonSeconds <= 0 || terminalAdmissionDrainSeconds < 0 || cleanupGraceSeconds <= 0) {
                throw new IllegalArgumentException("allocator V4 terminal boundary is invalid");
            }
        }
    }

    public record TailCollision(
            int offeredRate,
            long successorOrdinal,
            long bindingOrdinal,
            long predecessorGapMicros,
            long offerToCloseMicros) {
        public TailCollision {
            if (offeredRate <= 0
                    || successorOrdinal < 0
                    || bindingOrdinal < 0
                    || predecessorGapMicros <= 0
                    || offerToCloseMicros <= 0) {
                throw new IllegalArgumentException("allocator V4 terminal tail collision is invalid");
            }
        }
    }

    public record Result(
            String protocolVersion,
            TerminalBoundary terminalBoundary,
            AllocatorCampaignFeasibilityV3.Result structuralFeasibility,
            String scheduleProfileSha256,
            String executionProfileSha256,
            List<TailCollision> requiredTailCollisions,
            AllocatorCampaignFeasibilityV3.PhaseBudgets phaseBudgets,
            long hardCapSeconds,
            Status status) {
        public Result {
            Objects.requireNonNull(protocolVersion, "protocolVersion");
            Objects.requireNonNull(terminalBoundary, "terminalBoundary");
            Objects.requireNonNull(structuralFeasibility, "structuralFeasibility");
            requireDigest(scheduleProfileSha256);
            requireDigest(executionProfileSha256);
            requiredTailCollisions = List.copyOf(requiredTailCollisions);
            Objects.requireNonNull(phaseBudgets, "phaseBudgets");
            Objects.requireNonNull(status, "status");
            if (requiredTailCollisions.size() != 2
                    || phaseBudgets.totalSeconds() != 34_916
                    || phaseBudgets.totalSeconds() > hardCapSeconds
                    || structuralFeasibility.logicalCells() != 328
                    || structuralFeasibility.maximumTotalActions() != 720) {
                throw new IllegalArgumentException("allocator V4 feasibility accounting differs from ADR 0125");
            }
        }

        private static void requireDigest(String value) {
            if (value == null || !value.matches("[0-9a-f]{64}")) {
                throw new IllegalArgumentException("allocator V4 feasibility digest differs");
            }
        }
    }
}
