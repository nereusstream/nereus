/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.domain.registry.allocator;

import com.nereusstream.domain.registry.allocator.AllocatorCampaignFeasibilityV3.AdmissionTuple;
import java.util.Objects;

/** Pure offline ADR-0137 storm-aware feasibility gate. It does not promise allocator throughput. */
public final class AllocatorCampaignFeasibilityV5 {
    public static final String PROTOCOL_VERSION = "ADR-0137-NACP5";
    public static final AdmissionTuple FORMAL_ADMISSION = new AdmissionTuple(
            AllocatorEvidenceAdmissionPolicyV5.ACTOR_COUNT,
            AllocatorEvidenceAdmissionPolicyV5.MAX_ASYNC_OUTSTANDING_PER_ACTOR,
            AllocatorEvidenceAdmissionPolicyV5.MAX_GLOBAL_OUTSTANDING,
            AllocatorEvidenceAdmissionPolicyV5.MAX_ROLLOVER_OUTSTANDING_PER_BINDING);
    public static final AdmissionTuple V4_ADMISSION = AllocatorCampaignFeasibilityV3.FORMAL_ADMISSION;
    public static final int STORM_RATE = AllocatorEvidenceAdmissionPolicyV5.MAX_STORM_RATE;
    public static final int COMPLETION_BOUND_MILLIS = AllocatorEvidenceAdmissionPolicyV3.ROLLOVER_P99_MILLIS;
    public static final int STORM_SECONDS = 10;
    public static final int DRAIN_SECONDS = AllocatorEvidenceAdmissionPolicyV5.TERMINAL_ADMISSION_DRAIN_SECONDS;

    private AllocatorCampaignFeasibilityV5() {}

    public static Result evaluate(AdmissionTuple admission) {
        Objects.requireNonNull(admission, "admission");
        AllocatorCampaignFeasibilityV3.Result steady = AllocatorCampaignFeasibilityV3.evaluate(admission);
        long capacity = AllocatorCampaignFeasibilityV3.optimisticRequestsPerSecond(admission, COMPLETION_BOUND_MILLIS);
        long offeredDuringStorm = Math.multiplyExact((long) STORM_RATE, STORM_SECONDS);
        long serviceThroughDrain = Math.multiplyExact(capacity, Math.addExact(STORM_SECONDS, DRAIN_SECONDS));
        boolean instantaneous = capacity >= STORM_RATE;
        boolean drain = serviceThroughDrain >= offeredDuringStorm;
        Status status = steady.status() == AllocatorCampaignFeasibilityV3.Status.PLAN_FEASIBLE && instantaneous && drain
                ? Status.PLAN_FEASIBLE
                : Status.STORM_ADMISSION_INFEASIBLE;
        return new Result(
                PROTOCOL_VERSION,
                admission,
                steady,
                STORM_RATE,
                COMPLETION_BOUND_MILLIS,
                capacity,
                STORM_SECONDS,
                DRAIN_SECONDS,
                offeredDuringStorm,
                serviceThroughDrain,
                instantaneous,
                drain,
                status);
    }

    public static Result requireFormalFeasible() {
        Result result = evaluate(FORMAL_ADMISSION);
        if (result.status() != Status.PLAN_FEASIBLE) {
            throw new IllegalStateException("allocator V5 storm admission is structurally infeasible");
        }
        return result;
    }

    public enum Status {
        PLAN_FEASIBLE,
        STORM_ADMISSION_INFEASIBLE
    }

    public record Result(
            String protocolVersion,
            AdmissionTuple admission,
            AllocatorCampaignFeasibilityV3.Result steadyFeasibility,
            int stormRate,
            int completionBoundMillis,
            long optimisticRequestsPerSecond,
            int stormSeconds,
            int drainSeconds,
            long offeredDuringStorm,
            long serviceThroughDrain,
            boolean instantaneousStormFeasible,
            boolean terminalDrainFeasible,
            Status status) {
        public Result {
            Objects.requireNonNull(protocolVersion, "protocolVersion");
            Objects.requireNonNull(admission, "admission");
            Objects.requireNonNull(steadyFeasibility, "steadyFeasibility");
            Objects.requireNonNull(status, "status");
            if (stormRate != 2_000
                    || completionBoundMillis != 250
                    || stormSeconds != 10
                    || drainSeconds != 2
                    || offeredDuringStorm != 20_000
                    || steadyFeasibility.logicalCells() != 328
                    || steadyFeasibility.maximumTotalActions() != 720) {
                throw new IllegalArgumentException("allocator V5 storm feasibility accounting differs from ADR 0137");
            }
        }
    }
}
