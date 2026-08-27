/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.domain.registry.allocator;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Pure offline ADR-0108 structural feasibility gate. Its optimistic bound is not a throughput promise. */
public final class AllocatorCampaignFeasibilityV3 {
    public static final String PROTOCOL_VERSION = "ADR-0108-NACP3";
    public static final AdmissionTuple FORMAL_ADMISSION = new AdmissionTuple(
            AllocatorEvidenceAdmissionPolicyV3.ACTOR_COUNT,
            AllocatorEvidenceAdmissionPolicyV3.MAX_ASYNC_OUTSTANDING_PER_ACTOR,
            AllocatorEvidenceAdmissionPolicyV3.MAX_GLOBAL_OUTSTANDING,
            AllocatorEvidenceAdmissionPolicyV3.MAX_ROLLOVER_OUTSTANDING_PER_BINDING);
    public static final int FAULT_ACTIONS_MAX = 360;
    public static final int SCALE_ACTIONS_MAX = 32;
    public static final int TOTAL_ACTIONS_MAX = 720;
    public static final long HARD_CAP_SECONDS = 48_000;
    public static final PhaseBudgets PHASE_BUDGETS = new PhaseBudgets(900, 5_400, 7_200, 5_400, 13_120, 1_640, 600);
    public static final List<Integer> PROOF_LATENCIES_MILLIS = List.of(1, 5, 10, 25, 250);

    private AllocatorCampaignFeasibilityV3() {}

    public static Result evaluate(AdmissionTuple admission) {
        Objects.requireNonNull(admission, "admission");
        List<StructuralBound> bounds = new ArrayList<>();
        boolean frozenMatrixFeasible = true;
        for (int latency : PROOF_LATENCIES_MILLIS) {
            long capacity = optimisticRequestsPerSecond(admission, latency);
            for (int rate : AllocatorCampaignV3.DESCENDING_FIXED_RATES) {
                boolean feasible = capacity >= rate;
                bounds.add(new StructuralBound(latency, rate, capacity, feasible));
                if (latency <= 25 && !feasible) {
                    frozenMatrixFeasible = false;
                }
            }
        }
        Map<Integer, Integer> derivedFloors = new LinkedHashMap<>();
        for (int nativeRate : AllocatorCampaignV3.DESCENDING_FIXED_RATES) {
            derivedFloors.put(nativeRate, AllocatorCampaignV3.derivedRate(nativeRate));
        }
        boolean completionProof = optimisticRequestsPerSecond(admission, 250) >= 1_000;
        Status status = frozenMatrixFeasible && completionProof ? Status.PLAN_FEASIBLE : Status.PLAN_INFEASIBLE;
        return new Result(
                PROTOCOL_VERSION,
                admission,
                bounds,
                Map.copyOf(derivedFloors),
                AllocatorCampaignV3.LOGICAL_PERFORMANCE_CELLS,
                AllocatorCampaignV3.EXECUTED_PERFORMANCE_CELLS_MIN,
                AllocatorCampaignV3.EXECUTED_PERFORMANCE_CELLS_MIN_PROMOTABLE,
                AllocatorCampaignV3.EXECUTED_PERFORMANCE_CELLS_MAX,
                FAULT_ACTIONS_MAX,
                SCALE_ACTIONS_MAX,
                TOTAL_ACTIONS_MAX,
                PHASE_BUDGETS,
                HARD_CAP_SECONDS,
                status);
    }

    public static Result requireFormalFeasible() {
        Result result = evaluate(FORMAL_ADMISSION);
        if (result.status() != Status.PLAN_FEASIBLE) {
            throw new IllegalStateException("allocator V3 formal admission is structurally infeasible");
        }
        return result;
    }

    static long optimisticRequestsPerSecond(AdmissionTuple admission, int latencyMillis) {
        if (latencyMillis <= 0) {
            throw new IllegalArgumentException("allocator V3 structural latency must be positive");
        }
        long actorCapacity = Math.multiplyExact((long) admission.actorCount(), admission.maxOutstandingPerActor());
        long outstanding = Math.min(actorCapacity, admission.maxGlobalOutstanding());
        return Math.multiplyExact(outstanding, 1_000L) / latencyMillis;
    }

    public enum Status {
        PLAN_FEASIBLE,
        PLAN_INFEASIBLE
    }

    public record AdmissionTuple(
            int actorCount,
            int maxOutstandingPerActor,
            int maxGlobalOutstanding,
            int maxRolloverOutstandingPerBinding) {
        public AdmissionTuple {
            if (actorCount <= 0
                    || maxOutstandingPerActor <= 0
                    || maxGlobalOutstanding <= 0
                    || maxRolloverOutstandingPerBinding <= 0
                    || maxGlobalOutstanding > Math.multiplyExact(actorCount, maxOutstandingPerActor)) {
                throw new IllegalArgumentException("allocator V3 admission tuple is invalid");
            }
        }
    }

    public record StructuralBound(
            int latencyMillis, int offeredRate, long optimisticRequestsPerSecond, boolean structurallyFeasible) {
        public StructuralBound {
            if (latencyMillis <= 0 || offeredRate <= 0 || optimisticRequestsPerSecond < 0) {
                throw new IllegalArgumentException("allocator V3 structural bound is invalid");
            }
        }
    }

    public record PhaseBudgets(
            long setupSeconds,
            long populationSeconds,
            long faultSeconds,
            long scaleSeconds,
            long intervalSeconds,
            long cleanupSeconds,
            long checkpointSeconds) {
        public PhaseBudgets {
            if (setupSeconds < 0
                    || populationSeconds < 0
                    || faultSeconds < 0
                    || scaleSeconds < 0
                    || intervalSeconds < 0
                    || cleanupSeconds < 0
                    || checkpointSeconds < 0) {
                throw new IllegalArgumentException("allocator V3 phase budget cannot be negative");
            }
        }

        public long totalSeconds() {
            long setupAndPopulation = Math.addExact(setupSeconds, populationSeconds);
            long faultAndScale = Math.addExact(faultSeconds, scaleSeconds);
            return Math.addExact(
                    Math.addExact(setupAndPopulation, faultAndScale),
                    Math.addExact(Math.addExact(intervalSeconds, cleanupSeconds), checkpointSeconds));
        }
    }

    public record Result(
            String protocolVersion,
            AdmissionTuple admission,
            List<StructuralBound> structuralBounds,
            Map<Integer, Integer> derivedFloors,
            int logicalCells,
            int minimumExecutedCells,
            int minimumPromotableExecutedCells,
            int maximumExecutedCells,
            int maximumFaultActions,
            int maximumScaleActions,
            int maximumTotalActions,
            PhaseBudgets phaseBudgets,
            long hardCapSeconds,
            Status status) {
        public Result {
            Objects.requireNonNull(protocolVersion, "protocolVersion");
            Objects.requireNonNull(admission, "admission");
            structuralBounds = List.copyOf(structuralBounds);
            derivedFloors = Map.copyOf(derivedFloors);
            Objects.requireNonNull(phaseBudgets, "phaseBudgets");
            Objects.requireNonNull(status, "status");
            if (logicalCells != 328
                    || minimumExecutedCells != 13
                    || minimumPromotableExecutedCells != 17
                    || maximumExecutedCells != 328
                    || maximumFaultActions != 360
                    || maximumScaleActions != 32
                    || maximumTotalActions != 720
                    || phaseBudgets.totalSeconds() > hardCapSeconds) {
                throw new IllegalArgumentException("allocator V3 feasibility accounting differs from ADR 0108");
            }
        }
    }
}
