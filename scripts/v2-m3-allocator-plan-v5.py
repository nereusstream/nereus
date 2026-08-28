#!/usr/bin/env python3
"""Pure source-bound plan projection for the ADR-0137 M3 V5 allocator campaign."""

from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parent.parent
V3_PLAN_PATH = ROOT / "scripts" / "v2-m3-allocator-plan-v3.py"
SPEC = importlib.util.spec_from_file_location("nereus_m3_allocator_plan_v3", V3_PLAN_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("allocator V3 plan module cannot be loaded")
V3 = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(V3)

SCHEMA = "NEREUS_V2_M3_ALLOCATOR_CAMPAIGN_PLAN_V5"
EXECUTION_PROFILE_SCHEMA = "NEREUS_V2_M3_ALLOCATOR_NATIVE_EXECUTION_PROFILE_V5"
OFFER_HORIZON_SECONDS = 40
TERMINAL_ADMISSION_DRAIN_SECONDS = 2
CLEANUP_GRACE_SECONDS = 5
INTERVAL_PHASE_BUDGET_SECONDS = 13_776
PHASE_BUDGETS = (900, 5_400, 7_200, 5_400, 13_776, 1_640, 600)


def native_execution_profile_canonical() -> str:
    fields = (
        EXECUTION_PROFILE_SCHEMA,
        f"nativeExecutionModel={V3.NATIVE_EXECUTION_MODEL}",
        f"nativeBridgeWorkers={V3.NATIVE_BRIDGE_WORKERS}",
        f"nativeBridgeQueueCapacity={V3.NATIVE_BRIDGE_QUEUE_CAPACITY}",
        f"hiddenDispatchQueue={V3.HIDDEN_DISPATCH_QUEUE}",
        "actorCount=4",
        "maxOutstandingPerActor=128",
        "maxGlobalOutstanding=512",
        "maxOutstandingPerBinding=1",
        f"offerHorizonSeconds={OFFER_HORIZON_SECONDS}",
        f"terminalAdmissionDrainSeconds={TERMINAL_ADMISSION_DRAIN_SECONDS}",
        f"cleanupGraceSeconds={CLEANUP_GRACE_SECONDS}",
        f"scheduleProfileSha256={V3.workload_schedule_sha256()}",
    )
    return "\n".join(fields) + "\n"


def native_execution_profile_sha256() -> str:
    return hashlib.sha256(native_execution_profile_canonical().encode()).hexdigest()


def zero_decision_plan_sha256() -> str:
    fields = (
        SCHEMA,
        f"logicalPlannerVersion={V3.PLANNER_VERSION}",
        f"logicalIntervalCells={V3.LOGICAL_INTERVAL_CELLS}",
        f"minimumValidEvaluationCells={V3.MINIMUM_VALID_EVALUATION_CELLS}",
        f"minimumPromotableCells={V3.MINIMUM_PROMOTABLE_CELLS}",
        f"maximumExecutedIntervalCells={V3.MAXIMUM_EXECUTED_INTERVAL_CELLS}",
        f"maximumExecutedFaultActions={V3.MAXIMUM_EXECUTED_FAULT_ACTIONS}",
        f"maximumExecutedScaleActions={V3.MAXIMUM_EXECUTED_SCALE_ACTIONS}",
        f"maximumTotalExecutedActions={V3.MAXIMUM_TOTAL_EXECUTED_ACTIONS}",
        f"campaignWallClockCapSeconds={V3.CAMPAIGN_WALL_CLOCK_CAP_SECONDS}",
        f"offerHorizonSeconds={OFFER_HORIZON_SECONDS}",
        f"terminalAdmissionDrainSeconds={TERMINAL_ADMISSION_DRAIN_SECONDS}",
        f"cleanupGraceSeconds={CLEANUP_GRACE_SECONDS}",
        "phaseBudgetSeconds=" + ",".join(str(value) for value in PHASE_BUDGETS),
        f"nativeExecutionProfileSha256={native_execution_profile_sha256()}",
        f"workloadScheduleSha256={V3.workload_schedule_sha256()}",
        *V3.zero_decision_actions(),
    )
    return hashlib.sha256(("\n".join(fields) + "\n").encode()).hexdigest()


def plan() -> dict[str, object]:
    result = V3.plan()
    result.update(
        {
            "schema": SCHEMA,
            "zeroDecisionPlanSha256": zero_decision_plan_sha256(),
            "admission": {
                "actorCount": 4,
                "maxAsyncOutstandingPerActor": 128,
                "maxGlobalOutstanding": 512,
                "maxRolloverOutstandingPerBinding": 1,
                "preAdmissionQueueRateMultiplier": 2,
                "derivedOutstandingPerActor": 125,
            },
            "nativeExecution": {
                "nativeExecutionModel": V3.NATIVE_EXECUTION_MODEL,
                "nativeBridgeWorkers": V3.NATIVE_BRIDGE_WORKERS,
                "nativeBridgeQueueCapacity": V3.NATIVE_BRIDGE_QUEUE_CAPACITY,
                "hiddenDispatchQueue": V3.HIDDEN_DISPATCH_QUEUE,
                "nativeExecutionProfileSha256": native_execution_profile_sha256(),
                "workloadScheduleSha256": V3.workload_schedule_sha256(),
                "offerHorizonSeconds": OFFER_HORIZON_SECONDS,
                "terminalAdmissionDrainSeconds": TERMINAL_ADMISSION_DRAIN_SECONDS,
                "cleanupGraceSeconds": CLEANUP_GRACE_SECONDS,
            },
            "interval": {
                "warmupSeconds": 10,
                "measuredSeconds": 30,
                "offerHorizonSeconds": OFFER_HORIZON_SECONDS,
                "terminalAdmissionDrainSeconds": TERMINAL_ADMISSION_DRAIN_SECONDS,
                "cleanupGraceSeconds": CLEANUP_GRACE_SECONDS,
                "totalBudgetedSeconds": OFFER_HORIZON_SECONDS + TERMINAL_ADMISSION_DRAIN_SECONDS,
                "minimumValidEvaluationSeconds": V3.MINIMUM_VALID_EVALUATION_CELLS * 42,
                "minimumPromotableSeconds": V3.MINIMUM_PROMOTABLE_CELLS * 42,
                "maximumSeconds": INTERVAL_PHASE_BUDGET_SECONDS,
            },
            "independentPhaseBudgetsSeconds": {
                "setup": PHASE_BUDGETS[0],
                "population": PHASE_BUDGETS[1],
                "fault": PHASE_BUDGETS[2],
                "scale": PHASE_BUDGETS[3],
                "interval": PHASE_BUDGETS[4],
                "cleanup": PHASE_BUDGETS[5],
                "checkpointResumeEvaluationSeal": PHASE_BUDGETS[6],
                "sum": sum(PHASE_BUDGETS),
                "hardCapHeadroom": V3.CAMPAIGN_WALL_CLOCK_CAP_SECONDS - sum(PHASE_BUDGETS),
            },
            "optimisticStructuralBounds": {
                str(latency): 512 * 1_000 // latency for latency in (1, 5, 10, 25, 250)
            },
            "stormAdmissionFeasibility": {
                "offeredRate": 2_000,
                "offeredRequests": 20_000,
                "optimisticCompletionMillis": 250,
                "serviceWindowSeconds": 12,
                "v4": {
                    "maxGlobalOutstanding": 256,
                    "optimisticRequestsPerSecond": 1_024,
                    "optimisticServiceThroughRequests": 12_288,
                    "status": "STORM_ADMISSION_INFEASIBLE",
                },
                "v5": {
                    "maxGlobalOutstanding": 512,
                    "optimisticRequestsPerSecond": 2_048,
                    "optimisticServiceThroughRequests": 24_576,
                    "status": "PLAN_FEASIBLE",
                },
            },
            "terminalCensoringFeasibility": {
                "legacySingleCutoff": "TERMINAL_CENSORING_INFEASIBLE",
                "offerCloseSeconds": OFFER_HORIZON_SECONDS,
                "finalAdmissionDeadlineSeconds": (
                    OFFER_HORIZON_SECONDS + TERMINAL_ADMISSION_DRAIN_SECONDS
                ),
                "sameBindingTailGapMicros": [23_875, 3_624, 9_750],
                "status": "PLAN_FEASIBLE",
            },
        }
    )
    return result


def main() -> int:
    if len(sys.argv) != 1:
        print(f"usage: {sys.argv[0]}", file=sys.stderr)
        return 64
    print(json.dumps(plan(), ensure_ascii=True, indent=2, sort_keys=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
