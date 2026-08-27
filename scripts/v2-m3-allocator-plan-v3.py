#!/usr/bin/env python3
"""Pure frozen-plan projection for the M3 V3 bounded adaptive allocator campaign."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys


ROOT = Path(__file__).resolve().parent.parent
SOURCE_LOCKS = ROOT / "docs" / "v2" / "source-locks.json"
DEPENDENCY_LOCK_FILES = (
    ROOT / "gradle" / "libs.versions.toml",
    ROOT / "gradle" / "wrapper" / "gradle-wrapper.properties",
    ROOT
    / "gradle"
    / "locked-artifacts"
    / "oxia-client-java"
    / "091a42c2780d92da56e9ec1f02ce1c3d988adc16"
    / "m2"
    / "io"
    / "github"
    / "oxia-db"
    / "oxia-client"
    / "0.9.4"
    / "oxia-client-0.9.4.jar",
)

CANDIDATES = ("NATIVE", "STRICT", "RANGE_16", "RANGE_64", "RANGE_256", "RANGE_1024")
RANGE_CANDIDATES = CANDIDATES[2:]
POPULATIONS = (10_000, 100_000)
LATENCIES = (1, 5, 10, 25)
DESCENDING_RATES = (1000, 750, 500, 333, 250, 200)
DERIVED_SLOT_ORDINAL = len(DESCENDING_RATES)
FAULT_CUTS = (
    "RESERVE_RESPONSE_LOSS",
    "MODE_GRANT_READY_RESPONSE_LOSS_OR_STRICT_NO_INSTALL",
    "NODE_CREATE_RESPONSE_LOSS",
    "HEAD_PUBLISH_RESPONSE_LOSS",
    "CELL_CLEAR_RESPONSE_LOSS",
    "SINGLE_OWNER_TAKEOVER",
    "LATE_OLD_OWNER_WRITE",
    "BROKER_SESSION_CRASH_MASS_TAKEOVER",
    "SYNCHRONIZED_STORM",
)

SCHEMA = "NEREUS_V2_M3_ALLOCATOR_CAMPAIGN_PLAN_V3"
PLANNER_VERSION = 3
LOGICAL_INTERVAL_CELLS = 328
MINIMUM_VALID_EVALUATION_CELLS = 13
MINIMUM_PROMOTABLE_CELLS = 17
MAXIMUM_EXECUTED_INTERVAL_CELLS = 328
MAXIMUM_EXECUTED_FAULT_ACTIONS = 360
MAXIMUM_EXECUTED_SCALE_ACTIONS = 32
MAXIMUM_TOTAL_EXECUTED_ACTIONS = 720
CAMPAIGN_WALL_CLOCK_CAP_SECONDS = 48_000


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def git(*args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(ROOT), *args],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def context_id(candidate: str, population: int, latency: int, rate: int) -> int:
    row = POPULATIONS.index(population) * len(LATENCIES) + LATENCIES.index(latency)
    slot = DESCENDING_RATES.index(rate)
    if candidate == "NATIVE":
        logical = row * len(DESCENDING_RATES) + slot
    else:
        logical = 48 + (CANDIDATES.index(candidate) - 1) * 56
        logical += row * (len(DESCENDING_RATES) + 1) + slot
    return 3_000_000 + logical


def derived_context_id(candidate: str, population: int, latency: int) -> int:
    if candidate == "NATIVE":
        raise AssertionError("native V3 row has no derived slot")
    row = POPULATIONS.index(population) * len(LATENCIES) + LATENCIES.index(latency)
    logical = 48 + (CANDIDATES.index(candidate) - 1) * 56
    logical += row * (len(DESCENDING_RATES) + 1) + DERIVED_SLOT_ORDINAL
    return 3_000_000 + logical


def zero_decision_actions() -> list[str]:
    actions: list[str] = []
    for candidate in CANDIDATES:
        for population in POPULATIONS:
            for latency in LATENCIES:
                for rate in DESCENDING_RATES:
                    kind = "NATIVE_INTERVAL" if candidate == "NATIVE" else "CANDIDATE_INTERVAL"
                    actions.append(f"{kind}:{context_id(candidate, population, latency, rate)}")
                if candidate != "NATIVE":
                    actions.append(
                        f"CANDIDATE_INTERVAL:{derived_context_id(candidate, population, latency)}"
                    )
    for candidate in CANDIDATES[1:]:
        for population in POPULATIONS:
            for latency in LATENCIES:
                for cut in FAULT_CUTS:
                    actions.append(f"FAULT_ACTION:{candidate}:{population}:{latency}:{cut}")
    for candidate in RANGE_CANDIDATES:
        for population in POPULATIONS:
            for latency in LATENCIES:
                actions.append(f"SCALE_ACTION:{candidate}:{population}:{latency}")
    if len(actions) != MAXIMUM_TOTAL_EXECUTED_ACTIONS or len(set(actions)) != len(actions):
        raise AssertionError("frozen allocator physical action inventory differs")
    return actions


def zero_decision_plan_sha256() -> str:
    fields = (
        SCHEMA,
        f"plannerVersion={PLANNER_VERSION}",
        f"logicalIntervalCells={LOGICAL_INTERVAL_CELLS}",
        f"minimumValidEvaluationCells={MINIMUM_VALID_EVALUATION_CELLS}",
        f"minimumPromotableCells={MINIMUM_PROMOTABLE_CELLS}",
        f"maximumExecutedIntervalCells={MAXIMUM_EXECUTED_INTERVAL_CELLS}",
        f"maximumExecutedFaultActions={MAXIMUM_EXECUTED_FAULT_ACTIONS}",
        f"maximumExecutedScaleActions={MAXIMUM_EXECUTED_SCALE_ACTIONS}",
        f"maximumTotalExecutedActions={MAXIMUM_TOTAL_EXECUTED_ACTIONS}",
        f"campaignWallClockCapSeconds={CAMPAIGN_WALL_CLOCK_CAP_SECONDS}",
        *zero_decision_actions(),
    )
    return hashlib.sha256(("\n".join(fields) + "\n").encode()).hexdigest()


def dependency_lock_sha256() -> str:
    manifest = bytearray()
    for path in DEPENDENCY_LOCK_FILES:
        if not path.is_file():
            raise FileNotFoundError(f"allocator dependency lock input is absent: {path}")
        relative = path.relative_to(ROOT).as_posix()
        manifest.extend(relative.encode())
        manifest.extend(b"\x00")
        manifest.extend(sha256(path).encode())
        manifest.extend(b"\n")
    return hashlib.sha256(manifest).hexdigest()


def derived_rate(native_rate: int) -> int:
    if native_rate not in DESCENDING_RATES:
        raise AssertionError("native rate is outside the frozen V3 inventory")
    return max(200, native_rate - native_rate // 5)


def optimistic_requests_per_second(latency_millis: int) -> int:
    return 256_000 // latency_millis


def source_value(source_identity: str, field: str) -> str:
    match = re.search(rf"(?:^|\|){re.escape(field)}=([^|]+)", source_identity)
    if match is None:
        raise AssertionError(f"allocator source lock is missing {field}")
    return match.group(1)


def exact_source_tuple() -> dict[str, str]:
    locks = json.loads(SOURCE_LOCKS.read_bytes())
    bindings = locks["m3EvidenceBindings"]["bindings"]
    allocator = next(
        item
        for item in bindings
        if item["childKind"] == "ALLOCATOR_SELECTION" and item["backend"] == "OXIA_AND_PULSAR"
    )
    identity = allocator["sourceIdentity"]
    return {
        "nereusCommit": git("rev-parse", "HEAD"),
        "sourceLocksSha256": sha256(SOURCE_LOCKS),
        "dependencyLockSha256": dependency_lock_sha256(),
        "pulsarCommit": source_value(identity, "pulsarCommit"),
        "oxiaServerCommit": source_value(identity, "oxiaServerCommit"),
        "oxiaServerImageDigest": source_value(identity, "oxiaServerImageDigest"),
        "oxiaClientCommit": source_value(identity, "oxiaClientCommit"),
        "oxiaClientJarSha256": source_value(identity, "oxiaClientJarSha256"),
    }


def plan() -> dict[str, object]:
    actions = zero_decision_actions()
    intervals = sum(action.startswith(("NATIVE_INTERVAL:", "CANDIDATE_INTERVAL:")) for action in actions)
    faults = sum(action.startswith("FAULT_ACTION:") for action in actions)
    scales = sum(action.startswith("SCALE_ACTION:") for action in actions)
    assert (intervals, faults, scales) == (328, 360, 32)
    return {
        "schema": SCHEMA,
        "plannerVersion": PLANNER_VERSION,
        "logicalIntervalCells": LOGICAL_INTERVAL_CELLS,
        "minimumValidEvaluationCells": MINIMUM_VALID_EVALUATION_CELLS,
        "minimumPromotableCells": MINIMUM_PROMOTABLE_CELLS,
        "maximumExecutedIntervalCells": MAXIMUM_EXECUTED_INTERVAL_CELLS,
        "maximumExecutedFaultActions": MAXIMUM_EXECUTED_FAULT_ACTIONS,
        "maximumExecutedScaleActions": MAXIMUM_EXECUTED_SCALE_ACTIONS,
        "maximumTotalExecutedActions": MAXIMUM_TOTAL_EXECUTED_ACTIONS,
        "campaignWallClockCapSeconds": CAMPAIGN_WALL_CLOCK_CAP_SECONDS,
        "zeroDecisionPlanSha256": zero_decision_plan_sha256(),
        "exactSourceTuple": exact_source_tuple(),
        "admission": {
            "actorCount": 4,
            "maxAsyncOutstandingPerActor": 64,
            "maxGlobalOutstanding": 256,
            "maxRolloverOutstandingPerBinding": 1,
            "preAdmissionQueueRateMultiplier": 2,
            "derivedOutstandingPerActor": 63,
        },
        "derivedFloors": {str(rate): derived_rate(rate) for rate in DESCENDING_RATES},
        "optimisticStructuralBounds": {
            str(latency): optimistic_requests_per_second(latency)
            for latency in (1, 5, 10, 25, 250)
        },
        "feasibilityStatus": "PLAN_FEASIBLE",
        "interval": {
            "warmupSeconds": 10,
            "measuredSeconds": 30,
            "totalSeconds": 40,
            "minimumValidEvaluationSeconds": 520,
            "minimumPromotableSeconds": 680,
            "maximumSeconds": 13_120,
        },
        "independentPhaseBudgetsSeconds": {
            "setup": 900,
            "population": 5_400,
            "fault": 7_200,
            "scale": 5_400,
            "interval": 13_120,
            "cleanup": 1_640,
            "checkpointResumeEvaluationSeal": 600,
        },
        "budgetExhaustionMayCreateDisposition": False,
        "evaluationRequiresCompletedCampaign": True,
        "promotionRequiresUniqueQualifiedMode": True,
        "nativeBaselineUnavailableIsIndependentState": True,
        "duplicateDerivedFloorHasIndependentLogicalSlot": True,
        "accessesOxia": False,
        "accessesPulsar": False,
        "createsPopulation": False,
        "writesEvidence": False,
    }


def main() -> int:
    if len(sys.argv) != 1:
        print(f"usage: {sys.argv[0]}", file=sys.stderr)
        return 64
    print(json.dumps(plan(), ensure_ascii=True, indent=2, sort_keys=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
