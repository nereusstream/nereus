#!/usr/bin/env python3
"""Independent fail-closed parser/reproof helpers for M3 allocator V5 authority.

This module deliberately does not invoke the Java codec.  The governed M3 child
checker uses it to replay the V3 logical planner nested by NACP5, reconstruct
physical-action aggregates, and verify the fixed NAEV5/NADV5/NARS5 wires.
"""

from __future__ import annotations

import hashlib
import importlib.util
from pathlib import Path
import re
from typing import Any, Callable, Iterable
import xml.etree.ElementTree as ET


SCRIPT_DIR = Path(__file__).resolve().parent
PLAN_PATH = SCRIPT_DIR / "v2-m3-allocator-plan-v5.py"
PLAN_SPEC = importlib.util.spec_from_file_location("nereus_m3_allocator_plan_v5_contract", PLAN_PATH)
if PLAN_SPEC is None or PLAN_SPEC.loader is None:
    raise RuntimeError(f"cannot load allocator V5 plan contract: {PLAN_PATH}")
PLAN = importlib.util.module_from_spec(PLAN_SPEC)
PLAN_SPEC.loader.exec_module(PLAN)

COMMIT = re.compile(r"[0-9a-f]{40}")
SHA256 = re.compile(r"[0-9a-f]{64}")
LONG_MAX = (1 << 63) - 1
MAX_CHECKPOINT_BYTES = 2 * 1024 * 1024 + 512
MAX_PHYSICAL_BYTES = 32 * 1024 * 1024
MAX_PHYSICAL_FILES = 720
NAEV5_BYTES = 348
NADV5_BYTES = 308
NARS5_BYTES = 500
PLAN_DIGEST = PLAN.zero_decision_plan_sha256()
EXECUTION_PROFILE_DIGEST = PLAN.native_execution_profile_sha256()

CANDIDATES = ("NATIVE", "STRICT", "RANGE_16", "RANGE_64", "RANGE_256", "RANGE_1024")
RANGE_SIZES = {"STRICT": 1, "RANGE_16": 16, "RANGE_64": 64, "RANGE_256": 256, "RANGE_1024": 1024}
POPULATIONS = (10_000, 100_000)
LATENCIES = (1, 5, 10, 25)
RATES = (1000, 750, 500, 333, 250, 200)
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
DIAGNOSTIC_RAW_NAMES = (
    "allocator-workflow-diagnostic.json",
    "native-baseline-canary-summary.json",
    "native-baseline-row-00.json",
    "native-baseline-row-01.json",
    "native-baseline-row-02.json",
    "native-baseline-row-03.json",
    "native-baseline-row-04.json",
    "native-baseline-row-05.json",
    "native-baseline-row-06.json",
    "native-baseline-row-07.json",
    "native-baseline-row-08.json",
    "native-baseline-row-09.json",
    "range16-formal-sequence.json",
    "real-oxia-operation-diagnostic.json",
    "runner-only-diagnostic.json",
    "strict-formal-sequence.json",
    "v5-range1024-10ms-formal-sequence.json",
    "v5-range1024-25ms-formal-sequence.json",
    "v5-terminal-admission-drain-diagnostic.json",
)

DIAGNOSTIC_TESTS = {
    "com.nereusstream.metadata.oxia.v2.allocator.evidence.M3RealAllocatorStrictIntervalDiagnosticTest#replaysTheExactFormalSequenceWithoutUnexpectedWarmupFailure()",
    "com.nereusstream.metadata.oxia.v2.allocator.evidence.M3RealAllocatorStrictIntervalDiagnosticTest#replaysTheExactRange16ScaleThenFixedIntervalWithoutRetainingReservations()",
    "com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3AsyncActorLaneRunnerTest#evidenceAdmissionCapIsDerivedFromFrozenRateLatencyAndActorCount()",
    "com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3AsyncActorLaneRunnerTest#dispatcherReachesEveryFrozenOutstandingLevelWithoutBlockingOnCompletion()",
    "com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3AsyncActorLaneRunnerTest#controlledLatencyFuturesCoverFrozenAndDerivedRatesIncludingTwoHundredFiftyMillis()",
    "com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3AsyncActorLaneRunnerTest#twoHundredFiftyMillisAtOneThousandRpsReachesTheDerivedAsyncCap()",
    "com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3AsyncActorLaneRunnerTest#callbackReorderingStillProducesOneCanonicalTerminalPerOrdinal()",
    "com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3AsyncActorLaneRunnerTest#cutoffKeepsUndispatchedRequestsInThePreAdmissionDropPartition()",
    "com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3AsyncActorLaneRunnerTest#cleanupTimeoutClosesTheWorkflowGuardAndLateCompletionCannotDispatchNextOperation()",
    "com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3AsyncActorLaneRunnerTest#normalIntervalsSingleFlightBindingsWhileConflictProofRetainsSameKeyConcurrency()",
    "com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3AsyncActorLaneRunnerTest#everyFrozenRateRetainsOneOrdinalAuthoritativeMeasurementTransition()",
    "com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3AsyncActorLaneRunnerTest#scheduleRejectsWarmupAfterMeasurementAndRunnerContainsNoCorrectnessLockOrWorkerPool()",
    "com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3AsyncActorLaneRunnerTest#candidateWarmupLoadRejectionAllowsAdaptiveDescentButUnexpectedFailureDoesNot()",
    "com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3RealOxiaOperationDiagnosticTest#realOxiaOperationsRemainNonzeroAcrossEveryFrozenLatency()",
    "com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3AllocatorWorkflowDiagnosticTest#strictAndRangeRowsUseAsyncAdmissionAtTwoHundredAndFiveHundred()",
    "com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3AllocatorWorkflowDiagnosticTest#fourActorSameCellConflictStormPreservesUniqueLedgerIds()",
    "com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3NativePathDiagnosticTest#formalAndDiagnosticUseOneNonBlockingRuntimeAndFrozenSchedule()",
    "com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V3NativeBaselineCanaryTest#exactFormalScheduleClearsAllNativeBaselinesAndRepresentativeRows()",
    "com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V4AsyncActorLaneRunnerTest#admitsOnlyAlreadyOfferedWorkDuringTheTerminalDrain()",
    "com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V4AsyncActorLaneRunnerTest#dropsAnOnTimeRequestStillBlockedAtTheFinalAdmissionDeadline()",
    "com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V5AsyncActorLaneRunnerTest#reachesTheBoundedStormAdmissionCapWithoutChangingPerBindingSingleFlight()",
    "com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V5TerminalAdmissionDrainDiagnosticTest#exactRange16FixedAndDerivedRowsDrainEveryOnTimeOfferWithoutLoss()",
    "com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V5RangeLatencyDiagnosticTest#exactRange1024TenMillisSequenceAttributesOperationAndSchedulerCapacity()",
    "com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V5RangeLatencyDiagnosticTest#exactRange1024TwentyFiveMillisSequenceAttributesOperationAndSchedulerCapacity()",
}


class V5Error(RuntimeError):
    """Stable allocator V5 governed-evidence rejection."""


def sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


class Cursor:
    def __init__(self, raw: bytes, label: str) -> None:
        self.raw = raw
        self.label = label
        self.offset = 0

    def take(self, count: int) -> bytes:
        if count < 0 or self.offset + count > len(self.raw):
            raise V5Error(f"{self.label} is truncated")
        value = self.raw[self.offset : self.offset + count]
        self.offset += count
        return value

    def unsigned(self, width: int) -> int:
        return int.from_bytes(self.take(width), "big", signed=False)

    def nonnegative_long(self) -> int:
        value = self.unsigned(8)
        if value > LONG_MAX:
            raise V5Error(f"{self.label} contains a negative Java long")
        return value

    def finish(self) -> None:
        if self.offset != len(self.raw):
            raise V5Error(f"{self.label} has trailing bytes")


def source(cursor: Cursor) -> dict[str, str]:
    try:
        commit = cursor.take(40).decode("ascii")
    except UnicodeDecodeError as error:
        raise V5Error(f"{cursor.label} commit is not ASCII") from error
    if not COMMIT.fullmatch(commit):
        raise V5Error(f"{cursor.label} commit is not canonical")
    result = {
        "nereusCommit": commit,
        "oxiaImageDigest": cursor.take(32).hex(),
        "dependencyLockDigest": cursor.take(32).hex(),
        "executorDigest": cursor.take(32).hex(),
        "workloadDigest": cursor.take(32).hex(),
    }
    if any(value == "0" * 64 for key, value in result.items() if key != "nereusCommit"):
        raise V5Error(f"{cursor.label} source tuple contains a zero digest")
    return result


def logical_cells() -> list[dict[str, Any]]:
    cells: list[dict[str, Any]] = []
    for candidate_index, candidate in enumerate(CANDIDATES):
        for population_index, population in enumerate(POPULATIONS):
            for latency_index, latency in enumerate(LATENCIES):
                row_index = population_index * len(LATENCIES) + latency_index
                slots = 6 if candidate == "NATIVE" else 7
                for ordinal in range(slots):
                    if candidate == "NATIVE":
                        logical = row_index * 6 + ordinal
                    else:
                        logical = 48 + (candidate_index - 1) * 56 + row_index * 7 + ordinal
                    cells.append(
                        {
                            "candidate": candidate,
                            "candidateIndex": candidate_index,
                            "contextId": 3_000_000 + logical,
                            "derived": ordinal == 6,
                            "latency": latency,
                            "ordinal": ordinal,
                            "population": population,
                            "rate": None if ordinal == 6 else RATES[ordinal],
                        }
                    )
    if len(cells) != 328 or len({cell["contextId"] for cell in cells}) != 328:
        raise AssertionError("allocator V5 logical cell inventory differs")
    return cells


CELLS = logical_cells()
CELLS_BY_CONTEXT = {cell["contextId"]: cell for cell in CELLS}


def cell(candidate: str, population: int, latency: int, ordinal: int) -> dict[str, Any]:
    for value in CELLS:
        if (
            value["candidate"] == candidate
            and value["population"] == population
            and value["latency"] == latency
            and value["ordinal"] == ordinal
        ):
            return value
    raise AssertionError("allocator V5 logical cell lookup differs")


def rows(candidate: str) -> list[tuple[str, int, int]]:
    return [(candidate, population, latency) for population in POPULATIONS for latency in LATENCIES]


def derived_rate(native_rate: int) -> int:
    if native_rate not in RATES:
        raise V5Error("allocator V5 native rate is outside the frozen inventory")
    return max(200, native_rate - native_rate // 5)


def complete_zero_failure(observation: dict[str, Any]) -> bool:
    values = observation["values"]
    expected = observation["offeredRate"] * 30
    return (
        values[0] == expected
        and values[1] == expected
        and values[2] == 0
        and values[3] == expected
        and values[4] == 0
        and values[5] == 0
        and all(values[index] == 0 for index in (7, 8, 9, 10, 11, 18, 19, 20))
    )


def absolute_candidate_bounds_pass(observation: dict[str, Any]) -> bool:
    values = observation["values"]
    rate = observation["offeredRate"]
    return (
        complete_zero_failure(observation)
        and values[12] <= 250_000
        and values[13] <= 250_000
        and values[14] <= 1_000_000
        and values[15] <= 2 * rate
        and values[16] <= 2_000_000
        and values[17] <= 2_000_000
    )


def fault_bounds_pass(observation: dict[str, Any]) -> bool:
    recovery = 30_000_000 if observation["population"] == 10_000 else 60_000_000
    values = observation["values"]
    return (
        observation["cuts"] == 0x1FF
        and all(values[index] == 0 for index in range(8))
        and values[8] <= 1
        and values[9] <= recovery
    )


class Planner:
    """Independent transcription of AllocatorCampaignPlannerV3."""

    def __init__(self) -> None:
        self.native_rows = rows("NATIVE")
        self.native_results: dict[tuple[str, int, int], dict[str, Any]] = {}
        self.dispositions: list[tuple[int, int, tuple[int, ...]]] = []
        self.disposition_contexts: set[int] = set()
        self.qualified: list[int] = []
        self.row_contexts: list[int] = []
        self.candidate_qualification_contexts: list[int] = []
        self.phase = "NATIVE"
        self.native_row_index = 0
        self.native_rate_index = 0
        self.candidate_index = 1
        self.candidate_row_index = 0
        self.candidate_execution_index = 0
        self.candidate_eliminated = False
        self.elimination_dependency: int | None = None
        self.pending_sustainable: dict[str, Any] | None = None
        self.executed = 0

    def disposition(self, logical: dict[str, Any], kind: int, dependencies: Iterable[int], absent: bool = False) -> None:
        context = logical["contextId"]
        if context in self.disposition_contexts:
            if absent:
                return
            raise V5Error("allocator V5 deterministic planner produced a duplicate disposition")
        dependency_tuple = tuple(dependencies)
        if not dependency_tuple:
            raise V5Error("allocator V5 deterministic disposition has no dependency")
        self.disposition_contexts.add(context)
        self.dispositions.append((context, kind, dependency_tuple))

    def executions(self, candidate: str, population: int, latency: int, native_rate: int) -> list[tuple[dict[str, Any], int]]:
        floor = derived_rate(native_rate)
        result = [(cell(candidate, population, latency, ordinal), rate) for ordinal, rate in enumerate(RATES) if rate > floor]
        result.append((cell(candidate, population, latency, 6), floor))
        return result

    def next_action(self) -> tuple[str, Any] | None:
        while True:
            if self.phase == "COMPLETE":
                return None
            if self.phase == "NATIVE":
                if self.native_row_index == len(self.native_rows):
                    unavailable = [result for result in self.native_results.values() if result["rate"] == 0]
                    if unavailable:
                        dependencies = [result["contexts"][-1] for result in unavailable]
                        for logical in CELLS:
                            if logical["candidate"] != "NATIVE":
                                self.disposition(logical, 4, dependencies)
                        self.phase = "COMPLETE"
                    else:
                        self.phase = "CANDIDATE"
                    continue
                _, population, latency = self.native_rows[self.native_row_index]
                return "interval", (cell("NATIVE", population, latency, self.native_rate_index), RATES[self.native_rate_index])
            if self.candidate_index >= len(CANDIDATES):
                self.phase = "COMPLETE"
                continue
            candidate = CANDIDATES[self.candidate_index]
            candidate_rows = rows(candidate)
            if self.candidate_eliminated:
                if self.elimination_dependency is None:
                    raise V5Error("allocator V5 eliminated candidate lacks dependency")
                for _, population, latency in candidate_rows[self.candidate_row_index :]:
                    for ordinal in range(7):
                        self.disposition(
                            cell(candidate, population, latency, ordinal),
                            5,
                            [self.elimination_dependency],
                            absent=True,
                        )
                self.advance_candidate()
                continue
            if self.candidate_row_index == len(candidate_rows):
                self.qualified.append(self.candidate_index)
                if candidate.startswith("RANGE_"):
                    if len(self.candidate_qualification_contexts) != 8:
                        raise V5Error("allocator V5 qualified RANGE lacks all eight dependencies")
                    for later in CANDIDATES[self.candidate_index + 1 :]:
                        for _, population, latency in rows(later):
                            for ordinal in range(7):
                                self.disposition(
                                    cell(later, population, latency, ordinal),
                                    6,
                                    self.candidate_qualification_contexts,
                                )
                    self.phase = "COMPLETE"
                else:
                    self.advance_candidate()
                continue
            _, population, latency = candidate_rows[self.candidate_row_index]
            native = self.native_results.get(("NATIVE", population, latency))
            if native is None or native["rate"] == 0:
                raise V5Error("allocator V5 candidate row lacks a native baseline")
            if self.pending_sustainable is not None:
                return "fault", (self.candidate_index, population, latency)
            executions = self.executions(candidate, population, latency, native["rate"])
            floor = derived_rate(native["rate"])
            dependency = native["contexts"][-1]
            for ordinal, rate in enumerate(RATES):
                if rate < floor:
                    self.disposition(cell(candidate, population, latency, ordinal), 1, [dependency], absent=True)
                elif rate == floor:
                    self.disposition(cell(candidate, population, latency, ordinal), 2, [dependency], absent=True)
            return "interval", executions[self.candidate_execution_index]

    def accept(self, observation: dict[str, Any]) -> None:
        if observation["tag"] == "interval":
            self.executed += 1
            self.row_contexts.append(observation["cell"]["contextId"])
            if self.phase == "NATIVE":
                self.accept_native(observation)
            elif self.phase == "CANDIDATE":
                self.accept_candidate(observation)
            else:
                raise V5Error("allocator V5 interval appears after completion")
            return
        if self.pending_sustainable is None:
            raise V5Error("allocator V5 fault row lacks sustainable interval dependency")
        dependency = self.pending_sustainable["cell"]["contextId"]
        if not fault_bounds_pass(observation):
            self.eliminate(dependency)
            return
        self.candidate_qualification_contexts.append(dependency)
        self.candidate_row_index += 1
        self.candidate_execution_index = 0
        self.pending_sustainable = None
        self.row_contexts.clear()

    def accept_native(self, observation: dict[str, Any]) -> None:
        logical = observation["cell"]
        if complete_zero_failure(observation):
            for ordinal in range(self.native_rate_index + 1, len(RATES)):
                self.disposition(cell("NATIVE", logical["population"], logical["latency"], ordinal), 0, [logical["contextId"]])
            self.native_results[("NATIVE", logical["population"], logical["latency"])] = {
                "append": observation["values"][17],
                "contexts": list(self.row_contexts),
                "rate": observation["offeredRate"],
            }
            self.native_row_index += 1
            self.native_rate_index = 0
            self.row_contexts.clear()
        elif self.native_rate_index + 1 == len(RATES):
            self.native_results[("NATIVE", logical["population"], logical["latency"])] = {
                "append": 0,
                "contexts": list(self.row_contexts),
                "rate": 0,
            }
            self.native_row_index += 1
            self.native_rate_index = 0
            self.row_contexts.clear()
        else:
            self.native_rate_index += 1

    def accept_candidate(self, observation: dict[str, Any]) -> None:
        logical = observation["cell"]
        native = self.native_results[("NATIVE", logical["population"], logical["latency"])]
        executions = self.executions(logical["candidate"], logical["population"], logical["latency"], native["rate"])
        if not absolute_candidate_bounds_pass(observation):
            if self.candidate_execution_index + 1 < len(executions):
                self.candidate_execution_index += 1
            else:
                self.eliminate(logical["contextId"])
            return
        for execution, _ in executions[self.candidate_execution_index + 1 :]:
            self.disposition(execution, 3, [logical["contextId"]])
        if observation["values"][17] > native["append"] + 250_000:
            self.eliminate(logical["contextId"])
        else:
            self.pending_sustainable = observation

    def eliminate(self, dependency: int) -> None:
        self.candidate_eliminated = True
        self.elimination_dependency = dependency
        self.pending_sustainable = None
        self.candidate_row_index += 1
        self.candidate_execution_index = 0
        self.row_contexts.clear()

    def advance_candidate(self) -> None:
        self.candidate_index += 1
        self.candidate_row_index = 0
        self.candidate_execution_index = 0
        self.candidate_eliminated = False
        self.elimination_dependency = None
        self.pending_sustainable = None
        self.row_contexts.clear()
        self.candidate_qualification_contexts.clear()


def replay(observations: list[dict[str, Any]]) -> dict[str, Any]:
    planner = Planner()
    for observation in observations:
        expected = planner.next_action()
        if expected is None or expected[0] != observation["tag"]:
            raise V5Error("allocator V5 observation differs from deterministic planner action")
        if observation["tag"] == "interval":
            expected_cell, expected_rate = expected[1]
            if expected_cell["contextId"] != observation["cell"]["contextId"] or expected_rate != observation["offeredRate"]:
                raise V5Error("allocator V5 interval is reordered or differs from its logical slot")
        elif expected[1] != (observation["candidateIndex"], observation["population"], observation["latency"]):
            raise V5Error("allocator V5 fault row is reordered or differs")
        planner.accept(observation)
    completed = planner.next_action() is None
    if completed and planner.executed + len(planner.dispositions) != 328:
        raise V5Error("allocator V5 completed plan does not account for 328 cells")
    baseline = "INCOMPLETE"
    if len(planner.native_results) == 8:
        baseline = "AVAILABLE" if all(value["rate"] > 0 for value in planner.native_results.values()) else "UNAVAILABLE"
    return {
        "baseline": baseline,
        "completed": completed,
        "dispositions": planner.dispositions,
        "executed": planner.executed,
        "qualified": planner.qualified,
    }


def logical_campaign_id(source_value: dict[str, str]) -> str:
    digest = hashlib.sha256()
    digest.update(b"NEREUS-V2-M3-ALLOCATOR-CAMPAIGN-ID-V3")
    digest.update(source_value["nereusCommit"].encode("ascii"))
    for name in ("oxiaImageDigest", "dependencyLockDigest", "executorDigest", "workloadDigest"):
        digest.update(bytes.fromhex(source_value[name]))
    digest.update((3).to_bytes(4, "big"))
    for logical in CELLS:
        digest.update(logical["contextId"].to_bytes(4, "big"))
    return digest.hexdigest()


def campaign_id(source_value: dict[str, str], logical_id: str) -> str:
    digest = hashlib.sha256()
    digest.update(b"NEREUS-V2-M3-ALLOCATOR-CAMPAIGN-ID-V5")
    digest.update(source_value["nereusCommit"].encode("ascii"))
    for name in ("oxiaImageDigest", "dependencyLockDigest", "executorDigest", "workloadDigest"):
        digest.update(bytes.fromhex(source_value[name]))
    digest.update(bytes.fromhex(EXECUTION_PROFILE_DIGEST))
    digest.update(bytes.fromhex(PLAN_DIGEST))
    digest.update(bytes.fromhex(logical_id))
    return digest.hexdigest()


def attachment_root(aggregates: list[str]) -> str:
    digest = hashlib.sha256()
    digest.update(b"NEREUS-V2-M3-ALLOCATOR-ATTACHMENT-ROOT-V5")
    for value in aggregates:
        digest.update(bytes.fromhex(value))
    return digest.hexdigest()


def parse_checkpoint(raw: bytes) -> dict[str, Any]:
    if not raw or len(raw) > MAX_CHECKPOINT_BYTES:
        raise V5Error("allocator V5 NACP5 bytes are empty or over cap")
    outer = Cursor(raw, "allocator V5 NACP5")
    if outer.take(8) != b"NACP5\0\0\0" or outer.unsigned(2) != 5:
        raise V5Error("allocator V5 NACP5 magic/version differs")
    status = outer.unsigned(1)
    if status != 1 or outer.unsigned(1) != 0:
        raise V5Error("allocator V5 NACP5 is not completed")
    sequence = outer.nonnegative_long()
    execution_profile = outer.take(32).hex()
    plan_digest = outer.take(32).hex()
    outer_campaign = outer.take(32).hex()
    predecessor = outer.take(32).hex()
    outer_budgets = tuple(outer.nonnegative_long() for _ in range(7))
    logical_length = outer.unsigned(4)
    logical_raw = outer.take(logical_length)
    outer.finish()
    if execution_profile != EXECUTION_PROFILE_DIGEST or plan_digest != PLAN_DIGEST:
        raise V5Error("allocator V5 profile/plan digest differs")
    if (sequence == 0) != (predecessor == "0" * 64):
        raise V5Error("allocator V5 predecessor lineage differs")

    cursor = Cursor(logical_raw, "allocator V5 nested NACP3")
    if cursor.take(8) != b"NACP3\0\0\0" or cursor.unsigned(2) != 3:
        raise V5Error("allocator V5 nested NACP3 magic/version differs")
    logical_status = cursor.unsigned(1)
    if logical_status != status or cursor.unsigned(1) != 0:
        raise V5Error("allocator V5 nested status/reserved byte differs")
    logical_sequence = cursor.nonnegative_long()
    if logical_sequence != sequence:
        raise V5Error("allocator V5 outer/nested sequence differs")
    source_value = source(cursor)
    logical_campaign = cursor.take(32).hex()
    logical_predecessor = cursor.take(32).hex()
    logical_budgets = tuple(cursor.nonnegative_long() for _ in range(7))
    if (logical_sequence == 0) != (logical_predecessor == "0" * 64):
        raise V5Error("allocator V5 nested predecessor lineage differs")
    if source_value["workloadDigest"] != PLAN_DIGEST:
        raise V5Error("allocator V5 source workload digest differs from plan")
    if logical_campaign != logical_campaign_id(source_value) or outer_campaign != campaign_id(source_value, logical_campaign):
        raise V5Error("allocator V5 campaign identity differs")
    if cursor.unsigned(4) != len(CELLS):
        raise V5Error("allocator V5 logical inventory count differs")
    if [cursor.unsigned(4) for _ in CELLS] != [value["contextId"] for value in CELLS]:
        raise V5Error("allocator V5 logical inventory order differs")

    record_count = cursor.unsigned(4)
    if not 1 <= record_count <= 368:
        raise V5Error("allocator V5 execution record count differs")
    observations: list[dict[str, Any]] = []
    aggregates: list[str] = []
    executed_contexts: set[int] = set()
    fault_rows: set[tuple[int, int, int]] = set()
    for _ in range(record_count):
        tag = cursor.unsigned(1)
        if tag == 1:
            context_id = cursor.unsigned(4)
            logical = CELLS_BY_CONTEXT.get(context_id)
            offered_rate = cursor.unsigned(4)
            values = [cursor.nonnegative_long() for _ in range(21)]
            if logical is None or context_id in executed_contexts:
                raise V5Error("allocator V5 interval context is unknown or duplicated")
            if not 200 <= offered_rate <= 1000 or (not logical["derived"] and offered_rate != logical["rate"]):
                raise V5Error("allocator V5 interval offered rate differs from logical slot")
            if values[1] != values[3] + values[4] + values[5] or values[6] != values[1] or values[0] != values[2] + values[1]:
                raise V5Error("allocator V5 interval violates terminal conservation")
            executed_contexts.add(context_id)
            observations.append({"cell": logical, "offeredRate": offered_rate, "tag": "interval", "values": values})
        elif tag == 2:
            candidate_index = cursor.unsigned(1)
            population = cursor.unsigned(4)
            latency = cursor.unsigned(4)
            cuts = cursor.unsigned(4)
            values = [cursor.nonnegative_long() for _ in range(10)]
            row = (candidate_index, population, latency)
            if candidate_index not in range(1, 6) or population not in POPULATIONS or latency not in LATENCIES or cuts & ~0x1FF or row in fault_rows:
                raise V5Error("allocator V5 fault row/cut envelope differs")
            fault_rows.add(row)
            observations.append({
                "candidate": CANDIDATES[candidate_index],
                "candidateIndex": candidate_index,
                "cuts": cuts,
                "latency": latency,
                "population": population,
                "tag": "fault",
                "values": values,
            })
        else:
            raise V5Error("allocator V5 observation tag differs")
        aggregate = cursor.take(32).hex()
        if aggregate == "0" * 64 or aggregate in aggregates:
            raise V5Error("allocator V5 attachment aggregate is zero or aliased")
        aggregates.append(aggregate)

    disposition_count = cursor.unsigned(4)
    if disposition_count > 328:
        raise V5Error("allocator V5 disposition count exceeds inventory")
    dispositions: list[tuple[int, int, tuple[int, ...]]] = []
    disposition_contexts: set[int] = set()
    for _ in range(disposition_count):
        context = cursor.unsigned(4)
        kind = cursor.unsigned(1)
        dependency_count = cursor.unsigned(2)
        dependencies = tuple(cursor.unsigned(4) for _ in range(dependency_count))
        if (
            context not in CELLS_BY_CONTEXT
            or context in executed_contexts
            or context in disposition_contexts
            or kind > 6
            or not 1 <= dependency_count <= record_count
            or len(set(dependencies)) != dependency_count
            or any(dependency not in executed_contexts for dependency in dependencies)
        ):
            raise V5Error("allocator V5 disposition envelope differs")
        disposition_contexts.add(context)
        dispositions.append((context, kind, dependencies))
    cursor.finish()

    replayed = replay(observations)
    if not replayed["completed"] or dispositions != replayed["dispositions"]:
        raise V5Error("allocator V5 dispositions differ from deterministic replay")
    if replayed["executed"] + disposition_count != 328:
        raise V5Error("allocator V5 execution/disposition conservation differs")

    interval_count = sum(observation["tag"] == "interval" for observation in observations)
    fault_count = len(observations) - interval_count
    ten_candidates: set[int] = set()
    hundred_candidates: set[int] = set()
    for observation in observations:
        logical = observation["cell"] if observation["tag"] == "interval" else observation
        target = ten_candidates if logical["population"] == 10_000 else hundred_candidates
        target.add(logical["candidateIndex"])
    expected_logical_budgets = (
        0,
        5_400 - 900 * len(ten_candidates),
        7_200 - 180 * fault_count,
        5_400 - 900 * len(hundred_candidates),
        13_120 - 40 * interval_count,
        1_640 - 5 * interval_count,
        600,
    )
    expected_outer_budgets = expected_logical_budgets[:4] + (13_776 - 42 * interval_count,) + expected_logical_budgets[5:]
    if logical_budgets != expected_logical_budgets or outer_budgets != expected_outer_budgets or sequence < record_count:
        raise V5Error("allocator V5 budget/sequence accounting differs")

    return {
        "aggregates": aggregates,
        "attachmentRoot": attachment_root(aggregates),
        "baseline": replayed["baseline"],
        "campaignId": outer_campaign,
        "checkpointDigest": sha256(raw),
        "dispositions": disposition_count,
        "executed": replayed["executed"],
        "observations": observations,
        "qualified": replayed["qualified"],
        "sequence": sequence,
        "source": source_value,
    }


def expected_evaluation(checkpoint: dict[str, Any]) -> tuple[int, int]:
    if checkpoint["baseline"] == "UNAVAILABLE":
        return 4, 255
    qualified = checkpoint["qualified"]
    strict = 1 in qualified
    ranges = [candidate for candidate in qualified if candidate >= 2]
    selected_range = min(ranges) if ranges else None
    if strict and selected_range is None:
        return 0, 1
    if not strict and selected_range is not None:
        return 1, selected_range
    if not strict:
        return 2, 255
    return 3, 255


def parse_evaluation(raw: bytes, checkpoint: dict[str, Any]) -> dict[str, Any]:
    if len(raw) != NAEV5_BYTES:
        raise V5Error("allocator V5 NAEV5 fixed bytes differ")
    cursor = Cursor(raw, "allocator V5 NAEV5")
    if cursor.take(8) != b"NAEV5\0\0\0" or cursor.unsigned(2) != 5:
        raise V5Error("allocator V5 NAEV5 magic/version differs")
    status = cursor.unsigned(1)
    selected = cursor.unsigned(1)
    executed = cursor.unsigned(4)
    dispositions = cursor.unsigned(4)
    source_value = source(cursor)
    execution_profile = cursor.take(32).hex()
    plan = cursor.take(32).hex()
    campaign = cursor.take(32).hex()
    checkpoint_digest = cursor.take(32).hex()
    root = cursor.take(32).hex()
    cursor.finish()
    expected_status, expected_selected = expected_evaluation(checkpoint)
    if (
        (status, selected) != (expected_status, expected_selected)
        or status not in range(5)
        or executed != checkpoint["executed"]
        or dispositions != checkpoint["dispositions"]
        or executed + dispositions != 328
        or source_value != checkpoint["source"]
        or execution_profile != EXECUTION_PROFILE_DIGEST
        or plan != PLAN_DIGEST
        or campaign != checkpoint["campaignId"]
        or checkpoint_digest != checkpoint["checkpointDigest"]
        or root != checkpoint["attachmentRoot"]
    ):
        raise V5Error("allocator V5 NAEV5 accounting/selection/link differs")
    return {"digest": sha256(raw), "selected": selected, "status": status}


def parse_diagnostic(raw: bytes) -> dict[str, Any]:
    if len(raw) != NADV5_BYTES:
        raise V5Error("allocator V5 NADV5 fixed bytes differ")
    cursor = Cursor(raw, "allocator V5 NADV5")
    if cursor.take(8) != b"NADV5\0\0\0" or cursor.unsigned(2) != 5:
        raise V5Error("allocator V5 NADV5 magic/version differs")
    if cursor.unsigned(1) != 0x3F or cursor.unsigned(1) != 0:
        raise V5Error("allocator V5 NADV5 diagnostic mask/reserved differs")
    result = {
        "source": source(cursor),
        "executionProfile": cursor.take(32).hex(),
        "plan": cursor.take(32).hex(),
        "junitManifest": cursor.take(32).hex(),
        "rawManifest": cursor.take(32).hex(),
    }
    cursor.finish()
    if result["executionProfile"] != EXECUTION_PROFILE_DIGEST or result["plan"] != PLAN_DIGEST or result["source"]["workloadDigest"] != PLAN_DIGEST:
        raise V5Error("allocator V5 NADV5 source/profile/plan differs")
    result["digest"] = sha256(raw)
    return result


def junit_summary_digest(summary: dict[str, int]) -> str:
    digest = hashlib.sha256()
    digest.update(b"NEREUS-V2-M3-ALLOCATOR-JUNIT-SUMMARY-V5")
    for name in ("tests", "failures", "errors", "skipped"):
        digest.update(summary[name].to_bytes(8, "big"))
    return digest.hexdigest()


def parse_selection(raw: bytes, checkpoint: dict[str, Any], evaluation: dict[str, Any], diagnostic: dict[str, Any], formal_summary: dict[str, int]) -> dict[str, Any]:
    if len(raw) != NARS5_BYTES:
        raise V5Error("allocator V5 NARS5 fixed bytes differ")
    cursor = Cursor(raw, "allocator V5 NARS5")
    if cursor.take(8) != b"NARS5\0\0\0" or cursor.unsigned(2) != 5:
        raise V5Error("allocator V5 NARS5 magic/version differs")
    selected = cursor.unsigned(1)
    if cursor.unsigned(1) != 0:
        raise V5Error("allocator V5 NARS5 reserved byte differs")
    source_value = source(cursor)
    digests = [cursor.take(32).hex() for _ in range(10)]
    cursor.finish()
    if evaluation["status"] not in {0, 1} or selected != evaluation["selected"] or selected not in range(1, 6):
        raise V5Error("allocator V5 NARS5 candidate differs from evaluation")
    expected = [
        EXECUTION_PROFILE_DIGEST,
        PLAN_DIGEST,
        checkpoint["campaignId"],
        checkpoint["checkpointDigest"],
        evaluation["digest"],
        checkpoint["attachmentRoot"],
        diagnostic["digest"],
        diagnostic["junitManifest"],
        diagnostic["rawManifest"],
        junit_summary_digest(formal_summary),
    ]
    if source_value != checkpoint["source"] or source_value != diagnostic["source"] or digests != expected:
        raise V5Error("allocator V5 NARS5 source/digest wire differs")
    return {"candidate": CANDIDATES[selected], "digest": sha256(raw), "mode": "STRICT" if selected == 1 else "RANGE"}


def parse_junit(raw: bytes) -> tuple[dict[str, int], set[str]]:
    if not raw or len(raw) > 16 * 1024 * 1024 or b"<!DOCTYPE" in raw.upper() or b"<!ENTITY" in raw.upper():
        raise V5Error("allocator V5 JUnit is empty, unsafe, or over cap")
    try:
        xml = ET.fromstring(raw)
    except ET.ParseError as error:
        raise V5Error(f"cannot parse allocator V5 JUnit: {error}") from error
    suites = [xml] if xml.tag == "testsuite" else list(xml.findall("testsuite")) if xml.tag == "testsuites" else []
    if not suites:
        raise V5Error("allocator V5 JUnit root differs")
    summary = {"tests": 0, "failures": 0, "errors": 0, "skipped": 0}
    identities: set[str] = set()
    suite_names: set[str] = set()
    for suite in suites:
        if suite.findall("testsuite"):
            raise V5Error("allocator V5 JUnit suite nesting differs")
        name = suite.attrib.get("name")
        if not name or name in suite_names:
            raise V5Error("allocator V5 JUnit suite identity differs")
        suite_names.add(name)
        declared: dict[str, int] = {}
        for field in summary:
            try:
                declared[field] = int(suite.attrib.get(field, "0" if field == "skipped" else ""))
            except ValueError as error:
                raise V5Error(f"allocator V5 JUnit {field} attribute differs") from error
            if declared[field] < 0:
                raise V5Error(f"allocator V5 JUnit {field} is negative")
        cases = suite.findall("testcase")
        observed = {
            "tests": len(cases),
            "failures": sum(case.find("failure") is not None for case in cases),
            "errors": sum(case.find("error") is not None for case in cases),
            "skipped": sum(case.find("skipped") is not None for case in cases),
        }
        if declared != observed:
            raise V5Error("allocator V5 JUnit counters differ from testcase outcomes")
        for case in cases:
            identity = f"{case.attrib.get('classname', '')}#{case.attrib.get('name', '')}"
            if identity.startswith("#") or identity.endswith("#") or identity in identities:
                raise V5Error("allocator V5 JUnit testcase identity differs")
            identities.add(identity)
        for field in summary:
            summary[field] += declared[field]
    return summary, identities


def diagnostic_junit_manifest(files: list[tuple[str, bytes]]) -> tuple[str, dict[str, int]]:
    if len(files) != 10 or [name for name, _ in files] != sorted(name for name, _ in files):
        raise V5Error("allocator V5 diagnostic JUnit file inventory differs")
    manifest = bytearray(b"NEREUS_V2_M3_ALLOCATOR_DIAGNOSTIC_JUNIT_MANIFEST_V5\n")
    totals = {"tests": 0, "failures": 0, "errors": 0, "skipped": 0}
    identities: set[str] = set()
    for name, raw in files:
        summary, current = parse_junit(raw)
        if identities.intersection(current):
            raise V5Error("allocator V5 diagnostic JUnit identities alias")
        identities.update(current)
        for field in totals:
            totals[field] += summary[field]
        manifest.extend(name.encode("utf-8"))
        manifest.extend(b"\0")
        manifest.extend(str(len(raw)).encode("ascii"))
        manifest.extend(b"\0")
        manifest.extend(sha256(raw).encode("ascii"))
        manifest.extend(b"\n")
    if identities != DIAGNOSTIC_TESTS or totals != {"tests": 24, "failures": 0, "errors": 0, "skipped": 0}:
        raise V5Error("allocator V5 diagnostic JUnit inventory/result differs")
    return sha256(bytes(manifest)), totals


def diagnostic_raw_manifest(files: list[tuple[str, bytes]], tested_commit: str) -> str:
    if [name for name, _ in files] != sorted(DIAGNOSTIC_RAW_NAMES):
        raise V5Error("allocator V5 diagnostic raw file inventory differs")
    manifest = bytearray(b"NEREUS_V2_M3_ALLOCATOR_DIAGNOSTIC_RAW_MANIFEST_V5\n")
    observed_source = False
    for name, raw in files:
        try:
            import json
            value = json.loads(raw)
        except (UnicodeDecodeError, ValueError) as error:
            raise V5Error(f"allocator V5 diagnostic raw JSON differs: {name}") from error
        if not isinstance(value, dict) or value.get("diagnosticOnly") is not True or value.get("authority") is not False or value.get("selectionEligible") is not False:
            raise V5Error(f"allocator V5 diagnostic raw authority boundary differs: {name}")
        serialized = raw.decode("utf-8", errors="strict")
        declared_sources = re.findall(r'"sourceCommit":"([0-9a-f]{40})"', serialized)
        if any(value != tested_commit for value in declared_sources):
            raise V5Error(f"allocator V5 diagnostic raw source binding differs: {name}")
        observed_source = observed_source or bool(declared_sources)
        manifest.extend(name.encode("utf-8"))
        manifest.extend(b"\0")
        manifest.extend(str(len(raw)).encode("ascii"))
        manifest.extend(b"\0")
        manifest.extend(sha256(raw).encode("ascii"))
        manifest.extend(b"\n")
    if not observed_source:
        raise V5Error("allocator V5 diagnostic raw inventory lacks a source-bound receipt")
    return sha256(bytes(manifest))


def physical_aggregates(checkpoint: dict[str, Any], files: list[tuple[str, bytes]]) -> list[str]:
    if not 1 <= len(files) <= MAX_PHYSICAL_FILES or [name for name, _ in files] != sorted(name for name, _ in files):
        raise V5Error("allocator V5 physical attachment inventory differs")
    by_name = {name: raw for name, raw in files}
    if len(by_name) != len(files):
        raise V5Error("allocator V5 physical attachment names alias")
    consumed: set[str] = set()
    seen_rows: set[tuple[int, int, int]] = set()
    result: list[str] = []

    def consume(prefix: str, suffix: str) -> str:
        matches = [name for name in by_name if name.startswith(prefix) and name.endswith(suffix)]
        if len(matches) != 1 or matches[0] in consumed:
            raise V5Error(f"allocator V5 physical attachment identity differs: {prefix}")
        name = matches[0]
        raw = by_name[name]
        if not raw or len(raw) > MAX_PHYSICAL_BYTES:
            raise V5Error(f"allocator V5 physical attachment bytes exceed cap: {name}")
        digest = sha256(raw)
        if name != f"{prefix}{digest}{suffix}":
            raise V5Error(f"allocator V5 physical attachment filename digest differs: {name}")
        consumed.add(name)
        return digest

    for observation, expected in zip(checkpoint["observations"], checkpoint["aggregates"], strict=True):
        physical: list[str] = []
        if observation["tag"] == "interval":
            logical = observation["cell"]
            row = (logical["candidateIndex"], logical["population"], logical["latency"])
            first = row not in seen_rows
            seen_rows.add(row)
            if logical["candidate"].startswith("RANGE_") and first:
                physical.append(consume(f"scale-{logical['candidate']}-{logical['population']}-{logical['latency']}-", ".json"))
            physical.append(consume(f"interval-{logical['contextId']}-", ".json"))
        else:
            for cut in FAULT_CUTS:
                physical.append(consume(f"fault-{observation['candidate']}-{observation['population']}-{observation['latency']}-{cut}-", ".nare1"))
        aggregate = sha256(b"".join(bytes.fromhex(value) for value in physical))
        if aggregate != expected or aggregate in result:
            raise V5Error("allocator V5 physical attachment aggregate differs or aliases")
        result.append(aggregate)
    if consumed != set(by_name):
        raise V5Error("allocator V5 physical attachment inventory contains an unbound file")
    return result


def promotion_decision_expected(
    checkpoint_raw: bytes,
    evaluation_raw: bytes,
    diagnostic_raw: bytes,
    diagnostic_junit_manifest_digest: str,
    diagnostic_raw_manifest_digest: str,
    formal_junit_raw: bytes,
    selected_candidate: str,
) -> dict[str, str]:
    return {
        "schema": "NEREUS_V2_M3_ALLOCATOR_PROMOTION_DECISION_V5",
        "status": "PROMOTABLE",
        "selectedCandidate": selected_candidate,
        "checkpointSha256": sha256(checkpoint_raw),
        "evaluationSha256": sha256(evaluation_raw),
        "diagnosticSha256": sha256(diagnostic_raw),
        "diagnosticJUnitSha256": diagnostic_junit_manifest_digest,
        "diagnosticRawManifestSha256": diagnostic_raw_manifest_digest,
        "formalJUnitSha256": sha256(formal_junit_raw),
    }
