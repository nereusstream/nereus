#!/usr/bin/env python3
"""Pure fail-closed contracts for the independent allocator V5 authority checker."""

from __future__ import annotations

import hashlib
import importlib.util
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parent.parent
CHECKER_PATH = ROOT / "scripts" / "check-v2-m3-allocator-v5.py"
SPEC = importlib.util.spec_from_file_location("check_v2_m3_allocator_v5_contract", CHECKER_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"cannot load allocator V5 checker: {CHECKER_PATH}")
CHECKER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECKER)


def unsigned(value: int, width: int) -> bytes:
    return value.to_bytes(width, "big", signed=False)


def digest(label: str) -> str:
    return hashlib.sha256(label.encode("ascii")).hexdigest()


def source_bytes(source: dict[str, str]) -> bytes:
    return source["nereusCommit"].encode("ascii") + b"".join(
        bytes.fromhex(source[name])
        for name in (
            "oxiaImageDigest",
            "dependencyLockDigest",
            "executorDigest",
            "workloadDigest",
        )
    )


def interval(logical: dict[str, object], rate: int, passes: bool) -> dict[str, object]:
    measured = rate * 30
    admitted = measured if passes else measured - 1
    values = [
        measured,
        admitted,
        measured - admitted,
        admitted,
        0,
        0,
        admitted,
        0,
        0,
        0,
        0,
        0,
        100_000,
        100_000,
        100_000,
        1,
        100_000,
        100_000,
        0,
        0,
        0,
    ]
    return {"cell": logical, "offeredRate": rate, "tag": "interval", "values": values}


def successful_fault(candidate: int, population: int, latency: int) -> dict[str, object]:
    return {
        "candidate": CHECKER.CANDIDATES[candidate],
        "candidateIndex": candidate,
        "cuts": 0x1FF,
        "latency": latency,
        "population": population,
        "tag": "fault",
        "values": [0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
    }


def range64_campaign() -> tuple[list[dict[str, object]], list[tuple[int, int, tuple[int, ...]]]]:
    planner = CHECKER.Planner()
    observations: list[dict[str, object]] = []
    while (action := planner.next_action()) is not None:
        kind, payload = action
        if kind == "fault":
            candidate, population, latency = payload
            observation = successful_fault(candidate, population, latency)
        else:
            logical, rate = payload
            observation = interval(logical, rate, logical["candidate"] not in {"STRICT", "RANGE_16"})
        observations.append(observation)
        planner.accept(observation)
    if planner.qualified != [3] or planner.executed != 20 or len(observations) != 28:
        raise AssertionError("synthetic V5 RANGE-64 campaign shape differs")
    return observations, planner.dispositions


def physical_inventory(
    observations: list[dict[str, object]],
) -> tuple[list[str], list[tuple[str, bytes]]]:
    aggregates: list[str] = []
    files: list[tuple[str, bytes]] = []
    seen_rows: set[tuple[int, int, int]] = set()

    def add(prefix: str, suffix: str) -> str:
        raw = f"{prefix}-synthetic-payload".encode("ascii")
        value = hashlib.sha256(raw).hexdigest()
        files.append((f"{prefix}{value}{suffix}", raw))
        return value

    for observation in observations:
        values: list[str] = []
        if observation["tag"] == "interval":
            logical = observation["cell"]
            row = (logical["candidateIndex"], logical["population"], logical["latency"])
            first = row not in seen_rows
            seen_rows.add(row)
            if str(logical["candidate"]).startswith("RANGE_") and first:
                values.append(
                    add(
                        f"scale-{logical['candidate']}-{logical['population']}-{logical['latency']}-",
                        ".json",
                    )
                )
            values.append(add(f"interval-{logical['contextId']}-", ".json"))
        else:
            for cut in CHECKER.FAULT_CUTS:
                values.append(
                    add(
                        f"fault-{observation['candidate']}-{observation['population']}-"
                        f"{observation['latency']}-{cut}-",
                        ".nare1",
                    )
                )
        aggregate = hashlib.sha256(
            b"".join(bytes.fromhex(value) for value in values)
        ).hexdigest()
        aggregates.append(aggregate)
    return aggregates, sorted(files)


def encode_checkpoint() -> bytes:
    observations, dispositions = range64_campaign()
    aggregates, _ = physical_inventory(observations)
    source = {
        "nereusCommit": "a" * 40,
        "oxiaImageDigest": digest("oxia-image"),
        "dependencyLockDigest": digest("dependency-lock"),
        "executorDigest": digest("executor"),
        "workloadDigest": CHECKER.PLAN_DIGEST,
    }
    logical_campaign = CHECKER.logical_campaign_id(source)
    outer_campaign = CHECKER.campaign_id(source, logical_campaign)
    sequence = len(observations)
    interval_count = sum(row["tag"] == "interval" for row in observations)
    fault_count = len(observations) - interval_count
    ten_candidates: set[int] = set()
    hundred_candidates: set[int] = set()
    for observation in observations:
        logical = observation["cell"] if observation["tag"] == "interval" else observation
        target = ten_candidates if logical["population"] == 10_000 else hundred_candidates
        target.add(logical["candidateIndex"])
    logical_budgets = (
        0,
        5_400 - 900 * len(ten_candidates),
        7_200 - 180 * fault_count,
        5_400 - 900 * len(hundred_candidates),
        13_120 - 40 * interval_count,
        1_640 - 5 * interval_count,
        600,
    )
    outer_budgets = logical_budgets[:4] + (13_776 - 42 * interval_count,) + logical_budgets[5:]

    nested = bytearray(b"NACP3\0\0\0")
    nested.extend(unsigned(3, 2))
    nested.extend(b"\x01\x00")
    nested.extend(unsigned(sequence, 8))
    nested.extend(source_bytes(source))
    nested.extend(bytes.fromhex(logical_campaign))
    nested.extend(bytes.fromhex(digest("nested-predecessor")))
    for value in logical_budgets:
        nested.extend(unsigned(value, 8))
    nested.extend(unsigned(len(CHECKER.CELLS), 4))
    for logical in CHECKER.CELLS:
        nested.extend(unsigned(logical["contextId"], 4))
    nested.extend(unsigned(len(observations), 4))
    for index, observation in enumerate(observations):
        if observation["tag"] == "interval":
            nested.extend(b"\x01")
            nested.extend(unsigned(observation["cell"]["contextId"], 4))
            nested.extend(unsigned(observation["offeredRate"], 4))
            for value in observation["values"]:
                nested.extend(unsigned(value, 8))
        else:
            nested.extend(b"\x02")
            nested.extend(unsigned(observation["candidateIndex"], 1))
            nested.extend(unsigned(observation["population"], 4))
            nested.extend(unsigned(observation["latency"], 4))
            nested.extend(unsigned(observation["cuts"], 4))
            for value in observation["values"]:
                nested.extend(unsigned(value, 8))
        nested.extend(bytes.fromhex(aggregates[index]))
    nested.extend(unsigned(len(dispositions), 4))
    for context, kind, dependencies in dispositions:
        nested.extend(unsigned(context, 4))
        nested.extend(unsigned(kind, 1))
        nested.extend(unsigned(len(dependencies), 2))
        for dependency in dependencies:
            nested.extend(unsigned(dependency, 4))

    outer = bytearray(b"NACP5\0\0\0")
    outer.extend(unsigned(5, 2))
    outer.extend(b"\x01\x00")
    outer.extend(unsigned(sequence, 8))
    outer.extend(bytes.fromhex(CHECKER.EXECUTION_PROFILE_DIGEST))
    outer.extend(bytes.fromhex(CHECKER.PLAN_DIGEST))
    outer.extend(bytes.fromhex(outer_campaign))
    outer.extend(bytes.fromhex(digest("outer-predecessor")))
    for value in outer_budgets:
        outer.extend(unsigned(value, 8))
    outer.extend(unsigned(len(nested), 4))
    outer.extend(nested)
    return bytes(outer)


def encode_evaluation(checkpoint_raw: bytes, checkpoint: dict[str, object]) -> bytes:
    status, selected = CHECKER.expected_evaluation(checkpoint)
    raw = bytearray(b"NAEV5\0\0\0")
    raw.extend(unsigned(5, 2))
    raw.extend(unsigned(status, 1))
    raw.extend(unsigned(selected, 1))
    raw.extend(unsigned(checkpoint["executed"], 4))
    raw.extend(unsigned(checkpoint["dispositions"], 4))
    raw.extend(source_bytes(checkpoint["source"]))
    for value in (
        CHECKER.EXECUTION_PROFILE_DIGEST,
        CHECKER.PLAN_DIGEST,
        checkpoint["campaignId"],
        hashlib.sha256(checkpoint_raw).hexdigest(),
        checkpoint["attachmentRoot"],
    ):
        raw.extend(bytes.fromhex(value))
    return bytes(raw)


def encode_diagnostic(checkpoint: dict[str, object]) -> tuple[bytes, str, str]:
    junit = digest("diagnostic-junit-manifest")
    raw_manifest = digest("diagnostic-raw-manifest")
    raw = bytearray(b"NADV5\0\0\0")
    raw.extend(unsigned(5, 2))
    raw.extend(b"\x3f\x00")
    raw.extend(source_bytes(checkpoint["source"]))
    for value in (CHECKER.EXECUTION_PROFILE_DIGEST, CHECKER.PLAN_DIGEST, junit, raw_manifest):
        raw.extend(bytes.fromhex(value))
    return bytes(raw), junit, raw_manifest


def encode_selection(
    checkpoint: dict[str, object],
    evaluation: dict[str, object],
    diagnostic_raw: bytes,
    diagnostic_junit: str,
    diagnostic_raw_manifest: str,
) -> bytes:
    raw = bytearray(b"NARS5\0\0\0")
    raw.extend(unsigned(5, 2))
    raw.extend(unsigned(evaluation["selected"], 1))
    raw.extend(b"\x00")
    raw.extend(source_bytes(checkpoint["source"]))
    for value in (
        CHECKER.EXECUTION_PROFILE_DIGEST,
        CHECKER.PLAN_DIGEST,
        checkpoint["campaignId"],
        checkpoint["checkpointDigest"],
        evaluation["digest"],
        checkpoint["attachmentRoot"],
        hashlib.sha256(diagnostic_raw).hexdigest(),
        diagnostic_junit,
        diagnostic_raw_manifest,
        CHECKER.junit_summary_digest({"tests": 1, "failures": 0, "errors": 0, "skipped": 0}),
    ):
        raw.extend(bytes.fromhex(value))
    return bytes(raw)


class AllocatorV5CheckerContractTest(unittest.TestCase):
    def test_synthetic_range64_authority_replays_and_tampering_fails_closed(self) -> None:
        checkpoint_raw = encode_checkpoint()
        checkpoint = CHECKER.parse_checkpoint(checkpoint_raw)
        self.assertEqual("AVAILABLE", checkpoint["baseline"])
        self.assertEqual([3], checkpoint["qualified"])
        self.assertEqual(20, checkpoint["executed"])
        self.assertEqual(308, checkpoint["dispositions"])
        _, physical = physical_inventory(checkpoint["observations"])
        self.assertEqual(
            checkpoint["aggregates"], CHECKER.physical_aggregates(checkpoint, physical)
        )

        evaluation_raw = encode_evaluation(checkpoint_raw, checkpoint)
        evaluation = CHECKER.parse_evaluation(evaluation_raw, checkpoint)
        self.assertEqual(1, evaluation["status"])
        self.assertEqual(3, evaluation["selected"])

        diagnostic_raw, junit_manifest, raw_manifest = encode_diagnostic(checkpoint)
        diagnostic = CHECKER.parse_diagnostic(diagnostic_raw)
        selection_raw = encode_selection(
            checkpoint, evaluation, diagnostic_raw, junit_manifest, raw_manifest
        )
        selection = CHECKER.parse_selection(
            selection_raw,
            checkpoint,
            evaluation,
            diagnostic,
            {"tests": 1, "failures": 0, "errors": 0, "skipped": 0},
        )
        self.assertEqual("RANGE_64", selection["candidate"])
        self.assertEqual("RANGE", selection["mode"])

        tampered = bytearray(checkpoint_raw)
        tampered[-1] ^= 1
        with self.assertRaises(CHECKER.V5Error):
            CHECKER.parse_checkpoint(bytes(tampered))

        physical_tamper = list(physical)
        name, raw = physical_tamper[0]
        physical_tamper[0] = (name, raw + b"tampered")
        with self.assertRaises(CHECKER.V5Error):
            CHECKER.physical_aggregates(checkpoint, physical_tamper)

        tampered_selection = bytearray(selection_raw)
        tampered_selection[-1] ^= 1
        with self.assertRaisesRegex(CHECKER.V5Error, "source/digest wire"):
            CHECKER.parse_selection(
                bytes(tampered_selection),
                checkpoint,
                evaluation,
                diagnostic,
                {"tests": 1, "failures": 0, "errors": 0, "skipped": 0},
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
