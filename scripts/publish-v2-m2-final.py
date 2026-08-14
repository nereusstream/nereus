#!/usr/bin/env python3
"""Publish the global V2 M2 receipt from the exact Kafka and Pulsar Final child roots."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys


ROOT = Path(__file__).resolve().parent.parent
KAFKA_PATH = Path("docs/v2/evidence/v2-m2/kafka/k10/kafka-final.json")
PULSAR_PATH = Path("docs/v2/evidence/v2-m2/pulsar/final/pulsar-final.json")
RECEIPT_PATH = Path("docs/v2/evidence/v2-m2/final/m2-final.json")
REGISTRY_PATH = Path("docs/v2/v2-scenarios.json")
SOURCE_LOCKS_PATH = Path("docs/v2/source-locks.json")
KAFKA_SCENARIOS = {
    "V2-BK-003", "V2-BK-014", "V2-BK-015", "V2-BK-016", "V2-BK-017",
    "V2-KAF-DATA-001", "V2-KAF-DATA-002", "V2-KAF-DATA-004", "V2-KAF-DATA-005",
    "V2-KAF-DATA-014",
}
PULSAR_SCENARIOS = {
    "V2-BK-001", "V2-BK-002", "V2-BK-004", "V2-BK-005", "V2-BK-006",
    "V2-BK-007", "V2-BK-008", "V2-BK-009", "V2-BK-010", "V2-BK-012", "V2-BK-013",
}
PLANNED_BOUNDARIES = {"V2-BK-011", "V2-PUL-001"}


def fail(message: str) -> None:
    raise SystemExit(f"V2 M2 Final publisher: {message}")


def load(path: Path) -> dict:
    try:
        value = json.loads((ROOT / path).read_text())
    except (OSError, json.JSONDecodeError) as error:
        fail(f"cannot read JSON {path}: {error}")
    if not isinstance(value, dict):
        fail(f"JSON root is not an object: {path}")
    return value


def sha(path: Path) -> str:
    return hashlib.sha256((ROOT / path).read_bytes()).hexdigest()


def child_row(path: Path, kind: str, receipt: dict, scenarios: set[str]) -> dict:
    absolute = ROOT / path
    if not absolute.is_file() or absolute.is_symlink():
        fail(f"child receipt is absent or symbolic: {path}")
    return {
        "bytes": absolute.stat().st_size,
        "kind": kind,
        "path": str(path),
        "result": receipt["result"],
        "scenarios": len(scenarios),
        "sha256": sha(path),
    }


def main() -> None:
    if len(sys.argv) != 2 or not re.fullmatch(r"[0-9a-f]{40}", sys.argv[1]):
        fail("usage: publish-v2-m2-final.py <tested-source-commit>")
    tested = sys.argv[1]
    if subprocess.run(
        ["git", "merge-base", "--is-ancestor", tested, "HEAD"], cwd=ROOT, check=False
    ).returncode != 0:
        fail("tested source is not an ancestor of HEAD")

    kafka = load(KAFKA_PATH)
    pulsar = load(PULSAR_PATH)
    registry = load(REGISTRY_PATH)
    kafka_ids = {row.get("scenarioId") for row in kafka.get("scenarios", [])}
    pulsar_ids = {row.get("scenarioId") for row in pulsar.get("scenarios", [])}
    if (
        kafka.get("schema") != "NEREUS_V2_M2_KAFKA_FINAL_RECEIPT_V1"
        or kafka.get("result") != "PASS_KAFKA_M2_FINAL"
        or kafka.get("promotionEligible") is not True
        or kafka_ids != KAFKA_SCENARIOS
    ):
        fail("Kafka Final identity, result, or exact scenario set differs")
    if (
        pulsar.get("schema") != "NEREUS_V2_M2_PULSAR_FINAL_RECEIPT_V1"
        or pulsar.get("result") != "PASS_PULSAR_M2_FINAL"
        or pulsar.get("promotionEligible") is not True
        or pulsar_ids != PULSAR_SCENARIOS
    ):
        fail("Pulsar Final identity, result, or exact scenario set differs")
    if kafka_ids & pulsar_ids or len(kafka_ids | pulsar_ids) != 21:
        fail("child scenario sets overlap or do not form the exact 21-row aggregate")

    kafka_source = kafka.get("sourceTuple", {})
    pulsar_source = pulsar.get("sourceTuple", {})
    source_locks_sha = sha(SOURCE_LOCKS_PATH)
    if (
        kafka_source.get("nereusCommit") != tested
        or pulsar_source.get("nereusCommit") != tested
        or kafka_source.get("sourceLocksSha256") != source_locks_sha
        or pulsar_source.get("sourceLocksSha256") != source_locks_sha
        or pulsar_source.get("kafkaFinalReceiptSha256") != sha(KAFKA_PATH)
    ):
        fail("child roots do not bind the same tested Nereus/source-lock tuple")

    registry_rows = {row["id"]: row for row in registry.get("scenarios", [])}
    child_paths = {
        **{scenario: str(KAFKA_PATH) for scenario in KAFKA_SCENARIOS},
        **{scenario: str(PULSAR_PATH) for scenario in PULSAR_SCENARIOS},
    }
    for scenario, path in child_paths.items():
        row = registry_rows.get(scenario, {})
        if row.get("status") != "PASSED_CURRENT_SOURCE" or row.get("evidenceReceipt") != path:
            fail(f"scenario registry does not bind the exact child receipt: {scenario}")
    for scenario in PLANNED_BOUNDARIES:
        row = registry_rows.get(scenario, {})
        if row.get("status") != "PLANNED" or row.get("evidenceReceipt") is not None:
            fail(f"downstream boundary was prematurely promoted: {scenario}")

    receipt = {
        "schema": "NEREUS_V2_M2_FINAL_V1",
        "kind": "V2_M2_FINAL",
        "result": "PASS_V2_M2_FINAL",
        "promotionEligible": True,
        "sourceTuple": {
            "bookKeeperSourceCommit": kafka_source["bookKeeperSourceCommit"],
            "kafkaFinalReceiptSha256": sha(KAFKA_PATH),
            "kafkaForkCommit": kafka_source["kafkaForkCommit"],
            "nereusCommit": tested,
            "pulsarFinalReceiptSha256": sha(PULSAR_PATH),
            "pulsarForkCommit": pulsar_source["pulsarForkCommit"],
            "sourceLocksSha256": source_locks_sha,
        },
        "childReceipts": [
            child_row(KAFKA_PATH, "KAFKA_M2_FINAL", kafka, KAFKA_SCENARIOS),
            child_row(PULSAR_PATH, "PULSAR_M2_FINAL", pulsar, PULSAR_SCENARIOS),
        ],
        "promotedScenarios": sorted(kafka_ids | pulsar_ids),
        "boundaries": {
            "excludedMilestones": ["M3_OBJECT_WAL", "M6_PROCESS_ACTIVATION", "M8_NATIVE_PARITY"],
            "plannedScenarioIds": sorted(PLANNED_BOUNDARIES),
        },
    }
    absolute = ROOT / RECEIPT_PATH
    absolute.parent.mkdir(parents=True, exist_ok=True)
    absolute.write_text(json.dumps(receipt, indent=2) + "\n")
    print(
        "V2 M2 Final published: "
        f"source={tested} children=2 scenarios={len(receipt['promotedScenarios'])} "
        "plannedBoundaries=2"
    )


if __name__ == "__main__":
    main()
