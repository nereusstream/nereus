#!/usr/bin/env python3
"""Validate the global V2 M2 receipt and its exact child-root/scenario boundaries."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parent.parent
KAFKA_PATH = Path("docs/v2/evidence/v2-m2/kafka/k10/kafka-final.json")
PULSAR_PATH = Path("docs/v2/evidence/v2-m2/pulsar/final/pulsar-final.json")
RECEIPT_PATH = Path("docs/v2/evidence/v2-m2/final/m2-final.json")
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
    raise SystemExit(f"V2 M2 Final gate: {message}")


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


def child_identity(path: Path, kind: str, receipt: dict, count: int) -> dict:
    absolute = ROOT / path
    if not absolute.is_file() or absolute.is_symlink():
        fail(f"child receipt is absent or symbolic: {path}")
    return {
        "bytes": absolute.stat().st_size,
        "kind": kind,
        "path": str(path),
        "result": receipt["result"],
        "scenarios": count,
        "sha256": sha(path),
    }


receipt = load(RECEIPT_PATH)
kafka = load(KAFKA_PATH)
pulsar = load(PULSAR_PATH)
registry = load(Path("docs/v2/v2-scenarios.json"))
source = receipt.get("sourceTuple", {})
tested = source.get("nereusCommit")
if not isinstance(tested, str) or len(tested) != 40:
    fail("tested Nereus source is not canonical")
if subprocess.run(
    ["git", "merge-base", "--is-ancestor", tested, "HEAD"], cwd=ROOT, check=False
).returncode != 0:
    fail("receipt is not an evidence descendant of its tested source")

kafka_ids = {row.get("scenarioId") for row in kafka.get("scenarios", [])}
pulsar_ids = {row.get("scenarioId") for row in pulsar.get("scenarios", [])}
if (
    kafka.get("result") != "PASS_KAFKA_M2_FINAL"
    or kafka.get("promotionEligible") is not True
    or kafka_ids != KAFKA_SCENARIOS
):
    fail("Kafka Final child identity/result/scenario set differs")
if (
    pulsar.get("result") != "PASS_PULSAR_M2_FINAL"
    or pulsar.get("promotionEligible") is not True
    or pulsar_ids != PULSAR_SCENARIOS
):
    fail("Pulsar Final child identity/result/scenario set differs")
if kafka_ids & pulsar_ids or len(kafka_ids | pulsar_ids) != 21:
    fail("child scenario sets overlap or do not form exactly 21 rows")

kafka_source = kafka.get("sourceTuple", {})
pulsar_source = pulsar.get("sourceTuple", {})
expected_source = {
    "bookKeeperSourceCommit": kafka_source.get("bookKeeperSourceCommit"),
    "kafkaFinalReceiptSha256": sha(KAFKA_PATH),
    "kafkaForkCommit": kafka_source.get("kafkaForkCommit"),
    "nereusCommit": tested,
    "pulsarFinalReceiptSha256": sha(PULSAR_PATH),
    "pulsarForkCommit": pulsar_source.get("pulsarForkCommit"),
    "sourceLocksSha256": sha(Path("docs/v2/source-locks.json")),
}
if (
    source != expected_source
    or kafka_source.get("nereusCommit") != tested
    or pulsar_source.get("nereusCommit") != tested
    or kafka_source.get("sourceLocksSha256") != source["sourceLocksSha256"]
    or pulsar_source.get("sourceLocksSha256") != source["sourceLocksSha256"]
    or pulsar_source.get("kafkaFinalReceiptSha256") != source["kafkaFinalReceiptSha256"]
):
    fail("global and child roots do not bind one exact source tuple")

expected_children = [
    child_identity(KAFKA_PATH, "KAFKA_M2_FINAL", kafka, 10),
    child_identity(PULSAR_PATH, "PULSAR_M2_FINAL", pulsar, 11),
]
if (
    receipt.get("schema") != "NEREUS_V2_M2_FINAL_V1"
    or receipt.get("kind") != "V2_M2_FINAL"
    or receipt.get("result") != "PASS_V2_M2_FINAL"
    or receipt.get("promotionEligible") is not True
    or receipt.get("childReceipts") != expected_children
    or receipt.get("promotedScenarios") != sorted(kafka_ids | pulsar_ids)
    or receipt.get("boundaries") != {
        "excludedMilestones": ["M3_OBJECT_WAL", "M6_PROCESS_ACTIVATION", "M8_NATIVE_PARITY"],
        "plannedScenarioIds": sorted(PLANNED_BOUNDARIES),
    }
):
    fail("global receipt schema, child roots, scenarios, or boundaries differ")

registry_rows = {row["id"]: row for row in registry.get("scenarios", [])}
expected_bindings = {
    **{scenario: str(KAFKA_PATH) for scenario in KAFKA_SCENARIOS},
    **{scenario: str(PULSAR_PATH) for scenario in PULSAR_SCENARIOS},
}
for scenario, path in expected_bindings.items():
    row = registry_rows.get(scenario, {})
    if row.get("status") != "PASSED_CURRENT_SOURCE" or row.get("evidenceReceipt") != path:
        fail(f"scenario does not retain its exact child receipt: {scenario}")
for scenario in PLANNED_BOUNDARIES:
    row = registry_rows.get(scenario, {})
    if row.get("status") != "PLANNED" or row.get("evidenceReceipt") is not None:
        fail(f"downstream scenario was prematurely promoted: {scenario}")

matrix_status = {}
for line in (ROOT / "docs/v2/09-scenario-evidence-matrix.md").read_text().splitlines():
    if line.startswith("| V2-"):
        columns = [column.strip() for column in line.strip("|").split("|")]
        if len(columns) == 5:
            matrix_status[columns[0]] = columns[4]
for scenario, row in registry_rows.items():
    if matrix_status.get(scenario) != row.get("status"):
        fail(f"scenario registries disagree: {scenario}")

index = (ROOT / "docs/v2/detailed_design/m2/README.md").read_text()
for literal in (
    "implementationStatus: Verified",
    "evidenceStatus: CurrentSourceReceipt",
    "receipt: docs/v2/evidence/v2-m2/final/m2-final.json",
    "PASS_V2_M2_FINAL",
):
    if literal not in index:
        fail(f"M2 index lacks current global status: {literal}")

print(
    "V2 M2 Final verified: "
    f"source={tested} children=2 scenarios=21 plannedBoundaries=2; "
    "M3/M6/M8 remain excluded"
)
