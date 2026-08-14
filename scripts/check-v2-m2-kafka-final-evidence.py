#!/usr/bin/env python3
# Licensed under the Apache License, Version 2.0.

from __future__ import annotations

from pathlib import Path
import hashlib
import json
import subprocess
import xml.etree.ElementTree as ET


ROOT = Path(__file__).resolve().parent.parent
RECEIPT_PATH = Path("docs/v2/evidence/v2-m2/kafka/k10/kafka-final.json")
K0_PATH = Path("docs/v2/evidence/v2-m2/kafka/k0-inputs/kafka-inputs.json")
K9_PATH = Path("docs/v2/evidence/v2-m2/kafka/k9/k9-evidence.json")
LOCAL_PATH = Path("docs/v2/evidence/v2-m2/kafka/k10/attachments/local-junit-summary.json")
EXACT_KAFKA_PATH = Path("docs/v2/evidence/v2-m2/kafka/k10/attachments/exact-kafka-result.json")
REAL_PATH = Path("docs/v2/evidence/v2-m2/kafka/k9/attachments/fault-tests.json")
SCALE_PATHS = {
    "nereus.kafka.m2.k9.scale.10000": Path(
        "docs/v2/evidence/v2-m2/kafka/k9/attachments/scale-10000.json"
    ),
    "nereus.kafka.m2.k9.scale.100000": Path(
        "docs/v2/evidence/v2-m2/kafka/k9/attachments/scale-100000.json"
    ),
}
EXPECTED_SCENARIOS = {
    "V2-BK-003",
    "V2-BK-014",
    "V2-BK-015",
    "V2-BK-016",
    "V2-BK-017",
    "V2-KAF-DATA-001",
    "V2-KAF-DATA-002",
    "V2-KAF-DATA-004",
    "V2-KAF-DATA-005",
    "V2-KAF-DATA-014",
}
EXACT_KAFKA_SUITE = "org.apache.kafka.clients.record.Kafka43AssignedRecordBatchConformance"
REAL_SUITES = {
    "com.nereusstream.kafka.bookkeeper.RealBookKeeperKafkaEngineV1Test",
    "com.nereusstream.storage.bookkeeper.RealBookKeeperCellSessionV1RealTest",
}


def fail(message: str) -> None:
    raise SystemExit(f"Kafka M2 Final gate: {message}")


def load(path: Path) -> dict:
    try:
        return json.loads((ROOT / path).read_bytes())
    except (OSError, json.JSONDecodeError) as error:
        fail(f"cannot read canonical input {path}: {error}")


def sha(path: Path) -> str:
    return hashlib.sha256((ROOT / path).read_bytes()).hexdigest()


def git_lines(*arguments: str) -> list[str]:
    return subprocess.check_output(["git", *arguments], cwd=ROOT, text=True).splitlines()


receipt = load(RECEIPT_PATH)
k0 = load(K0_PATH)
k9 = load(K9_PATH)
source = receipt.get("sourceTuple", {})
tested = source.get("nereusCommit")
if not isinstance(tested, str) or len(tested) != 40:
    fail("tested Nereus commit is not canonical")
if subprocess.run(
    ["git", "merge-base", "--is-ancestor", tested, "HEAD"], cwd=ROOT, check=False
).returncode != 0:
    fail("HEAD is not an evidence descendant of the tested source")

allowed_evidence_paths = (
    "build.gradle.kts",
    "docs/v2/08-implementation-plan-and-gates.md",
    "docs/v2/09-scenario-evidence-matrix.md",
    "docs/v2/open-questions.md",
    "docs/v2/v2-scenarios.json",
    "docs/v2/detailed_design/m2/README.md",
    "docs/v2/detailed_design/m2/kafka-m2-k9-real-bookkeeper-evidence.md",
    "docs/v2/detailed_design/m2/kafka-m2-k10-final-evidence.md",
    "docs/v2/detailed_design/m2/pulsar-m2-p6-provider-and-block-policy.md",
    "docs/v2/detailed_design/m2/pulsar-m2-final-evidence.md",
    "docs/v2/evidence/v2-m2/kafka/k0-inputs/kafka-inputs.json",
    "docs/v2/evidence/v2-m2/kafka/k9/",
    "docs/v2/evidence/v2-m2/kafka/k10/",
    "docs/v2/evidence/v2-m2/pulsar/p6/",
    "docs/v2/evidence/v2-m2/pulsar/final/",
    "docs/v2/evidence/v2-m2/final/",
    "scripts/check-v2-documentation.sh",
    "scripts/check-v2-m2-kafka-inputs-source.sh",
    "scripts/check-v2-m2-kafka-k9-evidence.sh",
    "scripts/check-v2-m2-kafka-final-evidence.py",
    "scripts/check-v2-m2-pulsar-p6.py",
    "scripts/check-v2-m2-pulsar-final-evidence.py",
    "scripts/check-v2-m2-final.py",
    "scripts/publish-v2-m2-kafka-k9-evidence.py",
    "scripts/publish-v2-m2-kafka-final-evidence.py",
    "scripts/publish-v2-m2-pulsar-final-evidence.py",
    "scripts/publish-v2-m2-final.py",
)
changed = set(git_lines("diff", "--name-only", f"{tested}..HEAD"))
changed.update(git_lines("diff", "--name-only"))
changed.update(git_lines("diff", "--cached", "--name-only"))
changed.update(git_lines("ls-files", "--others", "--exclude-standard"))
for path in sorted(changed):
    if not any(path == allowed or path.startswith(allowed) for allowed in allowed_evidence_paths):
        fail(f"evidence descendant changes tested production/configuration path: {path}")

if k9.get("testedSourceCommit") != tested:
    fail("K9 evidence is not bound to the Kafka Final tested source")
if source != {
    "bookKeeperSourceCommit": k0["sourceTuple"]["bookKeeperSourceCommit"],
    "k9EvidenceReceiptSha256": sha(K9_PATH),
    "kafkaForkCommit": k0["sourceTuple"]["kafkaForkCommit"],
    "kafkaInputsReceiptSha256": sha(K0_PATH),
    "nereusCommit": tested,
    "sourceLocksSha256": sha(Path("docs/v2/source-locks.json")),
}:
    fail("source tuple differs from current K0, K9, source locks, or tested commit")

attachments = receipt.get("attachments", [])
expected_attachment_kinds = {
    str(K0_PATH): "K0_INPUTS_RECEIPT",
    str(K9_PATH): "K9_EVIDENCE_RECEIPT",
    str(LOCAL_PATH): "LOCAL_JUNIT_REPORT",
    str(EXACT_KAFKA_PATH): "EXACT_KAFKA_RESULT",
    str(REAL_PATH): "REAL_BOOKKEEPER_JUNIT_REPORT",
    str(SCALE_PATHS["nereus.kafka.m2.k9.scale.10000"]): "SCALE_RESULT",
    str(SCALE_PATHS["nereus.kafka.m2.k9.scale.100000"]): "SCALE_RESULT",
}
if [row.get("path") for row in attachments] != sorted(expected_attachment_kinds):
    fail("attachment paths are not the exact sorted seven-file set")
for row in attachments:
    relative = Path(row["path"])
    absolute = ROOT / relative
    if row.get("kind") != expected_attachment_kinds[row["path"]]:
        fail(f"attachment kind differs: {relative}")
    if not absolute.is_file() or absolute.is_symlink():
        fail(f"attachment is not a regular non-symbolic file: {relative}")
    if row.get("bytes") != absolute.stat().st_size or row.get("sha256") != sha(relative):
        fail(f"attachment length or digest differs: {relative}")

local = load(LOCAL_PATH)
if (
    local.get("schema") != "NEREUS_V2_M2_KAFKA_K10_LOCAL_JUNIT_SUMMARY_V1"
    or local.get("result") != "PASS"
    or local.get("testedSourceCommit") != tested
):
    fail("local JUnit summary identity/source differs")
local_rows = local.get("suiteResults", [])
local_names = [row.get("name") for row in local_rows]
if local_names != sorted(local_names) or len(local_names) != 22 or len(set(local_names)) != 22:
    fail("local JUnit suite allowlist is not exact, sorted, and unique")
local_totals = {key: 0 for key in ("tests", "failures", "errors", "skipped")}
local_by_name = {}
for row in local_rows:
    report = ROOT / "nereus-kafka-bookkeeper/build/test-results/test" / f"TEST-{row['name']}.xml"
    if not report.is_file():
        fail(f"current local JUnit report is absent: {row['name']}")
    attributes = ET.parse(report).getroot().attrib
    actual = {key: int(attributes[key]) for key in local_totals}
    expected = {key: row.get(key) for key in local_totals}
    if actual != expected or any(actual[key] != 0 for key in ("failures", "errors", "skipped")):
        fail(f"current local JUnit result differs: {row['name']}")
    for key, value in actual.items():
        local_totals[key] += value
    local_by_name[row["name"]] = row
if local_totals != {"tests": 142, "failures": 0, "errors": 0, "skipped": 0}:
    fail(f"local JUnit totals differ: {local_totals}")
if any(local.get(key) != value for key, value in {"suites": 22, **local_totals}.items()):
    fail("local JUnit summary totals differ")

exact = load(EXACT_KAFKA_PATH)
template = Path("scripts/templates/v2-m2-kafka-k2/Kafka43AssignedRecordBatchConformance.java")
if (
    exact.get("schema") != "NEREUS_V2_M2_KAFKA_K10_EXACT_KAFKA_RESULT_V1"
    or exact.get("result") != "PASS"
    or exact.get("testedSourceCommit") != tested
    or exact.get("kafkaForkCommit") != source["kafkaForkCommit"]
    or exact.get("templateSha256") != sha(template)
    or {key: exact.get(key) for key in ("suites", "tests", "failures", "errors", "skipped")}
    != {"suites": 1, "tests": 13, "failures": 0, "errors": 0, "skipped": 0}
):
    fail("exact Kafka result identity, source, template, or totals differ")
exact_rows = exact.get("suiteResults", [])
if exact_rows != [
    {"name": EXACT_KAFKA_SUITE, "tests": 13, "failures": 0, "errors": 0, "skipped": 0}
]:
    fail("exact Kafka named result differs")

real = load(REAL_PATH)
if (
    real.get("result") != "PASS"
    or real.get("testedSourceCommit") != tested
    or {key: real.get(key) for key in ("suites", "tests", "failures", "errors", "skipped")}
    != {"suites": 2, "tests": 9, "failures": 0, "errors": 0, "skipped": 0}
):
    fail("real BookKeeper named result differs")
real_by_name = {row["name"]: row for row in real.get("suiteResults", [])}
if set(real_by_name) != REAL_SUITES:
    fail("real BookKeeper suite set differs")

scale_by_name = {}
for suite_id, path in SCALE_PATHS.items():
    scale = load(path)
    tier = int(suite_id.rsplit(".", 1)[1])
    if (
        scale.get("result") != "PASS"
        or scale.get("testedSourceCommit") != tested
        or scale.get("tierPartitions") != tier
        or scale.get("counts", {}).get("partitions") != tier
    ):
        fail(f"scale result/source/tier differs: {suite_id}")
    scale_by_name[suite_id] = scale

scenarios = receipt.get("scenarios", [])
if [row.get("scenarioId") for row in scenarios] != sorted(EXPECTED_SCENARIOS):
    fail("receipt scenario set differs from the exact ten-scenario promotion set")
suite_references = 0
referenced_local = set()
for scenario in scenarios:
    suite_ids = [row.get("suiteId") for row in scenario.get("suites", [])]
    if suite_ids != sorted(suite_ids) or len(suite_ids) != len(set(suite_ids)):
        fail(f"suite rows are not sorted and unique: {scenario['scenarioId']}")
    for row in scenario["suites"]:
        suite_id = row["suiteId"]
        invariant = {key: row.get(key) for key in ("failed", "errors", "skipped")}
        if invariant != {"failed": 0, "errors": 0, "skipped": 0}:
            fail(f"non-PASS suite reference: {suite_id}")
        if suite_id == EXACT_KAFKA_SUITE:
            expected_path, tests = EXACT_KAFKA_PATH, 13
        elif suite_id in REAL_SUITES:
            expected_path, tests = REAL_PATH, real_by_name[suite_id]["tests"]
        elif suite_id in SCALE_PATHS:
            expected_path, tests = SCALE_PATHS[suite_id], scale_by_name[suite_id]["tierPartitions"]
        elif suite_id in local_by_name:
            expected_path, tests = LOCAL_PATH, local_by_name[suite_id]["tests"]
            referenced_local.add(suite_id)
        else:
            fail(f"suite reference lacks a current named result: {suite_id}")
        if row.get("attachmentPath") != str(expected_path) or row.get("tests") != tests:
            fail(f"suite attachment or count differs: {suite_id}")
        suite_references += 1
if suite_references != 40 or referenced_local != set(local_by_name):
    fail("receipt does not reference exactly 40 policy suites and all 22 local named suites")

registry = load(Path("docs/v2/v2-scenarios.json"))
registry_rows = {row["id"]: row for row in registry.get("scenarios", [])}
final_rows = {
    scenario_id
    for scenario_id, row in registry_rows.items()
    if row.get("evidenceReceipt") == str(RECEIPT_PATH)
}
if final_rows != EXPECTED_SCENARIOS:
    fail("machine-readable registry binds Kafka Final to an extra or missing scenario")
for scenario_id in EXPECTED_SCENARIOS:
    if registry_rows[scenario_id].get("status") != "PASSED_CURRENT_SOURCE":
        fail(f"machine-readable scenario is not promoted: {scenario_id}")
for scenario_id, row in registry_rows.items():
    mixed_kafka = scenario_id.startswith("V2-KAF-DATA-") and "/" in row.get("milestone", "")
    mixed_bookkeeper = scenario_id == "V2-BK-011"
    if (mixed_kafka or mixed_bookkeeper) and (
        row.get("status") != "PLANNED" or row.get("evidenceReceipt") is not None
    ):
        fail(f"mixed downstream scenario was prematurely promoted: {scenario_id}")

matrix_status = {}
for line in (ROOT / "docs/v2/09-scenario-evidence-matrix.md").read_text().splitlines():
    if line.startswith("| V2-"):
        columns = [column.strip() for column in line.strip("|").split("|")]
        if len(columns) == 5:
            matrix_status[columns[0]] = columns[4]
for scenario_id in EXPECTED_SCENARIOS:
    if matrix_status.get(scenario_id) != "PASSED_CURRENT_SOURCE":
        fail(f"Markdown scenario is not synchronized: {scenario_id}")
for scenario_id, row in registry_rows.items():
    if matrix_status.get(scenario_id) != row.get("status"):
        fail(f"scenario registries disagree: {scenario_id}")

print(
    "Kafka M2 Final evidence verified: source="
    f"{tested} scenarios=10 suiteReferences=40 attachments=7 local=22/142 exactKafka=1/13 "
    "realBookKeeper=2/9; Kafka-only promotion boundary preserved"
)
