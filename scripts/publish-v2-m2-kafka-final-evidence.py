#!/usr/bin/env python3
"""Publish the canonical source-bound Kafka M2 Final evidence receipt."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import xml.etree.ElementTree as ET


ROOT = Path(__file__).resolve().parent.parent
FINAL_DIR = ROOT / "docs/v2/evidence/v2-m2/kafka/k10"
ATTACHMENT_DIR = FINAL_DIR / "attachments"
K0 = Path("docs/v2/evidence/v2-m2/kafka/k0-inputs/kafka-inputs.json")
K9 = Path("docs/v2/evidence/v2-m2/kafka/k9/k9-evidence.json")
REAL = Path("docs/v2/evidence/v2-m2/kafka/k9/attachments/fault-tests.json")
SCALE_10K = Path("docs/v2/evidence/v2-m2/kafka/k9/attachments/scale-10000.json")
SCALE_100K = Path("docs/v2/evidence/v2-m2/kafka/k9/attachments/scale-100000.json")
LOCAL = Path("docs/v2/evidence/v2-m2/kafka/k10/attachments/local-junit-summary.json")
EXACT = Path("docs/v2/evidence/v2-m2/kafka/k10/attachments/exact-kafka-result.json")
RECEIPT = Path("docs/v2/evidence/v2-m2/kafka/k10/kafka-final.json")
EXACT_SUITE = "org.apache.kafka.clients.record.Kafka43AssignedRecordBatchConformance"


def fail(message: str) -> None:
    raise SystemExit(f"Kafka M2 Final publisher: {message}")


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


def write(path: Path, value: dict) -> None:
    absolute = ROOT / path
    absolute.parent.mkdir(parents=True, exist_ok=True)
    absolute.write_text(json.dumps(value, indent=2, sort_keys=False) + "\n")


def local_summary(tested: str) -> tuple[dict, dict[str, dict]]:
    previous = load(LOCAL)
    names = [row.get("name") for row in previous.get("suiteResults", [])]
    if names != sorted(names) or len(names) != 22 or len(set(names)) != 22:
        fail("existing local suite allowlist is not exact, sorted, and unique")
    rows = []
    totals = {key: 0 for key in ("tests", "failures", "errors", "skipped")}
    for name in names:
        path = ROOT / "nereus-kafka-bookkeeper/build/test-results/test" / f"TEST-{name}.xml"
        if not path.is_file() or path.is_symlink():
            fail(f"current local JUnit report is absent or symbolic: {name}")
        attributes = ET.parse(path).getroot().attrib
        row = {"name": name}
        for key in totals:
            row[key] = int(attributes[key])
            totals[key] += row[key]
        if any(row[key] != 0 for key in ("failures", "errors", "skipped")):
            fail(f"current local JUnit result is not PASS: {name}")
        rows.append(row)
    if totals != {"tests": 142, "failures": 0, "errors": 0, "skipped": 0}:
        fail(f"local JUnit totals differ: {totals}")
    summary = {
        "schema": "NEREUS_V2_M2_KAFKA_K10_LOCAL_JUNIT_SUMMARY_V1",
        "result": "PASS",
        "testedSourceCommit": tested,
        "suites": len(rows),
        **totals,
        "suiteResults": rows,
    }
    return summary, {row["name"]: row for row in rows}


def exact_result(tested: str, kafka_checkout: Path, kafka_commit: str) -> dict:
    if not kafka_checkout.is_dir() or not (kafka_checkout / ".git").exists():
        fail("Kafka checkout is not a Git repository")
    process = subprocess.run(
        ["bash", "scripts/check-v2-m2-kafka-k2.sh", str(kafka_checkout)],
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    expected = "Kafka 4.3 K2 exact-source conformance: suites=1 tests=13 failures=0 errors=0 skips=0"
    if process.returncode != 0 or expected not in process.stdout:
        fail(f"exact Kafka conformance did not PASS:\n{process.stdout[-4000:]}")
    template = Path("scripts/templates/v2-m2-kafka-k2/Kafka43AssignedRecordBatchConformance.java")
    return {
        "schema": "NEREUS_V2_M2_KAFKA_K10_EXACT_KAFKA_RESULT_V1",
        "result": "PASS",
        "testedSourceCommit": tested,
        "kafkaForkCommit": kafka_commit,
        "templateSha256": sha(template),
        "suites": 1,
        "tests": 13,
        "failures": 0,
        "errors": 0,
        "skipped": 0,
        "suiteResults": [
            {
                "name": EXACT_SUITE,
                "tests": 13,
                "failures": 0,
                "errors": 0,
                "skipped": 0,
            }
        ],
    }


def main() -> None:
    if len(sys.argv) != 3:
        fail("usage: publish-v2-m2-kafka-final-evidence.py <tested-source-commit> <kafka-checkout>")
    tested = sys.argv[1]
    kafka_checkout = Path(sys.argv[2]).resolve()
    if not re.fullmatch(r"[0-9a-f]{40}", tested):
        fail("tested source commit is not canonical")
    if subprocess.run(
        ["git", "-C", str(ROOT), "merge-base", "--is-ancestor", tested, "HEAD"], check=False
    ).returncode != 0:
        fail("tested source commit is not an ancestor of HEAD")

    old_receipt = load(RECEIPT)
    k0 = load(K0)
    k9 = load(K9)
    locks = load(Path("docs/v2/source-locks.json"))
    kafka_commit = locks["m2KafkaK0InputSourceBinding"]["kafkaInput"]["forkCommit"]
    if k0.get("sourceTuple", {}).get("nereusCommit") != tested:
        fail("K0 receipt is not bound to the tested source")
    if k9.get("testedSourceCommit") != tested:
        fail("K9 receipt is not bound to the tested source")

    local, local_by_name = local_summary(tested)
    exact = exact_result(tested, kafka_checkout, kafka_commit)
    write(LOCAL, local)
    write(EXACT, exact)

    real = load(REAL)
    real_by_name = {row["name"]: row for row in real.get("suiteResults", [])}
    scale_by_name = {
        "nereus.kafka.m2.k9.scale.10000": load(SCALE_10K),
        "nereus.kafka.m2.k9.scale.100000": load(SCALE_100K),
    }
    for scenario in old_receipt.get("scenarios", []):
        for row in scenario.get("suites", []):
            suite = row["suiteId"]
            if suite == EXACT_SUITE:
                expected_path, tests = EXACT, 13
            elif suite in local_by_name:
                expected_path, tests = LOCAL, local_by_name[suite]["tests"]
            elif suite in real_by_name:
                expected_path, tests = REAL, real_by_name[suite]["tests"]
            elif suite in scale_by_name:
                expected_path = SCALE_10K if suite.endswith("10000") else SCALE_100K
                tests = scale_by_name[suite]["tierPartitions"]
            else:
                fail(f"scenario suite lacks a current named result: {suite}")
            if (
                row.get("attachmentPath") != str(expected_path)
                or row.get("tests") != tests
                or {key: row.get(key) for key in ("failed", "errors", "skipped")}
                != {"failed": 0, "errors": 0, "skipped": 0}
            ):
                fail(f"scenario suite reference differs: {scenario['scenarioId']}:{suite}")

    kinds = {
        K0: "K0_INPUTS_RECEIPT",
        EXACT: "EXACT_KAFKA_RESULT",
        LOCAL: "LOCAL_JUNIT_REPORT",
        REAL: "REAL_BOOKKEEPER_JUNIT_REPORT",
        SCALE_10K: "SCALE_RESULT",
        SCALE_100K: "SCALE_RESULT",
        K9: "K9_EVIDENCE_RECEIPT",
    }
    attachments = []
    for path in sorted(kinds, key=str):
        absolute = ROOT / path
        if not absolute.is_file() or absolute.is_symlink():
            fail(f"attachment is absent or symbolic: {path}")
        attachments.append(
            {
                "bytes": absolute.stat().st_size,
                "kind": kinds[path],
                "path": str(path),
                "sha256": sha(path),
            }
        )
    receipt = {
        "attachments": attachments,
        "kind": "KAFKA_M2_FINAL",
        "promotionEligible": True,
        "result": "PASS_KAFKA_M2_FINAL",
        "scenarios": old_receipt["scenarios"],
        "schema": "NEREUS_V2_M2_KAFKA_FINAL_RECEIPT_V1",
        "sourceTuple": {
            "bookKeeperSourceCommit": k0["sourceTuple"]["bookKeeperSourceCommit"],
            "k9EvidenceReceiptSha256": sha(K9),
            "kafkaForkCommit": kafka_commit,
            "kafkaInputsReceiptSha256": sha(K0),
            "nereusCommit": tested,
            "sourceLocksSha256": sha(Path("docs/v2/source-locks.json")),
        },
    }
    encoded = json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode()
    (ROOT / RECEIPT).write_bytes(encoded)
    print(
        "Kafka M2 Final evidence published: "
        f"source={tested} scenarios={len(receipt['scenarios'])} "
        f"suiteReferences={sum(len(row['suites']) for row in receipt['scenarios'])} attachments={len(attachments)}"
    )


if __name__ == "__main__":
    main()
