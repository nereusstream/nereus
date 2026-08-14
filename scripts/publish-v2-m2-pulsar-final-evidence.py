#!/usr/bin/env python3
"""Publish the canonical source-bound Pulsar M2 Final receipt and summaries."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import xml.etree.ElementTree as ET


ROOT = Path(__file__).resolve().parent.parent
FINAL = Path("docs/v2/evidence/v2-m2/pulsar/final")
LOCAL_SUMMARY = FINAL / "attachments/local-junit-summary.json"
NATIVE_SUMMARY = FINAL / "attachments/native-junit-summary.json"
PROVIDER_SUMMARY = FINAL / "attachments/p6-provider-junit-summary.json"
RECEIPT = FINAL / "pulsar-final.json"
KAFKA_FINAL = Path("docs/v2/evidence/v2-m2/kafka/k10/kafka-final.json")
P6_ROOT = Path("docs/v2/evidence/v2-m2/pulsar/p6")
P6_EXECUTION = P6_ROOT / "execution.json"
CANDIDATE = P6_ROOT / "candidate-matrix.json"
NATIVE_BASELINE = P6_ROOT / "native-baseline.json"
MINIO = P6_ROOT / "minio-provider.json"

LOCAL_SUITES = {
    "com.nereusstream.pulsar.offload.NereusPulsarLedgerOffloaderV1Test": 3,
    "com.nereusstream.pulsar.offload.PulsarBookKeeperDeletionCoordinatorV1Test": 9,
    "com.nereusstream.pulsar.offload.PulsarDualSourceReadHandleV1Test": 10,
    "com.nereusstream.pulsar.offload.PulsarObjectReadHandleV1Test": 8,
    "com.nereusstream.pulsar.offload.PulsarOffloadBlockPolicyV1Test": 3,
    "com.nereusstream.pulsar.offload.PulsarOffloadP0ContractTest": 11,
    "com.nereusstream.pulsar.offload.PulsarSealedLedgerPublisherV1Test": 9,
    "com.nereusstream.pulsar.offload.npd1.Npd1CodecV1Test": 15,
    "com.nereusstream.pulsar.offload.npo1.Npo1CodecV1Test": 17,
}
NATIVE_SUITES = {
    "org.apache.bookkeeper.mledger.impl.DualSourceReadHandleTest": 13,
    "org.apache.bookkeeper.mledger.impl.OffloadLedgerDeleteTest": 13,
}
PROVIDER_SUITES = {
    "com.nereusstream.pulsar.offload.P6MinioProviderEvidenceTest": ("p6RealProviderTest", 1),
    "com.nereusstream.pulsar.offload.PulsarP6CandidateEvidenceTest": ("p6EvidenceTest", 1),
    "com.nereusstream.pulsar.offload.S3PulsarOffloadObjectStoreV1Test": ("p6ProviderTest", 3),
}
POLICY = {
    "V2-BK-001": {
        "com.nereusstream.pulsar.offload.NereusPulsarLedgerOffloaderV1Test",
        "org.apache.bookkeeper.mledger.impl.OffloadLedgerDeleteTest",
        "nereus.kafka.m2.final",
    },
    "V2-BK-002": {
        "com.nereusstream.pulsar.offload.NereusPulsarLedgerOffloaderV1Test",
        "com.nereusstream.pulsar.offload.PulsarOffloadP0ContractTest",
        "org.apache.bookkeeper.mledger.impl.OffloadLedgerDeleteTest",
    },
    "V2-BK-004": {
        "com.nereusstream.pulsar.offload.NereusPulsarLedgerOffloaderV1Test",
        "com.nereusstream.pulsar.offload.PulsarSealedLedgerPublisherV1Test",
        "nereus.pulsar.m2.p6.minio-provider",
    },
    "V2-BK-005": {
        "com.nereusstream.pulsar.offload.P6MinioProviderEvidenceTest",
        "com.nereusstream.pulsar.offload.PulsarOffloadP0ContractTest",
        "com.nereusstream.pulsar.offload.PulsarSealedLedgerPublisherV1Test",
    },
    "V2-BK-006": {"com.nereusstream.pulsar.offload.npo1.Npo1CodecV1Test"},
    "V2-BK-007": {
        "com.nereusstream.pulsar.offload.PulsarBookKeeperDeletionCoordinatorV1Test",
        "com.nereusstream.pulsar.offload.PulsarObjectReadHandleV1Test",
        "org.apache.bookkeeper.mledger.impl.OffloadLedgerDeleteTest",
    },
    "V2-BK-008": {
        "com.nereusstream.pulsar.offload.PulsarDualSourceReadHandleV1Test",
        "org.apache.bookkeeper.mledger.impl.DualSourceReadHandleTest",
        "nereus.pulsar.m2.p6.native-baseline",
    },
    "V2-BK-009": {
        "com.nereusstream.pulsar.offload.PulsarObjectReadHandleV1Test",
        "com.nereusstream.pulsar.offload.npd1.Npd1CodecV1Test",
        "com.nereusstream.pulsar.offload.npo1.Npo1CodecV1Test",
    },
    "V2-BK-010": {
        "com.nereusstream.pulsar.offload.PulsarBookKeeperDeletionCoordinatorV1Test",
        "com.nereusstream.pulsar.offload.PulsarDualSourceReadHandleV1Test",
        "org.apache.bookkeeper.mledger.impl.DualSourceReadHandleTest",
        "org.apache.bookkeeper.mledger.impl.OffloadLedgerDeleteTest",
    },
    "V2-BK-012": {
        "com.nereusstream.pulsar.offload.PulsarP6CandidateEvidenceTest",
        "com.nereusstream.pulsar.offload.S3PulsarOffloadObjectStoreV1Test",
        "com.nereusstream.pulsar.offload.npd1.Npd1CodecV1Test",
    },
    "V2-BK-013": {
        "com.nereusstream.pulsar.offload.NereusPulsarLedgerOffloaderV1Test",
        "com.nereusstream.pulsar.offload.PulsarOffloadBlockPolicyV1Test",
        "nereus.pulsar.m2.p6.candidate-matrix",
    },
}


def fail(message: str) -> None:
    raise SystemExit(f"Pulsar M2 Final publisher: {message}")


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


def report(path: Path, name: str, expected_tests: int) -> dict:
    if not path.is_file() or path.is_symlink():
        fail(f"JUnit report is absent or symbolic: {path}")
    attributes = ET.parse(path).getroot().attrib
    row = {"name": name}
    for key in ("tests", "failures", "errors", "skipped"):
        row[key] = int(attributes[key])
    if row != {
        "name": name,
        "tests": expected_tests,
        "failures": 0,
        "errors": 0,
        "skipped": 0,
    }:
        fail(f"JUnit result differs: {row}")
    return row


def summary(schema: str, tested: str, rows: list[dict]) -> dict:
    rows.sort(key=lambda row: row["name"])
    totals = {
        key: sum(row[key] for row in rows)
        for key in ("tests", "failures", "errors", "skipped")
    }
    if any(totals[key] for key in ("failures", "errors", "skipped")):
        fail(f"summary is not PASS: {schema}")
    return {
        "schema": schema,
        "result": "PASS",
        "testedSourceCommit": tested,
        "suites": len(rows),
        **totals,
        "suiteResults": rows,
    }


def main() -> None:
    if len(sys.argv) != 3:
        fail("usage: publish-v2-m2-pulsar-final-evidence.py <tested-source-commit> <pulsar-checkout>")
    tested = sys.argv[1]
    pulsar = Path(sys.argv[2]).resolve()
    if not re.fullmatch(r"[0-9a-f]{40}", tested):
        fail("tested source commit is not canonical")
    if subprocess.run(
        ["git", "-C", str(ROOT), "merge-base", "--is-ancestor", tested, "HEAD"], check=False
    ).returncode != 0:
        fail("tested source commit is not an ancestor of HEAD")

    locks = load(Path("docs/v2/source-locks.json"))
    binding = locks["m2PulsarNativeBinding"]
    pulsar_commit = binding["finalForkCommit"]
    if (
        subprocess.check_output(["git", "-C", str(pulsar), "rev-parse", "HEAD"], text=True).strip()
        != pulsar_commit
        or subprocess.check_output(["git", "-C", str(pulsar), "branch", "--show-current"], text=True).strip()
        != binding["branch"]
        or subprocess.check_output(["git", "-C", str(pulsar), "status", "--porcelain"], text=True).strip()
        or subprocess.check_output(
            ["git", "-C", str(pulsar), "rev-parse", f"origin/{binding['branch']}"], text=True
        ).strip()
        != pulsar_commit
    ):
        fail("Pulsar checkout does not match the exact clean pushed source lock")

    kafka = load(KAFKA_FINAL)
    p6 = load(P6_EXECUTION)
    if kafka.get("sourceTuple", {}).get("nereusCommit") != tested:
        fail("Kafka Final is not bound to the tested source")
    if p6.get("nereusSourceCommit") != tested or p6.get("pulsarSourceCommit") != pulsar_commit:
        fail("P6 execution is not bound to the tested source tuple")

    local_root = ROOT / "nereus-pulsar-offload/build/test-results/test"
    local_rows = [
        report(local_root / f"TEST-{name}.xml", name, tests)
        for name, tests in LOCAL_SUITES.items()
    ]
    local = summary("NEREUS_V2_M2_PULSAR_FINAL_LOCAL_JUNIT_SUMMARY_V1", tested, local_rows)
    if (local["suites"], local["tests"]) != (9, 85):
        fail("local functional suite totals differ")
    write(LOCAL_SUMMARY, local)

    native_root = pulsar / "managed-ledger/build/test-results/test"
    native_rows = [
        report(native_root / f"TEST-{name}.xml", name, tests)
        for name, tests in NATIVE_SUITES.items()
    ]
    native = summary("NEREUS_V2_M2_PULSAR_FINAL_NATIVE_JUNIT_SUMMARY_V1", tested, native_rows)
    if (native["suites"], native["tests"]) != (2, 26):
        fail("native suite totals differ")
    write(NATIVE_SUMMARY, native)

    provider_rows = []
    for name, (task, tests) in PROVIDER_SUITES.items():
        path = ROOT / "nereus-pulsar-offload/build/test-results" / task / f"TEST-{name}.xml"
        provider_rows.append(report(path, name, tests))
    provider = summary("NEREUS_V2_M2_PULSAR_FINAL_P6_PROVIDER_JUNIT_SUMMARY_V1", tested, provider_rows)
    if (provider["suites"], provider["tests"]) != (3, 5):
        fail("P6 provider suite totals differ")
    write(PROVIDER_SUMMARY, provider)

    suite_results = {
        **{name: (LOCAL_SUMMARY, tests) for name, tests in LOCAL_SUITES.items()},
        **{name: (NATIVE_SUMMARY, tests) for name, tests in NATIVE_SUITES.items()},
        **{name: (PROVIDER_SUMMARY, value[1]) for name, value in PROVIDER_SUITES.items()},
        "nereus.kafka.m2.final": (KAFKA_FINAL, 1),
        "nereus.pulsar.m2.p6.candidate-matrix": (CANDIDATE, 1),
        "nereus.pulsar.m2.p6.native-baseline": (NATIVE_BASELINE, 1),
        "nereus.pulsar.m2.p6.minio-provider": (MINIO, 1),
    }
    scenarios = []
    for scenario_id in sorted(POLICY):
        suites = []
        for suite_id in sorted(POLICY[scenario_id]):
            if suite_id not in suite_results:
                fail(f"policy suite lacks a result: {suite_id}")
            attachment, tests = suite_results[suite_id]
            suites.append(
                {
                    "attachmentPath": str(attachment),
                    "errors": 0,
                    "failed": 0,
                    "skipped": 0,
                    "suiteId": suite_id,
                    "tests": tests,
                }
            )
        scenarios.append({"scenarioId": scenario_id, "suites": suites})
    if sum(len(row["suites"]) for row in scenarios) != 32:
        fail("policy suite reference total differs")

    kinds = {
        KAFKA_FINAL: "KAFKA_FINAL_RECEIPT",
        LOCAL_SUMMARY: "LOCAL_JUNIT_SUMMARY",
        NATIVE_SUMMARY: "NATIVE_JUNIT_SUMMARY",
        PROVIDER_SUMMARY: "P6_PROVIDER_JUNIT_SUMMARY",
        CANDIDATE: "P6_CANDIDATE_MATRIX",
        P6_EXECUTION: "P6_EXECUTION_RECEIPT",
        NATIVE_BASELINE: "P6_NATIVE_BASELINE",
        MINIO: "P6_REAL_PROVIDER",
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
        "kind": "PULSAR_M2_FINAL",
        "promotionEligible": True,
        "result": "PASS_PULSAR_M2_FINAL",
        "scenarios": scenarios,
        "schema": "NEREUS_V2_M2_PULSAR_FINAL_RECEIPT_V1",
        "sourceTuple": {
            "kafkaFinalReceiptSha256": sha(KAFKA_FINAL),
            "nereusCommit": tested,
            "p6ExecutionReceiptSha256": sha(P6_EXECUTION),
            "pulsarForkCommit": pulsar_commit,
            "sourceLocksSha256": sha(Path("docs/v2/source-locks.json")),
        },
    }
    encoded = json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode()
    (ROOT / RECEIPT).parent.mkdir(parents=True, exist_ok=True)
    (ROOT / RECEIPT).write_bytes(encoded)
    print(
        "Pulsar M2 Final evidence published: "
        f"source={tested} scenarios={len(scenarios)} suiteReferences=32 attachments={len(attachments)} "
        f"local={local['suites']}/{local['tests']} native={native['suites']}/{native['tests']} "
        f"provider={provider['suites']}/{provider['tests']}"
    )


if __name__ == "__main__":
    main()
