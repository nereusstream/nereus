#!/usr/bin/env python3
"""Validate published Pulsar M2 Final evidence against live reports and registries."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import subprocess
import sys
import xml.etree.ElementTree as ET


ROOT = Path(__file__).resolve().parent.parent
RECEIPT = Path("docs/v2/evidence/v2-m2/pulsar/final/pulsar-final.json")
LOCAL = Path("docs/v2/evidence/v2-m2/pulsar/final/attachments/local-junit-summary.json")
NATIVE = Path("docs/v2/evidence/v2-m2/pulsar/final/attachments/native-junit-summary.json")
PROVIDER = Path("docs/v2/evidence/v2-m2/pulsar/final/attachments/p6-provider-junit-summary.json")
KAFKA_FINAL = Path("docs/v2/evidence/v2-m2/kafka/k10/kafka-final.json")
P6_ROOT = Path("docs/v2/evidence/v2-m2/pulsar/p6")
P6_EXECUTION = P6_ROOT / "execution.json"
CANDIDATE = P6_ROOT / "candidate-matrix.json"
NATIVE_BASELINE = P6_ROOT / "native-baseline.json"
MINIO = P6_ROOT / "minio-provider.json"
EXPECTED_SCENARIOS = {
    "V2-BK-001",
    "V2-BK-002",
    "V2-BK-004",
    "V2-BK-005",
    "V2-BK-006",
    "V2-BK-007",
    "V2-BK-008",
    "V2-BK-009",
    "V2-BK-010",
    "V2-BK-012",
    "V2-BK-013",
}
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
    raise SystemExit(f"Pulsar M2 Final gate: {message}")


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


def git_lines(*arguments: str) -> list[str]:
    return subprocess.check_output(["git", *arguments], cwd=ROOT, text=True).splitlines()


def verify_summary(
    path: Path,
    schema: str,
    tested: str,
    expected: dict[str, int],
    report_paths: dict[str, Path],
) -> dict[str, dict]:
    value = load(path)
    if value.get("schema") != schema or value.get("result") != "PASS" or value.get("testedSourceCommit") != tested:
        fail(f"summary identity/source differs: {path}")
    rows = value.get("suiteResults", [])
    if [row.get("name") for row in rows] != sorted(expected) or len(rows) != len(expected):
        fail(f"summary suite set differs: {path}")
    totals = {key: 0 for key in ("tests", "failures", "errors", "skipped")}
    by_name = {}
    for row in rows:
        name = row["name"]
        report = report_paths[name]
        if not report.is_file() or report.is_symlink():
            fail(f"live JUnit report is absent or symbolic: {report}")
        attributes = ET.parse(report).getroot().attrib
        actual = {key: int(attributes[key]) for key in totals}
        expected_row = {key: row.get(key) for key in totals}
        if actual != expected_row or actual != {
            "tests": expected[name],
            "failures": 0,
            "errors": 0,
            "skipped": 0,
        }:
            fail(f"live JUnit result differs: {name}")
        for key, count in actual.items():
            totals[key] += count
        by_name[name] = row
    if any(value.get(key) != count for key, count in {"suites": len(rows), **totals}.items()):
        fail(f"summary totals differ: {path}")
    return by_name


def main() -> None:
    if len(sys.argv) != 2:
        fail("usage: check-v2-m2-pulsar-final-evidence.py <pulsar-checkout>")
    pulsar = Path(sys.argv[1]).resolve()
    receipt = load(RECEIPT)
    source = receipt.get("sourceTuple", {})
    tested = source.get("nereusCommit")
    if not isinstance(tested, str) or len(tested) != 40:
        fail("tested Nereus commit is not canonical")
    if subprocess.run(
        ["git", "merge-base", "--is-ancestor", tested, "HEAD"], cwd=ROOT, check=False
    ).returncode != 0:
        fail("HEAD is not an evidence descendant of the tested source")

    allowed = (
        "build.gradle.kts",
        "docs/v2/08-implementation-plan-and-gates.md",
        "docs/v2/09-scenario-evidence-matrix.md",
        "docs/v2/open-questions.md",
        "docs/v2/v2-scenarios.json",
        "docs/v2/detailed_design/m2/README.md",
        "docs/v2/detailed_design/m2/kafka-bookkeeper-offset-range-index.md",
        "docs/v2/detailed_design/m2/kafka-produce-fetch-frontiers-and-recovery.md",
        "docs/v2/detailed_design/m2/kafka-m2-k0-evidence-and-input-gate.md",
        "docs/v2/detailed_design/m2/kafka-m2-k0-implementation-input-closure.md",
        "docs/v2/detailed_design/m2/kafka-m2-k0-numeric-admission.md",
        "docs/v2/detailed_design/m2/kafka-m2-k1-frontier-publication.md",
        "docs/v2/detailed_design/m2/kafka-m2-k2-assigned-record-batch-adapter.md",
        "docs/v2/detailed_design/m2/kafka-m2-k3-run-lifecycle.md",
        "docs/v2/detailed_design/m2/kafka-m2-k4-ordered-pipeline.md",
        "docs/v2/detailed_design/m2/kafka-m2-k5-coherent-protocol-publication.md",
        "docs/v2/detailed_design/m2/kafka-m2-k6-targeted-reader.md",
        "docs/v2/detailed_design/m2/kafka-m2-k7-checkpoint-recovery.md",
        "docs/v2/detailed_design/m2/kafka-m2-k8-replica-observation.md",
        "docs/v2/detailed_design/m2/pulsar-m2-p0-input-closure.md",
        "docs/v2/detailed_design/m2/pulsar-m2-p1-npd1-codec.md",
        "docs/v2/detailed_design/m2/pulsar-m2-p2-npo1-root.md",
        "docs/v2/detailed_design/m2/pulsar-m2-p3-publication-engine.md",
        "docs/v2/detailed_design/m2/pulsar-m2-p4-object-reader.md",
        "docs/v2/detailed_design/m2/pulsar-m2-p4-dual-source-and-delete.md",
        "docs/v2/detailed_design/m2/pulsar-m2-p5-native-provider.md",
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
        if not any(path == item or path.startswith(item) for item in allowed):
            fail(f"evidence descendant changes tested production/configuration path: {path}")

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
        fail("Pulsar checkout differs from the exact clean pushed source lock")

    kafka = load(KAFKA_FINAL)
    p6 = load(P6_EXECUTION)
    expected_source = {
        "kafkaFinalReceiptSha256": sha(KAFKA_FINAL),
        "nereusCommit": tested,
        "p6ExecutionReceiptSha256": sha(P6_EXECUTION),
        "pulsarForkCommit": pulsar_commit,
        "sourceLocksSha256": sha(Path("docs/v2/source-locks.json")),
    }
    if source != expected_source:
        fail("source tuple differs from Kafka Final, P6, Pulsar fork, or source locks")
    if kafka.get("result") != "PASS_KAFKA_M2_FINAL" or kafka.get("sourceTuple", {}).get("nereusCommit") != tested:
        fail("Kafka Final prerequisite identity/source differs")
    if (
        p6.get("result") != "PASS_P6_PROVIDER_AND_BLOCK_POLICY_ONLY"
        or p6.get("nereusSourceCommit") != tested
        or p6.get("pulsarSourceCommit") != pulsar_commit
        or p6.get("promotionEligible") is not False
    ):
        fail("P6 prerequisite identity/source/boundary differs")

    kinds = {
        str(KAFKA_FINAL): "KAFKA_FINAL_RECEIPT",
        str(LOCAL): "LOCAL_JUNIT_SUMMARY",
        str(NATIVE): "NATIVE_JUNIT_SUMMARY",
        str(PROVIDER): "P6_PROVIDER_JUNIT_SUMMARY",
        str(CANDIDATE): "P6_CANDIDATE_MATRIX",
        str(P6_EXECUTION): "P6_EXECUTION_RECEIPT",
        str(NATIVE_BASELINE): "P6_NATIVE_BASELINE",
        str(MINIO): "P6_REAL_PROVIDER",
    }
    attachments = receipt.get("attachments", [])
    if [row.get("path") for row in attachments] != sorted(kinds):
        fail("attachment paths are not the exact sorted eight-file set")
    for row in attachments:
        path = Path(row["path"])
        absolute = ROOT / path
        if (
            row.get("kind") != kinds[row["path"]]
            or not absolute.is_file()
            or absolute.is_symlink()
            or row.get("bytes") != absolute.stat().st_size
            or row.get("sha256") != sha(path)
        ):
            fail(f"attachment identity/length/digest differs: {path}")

    local_reports = {
        name: ROOT / "nereus-pulsar-offload/build/test-results/test" / f"TEST-{name}.xml"
        for name in LOCAL_SUITES
    }
    local_by_name = verify_summary(
        LOCAL,
        "NEREUS_V2_M2_PULSAR_FINAL_LOCAL_JUNIT_SUMMARY_V1",
        tested,
        LOCAL_SUITES,
        local_reports,
    )
    native_reports = {
        name: pulsar / "managed-ledger/build/test-results/test" / f"TEST-{name}.xml"
        for name in NATIVE_SUITES
    }
    native_by_name = verify_summary(
        NATIVE,
        "NEREUS_V2_M2_PULSAR_FINAL_NATIVE_JUNIT_SUMMARY_V1",
        tested,
        NATIVE_SUITES,
        native_reports,
    )
    provider_expected = {name: value[1] for name, value in PROVIDER_SUITES.items()}
    provider_reports = {
        name: ROOT
        / "nereus-pulsar-offload/build/test-results"
        / task
        / f"TEST-{name}.xml"
        for name, (task, _) in PROVIDER_SUITES.items()
    }
    provider_by_name = verify_summary(
        PROVIDER,
        "NEREUS_V2_M2_PULSAR_FINAL_P6_PROVIDER_JUNIT_SUMMARY_V1",
        tested,
        provider_expected,
        provider_reports,
    )
    if sum(row["tests"] for row in local_by_name.values()) != 85:
        fail("local functional total differs")
    if sum(row["tests"] for row in native_by_name.values()) != 26:
        fail("native total differs")
    if sum(row["tests"] for row in provider_by_name.values()) != 5:
        fail("provider total differs")

    candidate = load(CANDIDATE)
    native_baseline = load(NATIVE_BASELINE)
    minio = load(MINIO)
    if (
        candidate.get("schema") != "NEREUS_V2_M2_PULSAR_P6_CANDIDATE_V1"
        or candidate.get("testedSourceCommit") != tested
        or candidate.get("pulsarSourceCommit") != pulsar_commit
    ):
        fail("candidate matrix identity/source differs")
    if (
        native_baseline.get("schema") != "NEREUS_V2_M2_PULSAR_P6_NATIVE_BASELINE_V1"
        or native_baseline.get("pulsarSourceCommit") != pulsar_commit
    ):
        fail("native baseline identity/source differs")
    if (
        minio.get("schema") != "NEREUS_V2_M2_PULSAR_P6_REAL_PROVIDER_V1"
        or minio.get("testedSourceCommit") != tested
        or minio.get("result") != "PASS_MINIO_PROVIDER_ONLY"
    ):
        fail("MinIO provider identity/source/result differs")

    suite_results = {
        **{name: (str(LOCAL), tests) for name, tests in LOCAL_SUITES.items()},
        **{name: (str(NATIVE), tests) for name, tests in NATIVE_SUITES.items()},
        **{name: (str(PROVIDER), value[1]) for name, value in PROVIDER_SUITES.items()},
        "nereus.kafka.m2.final": (str(KAFKA_FINAL), 1),
        "nereus.pulsar.m2.p6.candidate-matrix": (str(CANDIDATE), 1),
        "nereus.pulsar.m2.p6.native-baseline": (str(NATIVE_BASELINE), 1),
        "nereus.pulsar.m2.p6.minio-provider": (str(MINIO), 1),
    }
    scenarios = receipt.get("scenarios", [])
    if [row.get("scenarioId") for row in scenarios] != sorted(EXPECTED_SCENARIOS):
        fail("receipt scenario set differs")
    references = 0
    for scenario in scenarios:
        scenario_id = scenario["scenarioId"]
        suites = scenario.get("suites", [])
        if [row.get("suiteId") for row in suites] != sorted(POLICY[scenario_id]):
            fail(f"scenario suite set differs: {scenario_id}")
        for row in suites:
            expected_path, tests = suite_results[row["suiteId"]]
            if (
                row.get("attachmentPath") != expected_path
                or row.get("tests") != tests
                or {key: row.get(key) for key in ("failed", "errors", "skipped")}
                != {"failed": 0, "errors": 0, "skipped": 0}
            ):
                fail(f"scenario suite result differs: {scenario_id}:{row['suiteId']}")
            references += 1
    if references != 32:
        fail("scenario suite reference total differs")

    registry = load(Path("docs/v2/v2-scenarios.json"))
    registry_rows = {row["id"]: row for row in registry.get("scenarios", [])}
    bound = {
        scenario_id
        for scenario_id, row in registry_rows.items()
        if row.get("evidenceReceipt") == str(RECEIPT)
    }
    if bound != EXPECTED_SCENARIOS:
        fail("machine-readable registry binds Pulsar Final to an extra or missing scenario")
    for scenario_id in EXPECTED_SCENARIOS:
        if registry_rows[scenario_id].get("status") != "PASSED_CURRENT_SOURCE":
            fail(f"machine-readable scenario is not promoted: {scenario_id}")
    for scenario_id in ("V2-BK-011", "V2-PUL-001"):
        row = registry_rows[scenario_id]
        if row.get("status") != "PLANNED" or row.get("evidenceReceipt") is not None:
            fail(f"downstream scenario was prematurely promoted: {scenario_id}")

    matrix_status = {}
    for line in (ROOT / "docs/v2/09-scenario-evidence-matrix.md").read_text().splitlines():
        if line.startswith("| V2-"):
            columns = [column.strip() for column in line.strip("|").split("|")]
            if len(columns) == 5:
                matrix_status[columns[0]] = columns[4]
    for scenario_id, row in registry_rows.items():
        if matrix_status.get(scenario_id) != row.get("status"):
            fail(f"scenario registries disagree: {scenario_id}")

    design = (ROOT / "docs/v2/detailed_design/m2/pulsar-m2-final-evidence.md").read_text()
    for literal in (
        "implementationStatus: Verified",
        "evidenceStatus: CurrentSourceReceipt",
        "receipt: docs/v2/evidence/v2-m2/pulsar/final/pulsar-final.json",
    ):
        if literal not in design:
            fail(f"Pulsar Final design lacks current status: {literal}")

    print(
        "Pulsar M2 Final evidence verified: "
        f"source={tested} scenarios=11 suiteReferences=32 attachments=8 "
        "local=9/85 native=2/26 provider=3/5; downstream boundary preserved"
    )


if __name__ == "__main__":
    main()
