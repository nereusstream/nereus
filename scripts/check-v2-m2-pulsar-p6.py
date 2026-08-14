#!/usr/bin/env python3
"""Validate the source-bound M2-P6 provider and block-policy receipts."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import xml.etree.ElementTree as ET


def fail(message: str) -> None:
    raise SystemExit(f"Pulsar P6 gate: {message}")


def git(repo: Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(repo), *args], text=True).strip()


def load(path: Path) -> dict:
    try:
        value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as failure:
        fail(f"cannot read canonical JSON {path}: {failure}")
    if not isinstance(value, dict):
        fail(f"canonical JSON is not an object: {path}")
    return value


def test_result(path: Path, expected_tests: int) -> None:
    if not path.is_file():
        fail(f"required JUnit report is absent: {path}")
    attributes = ET.parse(path).getroot().attrib
    actual = tuple(int(attributes[key]) for key in ("tests", "failures", "errors", "skipped"))
    if actual != (expected_tests, 0, 0, 0):
        fail(f"JUnit result differs for {path.name}: {actual}")


def main() -> None:
    if len(sys.argv) != 2:
        fail("usage: check-v2-m2-pulsar-p6.py <pulsar-checkout>")
    root = Path(__file__).resolve().parent.parent
    pulsar = Path(sys.argv[1]).resolve()
    evidence = root / "docs/v2/evidence/v2-m2/pulsar/p6"
    matrix = load(evidence / "candidate-matrix.json")
    native = load(evidence / "native-baseline.json")
    minio = load(evidence / "minio-provider.json")
    execution = load(evidence / "execution.json")
    binding = load(root / "docs/v2/source-locks.json")["m2PulsarNativeBinding"]

    nereus_source = execution.get("nereusSourceCommit")
    pulsar_source = execution.get("pulsarSourceCommit")
    if not isinstance(nereus_source, str) or len(nereus_source) != 40:
        fail("execution receipt lacks the exact Nereus source commit")
    if pulsar_source != binding["finalForkCommit"]:
        fail("execution receipt differs from the canonical Pulsar source lock")
    if git(root, "merge-base", "--is-ancestor", nereus_source, "HEAD"):
        pass
    if subprocess.run(
        ["git", "-C", str(root), "diff", "--quiet", nereus_source, "HEAD", "--", "nereus-pulsar-offload/src/main"],
        check=False,
    ).returncode != 0:
        fail("production Pulsar offload source changed after the tested P6 commit")
    if git(pulsar, "rev-parse", "HEAD") != pulsar_source:
        fail("Pulsar checkout HEAD differs from the P6 receipt")
    if git(pulsar, "branch", "--show-current") != binding["branch"]:
        fail("Pulsar checkout branch differs from the P6 source lock")
    if git(pulsar, "status", "--porcelain"):
        fail("Pulsar checkout is dirty")
    if git(pulsar, "rev-parse", f"origin/{binding['branch']}") != pulsar_source:
        fail("Pulsar remote-tracking branch differs from the P6 receipt")

    if matrix.get("schema") != "NEREUS_V2_M2_PULSAR_P6_CANDIDATE_V1":
        fail("candidate matrix schema differs")
    if matrix.get("testedSourceCommit") != nereus_source or matrix.get("pulsarSourceCommit") != pulsar_source:
        fail("candidate matrix source tuple differs")
    rows = matrix.get("matrix")
    if not isinstance(rows, list) or len(rows) != 16:
        fail("candidate matrix must contain exactly sixteen workload/target/codec rows")
    identities = {
        (row.get("workload"), row.get("targetBytes"), row.get("compressionPolicy")) for row in rows
    }
    expected = {
        (workload, target, policy)
        for workload in ("max-entries-100b", "scan-20mb")
        for target in (1 << 20, 4 << 20, 8 << 20, 16 << 20)
        for policy in ("FIXED_NONE", "ZSTD_IF_SMALLER")
    }
    if identities != expected:
        fail("candidate matrix identities are incomplete or duplicated")
    for row in rows:
        for field in (
            "blockCount",
            "encodedBytes",
            "decodedBytes",
            "wallMicros",
            "processCpuMicros",
            "randomP50Micros",
            "randomP99Micros",
            "sequential1000Micros",
            "providerRequests",
            "providerTransferredBytes",
            "providerP50Micros",
            "providerP99Micros",
        ):
            if not isinstance(row.get(field), int) or row[field] <= 0:
                fail(f"candidate row lacks positive measured field {field}")
        if row.get("concurrency") != 4:
            fail("candidate row did not execute concurrency four")
    selection = matrix.get("selection")
    if selection != {
        "classes": ["latency-1mib", "balanced-4mib", "scan-8mib"],
        "deploymentDefault": "balanced-4mib",
        "excludedCandidateBytes": 16 << 20,
    }:
        fail("selected block catalog/default differs")
    limits = matrix.get("selectedHardLimits")
    if limits != {
        "maxDataObjectBytes": 4 << 30,
        "maxMultipartParts": 1024,
        "maxEntryBytes": 64 << 20,
        "maxDecodedBlockBytes": 64 << 20,
        "maxEntriesPerBlock": 65536,
    }:
        fail("selected hard limits differ")
    boundaries = {row.get("name"): row for row in matrix.get("boundaryCoverage", [])}
    if boundaries.get("stock-5mib", {}).get("decodedBytes") != 5 << 20:
        fail("stock 5-MiB boundary coverage is absent")
    if boundaries.get("near-hard-cap", {}).get("decodedBytes") != (64 << 20) - 1024:
        fail("near-hard-cap dedicated-entry coverage is absent")
    scan = {(row["targetBytes"], row["compressionPolicy"]): row for row in rows if row["workload"] == "scan-20mb"}
    for policy in ("FIXED_NONE", "ZSTD_IF_SMALLER"):
        eight = scan[(8 << 20, policy)]
        sixteen = scan[(16 << 20, policy)]
        if sixteen["providerRequests"] < eight["providerRequests"] - 1:
            fail("16-MiB exclusion rationale no longer matches provider request evidence")
        if sixteen["randomP50Micros"] <= eight["randomP50Micros"]:
            fail("16-MiB exclusion rationale no longer matches random-read evidence")

    if native.get("schema") != "NEREUS_V2_M2_PULSAR_P6_NATIVE_BASELINE_V1":
        fail("native baseline schema differs")
    if native.get("pulsarSourceCommit") != pulsar_source or native.get("sourceDeclaredReadBufferBytes") != 1 << 20:
        fail("native source/read-buffer baseline differs")
    if native.get("entryCount") != 50_000 or native.get("entryBytes") != 100:
        fail("native maximum-entry-count workload differs")

    if minio.get("schema") != "NEREUS_V2_M2_PULSAR_P6_REAL_PROVIDER_V1":
        fail("real-provider receipt schema differs")
    if minio.get("testedSourceCommit") != nereus_source or minio.get("result") != "PASS_MINIO_PROVIDER_ONLY":
        fail("real-provider receipt source or result differs")
    for field in (
        "conditionalCreate",
        "boundedRangeRead",
        "deleteAndProveAbsent",
        "multipartCleanupAndRelist",
        "canonicalNpd1Npo1RoundTrip",
    ):
        if minio.get(field) is not True:
            fail(f"real-provider receipt lacks {field}")
    if minio.get("imageDigest") != execution.get("minio", {}).get("repoDigest", "").split("@")[-1]:
        fail("MinIO image digest differs between receipts")

    expected_results = {
        "s3Adapter": 3,
        "candidateMatrix": 1,
        "minioProvider": 1,
        "nativeBaseline": 1,
    }
    for name, tests in expected_results.items():
        result = execution.get("results", {}).get(name)
        if result != {"tests": tests, "failures": 0, "errors": 0, "skipped": 0}:
            fail(f"execution result differs for {name}")
    if execution.get("result") != "PASS_P6_PROVIDER_AND_BLOCK_POLICY_ONLY" or execution.get("promotionEligible"):
        fail("P6 execution boundary differs")

    test_result(
        root
        / "nereus-pulsar-offload/build/test-results/p6ProviderTest/"
        "TEST-com.nereusstream.pulsar.offload.S3PulsarOffloadObjectStoreV1Test.xml",
        3,
    )
    test_result(
        pulsar
        / "tiered-storage/jcloud/build/test-results/test/"
        "TEST-org.apache.bookkeeper.mledger.offload.jcloud.impl.P6NativeLocalStackEvidenceTest.xml",
        1,
    )

    design = (root / "docs/v2/detailed_design/m2/pulsar-m2-p6-provider-and-block-policy.md").read_text()
    for literal in (
        "implementationStatus: Verified",
        "evidenceStatus: CurrentSourceReceipt",
        "receipt: docs/v2/evidence/v2-m2/pulsar/p6/execution.json",
        "P6 does not wire a broker process or NAR",
        "scan-8mib",
    ):
        if literal not in design:
            fail(f"P6 design lacks required status or boundary: {literal}")

    print(
        "Pulsar M2-P6 verified: provider=3 candidate=1 minio=1 native=1; "
        "failures=0 errors=0 skips=0; selected=1/4/8MiB default=4MiB"
    )


if __name__ == "__main__":
    main()
