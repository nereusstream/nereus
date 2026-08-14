#!/usr/bin/env bash
# Licensed under the Apache License, Version 2.0.
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

python3 - <<'PY'
from pathlib import Path
import hashlib
import json
import subprocess
import xml.etree.ElementTree as ET

root = Path.cwd()
receipt_path = root / "docs/v2/evidence/v2-m2/kafka/k9/k9-evidence.json"
receipt = json.loads(receipt_path.read_text())
if receipt.get("schema") != "NEREUS_V2_M2_KAFKA_K9_EVIDENCE_V1":
    raise SystemExit("K9 receipt schema differs")
if receipt.get("result") != "PASS_KAFKA_M2_K9_REAL_BOOKKEEPER_EVIDENCE":
    raise SystemExit("K9 receipt result differs")
if receipt.get("promotionEligible") is not False:
    raise SystemExit("K9 evidence cannot promote a scenario")

def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()

def properties(path: Path) -> dict[str, str]:
    result = {}
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if line and not line.startswith("#"):
            key, value = line.split("=", 1)
            result[key] = value
    return result

tested = receipt["testedSourceCommit"]
predeclared = receipt["prerequisite"]["predeclaredPlanCommit"]
selected_defaults = receipt["prerequisite"].get("selectedDefaultsCommit")
for commit in (predeclared, selected_defaults, tested):
    if not isinstance(commit, str) or len(commit) != 40:
        raise SystemExit("K9 receipt commit is not canonical")
if subprocess.run(
    ["git", "merge-base", "--is-ancestor", predeclared, tested], cwd=root, check=False
).returncode != 0:
    raise SystemExit("K9 tested source does not descend from the predeclared plan")
if subprocess.run(
    ["git", "merge-base", "--is-ancestor", selected_defaults, tested], cwd=root, check=False
).returncode != 0:
    raise SystemExit("K9 tested source does not descend from the selected defaults")
if subprocess.run(
    ["git", "merge-base", "--is-ancestor", tested, "HEAD"], cwd=root, check=False
).returncode != 0:
    raise SystemExit("K9 receipt is not an evidence descendant of its tested source")

allowed_evidence_paths = (
    "build.gradle.kts",
    "docs/v2/09-scenario-evidence-matrix.md",
    "docs/v2/v2-scenarios.json",
    "docs/v2/detailed_design/m2/README.md",
    "docs/v2/detailed_design/m2/kafka-m2-k9-real-bookkeeper-evidence.md",
    "docs/v2/detailed_design/m2/kafka-m2-k10-final-evidence.md",
    "docs/v2/detailed_design/m2/pulsar-m2-p6-provider-and-block-policy.md",
    "docs/v2/evidence/v2-m2/kafka/k9/",
    "docs/v2/evidence/v2-m2/kafka/k10/",
    "docs/v2/evidence/v2-m2/kafka/k0-inputs/kafka-inputs.json",
    "docs/v2/evidence/v2-m2/pulsar/p6/",
    "scripts/check-v2-documentation.sh",
    "scripts/check-v2-m2-kafka-inputs-source.sh",
    "scripts/check-v2-m2-kafka-final-evidence.py",
    "scripts/check-v2-m2-kafka-k9-evidence.sh",
    "scripts/check-v2-m2-pulsar-p6.py",
    "scripts/publish-v2-m2-kafka-k9-evidence.py",
    "scripts/publish-v2-m2-kafka-final-evidence.py",
)
changed = subprocess.check_output(
    ["git", "diff", "--name-only", f"{tested}..HEAD"], cwd=root, text=True
).splitlines()
for path in changed:
    if not any(path == allowed or path.startswith(allowed) for allowed in allowed_evidence_paths):
        raise SystemExit(f"K9 evidence descendant changes tested production/configuration path: {path}")

source = receipt["sourceTuple"]
actual_source_hashes = {
    "sourceLocksSha256": sha(root / "docs/v2/source-locks.json"),
    "m1FinalIndexSha256": sha(root / "docs/v2/evidence/v2-m1/n3/final-index.json"),
    "bookKeeperConfigurationSha256": sha(
        root / "config/v2/m2/kafka/k9/bookkeeper-conformance.properties"
    ),
    "bookKeeperComposeSha256": sha(
        root / "config/v2/m2/kafka/k9/bookkeeper-conformance.compose.yml"
    ),
    "scalePlanSha256": sha(root / "config/v2/m2/kafka/k9/bookkeeper-scale-plan.properties"),
    "selectedDefaultsSha256": sha(
        root / "docs/v2/wire/kafka-bookkeeper-m2-k9-selected-defaults-v1.json"
    ),
}
for key, value in actual_source_hashes.items():
    if source.get(key) != value:
        raise SystemExit(f"K9 source input drifted: {key}")

k0_path = root / "docs/v2/evidence/v2-m2/kafka/k0-inputs/kafka-inputs.json"
k0 = json.loads(k0_path.read_text())
if receipt["prerequisite"].get("kafkaInputsReceiptSha256") != sha(k0_path):
    raise SystemExit("K9 K0 receipt digest differs")
if k0.get("result") != receipt["prerequisite"].get("kafkaInputsResult"):
    raise SystemExit("K9 K0 prerequisite result differs")
k0_source = k0["sourceTuple"]
for key in (
    "m1FinalIndexSha256", "n1SourceCommit", "n1ManifestSha256", "kafkaForkCommit",
    "bookKeeperSourceCommit", "bookKeeperClientJarSha256", "bookKeeperImageManifestDigest",
    "bookKeeperImageConfigDigest",
):
    if source.get(key) != k0_source.get(key):
        raise SystemExit(f"K9 source tuple differs from K0: {key}")

attachments = receipt.get("attachments", [])
paths = [item.get("path") for item in attachments]
if paths != sorted(paths) or len(paths) != 6 or len(paths) != len(set(paths)):
    raise SystemExit("K9 attachment allowlist is not exact, sorted, and unique")
for item in attachments:
    path = root / item["path"]
    if not path.is_file() or path.stat().st_size != item.get("bytes") or sha(path) != item.get("sha256"):
        raise SystemExit(f"K9 attachment changed: {item['path']}")

plan = properties(root / "config/v2/m2/kafka/k9/bookkeeper-scale-plan.properties")
scale_results = []
for tier in (10000, 100000):
    result = json.loads(
        (root / f"docs/v2/evidence/v2-m2/kafka/k9/attachments/scale-{tier}.json").read_text()
    )
    if result.get("schema") != "NEREUS_V2_M2_KAFKA_K9_SCALE_RESULT_V1":
        raise SystemExit(f"K9 scale-{tier} schema differs")
    if result.get("result") != "PASS" or result.get("testedSourceCommit") != tested:
        raise SystemExit(f"K9 scale-{tier} result/source differs")
    if result.get("tierPartitions") != tier or result["counts"].get("partitions") != tier:
        raise SystemExit(f"K9 scale-{tier} did not execute the exact tier")
    if result.get("planSha256") != source["scalePlanSha256"]:
        raise SystemExit(f"K9 scale-{tier} plan binding differs")
    if result.get("conformanceConfigurationSha256") != source["bookKeeperConfigurationSha256"]:
        raise SystemExit(f"K9 scale-{tier} configuration binding differs")
    workload = result["workload"]
    exact_workload = {
        "ioConcurrency": int(plan["ioConcurrency"]),
        "hotLedgerAdmission": int(plan["hotLedgerAdmission"]),
        "payloadBytes": int(plan["payloadBytes"]),
        "tailEntries": int(plan["tailEntries"]),
        "readSamples": int(plan["readSamples"]),
        "recoverySamples": int(plan["recoverySamples"]),
        "rolloverSamples": int(plan["rolloverSamples"]),
    }
    if workload != exact_workload:
        raise SystemExit(f"K9 scale-{tier} workload differs from the predeclared plan")
    counts = result["counts"]
    expected_entries = tier + exact_workload["recoverySamples"] * (
        exact_workload["tailEntries"] - 1
    ) + exact_workload["rolloverSamples"]
    if counts.get("ledgersCreated") != tier + exact_workload["rolloverSamples"]:
        raise SystemExit(f"K9 scale-{tier} ledger/rollover count differs")
    if counts.get("entriesAppended") != expected_entries:
        raise SystemExit(f"K9 scale-{tier} append count differs")
    if counts.get("maximumOwnedHandles") != exact_workload["hotLedgerAdmission"]:
        raise SystemExit(f"K9 scale-{tier} handle admission differs")
    thresholds = (
        (result["resources"]["maximumHeapBytes"], int(plan["maxHarnessHeapBytes"])),
        (result["resources"]["maximumDirectBytes"], int(plan["maxHarnessDirectBytes"])),
        (
            result["resources"]["maximumOpenFileDescriptors"],
            int(plan["maxHarnessOpenFileDescriptors"]),
        ),
        (result["latencies"]["create"]["p99Nanos"], int(plan["maxCreateP99Nanos"])),
        (result["latencies"]["append"]["p99Nanos"], int(plan["maxAppendP99Nanos"])),
        (result["latencies"]["close"]["p99Nanos"], int(plan["maxCloseP99Nanos"])),
        (result["latencies"]["read"]["p99Nanos"], int(plan["maxReadP99Nanos"])),
        (result["latencies"]["recovery"]["p99Nanos"], int(plan["maxRecoveryP99Nanos"])),
        (result["elapsedNanos"], int(plan[f"maxTier{tier}ElapsedNanos"])),
    )
    if any(actual > maximum for actual, maximum in thresholds):
        raise SystemExit(f"K9 scale-{tier} crossed a predeclared maximum")
    if result["partitionOperationsPerSecond"] < int(plan["minimumPartitionOperationsPerSecond"]):
        raise SystemExit(f"K9 scale-{tier} crossed the predeclared throughput minimum")
    scale_results.append(result)

environment = json.loads(
    (root / "docs/v2/evidence/v2-m2/kafka/k9/attachments/environment-summary.json").read_text()
)
if environment.get("result") != "PASS" or environment.get("testedSourceCommit") != tested:
    raise SystemExit("K9 environment result/source differs")
if environment.get("imageReference") != source["bookKeeperImageManifestDigest"].join(
    ["apache/bookkeeper@", ""]
):
    raise SystemExit("K9 environment image differs")
if environment.get("imageId") != source["bookKeeperImageConfigDigest"]:
    raise SystemExit("K9 environment image ID differs")
if environment.get("platform") != "linux/amd64" or environment.get("freshVolumePerTier") is not True:
    raise SystemExit("K9 environment platform/fresh-volume proof differs")
if environment.get("cleanupProof") != {
    "matchingContainersRemaining": 0,
    "matchingVolumesRemaining": 0,
}:
    raise SystemExit("K9 exact cluster cleanup proof differs")
if [tier.get("partitions") for tier in environment.get("tiers", [])] != [10000, 100000]:
    raise SystemExit("K9 environment tier accounting differs")
for tier in environment.get("tiers", []):
    if tier["volumeBytes"]["metadata-service"] > int(plan["maxMetadataVolumeBytes"]):
        raise SystemExit("K9 metadata volume crossed the predeclared bound")
    for service in ("bookie-0", "bookie-1", "bookie-2"):
        if tier["volumeBytes"][service] > int(plan["maxBookieVolumeBytesEach"]):
            raise SystemExit("K9 bookie volume crossed the predeclared bound")

fault = json.loads(
    (root / "docs/v2/evidence/v2-m2/kafka/k9/attachments/fault-tests.json").read_text()
)
if (
    fault.get("result"), fault.get("testedSourceCommit"), fault.get("suites"),
    fault.get("tests"), fault.get("failures"), fault.get("errors"), fault.get("skipped")
) != ("PASS", tested, 2, 9, 0, 0, 0):
    raise SystemExit("K9 real fault test accounting differs")
if [suite["tests"] for suite in fault.get("suiteResults", [])] != [6, 3]:
    raise SystemExit("K9 real fault suite split differs")

log_audit = json.loads(
    (root / "docs/v2/evidence/v2-m2/kafka/k9/attachments/log-audit.json").read_text()
)
if log_audit.get("result") != "PASS_ALLOWLISTED_FRESH_START_ONLY":
    raise SystemExit("K9 log audit result differs")
if log_audit.get("runtimeErrorOrFatalCount") != 0 or len(log_audit.get("rawLogDigests", [])) != 8:
    raise SystemExit("K9 log audit runtime/coverage differs")
if log_audit.get("allowedFreshStartMessages") != [
    {
        "service": "metadata-service",
        "perTierCount": 2,
        "classification": "standalone ZooKeeper ignores one-server quorum configuration",
    },
    {
        "service": "bookie-*",
        "perBookiePerTierCount": 1,
        "classification": "fresh empty volume has no prior lastMark; upstream message explicitly says this is okay",
    },
    {
        "service": "metadata-initializer-bookie",
        "perTierCount": 5,
        "classification": "one-shot metadata shell cannot create its optional local rolling log; bookie health and runtime are unaffected",
    },
]:
    raise SystemExit("K9 log audit allowlist differs")

artifact = json.loads(
    (root / "docs/v2/evidence/v2-m2/kafka/k9/attachments/artifact-report.json").read_text()
)
if artifact.get("result") != "PASS" or artifact.get("testedSourceCommit") != tested:
    raise SystemExit("K9 artifact report result/source differs")
if artifact.get("localTests") != {
    "suites": 42, "tests": 239, "failures": 0, "errors": 0, "skipped": 0
} or artifact.get("realBookKeeperTests") != {
    "suites": 2, "tests": 9, "failures": 0, "errors": 0, "skipped": 0
}:
    raise SystemExit("K9 artifact report test accounting differs")
artifact_paths = {
    ("com.nereusstream:nereus-storage-bookkeeper:0.2.0-SNAPSHOT", "binary"):
        root / "nereus-storage-bookkeeper/build/libs/nereus-storage-bookkeeper-0.2.0-SNAPSHOT.jar",
    ("com.nereusstream:nereus-storage-bookkeeper:0.2.0-SNAPSHOT", "sources"):
        root / "nereus-storage-bookkeeper/build/libs/nereus-storage-bookkeeper-0.2.0-SNAPSHOT-sources.jar",
    ("com.nereusstream:nereus-kafka-bookkeeper:0.2.0-SNAPSHOT", "binary"):
        root / "nereus-kafka-bookkeeper/build/libs/nereus-kafka-bookkeeper-0.2.0-SNAPSHOT.jar",
    ("com.nereusstream:nereus-kafka-bookkeeper:0.2.0-SNAPSHOT", "sources"):
        root / "nereus-kafka-bookkeeper/build/libs/nereus-kafka-bookkeeper-0.2.0-SNAPSHOT-sources.jar",
}
for item in artifact.get("artifacts", []):
    path = artifact_paths.pop((item["coordinate"], item["classifier"]), None)
    if path is None or path.stat().st_size != item["bytes"] or sha(path) != item["sha256"]:
        raise SystemExit("K9 production artifact changed")
if artifact_paths:
    raise SystemExit("K9 artifact report misses a required artifact")

suite_count = test_count = failures = errors = skipped = 0
for module in ("nereus-storage-bookkeeper", "nereus-kafka-bookkeeper"):
    for report in (root / module / "build/test-results/test").glob("TEST-*.xml"):
        attributes = ET.parse(report).getroot().attrib
        suite_count += 1
        test_count += int(attributes["tests"])
        failures += int(attributes["failures"])
        errors += int(attributes["errors"])
        skipped += int(attributes["skipped"])
if (suite_count, test_count, failures, errors, skipped) != (42, 239, 0, 0, 0):
    raise SystemExit("K9 current local test accounting differs")
tests = receipt["tests"]
if (tests["localSuites"], tests["localTests"], tests["realBookKeeperSuites"], tests["realBookKeeperTests"]) != (
    42, 239, 2, 9
):
    raise SystemExit("K9 receipt test accounting differs")

summary = receipt["scale"]
if summary.get("tiers") != [10000, 100000]:
    raise SystemExit("K9 receipt tier summary differs")
if summary.get("actualPartitions") != sum(result["counts"]["partitions"] for result in scale_results):
    raise SystemExit("K9 receipt partition summary differs")
if summary.get("actualLedgersCreated") != sum(result["counts"]["ledgersCreated"] for result in scale_results):
    raise SystemExit("K9 receipt ledger summary differs")
for key, actual in {
    "actualEntriesAppended": sum(result["counts"]["entriesAppended"] for result in scale_results),
    "actualBytesAppended": sum(result["counts"]["bytesAppended"] for result in scale_results),
    "actualMetadataMutations": sum(result["counts"]["metadataMutations"] for result in scale_results),
    "maximumOwnedHandles": max(result["counts"]["maximumOwnedHandles"] for result in scale_results),
    "minimumObservedPartitionOperationsPerSecond": min(
        result["partitionOperationsPerSecond"] for result in scale_results
    ),
    "maximumObservedRecoveryP99Nanos": max(
        result["latencies"]["recovery"]["p99Nanos"] for result in scale_results
    ),
    "maximumObservedHarnessHeapBytes": max(
        result["resources"]["maximumHeapBytes"] for result in scale_results
    ),
    "maximumObservedHarnessDirectBytes": max(
        result["resources"]["maximumDirectBytes"] for result in scale_results
    ),
    "maximumObservedOpenFileDescriptors": max(
        result["resources"]["maximumOpenFileDescriptors"] for result in scale_results
    ),
    "maximumObservedAggregateContainerMemoryBytes": max(
        tier["aggregatePeakContainerMemoryBytes"] for tier in environment["tiers"]
    ),
    "maximumObservedMetadataVolumeBytes": max(
        tier["volumeBytes"]["metadata-service"] for tier in environment["tiers"]
    ),
    "maximumObservedBookieVolumeBytesEach": max(
        tier["volumeBytes"][service]
        for tier in environment["tiers"]
        for service in ("bookie-0", "bookie-1", "bookie-2")
    ),
}.items():
    if summary.get(key) != actual:
        raise SystemExit(f"K9 receipt scale summary differs: {key}")
if receipt.get("selectedDefaults") != {
    "selectionId": "KAFKA_BOOKKEEPER_M2_K9_2026_08_14_V1",
    "completeK0OperationalDomains": 10,
    "topicMayEnlarge": False,
    "cellOrHostMayOnlyLower": True,
}:
    raise SystemExit("K9 selected-defaults receipt boundary differs")

print(
    "K9 current-source real BookKeeper evidence verified: local=42/239, real=2/9, "
    "actual partitions=110000, ledgers=110256, zero failure/error/skip, promotionEligible=false"
)
PY

echo "K9 evidence selects engine defaults only; scenario promotion, Kafka Final, native runtime, Pulsar M2, and global M2 PASS remain outside this gate."
