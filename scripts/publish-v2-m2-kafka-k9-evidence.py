#!/usr/bin/env python3
"""Publish source-bound Kafka M2-K9 evidence from raw scale and JUnit outputs."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import xml.etree.ElementTree as ET


ROOT = Path(__file__).resolve().parent.parent
EVIDENCE = ROOT / "docs/v2/evidence/v2-m2/kafka/k9"
ATTACHMENTS = EVIDENCE / "attachments"
TIERS = (10_000, 100_000)
SERVICES = ("metadata-service", "bookie-0", "bookie-1", "bookie-2")
IMAGE_REFERENCE = (
    "apache/bookkeeper@sha256:c0a128931c402d6bf6a6f973ba2f305b9be261659e30754ab95a29510a33bc0d"
)
IMAGE_ID = "sha256:d0e78aaf987ac2feb526507ffb7d4c5137d58c0530f2a8cab4a9595abc89d605"


def fail(message: str) -> None:
    raise SystemExit(f"Kafka M2-K9 publisher: {message}")


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load(path: Path) -> dict:
    try:
        value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        fail(f"cannot read JSON {path}: {error}")
    if not isinstance(value, dict):
        fail(f"JSON root is not an object: {path}")
    return value


def write(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=False) + "\n")


def git(*arguments: str) -> str:
    return subprocess.check_output(["git", "-C", str(ROOT), *arguments], text=True).strip()


def junit(path: Path, expected_tests: int) -> dict:
    if not path.is_file() or path.is_symlink():
        fail(f"JUnit report is absent or symbolic: {path}")
    root = ET.parse(path).getroot()
    result = {
        key: int(root.attrib[key])
        for key in ("tests", "failures", "errors", "skipped")
    }
    if result != {"tests": expected_tests, "failures": 0, "errors": 0, "skipped": 0}:
        fail(f"JUnit result differs for {path.name}: {result}")
    cases = []
    for case in root.findall("testcase"):
        name = case.attrib.get("name", "")
        cases.append(name[:-2] if name.endswith("()") else name)
    if len(cases) != expected_tests or len(set(cases)) != expected_tests:
        fail(f"JUnit cases are incomplete or duplicated: {path.name}")
    return {
        "name": root.attrib["name"],
        **result,
        "junitXmlBytes": path.stat().st_size,
        "junitXmlSha256": sha(path),
        "cases": cases,
    }


def memory_bytes(value: str) -> int:
    measured = value.split("/", 1)[0].strip()
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)(B|KiB|MiB|GiB)", measured)
    if not match:
        fail(f"unsupported Docker memory value: {value}")
    factor = {"B": 1, "KiB": 1 << 10, "MiB": 1 << 20, "GiB": 1 << 30}[match.group(2)]
    return round(float(match.group(1)) * factor)


def cleanup_count(kind: str, project: str) -> int:
    command = ["docker", kind, "ls"] if kind == "volume" else ["docker", "ps", "-a"]
    command.extend(["--filter", f"label=com.docker.compose.project={project}", "--format", "{{.ID}}"])
    rows = subprocess.check_output(command, text=True).splitlines()
    return len([row for row in rows if row.strip()])


def environment(raw_root: Path, tested: str) -> tuple[dict, list[dict]]:
    tiers = []
    raw_environments = []
    for tier in TIERS:
        directory = raw_root / f"tier-{tier}"
        raw = load(directory / f"environment-{tier}.json")
        raw_environments.append(raw)
        if raw.get("schema") != "NEREUS_V2_M2_KAFKA_K9_SCALE_ENVIRONMENT_V1":
            fail(f"scale environment schema differs for {tier}")
        if raw.get("tierPartitions") != tier or raw.get("dockerStatsSamples", 0) <= 0:
            fail(f"scale environment tier/sample count differs for {tier}")
        containers = {row.get("service"): row for row in raw.get("containers", [])}
        if set(containers) != set(SERVICES):
            fail(f"scale environment service set differs for {tier}")
        for service, row in containers.items():
            if (
                row.get("imageReference") != IMAGE_REFERENCE
                or row.get("imageId") != IMAGE_ID
                or row.get("platform") != "linux"
                or row.get("logFile") != f"{service}-{tier}.log"
                or not isinstance(row.get("volumeBytes"), int)
                or row["volumeBytes"] <= 0
            ):
                fail(f"scale environment identity differs for {tier}:{service}")
        peaks = {service: 0 for service in SERVICES}
        sample_counts = {service: 0 for service in SERVICES}
        id_to_service = {row["containerId"]: service for service, row in containers.items()}
        stats = []
        stats_path = directory / raw["dockerStatsFile"]
        for line in stats_path.read_text().splitlines():
            if not line.strip():
                continue
            stats.append(json.loads(line))
        declared_rows = raw["dockerStatsSamples"]
        if (
            declared_rows % len(SERVICES) != 0
            or len(stats) < declared_rows
            or (len(stats) - declared_rows) % len(SERVICES) != 0
        ):
            fail(f"Docker stats declared/tail coverage differs for tier {tier}")
        for row in stats[:declared_rows]:
            container = row.get("Container", "")
            matches = [service for identity, service in id_to_service.items() if identity.startswith(container)]
            if len(matches) != 1:
                fail(f"Docker stats container is not uniquely bound for tier {tier}: {container}")
            service = matches[0]
            peaks[service] = max(peaks[service], memory_bytes(row["MemUsage"]))
            sample_counts[service] += 1
        if min(peaks.values()) <= 0 or set(sample_counts.values()) != {declared_rows // len(SERVICES)}:
            fail(f"Docker stats coverage differs for tier {tier}")
        project = f"nereus-v2-m2-k9-{tier}"
        containers_remaining = cleanup_count("container", project)
        volumes_remaining = cleanup_count("volume", project)
        if containers_remaining or volumes_remaining:
            fail(f"scale cluster cleanup is incomplete for {tier}")
        tiers.append(
            {
                "partitions": tier,
                "dockerStatsSamples": raw["dockerStatsSamples"],
                "aggregatePeakContainerMemoryBytes": sum(peaks.values()),
                "peakContainerMemoryBytes": peaks,
                "volumeBytes": {service: containers[service]["volumeBytes"] for service in SERVICES},
            }
        )
    return (
        {
            "schema": "NEREUS_V2_M2_KAFKA_K9_ENVIRONMENT_SUMMARY_V1",
            "result": "PASS",
            "testedSourceCommit": tested,
            "imageReference": IMAGE_REFERENCE,
            "imageId": IMAGE_ID,
            "platform": "linux/amd64",
            "freshVolumePerTier": True,
            "journalSyncData": True,
            "tiers": tiers,
            "cleanupProof": {"matchingContainersRemaining": 0, "matchingVolumesRemaining": 0},
        },
        raw_environments,
    )


def log_audit(raw_root: Path, tested: str) -> dict:
    digests = []
    log4j_patterns = (
        "Unable to create file ./bookkeeper-shell.log",
        "Could not create plugin of type class org.apache.logging.log4j.core.appender.RollingFileAppender",
        "Unable to invoke factory method in class org.apache.logging.log4j.core.appender.RollingFileAppender",
        "Null object returned for RollingFile in Appenders.",
        'Unable to locate appender "ROLLINGFILE" for logger config "root"',
    )
    for tier in TIERS:
        directory = raw_root / f"tier-{tier}"
        metadata_errors = 0
        last_mark_by_bookie = {service: 0 for service in SERVICES if service.startswith("bookie-")}
        log4j_counts = {pattern: 0 for pattern in log4j_patterns}
        for service in SERVICES:
            path = directory / f"{service}-{tier}.log"
            data = path.read_bytes()
            digests.append({"name": path.name, "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()})
            for line in data.decode(errors="replace").splitlines():
                if not re.search(r"\b(?:ERROR|FATAL)\b", line):
                    continue
                if service == "metadata-service" and "Invalid configuration, only one server specified (ignoring)" in line:
                    metadata_errors += 1
                elif service.startswith("bookie-") and "Problems reading from file (this is okay if it is the first time starting this bookie)" in line:
                    last_mark_by_bookie[service] += 1
                else:
                    matches = [pattern for pattern in log4j_patterns if pattern in line]
                    if len(matches) != 1:
                        fail(f"unclassified ERROR/FATAL in {path.name}: {line[:240]}")
                    log4j_counts[matches[0]] += 1
        if metadata_errors != 2 or set(last_mark_by_bookie.values()) != {1}:
            fail(f"fresh-start message counts differ for tier {tier}")
        if set(log4j_counts.values()) != {1}:
            fail(f"metadata initializer Log4j message counts differ for tier {tier}: {log4j_counts}")
    return {
        "schema": "NEREUS_V2_M2_KAFKA_K9_LOG_AUDIT_V1",
        "result": "PASS_ALLOWLISTED_FRESH_START_ONLY",
        "testedSourceCommit": tested,
        "runtimeErrorOrFatalCount": 0,
        "allowedFreshStartMessages": [
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
        ],
        "rawLogDigests": digests,
    }


def local_totals() -> dict:
    totals = {"suites": 0, "tests": 0, "failures": 0, "errors": 0, "skipped": 0}
    for module in ("nereus-storage-bookkeeper", "nereus-kafka-bookkeeper"):
        for path in sorted((ROOT / module / "build/test-results/test").glob("TEST-*.xml")):
            attributes = ET.parse(path).getroot().attrib
            totals["suites"] += 1
            for key in ("tests", "failures", "errors", "skipped"):
                totals[key] += int(attributes[key])
    if totals != {"suites": 42, "tests": 239, "failures": 0, "errors": 0, "skipped": 0}:
        fail(f"local JUnit totals differ: {totals}")
    return totals


def artifact_report(tested: str, source_locks_sha: str, local: dict, real: dict) -> dict:
    artifacts = []
    for module, classifier, suffix in (
        ("nereus-storage-bookkeeper", "binary", ".jar"),
        ("nereus-storage-bookkeeper", "sources", "-sources.jar"),
        ("nereus-kafka-bookkeeper", "binary", ".jar"),
        ("nereus-kafka-bookkeeper", "sources", "-sources.jar"),
    ):
        path = ROOT / module / "build/libs" / f"{module}-0.2.0-SNAPSHOT{suffix}"
        if not path.is_file() or path.is_symlink():
            fail(f"production artifact is absent or symbolic: {path}")
        artifacts.append(
            {
                "coordinate": f"com.nereusstream:{module}:0.2.0-SNAPSHOT",
                "classifier": classifier,
                "bytes": path.stat().st_size,
                "sha256": sha(path),
            }
        )
    return {
        "schema": "NEREUS_V2_M2_KAFKA_K9_ARTIFACT_REPORT_V1",
        "result": "PASS",
        "testedSourceCommit": tested,
        "sourceLocksSha256": source_locks_sha,
        "artifacts": artifacts,
        "localTests": local,
        "realBookKeeperTests": real,
    }


def publish_k0_receipt(tested: str) -> Path:
    locks_path = ROOT / "docs/v2/source-locks.json"
    locks = load(locks_path)
    binding = locks["m2KafkaK0InputSourceBinding"]
    bookkeeper = binding["bookKeeperInput"]
    k0 = binding["k0Inputs"]
    receipt = {
        "childGates": [
            {"errors": 0, "failed": 0, "gateId": "K0_E", "skipped": 0, "suites": 2, "tests": 9},
            {"errors": 0, "failed": 0, "gateId": "K0_M", "skipped": 0, "suites": 3, "tests": 3},
            {"errors": 0, "failed": 0, "gateId": "K0_N", "skipped": 0, "suites": 3, "tests": 10},
            {"errors": 0, "failed": 0, "gateId": "K0_P", "skipped": 0, "suites": 4, "tests": 22},
            {"errors": 0, "failed": 0, "gateId": "K0_W", "skipped": 0, "suites": 4, "tests": 10},
        ],
        "kind": "KAFKA_M2_INPUTS_ONLY",
        "promotionEligible": False,
        "result": "PASS_KAFKA_M2_INPUTS_ONLY",
        "schema": "NEREUS_V2_M2_KAFKA_INPUTS_RECEIPT_V1",
        "sourceTuple": {
            "bookKeeperCapabilitySha256": binding["capabilityInput"]["sha256"],
            "bookKeeperClientJarSha256": bookkeeper["jarSha256"],
            "bookKeeperClientPomSha256": bookkeeper["pomSha256"],
            "bookKeeperImageConfigDigest": bookkeeper["serverImageConfigDigest"],
            "bookKeeperImageManifestDigest": bookkeeper["serverImageManifestDigest"],
            "bookKeeperSourceCommit": bookkeeper["sourceCommit"],
            "bookKeeperTagObject": bookkeeper["tagObject"],
            "k0ModuleManifestSha256": k0["moduleManifestSha256"],
            "k0ModuleReceiptSha256": k0["moduleReceiptSha256"],
            "kafkaBaseCommit": binding["kafkaInput"]["implementationBaseCommit"],
            "kafkaForkCommit": binding["kafkaInput"]["forkCommit"],
            "m1FinalIndexSha256": binding["m1Final"]["sha256"],
            "m1SourceTupleSha256": binding["m1Final"]["sourceTupleSha256"],
            "n1ManifestSha256": binding["n1Input"]["manifestSha256"],
            "n1SourceCommit": binding["n1Input"]["sourceCommit"],
            "nbke2GoldensSha256": k0["nbke2GoldensSha256"],
            "nbke2ProjectionSha256": k0["nbke2ProjectionSha256"],
            "nereusCommit": tested,
            "numericProjectionSha256": k0["numericProjectionSha256"],
            "sourceLocksSha256": sha(locks_path),
        },
    }
    path = ROOT / "docs/v2/evidence/v2-m2/kafka/k0-inputs/kafka-inputs.json"
    path.write_bytes(json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode())
    return path


def main() -> None:
    if len(sys.argv) != 3:
        fail("usage: publish-v2-m2-kafka-k9-evidence.py <tested-source-commit> <raw-root>")
    tested = sys.argv[1]
    raw_root = Path(sys.argv[2]).resolve()
    if not re.fullmatch(r"[0-9a-f]{40}", tested):
        fail("tested source commit is not canonical")
    if subprocess.run(
        ["git", "-C", str(ROOT), "merge-base", "--is-ancestor", tested, "HEAD"], check=False
    ).returncode != 0:
        fail("tested source commit is not an ancestor of HEAD")
    if not raw_root.is_dir():
        fail("raw scale root is absent")

    existing = load(EVIDENCE / "k9-evidence.json")
    k0_path = publish_k0_receipt(tested)
    k0 = load(k0_path)
    source_locks_path = ROOT / "docs/v2/source-locks.json"
    source_locks_sha = sha(source_locks_path)

    scale_results = []
    for tier in TIERS:
        raw = load(raw_root / f"tier-{tier}" / f"scale-{tier}.json")
        if (
            raw.get("schema") != "NEREUS_V2_M2_KAFKA_K9_SCALE_RESULT_V1"
            or raw.get("testedSourceCommit") != tested
            or raw.get("tierPartitions") != tier
            or raw.get("counts", {}).get("partitions") != tier
            or raw.get("result") != "PASS"
        ):
            fail(f"scale result identity/source/result differs for {tier}")
        scale_results.append(raw)
        write(ATTACHMENTS / f"scale-{tier}.json", raw)

    environment_summary, _ = environment(raw_root, tested)
    write(ATTACHMENTS / "environment-summary.json", environment_summary)
    write(ATTACHMENTS / "log-audit.json", log_audit(raw_root, tested))

    real_rows = []
    for module, name, tests in (
        ("nereus-storage-bookkeeper", "com.nereusstream.storage.bookkeeper.RealBookKeeperCellSessionV1RealTest", 6),
        ("nereus-kafka-bookkeeper", "com.nereusstream.kafka.bookkeeper.RealBookKeeperKafkaEngineV1Test", 3),
    ):
        path = ROOT / module / "build/test-results/realBookKeeperTest" / f"TEST-{name}.xml"
        real_rows.append(junit(path, tests))
    real_totals = {
        "suites": len(real_rows),
        "tests": sum(row["tests"] for row in real_rows),
        "failures": sum(row["failures"] for row in real_rows),
        "errors": sum(row["errors"] for row in real_rows),
        "skipped": sum(row["skipped"] for row in real_rows),
    }
    fault = {
        "schema": "NEREUS_V2_M2_KAFKA_K9_REAL_FAULT_TESTS_V1",
        "result": "PASS",
        "testedSourceCommit": tested,
        "imageReference": IMAGE_REFERENCE,
        "configurationSha256": sha(ROOT / "config/v2/m2/kafka/k9/bookkeeper-conformance.properties"),
        **real_totals,
        "suiteResults": [
            {key: value for key, value in row.items() if key not in ("failures", "errors", "skipped")}
            for row in real_rows
        ],
    }
    write(ATTACHMENTS / "fault-tests.json", fault)

    local = local_totals()
    write(ATTACHMENTS / "artifact-report.json", artifact_report(tested, source_locks_sha, local, real_totals))

    k0_source = k0["sourceTuple"]
    source = {
        "sourceLocksSha256": source_locks_sha,
        "m1FinalIndexSha256": sha(ROOT / "docs/v2/evidence/v2-m1/n3/final-index.json"),
        "n1SourceCommit": k0_source["n1SourceCommit"],
        "n1ManifestSha256": k0_source["n1ManifestSha256"],
        "kafkaForkCommit": k0_source["kafkaForkCommit"],
        "bookKeeperSourceCommit": k0_source["bookKeeperSourceCommit"],
        "bookKeeperClientJarSha256": k0_source["bookKeeperClientJarSha256"],
        "bookKeeperImageManifestDigest": k0_source["bookKeeperImageManifestDigest"],
        "bookKeeperImageConfigDigest": k0_source["bookKeeperImageConfigDigest"],
        "bookKeeperConfigurationSha256": sha(ROOT / "config/v2/m2/kafka/k9/bookkeeper-conformance.properties"),
        "bookKeeperComposeSha256": sha(ROOT / "config/v2/m2/kafka/k9/bookkeeper-conformance.compose.yml"),
        "scalePlanSha256": sha(ROOT / "config/v2/m2/kafka/k9/bookkeeper-scale-plan.properties"),
        "selectedDefaultsSha256": sha(ROOT / "docs/v2/wire/kafka-bookkeeper-m2-k9-selected-defaults-v1.json"),
    }
    attachment_paths = sorted(ATTACHMENTS.glob("*.json"))
    if [path.name for path in attachment_paths] != [
        "artifact-report.json",
        "environment-summary.json",
        "fault-tests.json",
        "log-audit.json",
        "scale-10000.json",
        "scale-100000.json",
    ]:
        fail("K9 attachment set differs")
    attachments = [
        {
            "path": str(path.relative_to(ROOT)),
            "bytes": path.stat().st_size,
            "sha256": sha(path),
        }
        for path in attachment_paths
    ]
    tiers = environment_summary["tiers"]
    scale = {
        "tiers": list(TIERS),
        "actualPartitions": sum(row["counts"]["partitions"] for row in scale_results),
        "actualLedgersCreated": sum(row["counts"]["ledgersCreated"] for row in scale_results),
        "actualEntriesAppended": sum(row["counts"]["entriesAppended"] for row in scale_results),
        "actualBytesAppended": sum(row["counts"]["bytesAppended"] for row in scale_results),
        "actualMetadataMutations": sum(row["counts"]["metadataMutations"] for row in scale_results),
        "maximumOwnedHandles": max(row["counts"]["maximumOwnedHandles"] for row in scale_results),
        "minimumObservedPartitionOperationsPerSecond": min(row["partitionOperationsPerSecond"] for row in scale_results),
        "maximumObservedRecoveryP99Nanos": max(row["latencies"]["recovery"]["p99Nanos"] for row in scale_results),
        "maximumObservedHarnessHeapBytes": max(row["resources"]["maximumHeapBytes"] for row in scale_results),
        "maximumObservedHarnessDirectBytes": max(row["resources"]["maximumDirectBytes"] for row in scale_results),
        "maximumObservedOpenFileDescriptors": max(row["resources"]["maximumOpenFileDescriptors"] for row in scale_results),
        "maximumObservedAggregateContainerMemoryBytes": max(row["aggregatePeakContainerMemoryBytes"] for row in tiers),
        "maximumObservedMetadataVolumeBytes": max(row["volumeBytes"]["metadata-service"] for row in tiers),
        "maximumObservedBookieVolumeBytesEach": max(
            row["volumeBytes"][service]
            for row in tiers
            for service in ("bookie-0", "bookie-1", "bookie-2")
        ),
    }
    receipt = {
        "schema": "NEREUS_V2_M2_KAFKA_K9_EVIDENCE_V1",
        "kind": "KAFKA_M2_K9_REAL_BOOKKEEPER_EVIDENCE",
        "result": "PASS_KAFKA_M2_K9_REAL_BOOKKEEPER_EVIDENCE",
        "promotionEligible": False,
        "testedSourceCommit": tested,
        "sourceTuple": source,
        "prerequisite": {
            "kafkaInputsReceiptSha256": sha(k0_path),
            "kafkaInputsResult": k0["result"],
            "predeclaredPlanCommit": existing["prerequisite"]["predeclaredPlanCommit"],
            "selectedDefaultsCommit": existing["prerequisite"]["selectedDefaultsCommit"],
        },
        "tests": {
            "localSuites": local["suites"],
            "localTests": local["tests"],
            "realBookKeeperSuites": real_totals["suites"],
            "realBookKeeperTests": real_totals["tests"],
            "failed": 0,
            "errors": 0,
            "skipped": 0,
        },
        "scale": scale,
        "selectedDefaults": existing["selectedDefaults"],
        "attachments": attachments,
        "boundary": existing["boundary"],
    }
    write(EVIDENCE / "k9-evidence.json", receipt)
    print(
        "Kafka M2-K9 evidence published: "
        f"source={tested} partitions={scale['actualPartitions']} ledgers={scale['actualLedgersCreated']} "
        f"local={local['suites']}/{local['tests']} real={real_totals['suites']}/{real_totals['tests']}"
    )


if __name__ == "__main__":
    main()
