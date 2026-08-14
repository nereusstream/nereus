#!/usr/bin/env python3
from pathlib import Path
import hashlib
import json
import subprocess
import sys
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]

def fail(message):
    raise SystemExit(f"V2 M2 Kafka Inputs publisher: {message}")

def git(*arguments):
    return subprocess.check_output(["git", "-C", str(ROOT), *arguments], text=True).strip()

def sha(relative):
    return hashlib.sha256((ROOT / relative).read_bytes()).hexdigest()

def suite(module, name, expected_tests):
    matches = list((ROOT / module / "build/test-results/test").glob(f"TEST-*.{name}.xml"))
    if len(matches) != 1:
        fail(f"missing unique JUnit suite {module}:{name}")
    attributes = ET.parse(matches[0]).getroot().attrib
    actual = int(attributes["tests"])
    failed = int(attributes["failures"])
    errors = int(attributes["errors"])
    skipped = int(attributes["skipped"])
    if actual != expected_tests or failed or errors or skipped:
        fail(f"suite is not exact PASS {module}:{name}")
    return actual

replace_existing = len(sys.argv) == 3 and sys.argv[1] == "--replace-existing"
if len(sys.argv) != 2 and not replace_existing:
    fail("usage: publish-v2-m2-kafka-inputs-receipt.py [--replace-existing] <canonical-output-path>")
output = Path(sys.argv[-1]).resolve()
canonical_output = (ROOT / "docs/v2/evidence/v2-m2/kafka/k0-inputs/kafka-inputs.json").resolve()
if output != canonical_output:
    fail("output is outside the one canonical repository path")
if git("status", "--porcelain"):
    fail("Nereus worktree is not clean")
if output.exists() and not replace_existing:
    fail(f"refusing existing output without --replace-existing: {output}")
if replace_existing:
    if not output.is_file() or output.is_symlink():
        fail("replace target is not one regular non-symlink file")
    tracked = subprocess.run(
        ["git", "-C", str(ROOT), "ls-files", "--error-unmatch", "--", str(output.relative_to(ROOT))],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if tracked.returncode != 0:
        fail("replace target is not the tracked canonical receipt")
head = git("rev-parse", "HEAD")
if head != git("rev-parse", "origin/main"):
    fail("HEAD differs from origin/main")

locks = json.loads((ROOT / "docs/v2/source-locks.json").read_text())
binding = locks["m2KafkaK0InputSourceBinding"]

suite("nereus-storage-bookkeeper", "BookKeeperV3Crc32cAddPayloadLimitV1Test", 3)
suite("nereus-kafka-bookkeeper", "KafkaM2InputsReceiptV1Test", 6)
for name, count in {
    "KafkaBookKeeperDataAdmissionV1Test": 5,
    "KafkaBookKeeperRecoveryEnvelopeV1Test": 4,
    "KafkaBookKeeperNumericProjectionV1Test": 1,
    "BookKeeperCapabilitySnapshotV1Test": 5,
    "ProviderOutcomeContractV1Test": 5,
    "KafkaRunRootAuthorityContractTest": 4,
    "CellSessionOperationRegistryTest": 8,
    "Nbke2CodecV1Test": 4,
    "Nbke2CorruptionMatrixV1Test": 4,
    "Nbke2GoldenVectorV1Test": 1,
    "Nbke2WireProjectionV1Test": 1,
}.items():
    module = "nereus-storage-api" if name in {
        "BookKeeperCapabilitySnapshotV1Test", "ProviderOutcomeContractV1Test", "KafkaRunRootAuthorityContractTest"
    } else "nereus-storage-bookkeeper" if name == "CellSessionOperationRegistryTest" else "nereus-kafka-bookkeeper"
    suite(module, name, count)

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
        "nereusCommit": head,
        "numericProjectionSha256": k0["numericProjectionSha256"],
        "sourceLocksSha256": sha("docs/v2/source-locks.json"),
    },
}
encoded = json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode()
output.parent.mkdir(parents=True, exist_ok=True)
temporary_output = output.with_name(f".{output.name}.tmp")
if temporary_output.exists():
    fail(f"refusing existing temporary output: {temporary_output}")
temporary_output.write_bytes(encoded)
temporary_output.replace(output)
print(f"V2 M2 Kafka Inputs canonical receipt written: path={output} bytes={len(encoded)} sha256={hashlib.sha256(encoded).hexdigest()} source={head}")
