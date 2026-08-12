#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

python3 - "$repo_root" <<'PY'
import hashlib
import json
import pathlib
import subprocess
import sys
import xml.etree.ElementTree as ET

root = pathlib.Path(sys.argv[1])
locks = json.loads((root / "docs/v2/source-locks.json").read_text())
binding = locks.get("r1RegistryAuthorityBinding")
if not isinstance(binding, dict):
    raise SystemExit("missing r1RegistryAuthorityBinding")

receipt_path = root / binding.get("receipt", "")
if not receipt_path.is_file() or receipt_path.is_symlink():
    raise SystemExit("R1 focused receipt is missing or unsafe")
receipt_bytes = receipt_path.read_bytes()
if len(receipt_bytes) != binding.get("receiptBytes"):
    raise SystemExit("R1 focused receipt length differs from source locks")
if hashlib.sha256(receipt_bytes).hexdigest() != binding.get("receiptSha256"):
    raise SystemExit("R1 focused receipt digest differs from source locks")
receipt = json.loads(receipt_bytes)

n1 = locks.get("n1ArtifactBinding", {})
forks = {item.get("id"): item for item in locks.get("dependencyForkOutputs", [])}
deps = locks.get("dependencyEvidenceBindings", {})
client = deps.get("oxiaClientArtifacts", {})
server = deps.get("oxiaServerRuntime", {})
expected_binding = {
    "implementationCommit": "8a213a85bfaa15769a9b9ea4f74ac7e0b2500b6d",
    "n1SourceCommit": n1.get("sourceCommit"),
    "oxiaClientSourceCommit": forks.get("oxia-client-notification-continuity", {}).get("finalForkCommit"),
    "oxiaClientManifestSha256": client.get("manifest", {}).get("sha256"),
    "oxiaServerSourceCommit": server.get("sourceCommit"),
    "oxiaServerImageReference": server.get("imageReference"),
    "oxiaServerImageDigest": server.get("imageDigest"),
    "receipt": "docs/v2/evidence/v2-m1/r1/r1-focused.json",
    "result": "PASS_R1_FOCUSED_ONLY",
    "conformanceKind": "REGISTRY_CONFORMANCE",
    "promotionEligible": False,
    "evidenceStatus": "FOCUSED_REAL_OXIA_EVIDENCE",
}
for key, value in expected_binding.items():
    if binding.get(key) != value:
        raise SystemExit(f"R1 source-lock binding differs at {key}")
if set(binding) != set(expected_binding) | {"receiptBytes", "receiptSha256"}:
    raise SystemExit("R1 source-lock binding has unknown or missing members")

expected_receipt = {
    "schema": "NEREUS_V2_R1_FOCUSED_RECEIPT_V1",
    "kind": "R1_FOCUSED_ONLY",
    "conformanceKind": "REGISTRY_CONFORMANCE",
    "sourceTupleId": locks.get("focusedEvidenceSourceTupleId"),
    "result": "PASS_R1_FOCUSED_ONLY",
    "selectionEligible": False,
    "promotionEligible": False,
    "scenarioPromotion": False,
    "nereusImplementationCommit": binding["implementationCommit"],
    "n1SourceCommit": n1.get("sourceCommit"),
    "oxiaClientSourceCommit": binding["oxiaClientSourceCommit"],
    "oxiaServerSourceCommit": binding["oxiaServerSourceCommit"],
    "oxiaServerImageDigest": binding["oxiaServerImageDigest"],
    "limits": {
        "maxWriterCount": 14,
        "maxRegistryCanonicalBytes": 51016,
        "maxRegistryAdmissionEvidenceBytes": 4842,
        "maxLifetimeAssignments": 256,
        "assignmentRowBytes": 192,
    },
    "tests": {
        "domain": {"suites": 4, "discovered": 35, "executed": 35, "passed": 35,
                   "failed": 0, "errors": 0, "skipped": 0},
        "metadata": {"suites": 2, "discovered": 8, "executed": 8, "passed": 8,
                     "failed": 0, "errors": 0, "skipped": 0},
        "realOxia": {"suites": 1, "discovered": 2, "executed": 2, "passed": 2,
                     "failed": 0, "errors": 0, "skipped": 0},
    },
    "allocatorModeSelected": False,
    "requiredGate": "v2M1R1FocusedCheck",
    "scope": [
        "REGISTRY_AUTHORITY_AND_INTERLOCK_ONLY",
        "NO_ALLOCATOR_MODE_SELECTION",
        "NO_PULSAR_DATA_PATH_ACTIVATION",
        "NO_SCENARIO_PROMOTION",
        "NO_V1_PRUNE_CLAIM",
        "NO_M1_PASS",
    ],
}
if receipt != expected_receipt:
    raise SystemExit("R1 focused receipt content or non-promotion boundary differs")

expected_reports = {
    root / "nereus-domain/build/test-results/r1RegistryDomainTest": {
        "com.nereusstream.domain.registry.RegistryCapacityEvidenceTest": 18,
        "com.nereusstream.domain.registry.Nvr1RegistryCodecV1Test": 7,
        "com.nereusstream.domain.registry.PulsarVirtualLedgerRegistryTransitionValidatorV1Test": 5,
        "com.nereusstream.domain.registry.RegistryAdmissionEvidenceV1Test": 5,
    },
    root / "nereus-metadata-oxia/build/test-results/r1MetadataTest": {
        "com.nereusstream.metadata.oxia.v2.codec.Nvr1RegistryAuthorityCodecTest": 3,
        "com.nereusstream.metadata.oxia.v2.capability.R1RegistryAuthorityTest": 5,
    },
    root / "nereus-metadata-oxia/build/test-results/r1OxiaIntegrationTest": {
        "com.nereusstream.metadata.oxia.v2.R1RegistryOxiaIntegrationTest": 2,
    },
}

def read_report(directory: pathlib.Path, expected: dict[str, int]) -> None:
    actual = {}
    totals = {name: 0 for name in ("tests", "failures", "errors", "skipped")}
    for report in sorted(directory.glob("TEST-*.xml")):
        suite = ET.parse(report).getroot()
        name = suite.attrib["name"]
        actual[name] = int(suite.attrib.get("tests", "0"))
        for field in totals:
            totals[field] += int(suite.attrib.get(field, "0"))
    if actual != expected:
        raise SystemExit(f"R1 focused suite inventory differs in {directory}: {actual}")
    if totals["tests"] <= 0 or any(totals[field] for field in ("failures", "errors", "skipped")):
        raise SystemExit(f"R1 focused test result is not clean in {directory}: {totals}")

for directory, expected in expected_reports.items():
    read_report(directory, expected)

real_report = next((root / "nereus-metadata-oxia/build/test-results/r1OxiaIntegrationTest").glob("TEST-*.xml"))
real_names = {case.attrib["name"] for case in ET.parse(real_report).getroot().findall("testcase")}
if real_names != {
    "concurrentExactCreatorsConvergeWithoutDuplicateAuthority()",
    "exactCreateCasDerivedViewAndRestartUseOneRegistryAuthority()",
}:
    raise SystemExit("R1 real-Oxia cut inventory differs")

implementation = binding["implementationCommit"]
subprocess.run(["git", "-C", str(root), "cat-file", "-e", implementation + "^{commit}"], check=True)
subprocess.run(["git", "-C", str(root), "merge-base", "--is-ancestor", implementation, "HEAD"], check=True)
subprocess.run(["git", "-C", str(root), "merge-base", "--is-ancestor", implementation, "origin/main"], check=True)
r1_paths = [
    "nereus-domain/src/main/java/com/nereusstream/domain/registry",
    "nereus-metadata-spi/src/main/java/com/nereusstream/metadata/spi/capability/PulsarVirtualLedgerNamespaceRegistryStore.java",
    "nereus-metadata-spi/src/main/java/com/nereusstream/metadata/spi/model/PulsarVirtualLedgerNamespaceRegistryValueV1.java",
    "nereus-metadata-spi/src/main/java/com/nereusstream/metadata/spi/model/VersionedRegistrySnapshot.java",
    "nereus-metadata-spi/src/main/java/com/nereusstream/metadata/spi/model/VersionedVirtualLedgerSliceViewV1.java",
    "nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/v2",
]
subprocess.run(["git", "-C", str(root), "diff", "--quiet", implementation, "HEAD", "--", *r1_paths], check=True)
subprocess.run(["git", "-C", str(root), "diff", "--quiet", "--", *r1_paths], check=True)

for directory in (
    root / "nereus-domain/src/main/java/com/nereusstream/domain/registry",
    root / "nereus-metadata-spi/src/main/java/com/nereusstream/metadata/spi",
    root / "nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/v2/registry",
):
    for source in directory.rglob("*.java"):
        text = source.read_text()
        if "STRICT_SERIALIZED" in text or "RANGE_LEASED" in text:
            raise SystemExit(f"R1 selected an allocator mode in production: {source}")

image = json.loads(subprocess.check_output(
    ["docker", "image", "inspect", binding["oxiaServerImageReference"]], text=True
))[0]
if image.get("Id") != binding["oxiaServerImageDigest"]:
    raise SystemExit("R1 real-Oxia image ID differs from source locks")
labels = image.get("Config", {}).get("Labels") or {}
if labels.get("org.opencontainers.image.revision") != binding["oxiaServerSourceCommit"]:
    raise SystemExit("R1 real-Oxia image source revision differs")

scenarios = json.loads((root / "docs/v2/v2-scenarios.json").read_text())["scenarios"]
position = {row["id"]: row for row in scenarios if row["id"] in {f"V2-POSITION-{i:03d}" for i in range(3, 10)}}
if set(position) != {f"V2-POSITION-{i:03d}" for i in range(3, 10)}:
    raise SystemExit("R1 scenario inventory is incomplete")
planned = all(
    row.get("status") == "PLANNED" and row.get("evidenceReceipt") is None
    for row in position.values()
)
promoted = all(
    row.get("status") == "PASSED_CURRENT_SOURCE"
    and row.get("evidenceReceipt") == "docs/v2/evidence/v2-m1/n3/registry-conformance.json"
    for row in position.values()
)
if not planned and not promoted:
    raise SystemExit("R1 scenario state is neither pre-N3 PLANNED nor the exact Registry N3 promotion footprint")
if promoted and not (root / "docs/v2/evidence/v2-m1/n3/final-index.json").is_file():
    raise SystemExit("R1 N3 promotion footprint exists without its Final index")

print("R1 focused Registry conformance verified: 4/35 domain, 2/8 metadata, 1/2 real Oxia; no allocator or promotion from focused evidence")
PY
