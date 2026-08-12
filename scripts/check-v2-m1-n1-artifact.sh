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
import zipfile

root = pathlib.Path(sys.argv[1])
locks = json.loads((root / "docs/v2/source-locks.json").read_text())
binding = locks.get("n1ArtifactBinding")
if not isinstance(binding, dict):
    raise SystemExit("missing n1ArtifactBinding")

source_commit = binding.get("sourceCommit")
coordinate = binding.get("coordinateVersion")
bundle_relative = binding.get("bundleRoot")
expected_coordinate = f"0.2.0-n1.{source_commit}"
if coordinate != expected_coordinate:
    raise SystemExit(f"N1 coordinate mismatch: {coordinate} != {expected_coordinate}")
if bundle_relative != f"gradle/locked-artifacts/nereus-n1/{source_commit}":
    raise SystemExit("N1 bundle root is not source-qualified")
if binding.get("requiredModules") != [
    f"com.nereusstream:nereus-domain:{coordinate}",
    f"com.nereusstream:nereus-metadata-spi:{coordinate}",
]:
    raise SystemExit("N1 required module set/order mismatch")
if binding.get("evidenceStatus") != "IMMUTABLE_INPUT_ONLY":
    raise SystemExit("N1 evidence status must remain IMMUTABLE_INPUT_ONLY")

bundle = root / bundle_relative
artifacts = binding.get("artifacts")
required_labels = {
    "domainJar",
    "domainSourceJar",
    "domainPom",
    "domainGradleMetadata",
    "metadataSpiJar",
    "metadataSpiSourceJar",
    "metadataSpiPom",
    "metadataSpiGradleMetadata",
    "sourceCommitFile",
    "coordinateVersionFile",
}
if not isinstance(artifacts, dict) or set(artifacts) != required_labels:
    raise SystemExit("N1 artifact inventory is not the closed ten-file set")

def sha256(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(65536):
            digest.update(chunk)
    return digest.hexdigest()

manifest_expected = {}
total_bytes = 0
for label, descriptor in artifacts.items():
    if set(descriptor) != {"relativePath", "bytes", "sha256"}:
        raise SystemExit(f"invalid descriptor fields for {label}")
    relative = descriptor["relativePath"]
    path = bundle / relative
    if not path.is_file() or path.is_symlink():
        raise SystemExit(f"missing or unsafe N1 artifact {relative}")
    actual_bytes = path.stat().st_size
    actual_sha = sha256(path)
    if actual_bytes != descriptor["bytes"] or actual_sha != descriptor["sha256"]:
        raise SystemExit(f"N1 artifact mismatch for {relative}")
    if actual_bytes <= 0:
        raise SystemExit(f"empty N1 artifact {relative}")
    manifest_expected[relative] = actual_sha
    total_bytes += actual_bytes

manifest_descriptor = binding.get("manifest")
if not isinstance(manifest_descriptor, dict) or set(manifest_descriptor) != {"relativePath", "bytes", "sha256"}:
    raise SystemExit("invalid N1 manifest descriptor")
manifest_path = bundle / manifest_descriptor["relativePath"]
if (
    not manifest_path.is_file()
    or manifest_path.stat().st_size != manifest_descriptor["bytes"]
    or sha256(manifest_path) != manifest_descriptor["sha256"]
):
    raise SystemExit("N1 manifest identity mismatch")
manifest_actual = {}
for line in manifest_path.read_text().splitlines():
    digest, relative = line.split("  ", 1)
    if relative in manifest_actual:
        raise SystemExit(f"duplicate N1 manifest path {relative}")
    manifest_actual[relative] = digest
if manifest_actual != manifest_expected:
    raise SystemExit("N1 manifest and source-lock artifact inventories differ")

if (bundle / "source-commit.txt").read_text() != source_commit + "\n":
    raise SystemExit("N1 source-commit.txt mismatch")
if (bundle / "coordinate-version.txt").read_text() != coordinate + "\n":
    raise SystemExit("N1 coordinate-version.txt mismatch")

for label in ("domainJar", "domainSourceJar", "metadataSpiJar", "metadataSpiSourceJar"):
    path = bundle / artifacts[label]["relativePath"]
    with zipfile.ZipFile(path) as archive:
        names = [entry.filename for entry in archive.infolist()]
        if len(names) != len(set(names)):
            raise SystemExit(f"duplicate ZIP entry in {path.name}")
        if not names or any(entry.date_time != (1980, 2, 1, 0, 0, 0) for entry in archive.infolist()):
            raise SystemExit(f"non-reproducible timestamp in {path.name}")

namespace = {"m": "http://maven.apache.org/POM/4.0.0"}
domain_pom_path = bundle / artifacts["domainPom"]["relativePath"]
spi_pom_path = bundle / artifacts["metadataSpiPom"]["relativePath"]
domain_pom = ET.parse(domain_pom_path).getroot()
spi_pom = ET.parse(spi_pom_path).getroot()
if domain_pom.findall("m:dependencies/m:dependency", namespace):
    raise SystemExit("N1 domain POM must have no production dependency")
spi_dependencies = spi_pom.findall("m:dependencies/m:dependency", namespace)
if len(spi_dependencies) != 1:
    raise SystemExit("N1 metadata SPI POM must have exactly one dependency")
dependency = spi_dependencies[0]
value = lambda name: dependency.findtext(f"m:{name}", namespaces=namespace)
if (value("groupId"), value("artifactId"), value("version"), value("scope")) != (
    "com.nereusstream",
    "nereus-domain",
    coordinate,
    "compile",
):
    raise SystemExit("N1 metadata SPI POM dependency is not the exact bundled domain")

for label, module_name in (
    ("domainGradleMetadata", "nereus-domain"),
    ("metadataSpiGradleMetadata", "nereus-metadata-spi"),
):
    metadata_path = bundle / artifacts[label]["relativePath"]
    metadata = json.loads(metadata_path.read_text())
    if metadata.get("component") != {
        "group": "com.nereusstream",
        "module": module_name,
        "version": coordinate,
        "attributes": {"org.gradle.status": "release"},
    }:
        raise SystemExit(f"N1 Gradle component mismatch for {module_name}")
    if "SNAPSHOT" in metadata_path.read_text() or "SNAPSHOT" in (bundle / artifacts[label.replace("GradleMetadata", "Pom")]["relativePath"]).read_text():
        raise SystemExit(f"dynamic SNAPSHOT metadata in {module_name}")

spi_metadata = json.loads((bundle / artifacts["metadataSpiGradleMetadata"]["relativePath"]).read_text())
spi_module_dependencies = [
    dependency
    for variant in spi_metadata.get("variants", [])
    for dependency in variant.get("dependencies", [])
]
if not spi_module_dependencies or any(
    dependency.get("group") != "com.nereusstream"
    or dependency.get("module") != "nereus-domain"
    or dependency.get("version", {}).get("requires") != coordinate
    for dependency in spi_module_dependencies
):
    raise SystemExit("N1 Gradle metadata does not pin the exact bundled domain")

receipt_path = root / binding.get("receipt", "")
receipt = json.loads(receipt_path.read_text())
if receipt.get("schema") != "NEREUS_V2_N1_ARTIFACT_RECEIPT_V1":
    raise SystemExit("invalid N1 receipt schema")
if receipt.get("sourceTupleId") != locks.get("focusedEvidenceSourceTupleId") or receipt.get("selectionEligible") is not False:
    raise SystemExit("N1 receipt source tuple/promotion boundary mismatch")
if receipt.get("sourceCommit") != source_commit or receipt.get("coordinateVersion") != coordinate:
    raise SystemExit("N1 receipt source/coordinate mismatch")
if receipt.get("manifestSha256") != manifest_descriptor["sha256"] or receipt.get("artifactBytes") != total_bytes:
    raise SystemExit("N1 receipt artifact summary mismatch")
if receipt.get("cleanBuilds") != 2 or receipt.get("byteIdentical") is not True:
    raise SystemExit("N1 receipt does not record the two-build reproducibility cut")

def git(*args: str) -> None:
    subprocess.run(["git", "-C", str(root), *args], check=True, stdout=subprocess.DEVNULL)

git("cat-file", "-e", f"{source_commit}^{{commit}}")
git("merge-base", "--is-ancestor", source_commit, "HEAD")
git("merge-base", "--is-ancestor", source_commit, "origin/main")

tests = failures = errors = skipped = 0
for report_root in (root / "nereus-domain/build/test-results/test", root / "nereus-metadata-spi/build/test-results/test"):
    reports = sorted(report_root.glob("TEST-*.xml"))
    if not reports:
        raise SystemExit(f"missing JUnit reports under {report_root}")
    for report in reports:
        suite = ET.parse(report).getroot()
        tests += int(suite.attrib.get("tests", 0))
        failures += int(suite.attrib.get("failures", 0))
        errors += int(suite.attrib.get("errors", 0))
        skipped += int(suite.attrib.get("skipped", 0))
if tests <= 0 or failures or errors or skipped:
    raise SystemExit(
        f"invalid N1 focused results: tests={tests} failures={failures} errors={errors} skipped={skipped}"
    )
receipt_tests = receipt.get("tests")
if receipt_tests != {
    "discovered": 119,
    "executed": 119,
    "passed": 119,
    "failed": 0,
    "errors": 0,
    "skipped": 0,
}:
    raise SystemExit("N1 receipt no longer records its frozen source-commit test inventory")

print(
    f"V2 N1 immutable artifact verified: source={source_commit} artifacts={len(artifacts)} "
    f"artifactBytes={total_bytes} frozenTests=119 currentRegressionTests={tests} "
    f"failures={failures} errors={errors} skipped={skipped}"
)
print("N1 is an immutable K1/P1/R1 input only; no source-tuple promotion or M1 PASS.")
PY
