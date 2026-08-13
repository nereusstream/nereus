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
binding = locks.get("p1MetadataCapabilityBinding")
if not isinstance(binding, dict):
    raise SystemExit("missing p1MetadataCapabilityBinding")

source = binding.get("sourceCommit")
coordinate = binding.get("coordinateVersion")
n1_source = binding.get("n1SourceCommit")
n1_coordinate = binding.get("n1CoordinateVersion")
oxia_source = binding.get("oxiaClientSourceCommit")
oxia_version = binding.get("oxiaClientVersion")
if coordinate != f"0.2.0-p1.{source}":
    raise SystemExit("P1 coordinate is not source-qualified")
if binding.get("bundleRoot") != f"gradle/locked-artifacts/nereus-p1/{source}":
    raise SystemExit("P1 bundle root is not source-qualified")
if binding.get("requiredModules") != [f"com.nereusstream:nereus-metadata-oxia-p1:{coordinate}"]:
    raise SystemExit("P1 required module set mismatch")
if binding.get("result") != "PASS_P1_ARTIFACT_INPUT_ONLY" or binding.get("promotionEligible") is not False:
    raise SystemExit("P1 artifact binding crossed its non-promotion boundary")
if binding.get("evidenceStatus") != "IMMUTABLE_INPUT_ONLY":
    raise SystemExit("P1 artifact evidence status mismatch")

n1 = locks.get("n1ArtifactBinding", {})
forks = {item.get("id"): item for item in locks.get("dependencyForkOutputs", [])}
if (n1_source, n1_coordinate) != (n1.get("sourceCommit"), n1.get("coordinateVersion")):
    raise SystemExit("P1 artifact does not bind the exact N1 input")
if oxia_source != forks.get("oxia-client-notification-continuity", {}).get("finalForkCommit"):
    raise SystemExit("P1 artifact does not bind the exact O1 client fork")
if oxia_version != "0.9.4":
    raise SystemExit("P1 Oxia client coordinate drifted")

bundle = root / binding["bundleRoot"]
artifacts = binding.get("artifacts")
required = {
    "p1Jar", "p1SourceJar", "p1Pom", "p1GradleMetadata", "sourceCommitFile",
    "coordinateVersionFile", "n1SourceCommitFile", "n1CoordinateVersionFile",
    "oxiaClientSourceCommitFile",
}
if not isinstance(artifacts, dict) or set(artifacts) != required:
    raise SystemExit("P1 artifact inventory is not the closed nine-file set")

def sha256(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(65536):
            digest.update(chunk)
    return digest.hexdigest()

expected_manifest = {}
artifact_bytes = 0
for label, descriptor in artifacts.items():
    if set(descriptor) != {"relativePath", "bytes", "sha256"}:
        raise SystemExit(f"invalid P1 descriptor {label}")
    relative = descriptor["relativePath"]
    path = bundle / relative
    if not path.is_file() or path.is_symlink() or path.stat().st_size <= 0:
        raise SystemExit(f"missing, empty, or unsafe P1 artifact {relative}")
    if path.stat().st_size != descriptor["bytes"] or sha256(path) != descriptor["sha256"]:
        raise SystemExit(f"P1 artifact identity mismatch: {relative}")
    expected_manifest[relative] = descriptor["sha256"]
    artifact_bytes += descriptor["bytes"]

manifest_descriptor = binding.get("manifest")
if not isinstance(manifest_descriptor, dict) or set(manifest_descriptor) != {"relativePath", "bytes", "sha256"}:
    raise SystemExit("invalid P1 manifest descriptor")
manifest_path = bundle / manifest_descriptor["relativePath"]
if (not manifest_path.is_file() or manifest_path.is_symlink()
        or manifest_path.stat().st_size != manifest_descriptor["bytes"]
        or sha256(manifest_path) != manifest_descriptor["sha256"]):
    raise SystemExit("P1 manifest identity mismatch")
actual_manifest = {}
for line in manifest_path.read_text().splitlines():
    digest, relative = line.split("  ", 1)
    if relative in actual_manifest:
        raise SystemExit(f"duplicate P1 manifest path {relative}")
    actual_manifest[relative] = digest
if actual_manifest != expected_manifest:
    raise SystemExit("P1 manifest and source-lock inventories differ")

descriptors = {
    "sourceCommitFile": source,
    "coordinateVersionFile": coordinate,
    "n1SourceCommitFile": n1_source,
    "n1CoordinateVersionFile": n1_coordinate,
    "oxiaClientSourceCommitFile": oxia_source,
}
for label, expected in descriptors.items():
    if (bundle / artifacts[label]["relativePath"]).read_text() != expected + "\n":
        raise SystemExit(f"P1 descriptor content mismatch: {label}")

allowed_binary_prefixes = ("META-INF/", "com/nereusstream/metadata/oxia/v2/")
for label in ("p1Jar", "p1SourceJar"):
    path = bundle / artifacts[label]["relativePath"]
    with zipfile.ZipFile(path) as archive:
        entries = archive.infolist()
        names = [entry.filename for entry in entries]
        if len(names) != len(set(names)):
            raise SystemExit(f"duplicate ZIP entry in {path.name}")
        if not entries or any(entry.date_time != (1980, 2, 1, 0, 0, 0) for entry in entries):
            raise SystemExit(f"non-reproducible timestamp in {path.name}")
        files = [name for name in names if not name.endswith("/")]
        if any(not name.startswith(allowed_binary_prefixes) for name in files):
            raise SystemExit(f"P1 artifact leaks outside the V2 capability package: {path.name}")
        if any("com/nereusstream/api/" in name or "/phase" in name.lower() for name in names):
            raise SystemExit(f"P1 artifact contains V1 API/runtime residue: {path.name}")
        if label == "p1SourceJar":
            install_source = archive.read(
                "com/nereusstream/metadata/oxia/v2/continuity/InstallPermit.java"
            ).decode("utf-8")
            if "public record InstallPermit" not in install_source:
                raise SystemExit("P1 artifact does not export the bounded continuity permit")

namespace = {"m": "http://maven.apache.org/POM/4.0.0"}
pom_path = bundle / artifacts["p1Pom"]["relativePath"]
pom = ET.parse(pom_path).getroot()
pom_dependencies = []
for dependency in pom.findall("m:dependencies/m:dependency", namespace):
    value = lambda name: dependency.findtext(f"m:{name}", namespaces=namespace)
    pom_dependencies.append((value("groupId"), value("artifactId"), value("version"), value("scope")))
expected_dependencies = [
    ("com.nereusstream", "nereus-domain", n1_coordinate, "compile"),
    ("com.nereusstream", "nereus-metadata-spi", n1_coordinate, "compile"),
    ("io.github.oxia-db", "oxia-client", oxia_version, "compile"),
]
if pom_dependencies != expected_dependencies:
    raise SystemExit("P1 POM dependency set/order mismatch")

module_path = bundle / artifacts["p1GradleMetadata"]["relativePath"]
module = json.loads(module_path.read_text())
if module.get("component") != {
    "group": "com.nereusstream",
    "module": "nereus-metadata-oxia-p1",
    "version": coordinate,
    "attributes": {"org.gradle.status": "release"},
}:
    raise SystemExit("P1 Gradle component mismatch")
for variant in module.get("variants", []):
    dependencies = variant.get("dependencies", [])
    if dependencies:
        normalized = [
            (item.get("group"), item.get("module"), item.get("version", {}).get("requires"))
            for item in dependencies
        ]
        if normalized != [(a, b, c) for a, b, c, _ in expected_dependencies]:
            raise SystemExit(f"P1 Gradle dependency mismatch in {variant.get('name')}")
if "SNAPSHOT" in pom_path.read_text() + module_path.read_text() or "changing=true" in module_path.read_text():
    raise SystemExit("P1 metadata contains a dynamic dependency")

receipt = json.loads((root / binding["receipt"]).read_text())
if receipt.get("schema") != "NEREUS_V2_P1_ARTIFACT_RECEIPT_V1":
    raise SystemExit("invalid P1 artifact receipt schema")
if receipt.get("sourceTupleId") != locks.get("focusedEvidenceSourceTupleId"):
    raise SystemExit("P1 artifact receipt source tuple mismatch")
if receipt.get("sourceCommit") != source or receipt.get("coordinateVersion") != coordinate:
    raise SystemExit("P1 artifact receipt source mismatch")
if receipt.get("manifestSha256") != manifest_descriptor["sha256"] or receipt.get("artifactBytes") != artifact_bytes:
    raise SystemExit("P1 artifact receipt summary mismatch")
if receipt.get("cleanBuilds") != 2 or receipt.get("byteIdentical") is not True:
    raise SystemExit("P1 artifact receipt lacks reproducibility proof")
tests = receipt.get("tests", {})
if (tests.get("discovered", 0) <= 0 or tests.get("executed") != tests.get("discovered")
        or tests.get("passed") != tests.get("executed")
        or any(tests.get(field) != 0 for field in ("failed", "errors", "skipped"))):
    raise SystemExit("P1 artifact receipt contains invalid focused test accounting")
if (receipt.get("result") != "PASS_P1_ARTIFACT_INPUT_ONLY"
        or receipt.get("selectionEligible") is not False
        or receipt.get("promotionEligible") is not False):
    raise SystemExit("P1 artifact receipt crossed its non-promotion boundary")

reports = sorted((root / "nereus-metadata-oxia/build/test-results/p1MetadataTest").glob("TEST-*.xml"))
if not reports:
    raise SystemExit("P1 artifact gate executed zero current focused tests")
current_tests = current_failures = current_errors = current_skipped = 0
for report in reports:
    suite = ET.parse(report).getroot()
    current_tests += int(suite.attrib.get("tests", 0))
    current_failures += int(suite.attrib.get("failures", 0))
    current_errors += int(suite.attrib.get("errors", 0))
    current_skipped += int(suite.attrib.get("skipped", 0))
if current_tests <= 0 or current_failures or current_errors or current_skipped:
    raise SystemExit("P1 current focused regression gate failed or skipped tests")
if tests.get("discovered") != current_tests:
    raise SystemExit(
        f"P1 artifact receipt test count differs from current execution: "
        f"{tests.get('discovered')} != {current_tests}"
    )

def git(*args: str) -> None:
    subprocess.run(["git", "-C", str(root), *args], check=True, stdout=subprocess.DEVNULL)

git("cat-file", "-e", f"{source}^{{commit}}")
git("merge-base", "--is-ancestor", source, "HEAD")
git("merge-base", "--is-ancestor", source, "origin/main")

print(
    f"V2 P1 immutable adapter artifact verified: source={source} files={len(artifacts)} "
    f"artifactBytes={artifact_bytes} currentTests={current_tests} failures={current_failures} "
    f"errors={current_errors} skipped={current_skipped}"
)
print("P1 artifact is an immutable adapter input only; no native capability, source promotion, or M1 PASS.")
PY
