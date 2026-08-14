#!/usr/bin/env python3
import hashlib
import json
import os
import pathlib
import re
import subprocess
import sys
import xml.etree.ElementTree as ET
import zipfile


ROOT = pathlib.Path(__file__).resolve().parent.parent
LOCKS_PATH = ROOT / "docs/v2/source-locks.json"
LOCKS = json.loads(LOCKS_PATH.read_text())
BINDING = LOCKS.get("m2KafkaK0ModuleBinding")


def fail(message: str) -> None:
    raise SystemExit(f"V2 M2 Kafka K0-M artifact gate: {message}")


def sha256(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(65536):
            digest.update(chunk)
    return digest.hexdigest()


if not isinstance(BINDING, dict):
    fail("missing m2KafkaK0ModuleBinding")
expected_fields = {
    "sourceTupleId",
    "sourceCommit",
    "coordinateVersion",
    "bundleRoot",
    "n1SourceCommit",
    "n1CoordinateVersion",
    "requiredModules",
    "artifactCount",
    "artifactBytes",
    "manifest",
    "bookKeeperInput",
    "receipt",
    "result",
    "promotionEligible",
    "evidenceStatus",
}
if set(BINDING) != expected_fields:
    fail("binding fields are not the closed K0-M set")

source_commit = BINDING["sourceCommit"]
coordinate = BINDING["coordinateVersion"]
if not re.fullmatch(r"[0-9a-f]{40}", source_commit):
    fail("source commit is not a full SHA")
if BINDING["sourceTupleId"] != "v2-m2" or coordinate != f"0.2.0-m2.{source_commit}":
    fail("source tuple or coordinate is not source-qualified M2")
if BINDING["bundleRoot"] != f"gradle/locked-artifacts/nereus-m2/{source_commit}":
    fail("bundle root is not source-qualified")
if BINDING["result"] != "PASS_K0_M_INPUT_ONLY" or BINDING["promotionEligible"] is not False:
    fail("binding overstates the K0-M promotion boundary")
if BINDING["evidenceStatus"] != "IMMUTABLE_INPUT_ONLY":
    fail("binding evidence status is not immutable-input-only")

n1 = LOCKS.get("n1ArtifactBinding", {})
if (
    BINDING["n1SourceCommit"] != n1.get("sourceCommit")
    or BINDING["n1CoordinateVersion"] != n1.get("coordinateVersion")
):
    fail("binding differs from the immutable N1 input")

modules = ("nereus-storage-api", "nereus-storage-bookkeeper", "nereus-kafka-bookkeeper")
expected_modules = [f"com.nereusstream:{module}:{coordinate}" for module in modules]
if BINDING["requiredModules"] != expected_modules:
    fail("required module inventory/order is not the closed three-module graph")

bundle = ROOT / BINDING["bundleRoot"]
manifest_descriptor = BINDING["manifest"]
if set(manifest_descriptor) != {"relativePath", "bytes", "sha256"}:
    fail("manifest descriptor fields are invalid")
manifest_path = bundle / manifest_descriptor["relativePath"]
if (
    not manifest_path.is_file()
    or manifest_path.is_symlink()
    or manifest_path.stat().st_size != manifest_descriptor["bytes"]
    or sha256(manifest_path) != manifest_descriptor["sha256"]
):
    fail("manifest identity differs from source locks")

manifest = {}
for line in manifest_path.read_text().splitlines():
    digest, relative = line.split("  ", 1)
    if relative in manifest or not re.fullmatch(r"[0-9a-f]{64}", digest):
        fail(f"invalid or duplicate manifest entry {relative}")
    manifest[relative] = digest

expected_paths = {
    "source-commit.txt",
    "coordinate-version.txt",
    "n1-source-commit.txt",
    "n1-coordinate-version.txt",
}
for module in modules:
    prefix = f"m2/com/nereusstream/{module}/{coordinate}/{module}-{coordinate}"
    expected_paths.update({f"{prefix}.jar", f"{prefix}-sources.jar", f"{prefix}.pom", f"{prefix}.module"})
if set(manifest) != expected_paths:
    fail("manifest is not the closed 16-file K0-M inventory")
if len(manifest) != BINDING["artifactCount"]:
    fail("artifact count differs from source locks")

artifact_bytes = 0
for relative, expected_sha in manifest.items():
    relative_path = pathlib.PurePosixPath(relative)
    if relative_path.is_absolute() or ".." in relative_path.parts:
        fail(f"unsafe manifest path {relative}")
    path = bundle / relative_path
    if not path.is_file() or path.is_symlink() or sha256(path) != expected_sha:
        fail(f"locked artifact differs: {relative}")
    artifact_bytes += path.stat().st_size
if artifact_bytes != BINDING["artifactBytes"]:
    fail("artifact byte total differs from source locks")

identities = {
    "source-commit.txt": source_commit,
    "coordinate-version.txt": coordinate,
    "n1-source-commit.txt": BINDING["n1SourceCommit"],
    "n1-coordinate-version.txt": BINDING["n1CoordinateVersion"],
}
for relative, expected in identities.items():
    if (bundle / relative).read_text() != expected + "\n":
        fail(f"identity file mismatch: {relative}")

for module in modules:
    for suffix in (".jar", "-sources.jar"):
        path = bundle / f"m2/com/nereusstream/{module}/{coordinate}/{module}-{coordinate}{suffix}"
        with zipfile.ZipFile(path) as archive:
            entries = archive.infolist()
            if not entries or len(entries) != len({entry.filename for entry in entries}):
                fail(f"invalid ZIP inventory in {path.name}")
            if any(entry.date_time != (1980, 2, 1, 0, 0, 0) for entry in entries):
                fail(f"non-reproducible ZIP timestamp in {path.name}")

namespace = {"m": "http://maven.apache.org/POM/4.0.0"}


def pom_dependencies(module: str) -> list[tuple[str, str, str, str]]:
    path = bundle / f"m2/com/nereusstream/{module}/{coordinate}/{module}-{coordinate}.pom"
    pom = ET.parse(path).getroot()
    return [
        (
            dependency.findtext("m:groupId", namespaces=namespace),
            dependency.findtext("m:artifactId", namespaces=namespace),
            dependency.findtext("m:version", namespaces=namespace),
            dependency.findtext("m:scope", namespaces=namespace),
        )
        for dependency in pom.findall("m:dependencies/m:dependency", namespace)
    ]


expected_dependencies = {
    "nereus-storage-api": [
        ("com.nereusstream", "nereus-domain", BINDING["n1CoordinateVersion"], "compile")
    ],
    "nereus-storage-bookkeeper": [
        ("com.nereusstream", "nereus-storage-api", coordinate, "compile"),
        ("org.apache.bookkeeper", "bookkeeper-server", "4.18.0", "runtime"),
    ],
    "nereus-kafka-bookkeeper": [
        ("com.nereusstream", "nereus-storage-api", coordinate, "compile"),
        ("com.nereusstream", "nereus-storage-bookkeeper", coordinate, "runtime"),
    ],
}
for module, expected in expected_dependencies.items():
    if pom_dependencies(module) != expected:
        fail(f"published dependency boundary differs for {module}")
    metadata_path = bundle / f"m2/com/nereusstream/{module}/{coordinate}/{module}-{coordinate}.module"
    metadata = json.loads(metadata_path.read_text())
    if metadata.get("component") != {
        "group": "com.nereusstream",
        "module": module,
        "version": coordinate,
        "attributes": {"org.gradle.status": "release"},
    }:
        fail(f"Gradle component identity differs for {module}")
    if "SNAPSHOT" in metadata_path.read_text():
        fail(f"SNAPSHOT leaked into {module} Gradle metadata")

bookkeeper = BINDING["bookKeeperInput"]
expected_bookkeeper = {
    "repository": "https://github.com/apache/bookkeeper.git",
    "tag": "release-4.18.0",
    "tagObject": "bb51381cfb1126a79000cd3211f6293dbd982554",
    "sourceCommit": "cd06340851d6d657b7c7546df01df365c18980de",
    "mavenCoordinate": "org.apache.bookkeeper:bookkeeper-server:4.18.0",
    "jarBytes": 2760614,
    "jarSha256": "8e64f2b7436bb814705f611eb0ac48d64d90de7a50d295905c459d89bc3f9d8f",
    "pomBytes": 12981,
    "pomSha256": "475960270066f0a03ae12b227ced2a2d9b019c7462fec8143a1ab6c92c09b730",
    "evidenceStatus": "EXACT_DEVELOPMENT_INPUT",
}
if bookkeeper != expected_bookkeeper:
    fail("BookKeeper release/source/Maven pin differs from the qualified input")

gradle_home = pathlib.Path(os.environ.get("GRADLE_USER_HOME", pathlib.Path.home() / ".gradle"))
cache_root = gradle_home / "caches/modules-2/files-2.1/org.apache.bookkeeper/bookkeeper-server/4.18.0"
for extension, bytes_key, sha_key in (("jar", "jarBytes", "jarSha256"), ("pom", "pomBytes", "pomSha256")):
    candidates = list(cache_root.glob(f"*/bookkeeper-server-4.18.0.{extension}"))
    if not any(path.stat().st_size == bookkeeper[bytes_key] and sha256(path) == bookkeeper[sha_key] for path in candidates):
        fail(f"resolved BookKeeper {extension.upper()} differs from the exact input")

receipt_path = ROOT / BINDING["receipt"]
receipt = json.loads(receipt_path.read_text())
binding_bytes = (json.dumps(BINDING, sort_keys=True, separators=(",", ":")) + "\n").encode()
binding_sha = hashlib.sha256(binding_bytes).hexdigest()
if (
    receipt.get("schema") != "NEREUS_V2_M2_KAFKA_K0_MODULE_RECEIPT_V1"
    or receipt.get("kind") != "K0_M_IMMUTABLE_MODULE_INPUT"
    or receipt.get("sourceTupleId") != "v2-m2"
    or receipt.get("result") != "PASS_K0_M_INPUT_ONLY"
    or receipt.get("promotionEligible") is not False
    or receipt.get("sourceCommit") != source_commit
    or receipt.get("coordinateVersion") != coordinate
    or receipt.get("manifestSha256") != manifest_descriptor["sha256"]
    or receipt.get("bindingSha256") != binding_sha
    or receipt.get("artifactCount") != len(manifest)
    or receipt.get("artifactBytes") != artifact_bytes
    or receipt.get("cleanBuilds") != 2
    or receipt.get("byteIdentical") is not True
    or receipt.get("tests")
    != {
        "modules": 3,
        "discovered": 3,
        "executed": 3,
        "passed": 3,
        "failed": 0,
        "errors": 0,
        "skipped": 0,
    }
):
    fail("receipt content, binding digest, or non-promotion boundary differs")

for revision in ("HEAD", "origin/main"):
    subprocess.run(["git", "-C", str(ROOT), "merge-base", "--is-ancestor", source_commit, revision], check=True)

print(
    "V2 M2 Kafka K0-M immutable bundle verified: "
    f"source={source_commit} artifacts={len(manifest)} bytes={artifact_bytes} manifest={manifest_descriptor['sha256']}"
)
print(
    "K0-M remains an immutable module/input receipt only; provider evidence is separate, while wire, scenarios, "
    "Kafka Inputs, and M2 Final remain absent."
)
