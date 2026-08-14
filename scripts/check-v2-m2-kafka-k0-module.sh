#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

fail() {
    echo "V2 M2 Kafka K0-M module gate: $*" >&2
    exit 1
}

n1_commit="$(jq -er '.n1ArtifactBinding.sourceCommit' docs/v2/source-locks.json)"
n1_version="$(jq -er '.n1ArtifactBinding.coordinateVersion' docs/v2/source-locks.json)"
n1_root="$(jq -er '.n1ArtifactBinding.bundleRoot' docs/v2/source-locks.json)"
n1_manifest_sha="$(jq -er '.n1ArtifactBinding.manifest.sha256' docs/v2/source-locks.json)"

[[ "$n1_version" == "0.2.0-n1.$n1_commit" ]] || fail "N1 coordinate/source mismatch"
[[ -f "$n1_root/manifest.sha256" ]] || fail "immutable N1 manifest is missing"
[[ "$(shasum -a 256 "$n1_root/manifest.sha256" | awk '{print $1}')" == "$n1_manifest_sha" ]] ||
    fail "immutable N1 manifest digest mismatch"

python3 scripts/check-v2-m2-kafka-k0-artifact.py

for module in nereus-storage-api nereus-storage-bookkeeper nereus-kafka-bookkeeper; do
    [[ -f "$module/build/libs/$module-0.2.0-SNAPSHOT.jar" ]] || fail "$module binary JAR is missing"
    [[ -f "$module/build/libs/$module-0.2.0-SNAPSHOT-sources.jar" ]] || fail "$module sources JAR is missing"
    [[ -f "$module/build/publications/mavenJava/pom-default.xml" ]] || fail "$module POM is missing"
    [[ -f "$module/build/publications/mavenJava/module.json" ]] || fail "$module Gradle metadata is missing"
done

grep -Fq "<version>$n1_version</version>" nereus-storage-api/build/publications/mavenJava/pom-default.xml ||
    fail "storage API POM does not pin the exact N1 coordinate"
grep -Fq '<artifactId>bookkeeper-server</artifactId>' \
    nereus-storage-bookkeeper/build/publications/mavenJava/pom-default.xml ||
    fail "BookKeeper adapter POM is missing bookkeeper-server"
grep -Fq '<version>4.18.0</version>' nereus-storage-bookkeeper/build/publications/mavenJava/pom-default.xml ||
    fail "BookKeeper adapter POM does not pin 4.18.0"

if rg -n 'kafka-clients|pulsar-|oxia-' nereus-storage-api/build.gradle.kts \
    nereus-storage-bookkeeper/build.gradle.kts; then
    fail "storage modules declare a forbidden protocol or metadata runtime"
fi

python3 - <<'PY'
from pathlib import Path
import sys
import xml.etree.ElementTree as ET

modules = ("nereus-storage-api", "nereus-storage-bookkeeper", "nereus-kafka-bookkeeper")
total = failures = errors = skipped = 0
for module in modules:
    reports = sorted(Path(module, "build", "test-results", "test").glob("TEST-*.xml"))
    if not reports:
        raise SystemExit(f"V2 M2 Kafka K0-M module gate: no JUnit XML for {module}")
    module_tests = 0
    for report in reports:
        root = ET.parse(report).getroot()
        module_tests += int(root.attrib["tests"])
        failures += int(root.attrib["failures"])
        errors += int(root.attrib["errors"])
        skipped += int(root.attrib["skipped"])
    if module_tests <= 0:
        raise SystemExit(f"V2 M2 Kafka K0-M module gate: zero tests for {module}")
    total += module_tests
if failures or errors or skipped:
    raise SystemExit(
        f"V2 M2 Kafka K0-M module gate: tests={total} failures={failures} errors={errors} skipped={skipped}"
    )
print(f"V2 M2 Kafka K0-M tests: modules={len(modules)} tests={total} failures=0 errors=0 skipped=0")
PY

grep -Fq 'include("nereus-storage-api")' settings.gradle.kts || fail "storage API module is not included"
grep -Fq 'include("nereus-storage-bookkeeper")' settings.gradle.kts || fail "BookKeeper module is not included"
grep -Fq 'include("nereus-kafka-bookkeeper")' settings.gradle.kts || fail "Kafka engine module is not included"
grep -Fq 'source-qualified M2 coordinate is restricted to the three Kafka K0 production artifacts' \
    build.gradle.kts || fail "filtered M2 publication guard is missing"

echo "V2 M2 Kafka K0-M module graph verified; provider, NBKE2, Kafka runtime, scenario promotion, and M2 Final remain absent."
