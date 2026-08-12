#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
production_root="$repo_root/nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/v2"
test_root="$repo_root/nereus-metadata-oxia/src/test/java/com/nereusstream/metadata/oxia/v2"
result_root="$repo_root/nereus-metadata-oxia/build/test-results/test"

fail() {
    echo "V2 M1.1a-O2 scaffold check: $*" >&2
    exit 1
}

[[ -d "$production_root" ]] || fail "V2 metadata-oxia production package is missing"
[[ -d "$test_root" ]] || fail "V2 metadata-oxia deterministic tests are missing"

if rg -n \
    'import com\.nereusstream\.(api|metadata\.oxia\.(?!v2))' \
    "$production_root" --glob '*.java' --pcre2; then
    fail "V2 package imports a V1 API or metadata implementation type"
fi

if rg -n \
    'OxiaMetadataStore|WatchRegistration|\.list\(|\.delete\(|\.rangeScan\(|\.notifications\(' \
    "$production_root" --glob '*.java'; then
    fail "V2 scaffold exposes or calls a forbidden broad/list/delete/watch surface"
fi

if rg -n 'org\.testcontainers|Docker' "$test_root" --glob '*.java'; then
    fail "O2 local deterministic tests depend on Docker/Testcontainers"
fi

if rg -n 'mavenLocal\s*\(|flatDir\s*\{' \
    "$repo_root/settings.gradle.kts" \
    "$repo_root/nereus-metadata-oxia/build.gradle.kts"; then
    fail "O1 resolution contains a Maven Local or flatDir fallback"
fi

rg -q 'exclusiveContent' "$repo_root/settings.gradle.kts" \
    || fail "O1 client modules are not isolated by exclusiveContent"
rg -q 'lockedOxiaO1' "$repo_root/settings.gradle.kts" \
    || fail "O1 immutable repository is not registered"
rg -q '^oxia = "0\.9\.4"$' "$repo_root/gradle/libs.versions.toml" \
    || fail "metadata-oxia does not select the qualified O1 version"

implementation_count="$(rg -l \
    'implements (TopicBindingAggregatePublisher|TopicBindingAggregateReader|PulsarTopicGenerationSelectorStore|PulsarVirtualLedgerNamespaceRegistryStore)' \
    "$production_root/capability" --glob '*.java' | wc -l | tr -d ' ')"
[[ "$implementation_count" == "4" ]] \
    || fail "expected exactly four V2 capability implementations, found $implementation_count"

if rg -n 'v2M1FinalCheck|PASSED_CURRENT_SOURCE|productionActivationReady\(\) \{[[:space:]]*return true' \
    "$production_root" --glob '*.java' --pcre2; then
    fail "O2 production sources claimed a final/runtime activation boundary"
fi

python3 - "$result_root" <<'PY'
import pathlib
import sys
import xml.etree.ElementTree as ET

result_root = pathlib.Path(sys.argv[1])
files = sorted(result_root.glob("TEST-com.nereusstream.metadata.oxia.v2*.xml"))
if not files:
    raise SystemExit("V2 M1.1a-O2 scaffold check: no O2 JUnit XML suites were discovered")

totals = {"tests": 0, "failures": 0, "errors": 0, "skipped": 0}
for path in files:
    root = ET.parse(path).getroot()
    for name in totals:
        totals[name] += int(root.attrib.get(name, "0"))

if totals["tests"] < 60:
    raise SystemExit(
        f"V2 M1.1a-O2 scaffold check: expected at least 60 focused tests, found {totals['tests']}"
    )
if totals["failures"] or totals["errors"] or totals["skipped"]:
    raise SystemExit(f"V2 M1.1a-O2 scaffold check: focused test totals are not clean: {totals}")

print(
    "V2 M1.1a-O2 focused tests: "
    f"suites={len(files)} tests={totals['tests']} failures=0 errors=0 skipped=0"
)
PY

echo "V2 M1.1a-O2 local scaffold verified; no Docker, P1/R1, runtime activation, scenario promotion, or M1 PASS."
