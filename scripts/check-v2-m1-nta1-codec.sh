#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
domain_main="$repo_root/nereus-domain/src/main/java/com/nereusstream/domain"
domain_results="$repo_root/nereus-domain/build/test-results/test"
oxia_main="$repo_root/nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/v2"
oxia_results="$repo_root/nereus-metadata-oxia/build/test-results/test"
goldens="$repo_root/nereus-domain/src/test/resources/com/nereusstream/domain/codec/nta1-v1-goldens.properties"
receipt="$repo_root/docs/v2/evidence/v2-m0/m1.1b/implementation.json"
scenarios="$repo_root/docs/v2/v2-scenarios.json"

fail() {
    echo "V2 M1.1b NTA1 codec check: $*" >&2
    exit 1
}

for required in \
    "$domain_main/codec/Nta1CodecV1.java" \
    "$domain_main/aggregate/TopicBindingAggregateValidatorV1.java" \
    "$domain_main/aggregate/FrameEncodingPolicyCatalogV1.java" \
    "$domain_main/protocol/PulsarNameInventoryAdmissionV1.java" \
    "$oxia_main/codec/Nta1AggregateAuthorityCodec.java" \
    "$goldens" \
    "$receipt"; do
    [[ -f "$required" ]] || fail "required production, golden, or receipt file is missing: $required"
done

if rg -n '^import ' "$domain_main" |
    rg -v '^.*:import (java\.|com\.nereusstream\.domain\.)' >/dev/null; then
    fail "nereus-domain imports a non-JDK or non-domain production package"
fi

if rg -n 'Nta1ReadinessHarness|Nta1ReadinessEvidenceTest|m1\.1b-q1/readiness\.json' \
    "$domain_main" "$oxia_main" --glob '*.java'; then
    fail "production runtime references the historical Q1 candidate codec or artifact"
fi

if rg -n 'org\.testcontainers|Docker|docker compose|docker-compose' \
    "$repo_root/nereus-domain/src/test/java/com/nereusstream/domain/codec" \
    "$repo_root/nereus-domain/src/test/java/com/nereusstream/domain/protocol" \
    "$repo_root/nereus-metadata-oxia/src/test/java/com/nereusstream/metadata/oxia/v2" --glob '*.java'; then
    fail "exact local NTA1 tests depend on Docker/Testcontainers"
fi

rg -q 'new Nta1AggregateAuthorityCodec()' "$oxia_main/codec/OxiaV2CodecSet.java" \
    || fail "production O2 codec set does not install NTA1 for aggregate authority"
rg -q 'new UnavailableSelectorAuthorityCodec()' "$oxia_main/codec/OxiaV2CodecSet.java" \
    || fail "selector codec is no longer fail closed"
rg -q 'new UnavailableRegistryAuthorityCodec()' "$oxia_main/codec/OxiaV2CodecSet.java" \
    || fail "Registry codec is no longer fail closed"
rg -q 'productionActivationReady' "$oxia_main/OxiaV2CapabilityStore.java" \
    || fail "production activation guard is missing"

if rg -n 'v2M1FinalCheck|PASSED_CURRENT_SOURCE|productionActivationReady\(\) \{[[:space:]]*return true' \
    "$domain_main" "$oxia_main" --glob '*.java' --pcre2; then
    fail "M1.1b production sources claimed runtime/scenario/M1 Final activation"
fi

python3 - "$domain_results" "$oxia_results" "$goldens" "$receipt" "$scenarios" <<'PY'
import json
import pathlib
import sys
import xml.etree.ElementTree as ET

domain_results, oxia_results, goldens_path, receipt_path, scenarios_path = map(pathlib.Path, sys.argv[1:])

def totals(result_root, predicate):
    files = sorted(path for path in result_root.glob("TEST-*.xml") if predicate(path.name))
    if not files:
        raise SystemExit(f"V2 M1.1b NTA1 codec check: no JUnit XML suites under {result_root}")
    result = {name: 0 for name in ("tests", "failures", "errors", "skipped")}
    for path in files:
        suite = ET.parse(path).getroot()
        for name in result:
            result[name] += int(suite.attrib.get(name, "0"))
    if result["tests"] == 0 or result["failures"] or result["errors"] or result["skipped"]:
        raise SystemExit(f"V2 M1.1b NTA1 codec check: unclean JUnit totals {result}")
    return len(files), result

domain_suites, domain = totals(domain_results, lambda name: True)
oxia_suites, oxia = totals(oxia_results, lambda name: ".oxia.v2" in name)

goldens = {}
for raw in goldens_path.read_text().splitlines():
    if raw and not raw.startswith("#"):
        key, value = raw.split("=", 1)
        goldens[key] = value
expected_golden_digests = {
    "kafka.minimum": (194, "30b12e545168aa1b0e21a7af895e33e2408265a154be3d70df95e4b5ff27879b"),
    "kafka.typical": (202, "48a6db09d4d7708984501f6b5c5c2a7385cd77a2181af58b154854a9b8512979"),
    "kafka.boundary": (442, "bca9855a872fc8a3ae14fd11c080c2e3bccdb057aadfcd431a660f98e8c3ae95"),
    "pulsar.minimum": (239, "14e04858189f8bf44379106b143cb9cad12c86145583c38b2220c3196dc87fad"),
    "pulsar.typical": (271, "c678ac0be4215fbada2dce86a9b2a95572ccef453d90a8f8cd4f1bc38fd3d6e8"),
    "pulsar.boundary": (8395, "90f45a1d09d59a6a677b40c261da913ea4166b66e3d387823c9c1c4ce0f2aaae"),
}
for name, (length, digest) in expected_golden_digests.items():
    if goldens.get(name + ".length") != str(length) or goldens.get(name + ".sha256") != digest:
        raise SystemExit(f"V2 M1.1b NTA1 codec check: golden drift for {name}")

receipt = json.loads(receipt_path.read_text())
if (
    receipt.get("schemaVersion") != 1
    or receipt.get("sourceTupleId") != "v2-m0"
    or receipt.get("result") != "PASS_LOCAL_NTA1_CODEC_ONLY"
    or receipt.get("promotionEligible") is not False
    or receipt.get("implementationCommit") != "01a70f17ec9176385e04242490a5fa4f6b230dda"
    or receipt.get("caps") != {
        "maxCellBytes": 54,
        "maxIncarnationBytes": 8214,
        "maxNta1Bytes": 8397,
        "maxPulsarPersistenceNameUtf8Bytes": 4096,
        "maxPulsarTopicNameUtf8Bytes": 4096,
    }
    or receipt.get("goldens") != [
        {"name": name, "length": length, "sha256": digest}
        for name, (length, digest) in expected_golden_digests.items()
    ]
    or receipt.get("scope", {}).get("runtimeActivated") is not False
    or receipt.get("scope", {}).get("scenarioPromotion") is not False
    or receipt.get("scope", {}).get("m1Pass") is not False
):
    raise SystemExit("V2 M1.1b NTA1 codec check: implementation receipt drifted")

statuses = {item["id"]: item["status"] for item in json.loads(scenarios_path.read_text())["scenarios"]}
if statuses.get("V2-META-003") != "IMPLEMENTED_NOT_RUN" or statuses.get("V2-META-004") != "PLANNED":
    raise SystemExit("V2 M1.1b NTA1 codec check: scenario status was promoted")

print(
    "V2 M1.1b exact local tests: "
    f"domain_suites={domain_suites} domain_tests={domain['tests']} "
    f"o2_suites={oxia_suites} o2_tests={oxia['tests']} failures=0 errors=0 skipped=0"
)
PY

echo "V2 M1.1b production NTA1 codec verified locally; no Docker, K1/P1/R1, runtime activation, scenario promotion, or M1 PASS."
