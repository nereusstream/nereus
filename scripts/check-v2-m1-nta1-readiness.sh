#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
production_root="$repo_root/nereus-domain/src/main/java/com/nereusstream/domain"
test_root="$repo_root/nereus-domain/src/test/java/com/nereusstream/domain/nta1"
result_file="$repo_root/nereus-domain/build/test-results/test/TEST-com.nereusstream.domain.nta1.Nta1ReadinessEvidenceTest.xml"
generated_evidence="$repo_root/nereus-domain/build/reports/v2-m1-nta1-readiness/readiness.json"
committed_evidence="$repo_root/docs/v2/evidence/v2-m0/m1.1b-q1/readiness.json"
design="$repo_root/docs/v2/detailed_design/m1/m1.1b-nta1-codec.md"
scenarios="$repo_root/docs/v2/v2-scenarios.json"
source_locks="$repo_root/docs/v2/source-locks.json"

fail() {
    echo "V2 M1.1b-Q1 readiness check: $*" >&2
    exit 1
}

[[ -d "$test_root" ]] || fail "test-scope evidence harness is missing"
[[ -f "$result_file" ]] || fail "focused JUnit XML result is missing"
[[ -f "$generated_evidence" ]] || fail "generated readiness JSON is missing"
[[ -f "$committed_evidence" ]] || fail "committed readiness JSON is missing"

if find "$production_root" -type f \( -iname '*nta1*' -o -iname '*topicbindingaggregatecodec*' \) -print -quit \
    | rg -q .; then
    fail "a production NTA1 encoder/parser class exists before design acceptance"
fi

if rg -n 'org\.testcontainers|Docker|docker compose|docker-compose' "$test_root" --glob '*.java'; then
    fail "readiness evidence depends on Docker/Testcontainers"
fi

if rg -n 'productionCodecImplemented["[:space:]]*:[[:space:]]*true|runtimeActivated["[:space:]]*:[[:space:]]*true|scenarioPromotion["[:space:]]*:[[:space:]]*true' \
    "$committed_evidence" "$generated_evidence"; then
    fail "readiness evidence claims production implementation, activation, or promotion"
fi

python3 - "$result_file" "$generated_evidence" "$committed_evidence" "$design" "$scenarios" "$source_locks" <<'PY'
import json
import pathlib
import sys
import xml.etree.ElementTree as ET

result_file, generated_path, committed_path, design_path, scenarios_path, locks_path = map(pathlib.Path, sys.argv[1:])

suite = ET.parse(result_file).getroot()
totals = {name: int(suite.attrib.get(name, "0")) for name in ("tests", "failures", "errors", "skipped")}
if totals["tests"] < 14:
    raise SystemExit(f"V2 M1.1b-Q1 readiness check: expected at least 14 tests, found {totals['tests']}")
if totals["failures"] or totals["errors"] or totals["skipped"]:
    raise SystemExit(f"V2 M1.1b-Q1 readiness check: focused test totals are not clean: {totals}")

generated = json.loads(generated_path.read_text())
committed = json.loads(committed_path.read_text())
if generated != committed:
    raise SystemExit("V2 M1.1b-Q1 readiness check: committed evidence differs from generated evidence")

expected = {
    "sourceTupleId": "v2-m0",
    "result": "READINESS_EVIDENCE_ONLY",
    "promotionEligible": False,
    "designStatus": "Proposed",
    "productionCodecImplemented": False,
    "runtimeActivated": False,
    "scenarioPromotion": False,
}
for key, value in expected.items():
    if committed.get(key) != value:
        raise SystemExit(f"V2 M1.1b-Q1 readiness check: invalid evidence field {key}")

if committed["coverage"]["legalProtocolProfileRows"] != 6:
    raise SystemExit("V2 M1.1b-Q1 readiness check: legality matrix does not cover all six rows")
if committed["fixedBytes"] != {
    "nta1ExcludingCellAndIncarnation": 129,
    "kafkaCell": 38,
    "pulsarCell": 54,
    "maxCellBytes": 54,
    "kafkaIncarnationFixed": 26,
    "pulsarIncarnationFixed": 22,
    "kafkaMaxIncarnationBytes": 275,
}:
    raise SystemExit("V2 M1.1b-Q1 readiness check: fixed-size evidence drifted")

locks = json.loads(locks_path.read_text())
fork_bases = {item["id"]: item["commit"] for item in locks["forkDevelopmentBases"]}
if committed["pinnedSources"]["kafka"] != fork_bases["kafka-v2-development-base"]:
    raise SystemExit("V2 M1.1b-Q1 readiness check: Kafka source lock mismatch")
if committed["pinnedSources"]["pulsar"] != fork_bases["pulsar-v2-development-base"]:
    raise SystemExit("V2 M1.1b-Q1 readiness check: Pulsar source lock mismatch")

statuses = {item["id"]: item["status"] for item in json.loads(scenarios_path.read_text())["scenarios"]}
if statuses.get("V2-META-003") != "IMPLEMENTED_NOT_RUN" or statuses.get("V2-META-004") != "PLANNED":
    raise SystemExit("V2 M1.1b-Q1 readiness check: scenario status was promoted")

design = design_path.read_text()
if "designStatus: Proposed" not in design or "production NTA1 remains blocked" not in design:
    raise SystemExit("V2 M1.1b-Q1 readiness check: Proposed/blocked design boundary is missing")

print(
    "V2 M1.1b-Q1 focused tests: "
    f"suites=1 tests={totals['tests']} failures=0 errors=0 skipped=0"
)
PY

echo "V2 M1.1b-Q1 readiness evidence verified; no production codec, Docker, runtime activation, scenario promotion, or M1 PASS."
