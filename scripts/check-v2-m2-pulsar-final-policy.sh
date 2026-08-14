#!/usr/bin/env bash
# Licensed under the Apache License, Version 2.0.
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

production_root="nereus-pulsar-offload/src/main/java/com/nereusstream/pulsar/offload/evidence"
for required in \
    PulsarM2FinalReceiptV1.java \
    PulsarM2FinalResolverV1.java \
    PulsarM2FinalReceiptCli.java \
    PulsarM2PromotionPolicyV1.java; do
    test -f "$production_root/$required" || {
        echo "Pulsar Final policy gate: missing production file $required" >&2
        exit 1
    }
done

if rg -n 'TODO|FIXME|PLACEHOLDER|NOT_IMPLEMENTED' \
    "$production_root"/PulsarM2Final*.java \
    "$production_root/PulsarM2PromotionPolicyV1.java"; then
    echo "Pulsar Final policy gate: production Final boundary contains an unfinished marker" >&2
    exit 1
fi

python3 - <<'PY'
from pathlib import Path
import re
import xml.etree.ElementTree as ET

policy_source = Path(
    "nereus-pulsar-offload/src/main/java/com/nereusstream/pulsar/offload/evidence/"
    "PulsarM2PromotionPolicyV1.java"
).read_text()
actual = re.findall(r'requirement\(\s*"(V2-[A-Z0-9-]+)"', policy_source)
expected = [
    "V2-BK-001",
    "V2-BK-002",
    "V2-BK-004",
    "V2-BK-005",
    "V2-BK-006",
    "V2-BK-007",
    "V2-BK-008",
    "V2-BK-009",
    "V2-BK-010",
    "V2-BK-012",
    "V2-BK-013",
]
if actual != expected:
    raise SystemExit(f"Pulsar Final policy gate: exact owned scenario policy differs: {actual}")

report = Path(
    "nereus-pulsar-offload/build/test-results/test/"
    "TEST-com.nereusstream.pulsar.offload.evidence.PulsarM2FinalResolverV1Test.xml"
)
if not report.is_file():
    raise SystemExit("Pulsar Final policy gate: focused production Final test report is absent")
attributes = ET.parse(report).getroot().attrib
actual_result = tuple(int(attributes[key]) for key in ("tests", "failures", "errors", "skipped"))
if actual_result != (9, 0, 0, 0):
    raise SystemExit(f"Pulsar Final policy gate: focused test result differs: {actual_result}")

design = Path("docs/v2/detailed_design/m2/pulsar-m2-final-evidence.md").read_text()
for literal in (
    "exactly these eleven complete M2 rows",
    "exactly 32 scenario-suite references and eight attachments",
    "V2-BK-011` remains `PLANNED",
    "Pulsar Final is not broker-process/NAR activation",
):
    if literal not in design:
        raise SystemExit(f"Pulsar Final policy gate: required design boundary is absent: {literal}")

print("Pulsar Final policy mechanics verified: scenarios=11 suiteReferences=32 tests=9 failures=0 errors=0 skipped=0")
PY

echo "Pulsar Final policy gate contains no scenario receipt and makes no Pulsar Final or global M2 PASS claim."
