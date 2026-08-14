#!/usr/bin/env bash
# Licensed under the Apache License, Version 2.0.
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

production_root="nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/evidence"
for required in \
    KafkaM2FinalReceiptV1.java \
    KafkaM2FinalResolverV1.java \
    KafkaM2FinalReceiptCli.java \
    KafkaM2PromotionPolicyV1.java; do
    test -f "$production_root/$required" || {
        echo "K10 policy gate: missing production file $required" >&2
        exit 1
    }
done

if rg -n 'TODO|FIXME|PLACEHOLDER|NOT_IMPLEMENTED' \
    "$production_root"/KafkaM2Final*.java \
    "$production_root/KafkaM2PromotionPolicyV1.java"; then
    echo "K10 policy gate: production Final boundary contains an unfinished marker" >&2
    exit 1
fi

python3 - <<'PY'
from pathlib import Path
import re
import xml.etree.ElementTree as ET

policy_source = Path(
    "nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/evidence/"
    "KafkaM2PromotionPolicyV1.java"
).read_text()
actual = re.findall(r'requirement\(\s*"(V2-[A-Z0-9-]+)"', policy_source)
expected = [
    "V2-BK-003",
    "V2-BK-014",
    "V2-BK-015",
    "V2-BK-016",
    "V2-BK-017",
    "V2-KAF-DATA-001",
    "V2-KAF-DATA-002",
    "V2-KAF-DATA-004",
    "V2-KAF-DATA-005",
    "V2-KAF-DATA-014",
]
if actual != expected:
    raise SystemExit(f"K10 policy gate: exact owned scenario policy differs: {actual}")

report = Path(
    "nereus-kafka-bookkeeper/build/test-results/test/"
    "TEST-com.nereusstream.kafka.bookkeeper.evidence.KafkaM2FinalResolverV1Test.xml"
)
if not report.is_file():
    raise SystemExit("K10 policy gate: focused production Final test report is absent")
attributes = ET.parse(report).getroot().attrib
actual_result = tuple(int(attributes[key]) for key in ("tests", "failures", "errors", "skipped"))
if actual_result != (9, 0, 0, 0):
    raise SystemExit(f"K10 policy gate: focused test result differs: {actual_result}")

print("K10 policy mechanics verified: scenarios=10 tests=9 failures=0 errors=0 skipped=0")
PY

echo "K10 policy gate contains no scenario receipt and makes no Kafka Final or global M2 PASS claim."
