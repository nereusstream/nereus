#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

fail() {
    echo "V2 M2 Kafka K6 gate: $*" >&2
    exit 1
}

read_root="nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/read"
required=(
    "$read_root/KafkaPackedBatchLocatorIndexV1.java"
    "$read_root/KafkaPackedIndexDirectoryV1.java"
    "$read_root/KafkaBookKeeperReadSnapshotV1.java"
    "$read_root/KafkaBookKeeperReadCursorV1.java"
    "$read_root/KafkaBookKeeperTargetedReaderV1.java"
    "nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/adapter/KafkaRawAssignedRecordBatchFactsV1.java"
)
for file_item in "${required[@]}"; do
    [[ -f "$file_item" ]] || fail "required production file is missing: $file_item"
done
if rg -n 'TODO|FIXME|PLACEHOLDER|NOT_IMPLEMENTED|org\.apache\.bookkeeper|org\.apache\.kafka' \
    "$read_root" "${required[@]}"; then
    fail "K6 production source contains an unfinished marker or upstream SDK type"
fi

python3 - <<'PY'
from pathlib import Path
import hashlib
import json
import subprocess
import xml.etree.ElementTree as ET

root = Path.cwd()
receipt = json.loads((root / "docs/v2/evidence/v2-m2/kafka/k0-inputs/kafka-inputs.json").read_bytes())
source_locks_sha = hashlib.sha256((root / "docs/v2/source-locks.json").read_bytes()).hexdigest()
if receipt.get("result") != "PASS_KAFKA_M2_INPUTS_ONLY" or receipt.get("promotionEligible") is not False:
    raise SystemExit("V2 M2 Kafka K6 gate: K0 prerequisite is not the input-only PASS")
if receipt.get("sourceTuple", {}).get("sourceLocksSha256") != source_locks_sha:
    raise SystemExit("V2 M2 Kafka K6 gate: K0 source-lock input changed after its receipt")
if subprocess.run(
    ["git", "merge-base", "--is-ancestor", "9b702e400af1bf2cf32a6ab2dba168e51080ae43", "HEAD"],
    cwd=root,
    check=False,
).returncode != 0:
    raise SystemExit("V2 M2 Kafka K6 gate: the reviewed K5 predecessor is not an ancestor")

expected = {
    "KafkaPackedLocatorLookupV1Test": 7,
    "KafkaBookKeeperTargetedReaderV1Test": 8,
    "KafkaBookKeeperSequentialReaderV1Test": 8,
}
report_root = root / "nereus-kafka-bookkeeper/build/test-results/test"
tests = failures = errors = skipped = 0
for suite, count in expected.items():
    matches = list(report_root.glob(f"TEST-*.{suite}.xml"))
    if len(matches) != 1:
        raise SystemExit(f"V2 M2 Kafka K6 gate: missing unique suite {suite}")
    attributes = ET.parse(matches[0]).getroot().attrib
    if int(attributes["tests"]) != count:
        raise SystemExit(f"V2 M2 Kafka K6 gate: {suite} count drifted")
    tests += int(attributes["tests"])
    failures += int(attributes["failures"])
    errors += int(attributes["errors"])
    skipped += int(attributes["skipped"])
if (tests, failures, errors, skipped) != (23, 0, 0, 0):
    raise SystemExit(
        f"V2 M2 Kafka K6 gate: tests={tests} failures={failures} errors={errors} skipped={skipped}"
    )
print("V2 M2 Kafka K6 targeted-reader matrix: suites=3 tests=23 failures=0 errors=0 skips=0")
PY

echo "V2 M2 Kafka K6 packed targeted/sequential reader verified; no runtime, recovery, real BK, or M2 PASS."
