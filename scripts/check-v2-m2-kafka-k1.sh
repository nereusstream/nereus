#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

fail() {
    echo "V2 M2 Kafka K1 gate: $*" >&2
    exit 1
}

production_root="nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/protocol"
required=(
    "$production_root/KafkaPartitionFenceV1.java"
    "$production_root/KafkaPartitionFrontiersV1.java"
    "$production_root/KafkaPartitionProtocolStateV1.java"
    "$production_root/KafkaPartitionReadSnapshotV1.java"
    "$production_root/KafkaPartitionCommitSlotV1.java"
    "$production_root/KafkaPartitionFenceTransitionV1.java"
    "$production_root/KafkaPartitionPublicationCellV1.java"
)
for file_item in "${required[@]}"; do
    [[ -f "$file_item" ]] || fail "required production file is missing: $file_item"
done

if rg -n 'TODO|FIXME|PLACEHOLDER|NOT_IMPLEMENTED|zero-test|SKIP' "$production_root"; then
    fail "K1 production source contains an unfinished or bypass marker"
fi

if rg -n '(org\.apache\.bookkeeper|com\.nereusstream\.storage\.api\.bookkeeper|com\.nereusstream\.storage\.bookkeeper)' \
    "$production_root"; then
    fail "pure K1 protocol state depends on a BookKeeper API or implementation"
fi

python3 - <<'PY'
from pathlib import Path
import hashlib
import json
import subprocess

root = Path.cwd()
receipt_path = root / "docs/v2/evidence/v2-m2/kafka/k0-inputs/kafka-inputs.json"
receipt = json.loads(receipt_path.read_bytes())
if receipt.get("schema") != "NEREUS_V2_M2_KAFKA_INPUTS_RECEIPT_V1" \
        or receipt.get("result") != "PASS_KAFKA_M2_INPUTS_ONLY" \
        or receipt.get("promotionEligible") is not False:
    raise SystemExit("V2 M2 Kafka K1 gate: K0 prerequisite receipt is not the closed non-promotable PASS")
source = receipt["sourceTuple"]
source_locks_sha = hashlib.sha256((root / "docs/v2/source-locks.json").read_bytes()).hexdigest()
if source.get("sourceLocksSha256") != source_locks_sha:
    raise SystemExit("V2 M2 Kafka K1 gate: K0 source-lock input changed after its aggregate receipt")
if subprocess.run(
        ["git", "merge-base", "--is-ancestor", source["nereusCommit"], "HEAD"], cwd=root, check=False
).returncode != 0:
    raise SystemExit("V2 M2 Kafka K1 gate: K0 tested source is not an ancestor of HEAD")
print(
    "V2 M2 Kafka K1 prerequisite: "
    f"K0 source={source['nereusCommit']} sourceLocks={source_locks_sha} promotionEligible=false"
)
PY

python3 - <<'PY'
from pathlib import Path
import xml.etree.ElementTree as ET

expected = {
    "KafkaPartitionFrontiersV1Test": 6,
    "KafkaPartitionProtocolStateV1Test": 4,
    "KafkaPartitionPublicationCellV1Test": 6,
    "KafkaPartitionFenceTransitionV1Test": 6,
    "KafkaPartitionPublicationInterleavingTest": 4,
}
root = Path("nereus-kafka-bookkeeper/build/test-results/test")
total = failures = errors = skipped = 0
for suite_name, expected_tests in expected.items():
    matches = list(root.glob(f"TEST-*.{suite_name}.xml"))
    if len(matches) != 1:
        raise SystemExit(f"V2 M2 Kafka K1 gate: missing unique suite {suite_name}")
    attributes = ET.parse(matches[0]).getroot().attrib
    tests = int(attributes["tests"])
    if tests != expected_tests:
        raise SystemExit(f"V2 M2 Kafka K1 gate: {suite_name} tests={tests}, expected={expected_tests}")
    total += tests
    failures += int(attributes["failures"])
    errors += int(attributes["errors"])
    skipped += int(attributes["skipped"])
if total != 26 or failures or errors or skipped:
    raise SystemExit(
        f"V2 M2 Kafka K1 gate: tests={total} failures={failures} errors={errors} skipped={skipped}"
    )
print(f"V2 M2 Kafka K1 tests: suites=5 tests={total} failures=0 errors=0 skipped=0")
PY

echo "V2 M2 Kafka K1 pure coherent publication verified; no BookKeeper I/O, writer, runtime activation, scenario promotion, or M2 PASS."
