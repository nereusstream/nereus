#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

fail() {
    echo "V2 M2 Kafka K7 gate: $*" >&2
    exit 1
}

checkpoint_root="nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/checkpoint"
recovery_root="nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/recovery"
required=(
    "$checkpoint_root/KafkaProtocolCheckpointStoreV1.java"
    "$checkpoint_root/KafkaProtocolCheckpointCodecV1.java"
    "$checkpoint_root/BookKeeperKafkaProtocolCheckpointStoreV1.java"
    "$recovery_root/KafkaBookKeeperTakeoverRecoveryV1.java"
    "$recovery_root/KafkaElectionRecoveryBoundaryV1.java"
    "$recovery_root/KafkaRecoveryBatchProtocolAdapterV1.java"
)
for file_item in "${required[@]}"; do
    [[ -f "$file_item" ]] || fail "required production file is missing: $file_item"
done
if rg -n 'TODO|FIXME|PLACEHOLDER|NOT_IMPLEMENTED|org\.apache\.bookkeeper|org\.apache\.kafka' \
    "$checkpoint_root" "$recovery_root" "${required[@]}"; then
    fail "K7 production source contains an unfinished marker or upstream SDK type"
fi

python3 - <<'PY'
from pathlib import Path
import hashlib
import json
import os
import subprocess
import xml.etree.ElementTree as ET

root = Path.cwd()
receipt = json.loads((root / "docs/v2/evidence/v2-m2/kafka/k0-inputs/kafka-inputs.json").read_bytes())
source_locks_path = Path(os.environ.get(
    "NEREUS_V2_M2_PREREQUISITE_SOURCE_LOCKS", root / "docs/v2/source-locks.json"
))
source_locks_sha = hashlib.sha256(source_locks_path.read_bytes()).hexdigest()
if receipt.get("result") != "PASS_KAFKA_M2_INPUTS_ONLY" or receipt.get("promotionEligible") is not False:
    raise SystemExit("V2 M2 Kafka K7 gate: K0 prerequisite is not the input-only PASS")
if receipt.get("sourceTuple", {}).get("sourceLocksSha256") != source_locks_sha:
    raise SystemExit("V2 M2 Kafka K7 gate: K0 source-lock input changed after its receipt")
if subprocess.run(
    ["git", "merge-base", "--is-ancestor", "8307769c1e4b6e199968279f19ffc94e12720bff", "HEAD"],
    cwd=root,
    check=False,
).returncode != 0:
    raise SystemExit("V2 M2 Kafka K7 gate: the reviewed K6 predecessor is not an ancestor")

expected = {
    "KafkaProtocolCheckpointCodecV1Test": 6,
    "BookKeeperKafkaProtocolCheckpointStoreV1Test": 5,
    "KafkaBookKeeperTakeoverRecoveryV1Test": 15,
}
report_root = root / "nereus-kafka-bookkeeper/build/test-results/test"
tests = failures = errors = skipped = 0
for suite, count in expected.items():
    matches = list(report_root.glob(f"TEST-*.{suite}.xml"))
    if len(matches) != 1:
        raise SystemExit(f"V2 M2 Kafka K7 gate: missing unique suite {suite}")
    attributes = ET.parse(matches[0]).getroot().attrib
    if int(attributes["tests"]) != count:
        raise SystemExit(f"V2 M2 Kafka K7 gate: {suite} count drifted")
    tests += int(attributes["tests"])
    failures += int(attributes["failures"])
    errors += int(attributes["errors"])
    skipped += int(attributes["skipped"])
if (tests, failures, errors, skipped) != (26, 0, 0, 0):
    raise SystemExit(
        f"V2 M2 Kafka K7 gate: tests={tests} failures={failures} errors={errors} skipped={skipped}"
    )
print("V2 M2 Kafka K7 checkpoint/recovery matrix: suites=3 tests=26 failures=0 errors=0 skips=0")
PY

echo "V2 M2 Kafka K7 checkpoint and election-bounded recovery verified; no HW recovery, runtime, real BK, or M2 PASS."
