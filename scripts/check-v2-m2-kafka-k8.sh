#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

fail() {
    echo "V2 M2 Kafka K8 gate: $*" >&2
    exit 1
}

replication_root="nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/replication"
required=(
    "$replication_root/KafkaReplicaCommitDescriptorCodecV1.java"
    "$replication_root/KafkaReplicaObservationRecordCodecV1.java"
    "$replication_root/KafkaReplicaObservationJournalV1.java"
    "$replication_root/KafkaReplicaFollowerKernelV1.java"
    "$replication_root/KafkaReplicaElectionValidatorV1.java"
)
for file_item in "${required[@]}"; do
    [[ -f "$file_item" ]] || fail "required production file is missing: $file_item"
done
if rg -n 'TODO|FIXME|PLACEHOLDER|NOT_IMPLEMENTED|org\.apache\.bookkeeper|org\.apache\.kafka' \
    "$replication_root"; then
    fail "K8 production source contains an unfinished marker or upstream SDK type"
fi

rg -q '0eb0a4afe61086fb6574e98279a68549d4a80e0bfd47c9d81147e9530946108d' \
    nereus-kafka-bookkeeper/src/test/java/com/nereusstream/kafka/bookkeeper/replication/KafkaReplicaDescriptorCodecV1Test.java \
    || fail "KRD1 golden SHA-256 is missing"
rg -q '6c8433d3a1b0e4f946b9a58ab0f9eab70f9bc54fd26ae8e2a2e75a67bcd0c58b' \
    nereus-kafka-bookkeeper/src/test/java/com/nereusstream/kafka/bookkeeper/replication/KafkaReplicaDescriptorCodecV1Test.java \
    || fail "KRO1 golden SHA-256 is missing"

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
    raise SystemExit("V2 M2 Kafka K8 gate: K0 prerequisite is not the input-only PASS")
if receipt.get("sourceTuple", {}).get("sourceLocksSha256") != source_locks_sha:
    raise SystemExit("V2 M2 Kafka K8 gate: K0 source-lock input changed after its receipt")
if subprocess.run(
    ["git", "merge-base", "--is-ancestor", "88d19c3b1d934b084fcaae117f0433c38e6650d1", "HEAD"],
    cwd=root,
    check=False,
).returncode != 0:
    raise SystemExit("V2 M2 Kafka K8 gate: the reviewed K7 predecessor is not an ancestor")

expected = {
    "KafkaReplicaDescriptorCodecV1Test": 7,
    "KafkaReplicaObservationJournalV1Test": 7,
    "KafkaReplicaFollowerKernelV1Test": 11,
}
report_root = root / "nereus-kafka-bookkeeper/build/test-results/test"
tests = failures = errors = skipped = 0
for suite, count in expected.items():
    matches = list(report_root.glob(f"TEST-*.{suite}.xml"))
    if len(matches) != 1:
        raise SystemExit(f"V2 M2 Kafka K8 gate: missing unique suite {suite}")
    attributes = ET.parse(matches[0]).getroot().attrib
    if int(attributes["tests"]) != count:
        raise SystemExit(f"V2 M2 Kafka K8 gate: {suite} count drifted")
    tests += int(attributes["tests"])
    failures += int(attributes["failures"])
    errors += int(attributes["errors"])
    skipped += int(attributes["skipped"])
if (tests, failures, errors, skipped) != (25, 0, 0, 0):
    raise SystemExit(
        f"V2 M2 Kafka K8 gate: tests={tests} failures={failures} errors={errors} skipped={skipped}"
    )
print("V2 M2 Kafka K8 replication-kernel matrix: suites=3 tests=25 failures=0 errors=0 skips=0")
PY

echo "V2 M2 Kafka K8 descriptor/journal/Observed/Applied kernel verified; no Kafka wire/runtime, disk adapter, ISR/HW, or M2 PASS."
