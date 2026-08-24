#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

fail() {
    echo "V2 M2 Kafka K5 gate: $*" >&2
    exit 1
}

commit_root="nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/commit"
required=(
    "nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/pipeline/KafkaAppendProtocolHooksV1.java"
    "nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/pipeline/KafkaOrderedDurableCommitV1.java"
    "nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/protocol/KafkaPartitionSpeculativeSlotV1.java"
    "nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/commit/KafkaCommittedProducerStateV1.java"
    "nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/commit/KafkaTransactionStateV1.java"
    "nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/commit/KafkaLeaderEpochIndexV1.java"
    "nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/commit/KafkaCoherentCommitCoordinatorV1.java"
)
for file_item in "${required[@]}"; do
    [[ -f "$file_item" ]] || fail "required production file is missing: $file_item"
done
if rg -n 'TODO|FIXME|PLACEHOLDER|NOT_IMPLEMENTED|org\.apache\.bookkeeper|org\.apache\.kafka' \
    "$commit_root" "${required[@]}"; then
    fail "K5 production source contains an unfinished marker or upstream SDK type"
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
    raise SystemExit("V2 M2 Kafka K5 gate: K0 prerequisite is not the input-only PASS")
if receipt.get("sourceTuple", {}).get("sourceLocksSha256") != source_locks_sha:
    raise SystemExit("V2 M2 Kafka K5 gate: K0 source-lock input changed after its receipt")
if subprocess.run(
    ["git", "merge-base", "--is-ancestor", "228b2b253406eddfbfb447de6a44c092d75053e0", "HEAD"],
    cwd=root,
    check=False,
).returncode != 0:
    raise SystemExit("V2 M2 Kafka K5 gate: the reviewed K4 predecessor is not an ancestor")

expected = {
    "KafkaPartitionSpeculativePublicationV1Test": 5,
    "KafkaProducerTransactionStateV1Test": 8,
    "KafkaCoherentCommitCoordinatorV1Test": 8,
}
report_root = root / "nereus-kafka-bookkeeper/build/test-results/test"
tests = failures = errors = skipped = 0
for suite, count in expected.items():
    matches = list(report_root.glob(f"TEST-*.{suite}.xml"))
    if len(matches) != 1:
        raise SystemExit(f"V2 M2 Kafka K5 gate: missing unique suite {suite}")
    attributes = ET.parse(matches[0]).getroot().attrib
    if int(attributes["tests"]) != count:
        raise SystemExit(f"V2 M2 Kafka K5 gate: {suite} count drifted")
    tests += int(attributes["tests"])
    failures += int(attributes["failures"])
    errors += int(attributes["errors"])
    skipped += int(attributes["skipped"])
if (tests, failures, errors, skipped) != (21, 0, 0, 0):
    raise SystemExit(
        f"V2 M2 Kafka K5 gate: tests={tests} failures={failures} errors={errors} skipped={skipped}"
    )
print("V2 M2 Kafka K5 coherent-publication matrix: suites=3 tests=21 failures=0 errors=0 skips=0")
PY

echo "V2 M2 Kafka K5 coherent publication verified; no ACK, HW/LSO advance, real BK, runtime, or M2 PASS."
