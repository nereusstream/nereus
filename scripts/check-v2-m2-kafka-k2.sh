#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
kafka_checkout="${1:?usage: check-v2-m2-kafka-k2.sh KAFKA_CHECKOUT}"
source_locks="$repo_root/docs/v2/source-locks.json"

fail() {
    echo "V2 M2 Kafka K2 gate: $*" >&2
    exit 1
}

production_root="$repo_root/nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/adapter"
for required_file in \
    KafkaNativeRecordBatchFactsV1.java \
    KafkaNativeAssignedRecordBatchV1.java \
    KafkaAssignedRecordBatchGroupAdapterV1.java \
    KafkaNbke2AssignedAppendGroupV1.java; do
    [[ -f "$production_root/$required_file" ]] || fail "required production file is missing: $required_file"
done
if rg -n 'org\.apache\.kafka|TODO|FIXME|PLACEHOLDER|NOT_IMPLEMENTED' "$production_root"; then
    fail "K2 production boundary imports Kafka SDK types or contains an unfinished marker"
fi

python3 - "$repo_root" <<'PY'
import hashlib
import json
import os
import pathlib
import subprocess
import sys
import xml.etree.ElementTree as ET

root = pathlib.Path(sys.argv[1])
receipt = json.loads((root / "docs/v2/evidence/v2-m2/kafka/k0-inputs/kafka-inputs.json").read_bytes())
if receipt.get("result") != "PASS_KAFKA_M2_INPUTS_ONLY" or receipt.get("promotionEligible") is not False:
    raise SystemExit("V2 M2 Kafka K2 gate: K0 prerequisite receipt is not the closed input-only PASS")
source_locks_path = pathlib.Path(os.environ.get(
    "NEREUS_V2_M2_PREREQUISITE_SOURCE_LOCKS", root / "docs/v2/source-locks.json"
))
source_locks_sha = hashlib.sha256(source_locks_path.read_bytes()).hexdigest()
source = receipt.get("sourceTuple", {})
if source.get("sourceLocksSha256") != source_locks_sha:
    raise SystemExit("V2 M2 Kafka K2 gate: K0 source-lock input changed after its receipt")
if subprocess.run(
    ["git", "merge-base", "--is-ancestor", source.get("nereusCommit", ""), "HEAD"],
    cwd=root,
    check=False,
).returncode != 0:
    raise SystemExit("V2 M2 Kafka K2 gate: K0 tested source is not an ancestor of HEAD")

expected = {
    "KafkaNativeAssignedRecordBatchV1Test": 8,
    "KafkaAssignedRecordBatchGroupAdapterV1Test": 6,
    "KafkaNbke2AssignedAppendGroupV1Test": 3,
}
report_root = root / "nereus-kafka-bookkeeper/build/test-results/test"
tests = failures = errors = skipped = 0
for suite, count in expected.items():
    matches = list(report_root.glob(f"TEST-*.{suite}.xml"))
    if len(matches) != 1:
        raise SystemExit(f"V2 M2 Kafka K2 gate: missing unique suite {suite}")
    attributes = ET.parse(matches[0]).getroot().attrib
    if int(attributes["tests"]) != count:
        raise SystemExit(f"V2 M2 Kafka K2 gate: {suite} count drifted")
    tests += int(attributes["tests"])
    failures += int(attributes["failures"])
    errors += int(attributes["errors"])
    skipped += int(attributes["skipped"])
if (tests, failures, errors, skipped) != (17, 0, 0, 0):
    raise SystemExit(
        f"V2 M2 Kafka K2 gate: tests={tests} failures={failures} errors={errors} skipped={skipped}"
    )
print("V2 M2 Kafka K2 local matrix: suites=3 tests=17 failures=0 errors=0 skips=0")
PY

lock_values=()
while IFS= read -r value; do
    lock_values[${#lock_values[@]}]="$value"
done < <(python3 - "$source_locks" <<'PY'
import json
import sys

source = json.load(open(sys.argv[1]))
kafka = source.get("m2KafkaK0InputSourceBinding", {}).get("kafkaInput", {})
for key in ("repository", "implementationBaseCommit", "branch", "forkCommit"):
    value = kafka.get(key)
    if not isinstance(value, str) or not value:
        raise SystemExit(f"M2 Kafka input is missing {key}")
    print(value)
PY
)
repository="${lock_values[0]}"
base_commit="${lock_values[1]}"
branch="${lock_values[2]}"
fork_commit="${lock_values[3]}"

[[ -d "$kafka_checkout/.git" ]] || fail "Kafka checkout is not a Git repository"
[[ -z "$(git -C "$kafka_checkout" status --porcelain=v1 --untracked-files=all)" ]] \
    || fail "Kafka checkout must be clean before execution"
[[ "$(git -C "$kafka_checkout" remote get-url origin)" == "$repository" ]] \
    || fail "Kafka origin does not match the source lock"
[[ "$(git -C "$kafka_checkout" rev-parse HEAD)" == "$fork_commit" ]] \
    || fail "Kafka HEAD does not match the source lock"
[[ "$(git -C "$kafka_checkout" rev-parse "refs/remotes/origin/$branch")" == "$fork_commit" ]] \
    || fail "Kafka remote-tracking branch does not match the source lock"
git -C "$kafka_checkout" merge-base --is-ancestor "$base_commit" "$fork_commit" \
    || fail "Kafka fork commit does not descend from the implementation base"
[[ "$(sed -n 's/^version=//p' "$kafka_checkout/gradle.properties")" == "4.3.0-SNAPSHOT" ]] \
    || fail "Kafka exact source is not the qualified 4.3.0-SNAPSHOT fork"

(cd "$kafka_checkout" && ./gradlew :clients:classes --no-daemon)
kafka_classpath_output="$(cd "$kafka_checkout" && ./gradlew -q :clients:nereusPrintRuntimeClasspath \
    -I "$repo_root/scripts/templates/v2-m2-kafka-k2/print-runtime-classpath.init.gradle" --no-daemon)"
kafka_classpath="$(printf '%s\n' "$kafka_classpath_output" \
    | sed -n 's/^NEREUS_KAFKA_RUNTIME_CLASSPATH=//p' | tail -1)"
[[ -n "$kafka_classpath" ]] || fail "Kafka exact-source runtime classpath could not be resolved"

temp_root="$(mktemp -d "${TMPDIR:-/tmp}/nereus-m2-k2.XXXXXX")"
trap 'rm -rf -- "$temp_root"' EXIT
classes="$temp_root/classes"
mkdir -p "$classes"
classpath="$kafka_checkout/clients/build/classes/java/main:$kafka_classpath:$repo_root/nereus-kafka-bookkeeper/build/classes/java/main:$repo_root/nereus-domain/build/classes/java/main"
javac --release 17 -cp "$classpath" -d "$classes" \
    "$repo_root/scripts/templates/v2-m2-kafka-k2/Kafka43AssignedRecordBatchConformance.java"
result="$(java -cp "$classes:$classpath" Kafka43AssignedRecordBatchConformance)"
echo "$result"
[[ "$result" == "Kafka 4.3 K2 exact-source conformance: suites=1 tests=13 failures=0 errors=0 skips=0" ]] \
    || fail "exact-source conformance result is not the closed zero-skip matrix"

[[ -z "$(git -C "$kafka_checkout" status --porcelain=v1 --untracked-files=all)" ]] \
    || fail "Kafka checkout changed during exact-source execution"

echo "V2 M2 Kafka K2 exact assigned-RecordBatch adapter verified; no appender, runtime activation, or scenario PASS."
