#!/usr/bin/env bash
set -euo pipefail

kafka_checkout="${1:?usage: check-phase9-kafka-fork-development-source-lock.sh CHECKOUT EXPECTED_HEAD EXPECTED_BASE EXPECTED_REMOTE_TRUNK EXPECTED_VERSION}"
expected_head="${2:?missing expected local fork HEAD}"
expected_base="${3:?missing expected Apache base}"
expected_remote_trunk="${4:?missing expected organization-fork trunk}"
expected_version="${5:?missing expected Kafka version}"
expected_branch="nereus/future9-native-kafka-storage"

fail() {
    echo "F9 Kafka fork development source lock: $*" >&2
    exit 1
}

[[ "$(git -C "$kafka_checkout" rev-parse --is-inside-work-tree 2>/dev/null)" == "true" ]] \
    || fail "missing Kafka fork worktree: $kafka_checkout"
[[ -z "$(git -C "$kafka_checkout" status --porcelain)" ]] \
    || fail "Kafka fork worktree has uncommitted changes"

actual_branch="$(git -C "$kafka_checkout" branch --show-current)"
[[ "$actual_branch" == "$expected_branch" ]] \
    || fail "expected branch $expected_branch, got $actual_branch"
actual_head="$(git -C "$kafka_checkout" rev-parse HEAD)"
[[ "$actual_head" == "$expected_head" ]] \
    || fail "fork HEAD drifted: expected $expected_head, got $actual_head"
actual_parent="$(git -C "$kafka_checkout" rev-parse HEAD^)"
[[ "$actual_parent" == "$expected_base" ]] \
    || fail "fork parent drifted: expected $expected_base, got $actual_parent"

actual_version="$(git -C "$kafka_checkout" show HEAD:gradle.properties \
    | sed -n 's/^version=//p' | head -n 1)"
[[ "$actual_version" == "$expected_version" ]] \
    || fail "Kafka version drifted: expected $expected_version, got $actual_version"

origin_url="$(git -C "$kafka_checkout" remote get-url origin)"
[[ "$origin_url" == "https://github.com/apache/kafka" \
        || "$origin_url" == "https://github.com/apache/kafka.git" \
        || "$origin_url" == "git@github.com:apache/kafka.git" ]] \
    || fail "origin is not apache/kafka: $origin_url"
nereus_url="$(git -C "$kafka_checkout" remote get-url nereus)"
[[ "$nereus_url" == "https://github.com/nereusstream/kafka" \
        || "$nereus_url" == "https://github.com/nereusstream/kafka.git" \
        || "$nereus_url" == "git@github.com:nereusstream/kafka.git" ]] \
    || fail "nereus remote is not nereusstream/kafka: $nereus_url"
actual_remote_trunk="$(git -C "$kafka_checkout" rev-parse refs/remotes/nereus/trunk)"
[[ "$actual_remote_trunk" == "$expected_remote_trunk" ]] \
    || fail "cached nereus/trunk drifted: expected $expected_remote_trunk, got $actual_remote_trunk"
git -C "$kafka_checkout" merge-base --is-ancestor "$expected_base" "$expected_remote_trunk" \
    || fail "locked Apache base is not an ancestor of organization-fork trunk"

actual_changes="$(git -C "$kafka_checkout" diff --name-only "$expected_base"..HEAD | LC_ALL=C sort)"
expected_changes="$(LC_ALL=C sort <<'FILES'
build.gradle
checkstyle/import-control-core.xml
core/src/main/java/kafka/log/nereus/NereusRecordTimestampInspector.java
core/src/test/java/kafka/log/nereus/NereusRecordTimestampInspectorTest.java
FILES
)"
[[ "$actual_changes" == "$expected_changes" ]] \
    || fail "fork change set differs from the reviewed four-file bridge"

while read -r expected path; do
    [[ -n "$expected" ]] || continue
    actual="$(git -C "$kafka_checkout" hash-object "$path")"
    [[ "$actual" == "$expected" ]] \
        || fail "fork source drifted: $path expected $expected, got $actual"
done <<'LOCKS'
f564fde6c76eef4913b2b82fa96e952d68293bf8 build.gradle
7055b5d6449a0737f995a73e6f1fe789b618b2b5 checkstyle/import-control-core.xml
aadcc658a9e74de9798b06d674ecb784947c8762 core/src/main/java/kafka/log/nereus/NereusRecordTimestampInspector.java
205989c5d3adf68127d71be28c6ff9f521abcbf1 core/src/test/java/kafka/log/nereus/NereusRecordTimestampInspectorTest.java
LOCKS

marker_start="$(grep -h -F -c 'Nereus inject start:' \
    "$kafka_checkout/build.gradle" "$kafka_checkout/checkstyle/import-control-core.xml" \
    | awk '{ total += $1 } END { print total + 0 }')"
marker_end="$(grep -h -F -c 'Nereus inject end:' \
    "$kafka_checkout/build.gradle" "$kafka_checkout/checkstyle/import-control-core.xml" \
    | awk '{ total += $1 } END { print total + 0 }')"
[[ "$marker_start" -gt 0 && "$marker_start" == "$marker_end" ]] \
    || fail "Nereus inject markers are absent or unbalanced: $marker_start/$marker_end"

bridge="$kafka_checkout/core/src/main/java/kafka/log/nereus/NereusRecordTimestampInspector.java"
grep -F -q 'implements KafkaRecordTimestampInspector' "$bridge" \
    || fail "timestamp bridge no longer implements the adapter seam"
grep -F -q 'firstAtOrAfter(' "$bridge" \
    || fail "timestamp bridge lost firstAtOrAfter"
grep -F -q 'maximum(' "$bridge" \
    || fail "timestamp bridge lost maximum"
if grep -E -q 'Class\.forName|MethodHandles|setAccessible' "$bridge"; then
    fail "timestamp bridge uses a forbidden reflection bypass"
fi

echo "F9 Kafka fork development source lock: local $actual_head from Apache $expected_base; cached organization trunk $actual_remote_trunk; four bridge blobs and markers match"
