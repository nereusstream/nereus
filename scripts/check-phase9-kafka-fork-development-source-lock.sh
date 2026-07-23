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
git -C "$kafka_checkout" merge-base --is-ancestor "$expected_base" "$actual_head" \
    || fail "locked Apache base is not an ancestor of fork HEAD"
actual_commit_count="$(git -C "$kafka_checkout" rev-list --count "$expected_base"..HEAD)"
[[ "$actual_commit_count" == "4" ]] \
    || fail "expected four reviewed fork commits, got $actual_commit_count"

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
core/src/main/java/kafka/log/nereus/NereusKafkaExceptionMapper.java
core/src/main/java/kafka/log/nereus/NereusListOffsetsBridge.java
core/src/main/java/kafka/log/nereus/NereusListOffsetsScanConfig.java
core/src/main/java/kafka/log/nereus/NereusRecordTimestampInspector.java
core/src/main/scala/kafka/cluster/Partition.scala
core/src/main/scala/kafka/log/nereus/NereusListOffsetsLifecycle.scala
core/src/main/scala/kafka/server/ReplicaManager.scala
core/src/test/java/kafka/log/nereus/NereusKafkaExceptionMapperTest.java
core/src/test/java/kafka/log/nereus/NereusListOffsetsBridgeTest.java
core/src/test/java/kafka/log/nereus/NereusRecordTimestampInspectorTest.java
core/src/test/scala/unit/kafka/cluster/PartitionTest.scala
core/src/test/scala/unit/kafka/log/nereus/NereusListOffsetsLifecycleTest.scala
storage/src/main/java/org/apache/kafka/storage/internals/log/LeaderEpochAwareOffsetLookup.java
FILES
)"
[[ "$actual_changes" == "$expected_changes" ]] \
    || fail "fork change set differs from the reviewed fifteen-file bridge/lifecycle slice"

while read -r expected path; do
    [[ -n "$expected" ]] || continue
    actual="$(git -C "$kafka_checkout" hash-object "$path")"
    [[ "$actual" == "$expected" ]] \
        || fail "fork source drifted: $path expected $expected, got $actual"
done <<'LOCKS'
eebf0d6ddc8bcdd57fc1dcfb79c30d8945000331 build.gradle
5fa025b8a70a52364d5a9bfdbff092f63ae7563d checkstyle/import-control-core.xml
60dbfb45a00f3c007c624ea31c1aca32ea49a8b2 core/src/main/java/kafka/log/nereus/NereusKafkaExceptionMapper.java
47eca0ad9a439e952794b2030d46c5b48714a839 core/src/main/java/kafka/log/nereus/NereusListOffsetsBridge.java
6f1e5f76fb4ed51f786e7f07a22c3fc3f46cf9ae core/src/main/java/kafka/log/nereus/NereusListOffsetsScanConfig.java
aadcc658a9e74de9798b06d674ecb784947c8762 core/src/main/java/kafka/log/nereus/NereusRecordTimestampInspector.java
b17be9d830b44ed3a00cee03fb2ee0c2aa14aab9 core/src/main/scala/kafka/cluster/Partition.scala
db85db65ccb5aff6074dad616d93791b9698240c core/src/main/scala/kafka/log/nereus/NereusListOffsetsLifecycle.scala
8467478e391ed739cccee9cef16c1ab704e2b957 core/src/main/scala/kafka/server/ReplicaManager.scala
f81ec4137daa9e9fff7b7262733ded7998c86eba core/src/test/java/kafka/log/nereus/NereusKafkaExceptionMapperTest.java
c2bd8e03152a23547044a42f439b33698ace4251 core/src/test/java/kafka/log/nereus/NereusListOffsetsBridgeTest.java
205989c5d3adf68127d71be28c6ff9f521abcbf1 core/src/test/java/kafka/log/nereus/NereusRecordTimestampInspectorTest.java
690f4ff0826499b250bc5753aa101f0ad5214b40 core/src/test/scala/unit/kafka/cluster/PartitionTest.scala
811a02f2a81e6a353d5383d176b74fe7c00c7fdc core/src/test/scala/unit/kafka/log/nereus/NereusListOffsetsLifecycleTest.scala
6a9a43c81b0b60e69fb95099a76d80e7894ba453 storage/src/main/java/org/apache/kafka/storage/internals/log/LeaderEpochAwareOffsetLookup.java
LOCKS

marker_start="$(grep -h -F -c 'Nereus inject start:' \
    "$kafka_checkout/build.gradle" "$kafka_checkout/checkstyle/import-control-core.xml" \
    "$kafka_checkout/core/src/main/scala/kafka/cluster/Partition.scala" \
    "$kafka_checkout/core/src/main/scala/kafka/server/ReplicaManager.scala" \
    | awk '{ total += $1 } END { print total + 0 }')"
marker_end="$(grep -h -F -c 'Nereus inject end:' \
    "$kafka_checkout/build.gradle" "$kafka_checkout/checkstyle/import-control-core.xml" \
    "$kafka_checkout/core/src/main/scala/kafka/cluster/Partition.scala" \
    "$kafka_checkout/core/src/main/scala/kafka/server/ReplicaManager.scala" \
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

list_offsets_bridge="$kafka_checkout/core/src/main/java/kafka/log/nereus/NereusListOffsetsBridge.java"
grep -F -q 'public OffsetResultHolder fetchOffsetByTimestamp(' "$list_offsets_bridge" \
    || fail "ListOffsets bridge lost the Kafka result-holder entry point"
grep -F -q 'KafkaListOffsetsResolver resolver' "$list_offsets_bridge" \
    || fail "ListOffsets bridge lost the adapter resolver dependency"
grep -F -q 'implements LeaderEpochAwareOffsetLookup' "$list_offsets_bridge" \
    || fail "ListOffsets bridge lost the stock request-path seam"
grep -F -q 'result.whenComplete(' "$list_offsets_bridge" \
    || fail "ListOffsets bridge lost asynchronous terminal mapping"
grep -F -q 'result.cancel(false)' "$list_offsets_bridge" \
    || fail "ListOffsets bridge lost cancellation propagation"

exception_mapper="$kafka_checkout/core/src/main/java/kafka/log/nereus/NereusKafkaExceptionMapper.java"
grep -F -q 'public static ApiException map(Throwable failure)' "$exception_mapper" \
    || fail "Kafka exception mapper lost its public boundary"
grep -F -q 'switch (code)' "$exception_mapper" \
    || fail "Kafka exception mapper lost exhaustive ErrorCode mapping"

lookup_seam="$kafka_checkout/storage/src/main/java/org/apache/kafka/storage/internals/log/LeaderEpochAwareOffsetLookup.java"
grep -F -q 'interface LeaderEpochAwareOffsetLookup' "$lookup_seam" \
    || fail "stock ListOffsets lookup seam is missing"
grep -F -q 'int expectedLeaderEpoch' "$lookup_seam" \
    || fail "stock ListOffsets lookup seam lost leader-epoch fencing"

partition="$kafka_checkout/core/src/main/scala/kafka/cluster/Partition.scala"
grep -F -q 'def installLeaderEpochAwareOffsetLookup(expectedLeaderEpoch: Int,' "$partition" \
    || fail "Partition lost exact-epoch lookup installation"
grep -F -q 'installedEpoch == leaderEpoch' "$partition" \
    || fail "Partition lost request-time lookup fencing"
grep -F -q 'leaderEpochAwareOffsetLookup = None' "$partition" \
    || fail "Partition lost lookup revocation"

replica_manager="$kafka_checkout/core/src/main/scala/kafka/server/ReplicaManager.scala"
grep -F -q 'delayedRemoteListOffsetsPurgatory.checkAndComplete' "$replica_manager" \
    || fail "ReplicaManager lost async ListOffsets wakeup"

list_offsets_lifecycle="$kafka_checkout/core/src/main/scala/kafka/log/nereus/NereusListOffsetsLifecycle.scala"
grep -F -q 'storageManager.openLeader(request)' "$list_offsets_lifecycle" \
    || fail "ListOffsets lifecycle no longer delegates leader recovery to the adapter manager"
grep -F -q 'new KafkaListOffsetsResolver(storage, inspector)' "$list_offsets_lifecycle" \
    || fail "ListOffsets lifecycle lost exact recovered-storage resolver construction"
grep -F -q 'installLeaderEpochAwareOffsetLookup(attempt.request.leaderEpoch(), lookup)' "$list_offsets_lifecycle" \
    || fail "ListOffsets lifecycle lost post-recovery exact-epoch installation"
grep -F -q 'removeLeaderEpochAwareOffsetLookup(' "$list_offsets_lifecycle" \
    || fail "ListOffsets lifecycle lost request-path revocation"
grep -F -q 'storageManager.resign(identity, observedLeaderEpoch, timeout)' "$list_offsets_lifecycle" \
    || fail "ListOffsets lifecycle no longer delegates resign to the adapter manager"

if grep -E -R -q 'Class\.forName|MethodHandles|setAccessible' \
        "$kafka_checkout/core/src/main/java/kafka/log/nereus" \
        "$kafka_checkout/core/src/main/scala/kafka/log/nereus"; then
    fail "Kafka bridge package uses a forbidden reflection bypass"
fi

echo "F9 Kafka fork development source lock: local $actual_head from Apache $expected_base; cached organization trunk $actual_remote_trunk; four commits, fifteen bridge/lifecycle blobs and markers match"
