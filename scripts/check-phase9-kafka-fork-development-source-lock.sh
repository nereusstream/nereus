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
[[ "$actual_commit_count" == "6" ]] \
    || fail "expected six reviewed fork commits, got $actual_commit_count"

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
core/src/main/scala/kafka/log/nereus/NereusTopicDeltaLifecycle.scala
core/src/main/scala/kafka/server/KafkaConfig.scala
core/src/main/scala/kafka/server/NereusKafkaConfigValidator.scala
core/src/main/scala/kafka/server/ReplicaManager.scala
core/src/main/scala/kafka/server/metadata/AsyncTopicDeltaLifecycle.scala
core/src/main/scala/kafka/server/metadata/BrokerMetadataPublisher.scala
core/src/test/java/kafka/log/nereus/NereusKafkaExceptionMapperTest.java
core/src/test/java/kafka/log/nereus/NereusListOffsetsBridgeTest.java
core/src/test/java/kafka/log/nereus/NereusRecordTimestampInspectorTest.java
core/src/test/scala/unit/kafka/cluster/PartitionTest.scala
core/src/test/scala/unit/kafka/log/nereus/NereusListOffsetsLifecycleTest.scala
core/src/test/scala/unit/kafka/log/nereus/NereusTopicDeltaLifecycleTest.scala
core/src/test/scala/unit/kafka/server/KafkaConfigTest.scala
core/src/test/scala/unit/kafka/server/NereusKafkaConfigValidatorTest.scala
core/src/test/scala/unit/kafka/server/ReplicaManagerTest.scala
core/src/test/scala/unit/kafka/server/metadata/BrokerMetadataPublisherTest.scala
server/src/main/java/org/apache/kafka/server/config/AbstractKafkaConfig.java
server/src/main/java/org/apache/kafka/server/config/NereusKafkaConfigs.java
server/src/main/java/org/apache/kafka/server/config/NereusKafkaStorageConfig.java
server/src/test/java/org/apache/kafka/server/config/NereusKafkaStorageConfigTest.java
storage/src/main/java/org/apache/kafka/storage/internals/log/LeaderEpochAwareOffsetLookup.java
FILES
)"
[[ "$actual_changes" == "$expected_changes" ]] \
    || fail "fork change set differs from the reviewed twenty-nine-file bridge/metadata-lifecycle/configuration slice"

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
165e66c97d3335b528f457df8725d4fbab0790d9 core/src/main/scala/kafka/cluster/Partition.scala
7a7a1d1f4c99de4be9766f199f2e39960613934b core/src/main/scala/kafka/log/nereus/NereusListOffsetsLifecycle.scala
a7a1d616651a146d6eec3377fcf2455c3777ef8c core/src/main/scala/kafka/log/nereus/NereusTopicDeltaLifecycle.scala
6a45b8cd21600cc95884492737a627c6656091a0 core/src/main/scala/kafka/server/ReplicaManager.scala
7a3674d0cb71daa8830ea1ef89273181733ba661 core/src/main/scala/kafka/server/metadata/AsyncTopicDeltaLifecycle.scala
7c4da64c61aff4cefe9769764a9ff05306e5de73 core/src/main/scala/kafka/server/metadata/BrokerMetadataPublisher.scala
f81ec4137daa9e9fff7b7262733ded7998c86eba core/src/test/java/kafka/log/nereus/NereusKafkaExceptionMapperTest.java
c2bd8e03152a23547044a42f439b33698ace4251 core/src/test/java/kafka/log/nereus/NereusListOffsetsBridgeTest.java
205989c5d3adf68127d71be28c6ff9f521abcbf1 core/src/test/java/kafka/log/nereus/NereusRecordTimestampInspectorTest.java
1707eb1ee360baaed845404ced5ba2e872bc62d4 core/src/test/scala/unit/kafka/cluster/PartitionTest.scala
898c9b8e37028a79183fab32a65e830554b1bd30 core/src/test/scala/unit/kafka/log/nereus/NereusListOffsetsLifecycleTest.scala
c1490dc3f9af754c2111a1c4d6d6bcfdfcb8c53f core/src/test/scala/unit/kafka/log/nereus/NereusTopicDeltaLifecycleTest.scala
457e08ad6714dd972abdb92d9f7471bb258469b7 core/src/main/scala/kafka/server/KafkaConfig.scala
1526e85d891d075c173fd50c22dc017219d8aa73 core/src/main/scala/kafka/server/NereusKafkaConfigValidator.scala
14358b2d91ae9a25ea683946509cd3fd1657b6ca core/src/test/scala/unit/kafka/server/KafkaConfigTest.scala
7bd6e2c1512fbc2e0879d4c9df3f3e8f8d40a7e2 core/src/test/scala/unit/kafka/server/NereusKafkaConfigValidatorTest.scala
4d4507ca06c365a23fc336adfcfd4b98a7836203 core/src/test/scala/unit/kafka/server/ReplicaManagerTest.scala
b69cb745a04454dc890429498d44ce61c6b4a70a core/src/test/scala/unit/kafka/server/metadata/BrokerMetadataPublisherTest.scala
3036df4e77ad23fabb6533d1dc173458356ea6b3 server/src/main/java/org/apache/kafka/server/config/AbstractKafkaConfig.java
159b5b49316f9284df524b855409837fae0641b1 server/src/main/java/org/apache/kafka/server/config/NereusKafkaConfigs.java
bcf3d34104255dba08937f27b9642ee20f40de5d server/src/main/java/org/apache/kafka/server/config/NereusKafkaStorageConfig.java
cb1fc8b5fca7a7c97ec0a5c383474d8eab9f23ec server/src/test/java/org/apache/kafka/server/config/NereusKafkaStorageConfigTest.java
6a9a43c81b0b60e69fb95099a76d80e7894ba453 storage/src/main/java/org/apache/kafka/storage/internals/log/LeaderEpochAwareOffsetLookup.java
LOCKS

marker_start="$(grep -h -F -c 'Nereus inject start:' \
    "$kafka_checkout/build.gradle" "$kafka_checkout/checkstyle/import-control-core.xml" \
    "$kafka_checkout/core/src/main/scala/kafka/cluster/Partition.scala" \
    "$kafka_checkout/core/src/main/scala/kafka/server/KafkaConfig.scala" \
    "$kafka_checkout/core/src/main/scala/kafka/server/ReplicaManager.scala" \
    "$kafka_checkout/core/src/main/scala/kafka/server/metadata/BrokerMetadataPublisher.scala" \
    "$kafka_checkout/server/src/main/java/org/apache/kafka/server/config/AbstractKafkaConfig.java" \
    | awk '{ total += $1 } END { print total + 0 }')"
marker_end="$(grep -h -F -c 'Nereus inject end:' \
    "$kafka_checkout/build.gradle" "$kafka_checkout/checkstyle/import-control-core.xml" \
    "$kafka_checkout/core/src/main/scala/kafka/cluster/Partition.scala" \
    "$kafka_checkout/core/src/main/scala/kafka/server/KafkaConfig.scala" \
    "$kafka_checkout/core/src/main/scala/kafka/server/ReplicaManager.scala" \
    "$kafka_checkout/core/src/main/scala/kafka/server/metadata/BrokerMetadataPublisher.scala" \
    "$kafka_checkout/server/src/main/java/org/apache/kafka/server/config/AbstractKafkaConfig.java" \
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
grep -F -q 'leaderEpochAwareOffsetLookupPending.contains(leaderEpoch)' "$partition" \
    || fail "Partition lost fail-closed lookup recovery routing"
grep -F -q 'def beginLeaderEpochAwareOffsetLookup(expectedLeaderEpoch: Int)' "$partition" \
    || fail "Partition lost synchronous exact-epoch recovery preparation"

replica_manager="$kafka_checkout/core/src/main/scala/kafka/server/ReplicaManager.scala"
grep -F -q 'delayedRemoteListOffsetsPurgatory.checkAndComplete' "$replica_manager" \
    || fail "ReplicaManager lost async ListOffsets wakeup"
grep -F -q 'onLeaderStatePublished: (Partition, Uuid, Int) => Unit' "$replica_manager" \
    || fail "ReplicaManager lost synchronous new-leader preparation callback"

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

topic_delta_lifecycle="$kafka_checkout/core/src/main/scala/kafka/log/nereus/NereusTopicDeltaLifecycle.scala"
grep -F -q 'extends AsyncTopicDeltaLifecycle' "$topic_delta_lifecycle" \
    || fail "Nereus topic-delta lifecycle no longer implements the stock-compatible seam"
grep -F -q 'delta.localChanges(brokerId)' "$topic_delta_lifecycle" \
    || fail "Nereus topic-delta lifecycle lost exact broker-local reconciliation"
grep -F -q 'new KafkaPartitionLeaderOpenRequest(' "$topic_delta_lifecycle" \
    || fail "Nereus topic-delta lifecycle lost exact leader-open request construction"
grep -F -q 'partitionLifecycle.delete(identity, metadataOffset, operationTimeout)' "$topic_delta_lifecycle" \
    || fail "Nereus topic-delta lifecycle lost metadata-ordered delete"

async_lifecycle="$kafka_checkout/core/src/main/scala/kafka/server/metadata/AsyncTopicDeltaLifecycle.scala"
grep -F -q 'trait AsyncTopicDeltaLifecycle' "$async_lifecycle" \
    || fail "stock-compatible asynchronous topic lifecycle seam is missing"
grep -F -q 'def onLeaderStatePublished(partition: Partition, topicId: Uuid, leaderEpoch: Int): Unit' "$async_lifecycle" \
    || fail "asynchronous topic lifecycle lost synchronous leader preparation"

metadata_publisher="$kafka_checkout/core/src/main/scala/kafka/server/metadata/BrokerMetadataPublisher.scala"
grep -F -q 'asyncTopicDeltaLifecycle: Option[AsyncTopicDeltaLifecycle] = None' "$metadata_publisher" \
    || fail "BrokerMetadataPublisher lost its stock-default optional lifecycle"
grep -F -q 'handleTopicsDeltaAsync(deltaName, topicsDelta, newImage, lifecycle)' "$metadata_publisher" \
    || fail "BrokerMetadataPublisher lost asynchronous topic lifecycle routing"
grep -F -q 'onAsyncLeaderReady' "$metadata_publisher" \
    || fail "BrokerMetadataPublisher lost post-recovery coordinator election"

config_def="$kafka_checkout/server/src/main/java/org/apache/kafka/server/config/NereusKafkaConfigs.java"
grep -F -q 'public static final boolean ENABLED_DEFAULT = false' "$config_def" \
    || fail "Nereus storage configuration lost its safe disabled default"
grep -F -q 'public static final ConfigDef CONFIG_DEF' "$config_def" \
    || fail "Nereus storage configuration surface is missing"
grep -F -q 'MAX_KAFKA_ENTRY_BYTES = 64L * MIB' "$config_def" \
    || fail "Nereus storage configuration lost the protocol entry hard limit"

typed_config="$kafka_checkout/server/src/main/java/org/apache/kafka/server/config/NereusKafkaStorageConfig.java"
grep -F -q 'public record NereusKafkaStorageConfig(' "$typed_config" \
    || fail "Nereus immutable storage configuration snapshot is missing"
grep -F -q 'if (enabled)' "$typed_config" \
    || fail "Nereus configuration lost enabled-only cross-field validation"
grep -F -q 'validateProviders(core)' "$typed_config" \
    || fail "Nereus configuration lost profile provider validation"

kafka_config="$kafka_checkout/core/src/main/scala/kafka/server/KafkaConfig.scala"
grep -F -q 'val nereusKafkaStorageConfig: NereusKafkaStorageConfig = NereusKafkaStorageConfig.from(this)' "$kafka_config" \
    || fail "KafkaConfig lost the immutable Nereus storage snapshot"
grep -F -q 'NereusKafkaConfigValidator.validate(this, nereusKafkaStorageConfig)' "$kafka_config" \
    || fail "KafkaConfig lost enabled-only Nereus/Kafka validation"

config_validator="$kafka_checkout/core/src/main/scala/kafka/server/NereusKafkaConfigValidator.scala"
grep -F -q 'if (!storage.enabled()) return' "$config_validator" \
    || fail "Nereus Kafka validator lost stock-disabled fallback"
grep -F -q 'requireSingleReplicaSemantics(config)' "$config_validator" \
    || fail "Nereus Kafka validator lost single-replica protocol enforcement"
grep -F -q 'requireConflictingStorageDisabled(config)' "$config_validator" \
    || fail "Nereus Kafka validator lost conflicting storage-mode rejection"

if grep -E -R -q 'Class\.forName|MethodHandles|setAccessible' \
        "$kafka_checkout/core/src/main/java/kafka/log/nereus" \
        "$kafka_checkout/core/src/main/scala/kafka/log/nereus" \
        "$async_lifecycle" "$metadata_publisher"; then
    fail "Kafka bridge package uses a forbidden reflection bypass"
fi

echo "F9 Kafka fork development source lock: local $actual_head from Apache $expected_base; cached organization trunk $actual_remote_trunk; six commits, twenty-nine bridge/metadata-lifecycle/configuration blobs and markers match"
