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
[[ "$actual_commit_count" == "15" ]] \
    || fail "expected fifteen reviewed fork commits, got $actual_commit_count"

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
core/src/main/java/kafka/log/nereus/NereusKafkaRecoveredState.java
core/src/main/java/kafka/log/nereus/NereusKafkaRecoveryStateCodec.java
core/src/main/java/kafka/log/nereus/NereusListOffsetsBridge.java
core/src/main/java/kafka/log/nereus/NereusListOffsetsScanConfig.java
core/src/main/java/kafka/log/nereus/NereusLocalLog.java
core/src/main/java/kafka/log/nereus/NereusRecordTimestampInspector.java
core/src/main/java/kafka/log/nereus/NereusUnifiedLog.java
core/src/main/java/kafka/server/builders/LogManagerBuilder.java
core/src/main/java/kafka/server/nereus/NereusKafkaClock.java
core/src/main/java/kafka/server/nereus/NereusKafkaDeferredRuntime.java
core/src/main/java/kafka/server/nereus/NereusKafkaMappedRuntimeConfiguration.java
core/src/main/java/kafka/server/nereus/NereusKafkaProductRuntimeCreator.java
core/src/main/java/kafka/server/nereus/NereusKafkaRecoveryStateFactory.java
core/src/main/java/kafka/server/nereus/NereusKafkaRecoveryStateFactoryBridge.java
core/src/main/java/kafka/server/nereus/NereusKafkaRuntimeConfigurationMapper.java
core/src/main/java/kafka/server/nereus/NereusKafkaStorageClusterSnapshotProvider.java
core/src/main/scala/kafka/cluster/Partition.scala
core/src/main/scala/kafka/log/LogManager.scala
core/src/main/scala/kafka/log/UnifiedLogFactory.scala
core/src/main/scala/kafka/log/nereus/NereusListOffsetsLifecycle.scala
core/src/main/scala/kafka/log/nereus/NereusTopicDeltaLifecycle.scala
core/src/main/scala/kafka/log/nereus/NereusUnifiedLogFactory.scala
core/src/main/scala/kafka/server/KafkaConfig.scala
core/src/main/scala/kafka/server/BrokerServer.scala
core/src/main/scala/kafka/server/KafkaRaftServer.scala
core/src/main/scala/kafka/server/NereusKafkaConfigValidator.scala
core/src/main/scala/kafka/server/ReplicaManager.scala
core/src/main/scala/kafka/server/metadata/AsyncTopicDeltaLifecycle.scala
core/src/main/scala/kafka/server/metadata/BrokerMetadataPublisher.scala
core/src/main/scala/kafka/server/nereus/NereusBrokerStorageRuntime.scala
core/src/main/scala/kafka/server/nereus/NereusBrokerStorageRuntimeFactory.scala
core/src/main/scala/kafka/server/storage/BrokerStorageDrainReason.scala
core/src/main/scala/kafka/server/storage/BrokerStorageRuntime.scala
core/src/main/scala/kafka/server/storage/BrokerStorageRuntimeContext.scala
core/src/main/scala/kafka/server/storage/BrokerStorageRuntimeFactory.scala
core/src/test/java/kafka/log/nereus/NereusKafkaExceptionMapperTest.java
core/src/test/java/kafka/log/nereus/NereusKafkaRecoveryStateCodecTest.java
core/src/test/java/kafka/log/nereus/NereusListOffsetsBridgeTest.java
core/src/test/java/kafka/log/nereus/NereusRecordTimestampInspectorTest.java
core/src/test/java/kafka/server/nereus/NereusKafkaContextAdaptersTest.java
core/src/test/java/kafka/server/nereus/NereusKafkaDeferredRuntimeTest.java
core/src/test/java/kafka/server/nereus/NereusKafkaRecoveryStateFactoryBridgeTest.java
core/src/test/java/kafka/server/nereus/NereusKafkaRecoveryStateFactoryTest.java
core/src/test/java/kafka/server/nereus/NereusKafkaRuntimeConfigurationMapperTest.java
core/src/test/scala/unit/kafka/cluster/PartitionTest.scala
core/src/test/scala/unit/kafka/log/nereus/NereusListOffsetsLifecycleTest.scala
core/src/test/scala/unit/kafka/log/nereus/NereusTopicDeltaLifecycleTest.scala
core/src/test/scala/unit/kafka/log/nereus/NereusUnifiedLogFactoryTest.scala
core/src/test/scala/unit/kafka/server/KafkaConfigTest.scala
core/src/test/scala/unit/kafka/server/NereusKafkaConfigValidatorTest.scala
core/src/test/scala/unit/kafka/server/ReplicaManagerTest.scala
core/src/test/scala/unit/kafka/server/metadata/BrokerMetadataPublisherTest.scala
core/src/test/scala/unit/kafka/server/nereus/NereusBrokerStorageRuntimeTest.scala
core/src/test/scala/unit/kafka/server/storage/BrokerStorageRuntimeFactoryTest.scala
server-common/src/main/java/org/apache/kafka/server/util/KafkaScheduler.java
server/src/main/java/org/apache/kafka/server/config/AbstractKafkaConfig.java
server/src/main/java/org/apache/kafka/server/config/NereusKafkaConfigs.java
server/src/main/java/org/apache/kafka/server/config/NereusKafkaStorageConfig.java
server/src/test/java/org/apache/kafka/server/config/NereusKafkaStorageConfigTest.java
server/src/test/java/org/apache/kafka/server/util/SchedulerTest.java
storage/src/main/java/org/apache/kafka/storage/internals/log/LeaderEpochAwareOffsetLookup.java
storage/src/main/java/org/apache/kafka/storage/internals/log/LeaderEpochAwareRecoveryState.java
FILES
)"
[[ "$actual_changes" == "$expected_changes" ]] \
    || fail "fork change set differs from the reviewed sixty-five-file log-shell/bridge/recovery/metadata-lifecycle/configuration/runtime-composition slice"

while read -r expected path; do
    [[ -n "$expected" ]] || continue
    actual="$(git -C "$kafka_checkout" hash-object "$path")"
    [[ "$actual" == "$expected" ]] \
        || fail "fork source drifted: $path expected $expected, got $actual"
done <<'LOCKS'
1f8b335c8f5394ee5c3035a4de7715e6d2582149 build.gradle
afac28a63df21ae134e5ffc08a0544eb6c161b5d checkstyle/import-control-core.xml
60dbfb45a00f3c007c624ea31c1aca32ea49a8b2 core/src/main/java/kafka/log/nereus/NereusKafkaExceptionMapper.java
f66943f69430386a1859c4e3c8c2f22fef9c339b core/src/main/java/kafka/log/nereus/NereusKafkaRecoveredState.java
b6513029a84c01b75095d93c8103ce97b6f3c533 core/src/main/java/kafka/log/nereus/NereusKafkaRecoveryStateCodec.java
47eca0ad9a439e952794b2030d46c5b48714a839 core/src/main/java/kafka/log/nereus/NereusListOffsetsBridge.java
6f1e5f76fb4ed51f786e7f07a22c3fc3f46cf9ae core/src/main/java/kafka/log/nereus/NereusListOffsetsScanConfig.java
6d11ff5c22062d4bf39d7f5c635af8fcc8b5748d core/src/main/java/kafka/log/nereus/NereusLocalLog.java
aadcc658a9e74de9798b06d674ecb784947c8762 core/src/main/java/kafka/log/nereus/NereusRecordTimestampInspector.java
e3cb21df624421a8cbaf5441ed4d313a04ff7278 core/src/main/java/kafka/log/nereus/NereusUnifiedLog.java
df74856a75146e0e35aaf5431b1ecb35531ec054 core/src/main/java/kafka/server/builders/LogManagerBuilder.java
5e2061bbb1655ab63a2796a3c0f12d34d7346ea7 core/src/main/java/kafka/server/nereus/NereusKafkaClock.java
22da430c45f92afcce47208f42c8ac1346ae6e1e core/src/main/java/kafka/server/nereus/NereusKafkaDeferredRuntime.java
93d44199a1e982d5a1f939d70f215154fa77e3f1 core/src/main/java/kafka/server/nereus/NereusKafkaMappedRuntimeConfiguration.java
10b5ac633a18acecca61c4be799a20737c14d717 core/src/main/java/kafka/server/nereus/NereusKafkaProductRuntimeCreator.java
5e8271f96b6677ac0fc5618776de9e812845dc10 core/src/main/java/kafka/server/nereus/NereusKafkaRecoveryStateFactory.java
b903540487b6553d4a1944b5f36e9567fc9262ba core/src/main/java/kafka/server/nereus/NereusKafkaRecoveryStateFactoryBridge.java
0c5b5f1012217416898f05f376b21708e036d9a2 core/src/main/java/kafka/server/nereus/NereusKafkaRuntimeConfigurationMapper.java
1970537a48ac13fd77c6bc32fd2bf1e99fb31670 core/src/main/java/kafka/server/nereus/NereusKafkaStorageClusterSnapshotProvider.java
e53af40ccbaa31ae63a34b6306dfc89576fc06fb core/src/main/scala/kafka/cluster/Partition.scala
d1e0f2999d6a9c7cd3ad8fc69002220f9c524545 core/src/main/scala/kafka/log/LogManager.scala
80bbb0cac25ac68a37af9e2d18975ed0ecf02c3f core/src/main/scala/kafka/log/UnifiedLogFactory.scala
5d48cd669ee816cd3215f93a4db0c9fc8b4e9a2f core/src/main/scala/kafka/log/nereus/NereusListOffsetsLifecycle.scala
21441c7e0e06556ff072f38b5c58e90514176748 core/src/main/scala/kafka/log/nereus/NereusTopicDeltaLifecycle.scala
856e0ebd5edb010f387245a289e1e7879e828066 core/src/main/scala/kafka/log/nereus/NereusUnifiedLogFactory.scala
320887500529409e4db7e31c1680572ae041f8db core/src/main/scala/kafka/server/BrokerServer.scala
457e08ad6714dd972abdb92d9f7471bb258469b7 core/src/main/scala/kafka/server/KafkaConfig.scala
1bb02848026399255535c83f667daa1d1777ad59 core/src/main/scala/kafka/server/KafkaRaftServer.scala
1526e85d891d075c173fd50c22dc017219d8aa73 core/src/main/scala/kafka/server/NereusKafkaConfigValidator.scala
6a45b8cd21600cc95884492737a627c6656091a0 core/src/main/scala/kafka/server/ReplicaManager.scala
7a3674d0cb71daa8830ea1ef89273181733ba661 core/src/main/scala/kafka/server/metadata/AsyncTopicDeltaLifecycle.scala
7c4da64c61aff4cefe9769764a9ff05306e5de73 core/src/main/scala/kafka/server/metadata/BrokerMetadataPublisher.scala
c7a6edf68ef77e338e1b5e40a9714fc9700a2396 core/src/main/scala/kafka/server/nereus/NereusBrokerStorageRuntime.scala
85051ba5ee6594bfee7032c3d4e4735a1b2f0c61 core/src/main/scala/kafka/server/nereus/NereusBrokerStorageRuntimeFactory.scala
876bde2298de1e772d6bcd4eee2e38bb0817bbde core/src/main/scala/kafka/server/storage/BrokerStorageDrainReason.scala
875eefb3b3474e9eb1cbab80c9010ed5221aa3cc core/src/main/scala/kafka/server/storage/BrokerStorageRuntime.scala
b2d6eccbc8169932d4104c6f494d945476becfd1 core/src/main/scala/kafka/server/storage/BrokerStorageRuntimeContext.scala
51260319d3b6e6196bc3303c0d74d6cf2b5bd95b core/src/main/scala/kafka/server/storage/BrokerStorageRuntimeFactory.scala
f81ec4137daa9e9fff7b7262733ded7998c86eba core/src/test/java/kafka/log/nereus/NereusKafkaExceptionMapperTest.java
759b84a731942ff991f415c2e87be001fbd961a7 core/src/test/java/kafka/log/nereus/NereusKafkaRecoveryStateCodecTest.java
c2bd8e03152a23547044a42f439b33698ace4251 core/src/test/java/kafka/log/nereus/NereusListOffsetsBridgeTest.java
205989c5d3adf68127d71be28c6ff9f521abcbf1 core/src/test/java/kafka/log/nereus/NereusRecordTimestampInspectorTest.java
7f36f601ae68ccb353878327bd9bdb0219b90186 core/src/test/java/kafka/server/nereus/NereusKafkaContextAdaptersTest.java
d7f0b8cca7dec9cfa4de9a542c8eb1b3c3c9cfe5 core/src/test/java/kafka/server/nereus/NereusKafkaDeferredRuntimeTest.java
ec32f2b8e23e9548a7a8b4e8bdb717a7949dc788 core/src/test/java/kafka/server/nereus/NereusKafkaRecoveryStateFactoryBridgeTest.java
db4738d28020366d4949f27f744acf2fa122fa9a core/src/test/java/kafka/server/nereus/NereusKafkaRecoveryStateFactoryTest.java
b13a3f941bb9f2592633a5086027bcf60848cd66 core/src/test/java/kafka/server/nereus/NereusKafkaRuntimeConfigurationMapperTest.java
8bf27c5e75df307df6a8ba24f8fb9e929b49d845 core/src/test/scala/unit/kafka/cluster/PartitionTest.scala
c28a29d488b51c0630cb1197b95b30bc6bf43a68 core/src/test/scala/unit/kafka/log/nereus/NereusListOffsetsLifecycleTest.scala
6d04c0ec33fbadef8207d2a3823519dfab412e13 core/src/test/scala/unit/kafka/log/nereus/NereusTopicDeltaLifecycleTest.scala
dbd7d63e1d1bd82d5b84d382663f4c16079ae567 core/src/test/scala/unit/kafka/log/nereus/NereusUnifiedLogFactoryTest.scala
14358b2d91ae9a25ea683946509cd3fd1657b6ca core/src/test/scala/unit/kafka/server/KafkaConfigTest.scala
7bd6e2c1512fbc2e0879d4c9df3f3e8f8d40a7e2 core/src/test/scala/unit/kafka/server/NereusKafkaConfigValidatorTest.scala
4d4507ca06c365a23fc336adfcfd4b98a7836203 core/src/test/scala/unit/kafka/server/ReplicaManagerTest.scala
b69cb745a04454dc890429498d44ce61c6b4a70a core/src/test/scala/unit/kafka/server/metadata/BrokerMetadataPublisherTest.scala
aa4cd7ba6e12e6785aee373d820e676923f3a1c4 core/src/test/scala/unit/kafka/server/nereus/NereusBrokerStorageRuntimeTest.scala
cd613585d465f4467e2fd9ffb6e80ad864c706c4 core/src/test/scala/unit/kafka/server/storage/BrokerStorageRuntimeFactoryTest.scala
1fbf9180a68bca9a5d45e38f9862841ea486f739 server-common/src/main/java/org/apache/kafka/server/util/KafkaScheduler.java
3036df4e77ad23fabb6533d1dc173458356ea6b3 server/src/main/java/org/apache/kafka/server/config/AbstractKafkaConfig.java
159b5b49316f9284df524b855409837fae0641b1 server/src/main/java/org/apache/kafka/server/config/NereusKafkaConfigs.java
bcf3d34104255dba08937f27b9642ee20f40de5d server/src/main/java/org/apache/kafka/server/config/NereusKafkaStorageConfig.java
cb1fc8b5fca7a7c97ec0a5c383474d8eab9f23ec server/src/test/java/org/apache/kafka/server/config/NereusKafkaStorageConfigTest.java
168371ca93e4cc0aa8e7168f82c880396dd723a2 server/src/test/java/org/apache/kafka/server/util/SchedulerTest.java
6a9a43c81b0b60e69fb95099a76d80e7894ba453 storage/src/main/java/org/apache/kafka/storage/internals/log/LeaderEpochAwareOffsetLookup.java
9920d51f0f7740f1db62064868ac6224a0db18b0 storage/src/main/java/org/apache/kafka/storage/internals/log/LeaderEpochAwareRecoveryState.java
LOCKS

marker_start="$(grep -h -F -c 'Nereus inject start:' \
    "$kafka_checkout/build.gradle" "$kafka_checkout/checkstyle/import-control-core.xml" \
    "$kafka_checkout/core/src/main/java/kafka/server/builders/LogManagerBuilder.java" \
    "$kafka_checkout/core/src/main/scala/kafka/cluster/Partition.scala" \
    "$kafka_checkout/core/src/main/scala/kafka/log/LogManager.scala" \
    "$kafka_checkout/core/src/main/scala/kafka/server/BrokerServer.scala" \
    "$kafka_checkout/core/src/main/scala/kafka/server/KafkaConfig.scala" \
    "$kafka_checkout/core/src/main/scala/kafka/server/KafkaRaftServer.scala" \
    "$kafka_checkout/core/src/main/scala/kafka/server/ReplicaManager.scala" \
    "$kafka_checkout/core/src/main/scala/kafka/server/metadata/BrokerMetadataPublisher.scala" \
    "$kafka_checkout/server/src/main/java/org/apache/kafka/server/config/AbstractKafkaConfig.java" \
    | awk '{ total += $1 } END { print total + 0 }')"
marker_end="$(grep -h -F -c 'Nereus inject end:' \
    "$kafka_checkout/build.gradle" "$kafka_checkout/checkstyle/import-control-core.xml" \
    "$kafka_checkout/core/src/main/java/kafka/server/builders/LogManagerBuilder.java" \
    "$kafka_checkout/core/src/main/scala/kafka/cluster/Partition.scala" \
    "$kafka_checkout/core/src/main/scala/kafka/log/LogManager.scala" \
    "$kafka_checkout/core/src/main/scala/kafka/server/BrokerServer.scala" \
    "$kafka_checkout/core/src/main/scala/kafka/server/KafkaConfig.scala" \
    "$kafka_checkout/core/src/main/scala/kafka/server/KafkaRaftServer.scala" \
    "$kafka_checkout/core/src/main/scala/kafka/server/ReplicaManager.scala" \
    "$kafka_checkout/core/src/main/scala/kafka/server/metadata/BrokerMetadataPublisher.scala" \
    "$kafka_checkout/server/src/main/java/org/apache/kafka/server/config/AbstractKafkaConfig.java" \
    | awk '{ total += $1 } END { print total + 0 }')"
[[ "$marker_start" -gt 0 && "$marker_start" == "$marker_end" ]] \
    || fail "Nereus inject markers are absent or unbalanced: $marker_start/$marker_end"

unified_log_factory="$kafka_checkout/core/src/main/scala/kafka/log/UnifiedLogFactory.scala"
grep -F -q 'val Local: UnifiedLogFactory = context => UnifiedLog.create(' "$unified_log_factory" \
    || fail "stock UnifiedLog factory lost exact local fallback construction"
grep -F -q 'def loadExistingLogs: Boolean = true' "$unified_log_factory" \
    || fail "stock UnifiedLog factory lost existing-log loading default"
grep -F -q 'def scheduleLocalMaintenance: Boolean = true' "$unified_log_factory" \
    || fail "stock UnifiedLog factory lost local-maintenance default"

log_manager="$kafka_checkout/core/src/main/scala/kafka/log/LogManager.scala"
grep -F -q 'unifiedLogFactory.open(UnifiedLogOpenContext(' "$log_manager" \
    || fail "LogManager no longer delegates log construction to the injected factory"
grep -F -q 'if (unifiedLogFactory.loadExistingLogs)' "$log_manager" \
    || fail "LogManager lost authoritative-mode existing-log isolation"
grep -F -q 'if (!unifiedLogFactory.scheduleLocalMaintenance) return' "$log_manager" \
    || fail "LogManager lost authoritative-mode local-maintenance isolation"

nereus_log_factory="$kafka_checkout/core/src/main/scala/kafka/log/nereus/NereusUnifiedLogFactory.scala"
grep -F -q 'override def loadExistingLogs: Boolean = false' "$nereus_log_factory" \
    || fail "Nereus log factory resumed treating local logs as durable truth"
grep -F -q 'override def scheduleLocalMaintenance: Boolean = false' "$nereus_log_factory" \
    || fail "Nereus log factory resumed stock local maintenance"
grep -F -q 'Nereus log creation requires a non-zero KRaft topic ID' "$nereus_log_factory" \
    || fail "Nereus log factory lost exact topic-ID validation"

nereus_unified_log="$kafka_checkout/core/src/main/java/kafka/log/nereus/NereusUnifiedLog.java"
grep -F -q 'public void installRecoveredState(' "$nereus_unified_log" \
    || fail "Nereus UnifiedLog lost recovered-state publication"
grep -F -q 'public void installStorage(' "$nereus_unified_log" \
    || fail "Nereus UnifiedLog lost exact storage publication"
grep -F -q 'public void removeStorage(' "$nereus_unified_log" \
    || fail "Nereus UnifiedLog lost identity-safe storage revocation"
grep -F -q 'throw dataPlanePending("Produce")' "$nereus_unified_log" \
    || fail "Nereus UnifiedLog no longer fails Produce closed before data-plane composition"
grep -F -q 'throw dataPlanePending("Fetch")' "$nereus_unified_log" \
    || fail "Nereus UnifiedLog no longer fails Fetch closed before data-plane composition"

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
grep -F -q 'def installNereusRecoveredState(expectedLeaderEpoch: Int,' "$partition" \
    || fail "Partition lost exact-epoch recovered-state publication"
grep -F -q 'def currentNereusRecoveredState(expectedLeaderEpoch: Int)' "$partition" \
    || fail "Partition lost exact-epoch recovered-state lookup"
grep -F -q 'state: LeaderEpochAwareRecoveryState' "$partition" \
    || fail "Partition recovered state no longer uses the stock-without-artifacts boundary"

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

runtime_factory="$kafka_checkout/core/src/main/scala/kafka/server/storage/BrokerStorageRuntimeFactory.scala"
grep -F -q 'val Disabled: BrokerStorageRuntimeFactory' "$runtime_factory" \
    || fail "broker storage runtime lost its stock-disabled factory"
grep -F -q 'requires an explicitly installed BrokerStorageRuntimeFactory' "$runtime_factory" \
    || fail "enabled storage no longer fails closed without an explicit runtime factory"

adapter_runtime="$kafka_checkout/core/src/main/scala/kafka/server/nereus/NereusBrokerStorageRuntime.scala"
grep -F -q 'delegate.beginDrain(drainReason(reason))' "$adapter_runtime" \
    || fail "adapter-backed broker runtime lost typed drain delegation"
grep -F -q 'new NereusTopicDeltaLifecycle(' "$adapter_runtime" \
    || fail "adapter-backed broker runtime lost exact ReplicaManager metadata lifecycle composition"
grep -F -q 'partitionLifecycle.foreach(_.beginDrain())' "$adapter_runtime" \
    || fail "adapter-backed broker runtime lost synchronous ListOffsets admission drain"

adapter_factory="$kafka_checkout/core/src/main/scala/kafka/server/nereus/NereusBrokerStorageRuntimeFactory.scala"
grep -F -q 'runtimeCreator: Function[BrokerStorageRuntimeContext, NereusKafkaRuntime]' "$adapter_factory" \
    || fail "adapter-backed runtime factory lost its explicit typed creator"
if grep -E -q 'Class\.forName|ServiceLoader|MethodHandles|setAccessible' "$adapter_factory" "$adapter_runtime"; then
    fail "adapter-backed runtime composition uses a forbidden reflection or service-loader bypass"
fi

broker_server="$kafka_checkout/core/src/main/scala/kafka/server/BrokerServer.scala"
grep -F -q 'brokerStorageRuntimeFactory.create(BrokerStorageRuntimeContext(' "$broker_server" \
    || fail "BrokerServer lost explicit storage runtime creation"
grep -F -q 'brokerStorageRuntime.asyncTopicDeltaLifecycle' "$broker_server" \
    || fail "BrokerServer lost storage metadata lifecycle composition"
grep -F -q '"the broker storage runtime to become ready"' "$broker_server" \
    || fail "BrokerServer lost pre-unfence storage readiness"
grep -F -q 'brokerStorageRuntime.beginDrain(brokerStorageDrainReason)' "$broker_server" \
    || fail "BrokerServer lost pre-handler storage admission drain"
grep -F -q 'brokerStorageRuntime.awaitDrained(drainTimeout)' "$broker_server" \
    || fail "BrokerServer lost pre-ReplicaManager storage drain wait"
grep -F -q 'closeBrokerStorageRuntime()' "$broker_server" \
    || fail "BrokerServer lost post-log storage runtime close"

kafka_raft_server="$kafka_checkout/core/src/main/scala/kafka/server/KafkaRaftServer.scala"
grep -F -q 'brokerStorageRuntimeFactory: BrokerStorageRuntimeFactory = BrokerStorageRuntimeFactory.Disabled' "$kafka_raft_server" \
    || fail "KafkaRaftServer lost explicit stock-default runtime injection"

runtime_mapper="$kafka_checkout/core/src/main/java/kafka/server/nereus/NereusKafkaRuntimeConfigurationMapper.java"
grep -F -q 'only OBJECT_WAL_SYNC_OBJECT has a production provider runtime' "$runtime_mapper" \
    || fail "runtime mapper lost its executable-profile fail-closed boundary"
grep -F -q 'only the explicit s3 provider token is supported' "$runtime_mapper" \
    || fail "runtime mapper gained an implicit provider-loading fallback"
grep -F -q 'new KafkaBrokerCapabilitySpecification(' "$runtime_mapper" \
    || fail "runtime mapper lost broker-epoch capability construction"
grep -F -q 'new NereusKafkaObjectWalRuntimeConfiguration(' "$runtime_mapper" \
    || fail "runtime mapper lost exact Object-WAL product configuration"
grep -F -q 'false,' "$runtime_mapper" \
    || fail "runtime mapper lost the no-legacy-auto-session configuration"

product_runtime_creator="$kafka_checkout/core/src/main/java/kafka/server/nereus/NereusKafkaProductRuntimeCreator.java"
grep -F -q 'NereusKafkaObjectWalRuntimeFactory.createActivated(' "$product_runtime_creator" \
    || fail "product runtime creator lost activation-backed Object-WAL construction"
grep -F -q 'scheduler, "scheduler").scheduledExecutorService()' "$product_runtime_creator" \
    || fail "product runtime creator lost the borrowed Kafka scheduler boundary"
grep -F -q 'new NereusKafkaStorageClusterSnapshotProvider(' "$product_runtime_creator" \
    || fail "product runtime creator lost the KRaft/local-log activation snapshot"

deferred_runtime="$kafka_checkout/core/src/main/java/kafka/server/nereus/NereusKafkaDeferredRuntime.java"
grep -F -q 'implements NereusKafkaRuntime' "$deferred_runtime" \
    || fail "deferred runtime no longer implements the product runtime contract"
grep -F -q 'brokerEpochSupplier.getAsLong()' "$deferred_runtime" \
    || fail "deferred runtime lost exact post-registration broker epoch acquisition"
grep -F -q 'runtime.admission().requireReady(operation)' "$deferred_runtime" \
    || fail "deferred manager lost per-operation product admission recheck"
grep -F -q 'pendingEpochPoll.cancel(false)' "$deferred_runtime" \
    || fail "deferred runtime lost owned broker-epoch poll cancellation"

cluster_snapshot="$kafka_checkout/core/src/main/java/kafka/server/nereus/NereusKafkaStorageClusterSnapshotProvider.java"
grep -F -q 'MetadataImage image = metadataCache.currentImage()' "$cluster_snapshot" \
    || fail "cluster snapshot no longer captures one immutable KRaft image"
grep -F -q 'authoritativeLocalLogsPresent()' "$cluster_snapshot" \
    || fail "cluster snapshot lost conservative local-log activation proof"

recovered_state="$kafka_checkout/core/src/main/java/kafka/log/nereus/NereusKafkaRecoveredState.java"
grep -F -q 'batch.ensureValid()' "$recovered_state" \
    || fail "fork recovery state lost stock RecordBatch CRC validation"
grep -F -q 'F9-M3 recovery accepts only non-idempotent non-transactional data batches' "$recovered_state" \
    || fail "fork recovery state lost the M3 producer/transaction fail-closed boundary"
grep -F -q 'void freeze(KafkaCheckpointSourceState source)' "$recovered_state" \
    || fail "fork recovery state lost exact frozen-source validation"

recovery_codec="$kafka_checkout/core/src/main/java/kafka/log/nereus/NereusKafkaRecoveryStateCodec.java"
grep -F -q 'implements KafkaRecoveryStateCodec<NereusKafkaRecoveredState>' "$recovery_codec" \
    || fail "fork recovery codec no longer implements the adapter seam"
grep -F -q 'Kafka recovery state codec is one-shot' "$recovery_codec" \
    || fail "fork recovery codec lost fresh per-open state ownership"

recovery_state_seam="$kafka_checkout/storage/src/main/java/org/apache/kafka/storage/internals/log/LeaderEpochAwareRecoveryState.java"
grep -F -q 'interface LeaderEpochAwareRecoveryState' "$recovery_state_seam" \
    || fail "stock recovery-state boundary is missing"
grep -F -q 'Uuid topicId()' "$recovery_state_seam" \
    || fail "stock recovery-state boundary lost exact topic identity"

recovery_factory="$kafka_checkout/core/src/main/java/kafka/server/nereus/NereusKafkaRecoveryStateFactory.java"
grep -F -q 'new NereusKafkaRecoveryStateCodec(' "$recovery_factory" \
    || fail "fork recovery state factory lost stock RecordBatch codec construction"
grep -F -q 'partition.installNereusRecoveredState(leaderEpoch, state)' "$recovery_factory" \
    || fail "fork recovery state factory lost exact live-Partition publication"

recovery_bridge="$kafka_checkout/core/src/main/java/kafka/server/nereus/NereusKafkaRecoveryStateFactoryBridge.java"
grep -F -q 'delegate.compareAndSet(null, exact)' "$recovery_bridge" \
    || fail "recovery state factory bridge lost one-time exact binding"

grep -F -q 'def production(' "$adapter_factory" \
    || fail "adapter-backed runtime factory lost explicit production composition"
grep -F -q 'new NereusKafkaDeferredRuntime(' "$adapter_factory" \
    || fail "adapter-backed runtime factory no longer defers provider construction to start"
grep -F -q 'deferred.bindRecoveryStateFactory(' "$adapter_runtime" \
    || fail "adapter-backed runtime lost exact ReplicaManager recovery-state binding"

kafka_scheduler="$kafka_checkout/server-common/src/main/java/org/apache/kafka/server/util/KafkaScheduler.java"
grep -F -q 'public synchronized ScheduledExecutorService scheduledExecutorService()' "$kafka_scheduler" \
    || fail "Kafka scheduler lost its explicit borrowed ScheduledExecutorService boundary"

if grep -E -R -q 'Class\.forName|MethodHandles|setAccessible' \
        "$kafka_checkout/core/src/main/java/kafka/log/nereus" \
        "$kafka_checkout/core/src/main/java/kafka/server/nereus" \
        "$kafka_checkout/core/src/main/scala/kafka/log/nereus" \
        "$async_lifecycle" "$metadata_publisher"; then
    fail "Kafka bridge package uses a forbidden reflection bypass"
fi

echo "F9 Kafka fork development source lock: local $actual_head from Apache $expected_base; cached organization trunk $actual_remote_trunk; fifteen commits, sixty-five log-shell/bridge/recovery/metadata-lifecycle/configuration/runtime-composition blobs and markers match"
