#!/usr/bin/env bash
set -euo pipefail

kafka_checkout="${1:?usage: check-phase9-kafka-fork-development-source-lock.sh CHECKOUT EXPECTED_HEAD EXPECTED_BASE EXPECTED_REMOTE_TRUNK EXPECTED_VERSION}"
expected_head="${2:?missing expected fork HEAD}"
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
[[ "$actual_commit_count" == "31" ]] \
    || fail "expected thirty-one reviewed fork commits, got $actual_commit_count"

actual_version="$(git -C "$kafka_checkout" show HEAD:gradle.properties \
    | sed -n 's/^version=//p' | head -n 1)"
[[ "$actual_version" == "$expected_version" ]] \
    || fail "Kafka version drifted: expected $expected_version, got $actual_version"

origin_fetch_url="$(git -C "$kafka_checkout" remote get-url origin)"
origin_push_url="$(git -C "$kafka_checkout" remote get-url --push origin)"
for origin_url in "$origin_fetch_url" "$origin_push_url"; do
    [[ "$origin_url" == "https://github.com/nereusstream/kafka" \
            || "$origin_url" == "https://github.com/nereusstream/kafka.git" \
            || "$origin_url" == "git@github.com:nereusstream/kafka.git" ]] \
        || fail "origin is not nereusstream/kafka: $origin_url"
done
actual_remote_trunk="$(git -C "$kafka_checkout" rev-parse refs/remotes/origin/trunk)"
[[ "$actual_remote_trunk" == "$expected_remote_trunk" ]] \
    || fail "cached origin/trunk drifted: expected $expected_remote_trunk, got $actual_remote_trunk"
git -C "$kafka_checkout" merge-base --is-ancestor "$expected_base" "$expected_remote_trunk" \
    || fail "locked Apache base is not an ancestor of organization-fork trunk"
actual_remote_head="$(git -C "$kafka_checkout" rev-parse "refs/remotes/origin/$expected_branch")"
[[ "$actual_remote_head" == "$expected_head" ]] \
    || fail "published fork branch drifted: expected $expected_head, got $actual_remote_head"

actual_changes="$(git -C "$kafka_checkout" diff --name-only "$expected_base"..HEAD | LC_ALL=C sort)"
expected_changes="$(LC_ALL=C sort <<'FILES'
bin/nereus-kafka-server-start.sh
build.gradle
checkstyle/import-control-core.xml
core/src/main/java/kafka/log/nereus/NereusKafkaExceptionMapper.java
core/src/main/java/kafka/log/nereus/NereusCanonicalLogState.java
core/src/main/java/kafka/log/nereus/NereusKafkaRecoveredState.java
core/src/main/java/kafka/log/nereus/NereusKafkaRecoveryStateCodec.java
core/src/main/java/kafka/log/nereus/NereusListOffsetsBridge.java
core/src/main/java/kafka/log/nereus/NereusListOffsetsScanConfig.java
core/src/main/java/kafka/log/nereus/NereusLocalLog.java
core/src/main/java/kafka/log/nereus/NereusLogSegment.java
core/src/main/java/kafka/log/nereus/NereusProducerStateManager.java
core/src/main/java/kafka/log/nereus/NereusRecordTimestampInspector.java
core/src/main/java/kafka/log/nereus/NereusTransactionIndex.java
core/src/main/java/kafka/log/nereus/NereusUnifiedLog.java
core/src/main/java/kafka/server/builders/LogManagerBuilder.java
core/src/main/java/kafka/server/builders/ReplicaManagerBuilder.java
core/src/main/java/kafka/server/nereus/NereusControllerStorageRuntime.java
core/src/main/java/kafka/server/nereus/NereusKafkaControllerActivation.java
core/src/main/java/kafka/server/nereus/NereusKafkaControllerActivationCreator.java
core/src/main/java/kafka/server/nereus/NereusKafkaControllerRuntimeConfiguration.java
core/src/main/java/kafka/server/nereus/NereusKafkaClock.java
core/src/main/java/kafka/server/nereus/NereusKafkaDeferredRuntime.java
core/src/main/java/kafka/server/nereus/NereusKafkaForkRuntimeBridges.java
core/src/main/java/kafka/server/nereus/NereusKafkaMappedRuntimeConfiguration.java
core/src/main/java/kafka/server/nereus/NereusKafkaProductRuntimeCreator.java
core/src/main/java/kafka/server/nereus/NereusKafkaRecoveryStateFactory.java
core/src/main/java/kafka/server/nereus/NereusKafkaRecoveryStateFactoryBridge.java
core/src/main/java/kafka/server/nereus/NereusKafkaRuntimeConfigurationMapper.java
core/src/main/java/kafka/server/nereus/NereusKafkaStorageClusterSnapshotProvider.java
core/src/main/scala/kafka/cluster/Partition.scala
core/src/main/scala/kafka/Kafka.scala
core/src/main/scala/kafka/log/LogManager.scala
core/src/main/scala/kafka/log/UnifiedLogFactory.scala
core/src/main/scala/kafka/log/nereus/NereusListOffsetsLifecycle.scala
core/src/main/scala/kafka/log/nereus/NereusTopicDeltaLifecycle.scala
core/src/main/scala/kafka/log/nereus/NereusUnifiedLogFactory.scala
core/src/main/scala/kafka/server/KafkaConfig.scala
core/src/main/scala/kafka/server/BrokerServer.scala
core/src/main/scala/kafka/server/ConfigHandler.scala
core/src/main/scala/kafka/server/ControllerServer.scala
core/src/main/scala/kafka/server/KafkaRaftServer.scala
core/src/main/scala/kafka/server/NereusKafkaConfigValidator.scala
core/src/main/scala/kafka/server/ReplicaManager.scala
core/src/main/scala/kafka/tools/StorageTool.scala
core/src/main/scala/kafka/server/metadata/AsyncTopicDeltaLifecycle.scala
core/src/main/scala/kafka/server/metadata/BrokerMetadataPublisher.scala
core/src/main/scala/kafka/server/metadata/DynamicConfigPublisher.scala
core/src/main/scala/kafka/server/nereus/NereusBrokerStorageAppendExecutor.scala
core/src/main/scala/kafka/server/nereus/NereusBrokerStorageFetchExecutor.scala
core/src/main/scala/kafka/server/nereus/NereusBrokerStorageRuntime.scala
core/src/main/scala/kafka/server/nereus/NereusBrokerStorageRuntimeFactory.scala
core/src/main/scala/kafka/server/nereus/NereusControllerStorageRuntimeFactory.scala
core/src/main/scala/kafka/server/nereus/NereusKafka.scala
core/src/main/scala/kafka/server/nereus/NereusKafkaOwnedPartitionSourceBridge.scala
core/src/main/scala/kafka/server/storage/BrokerStorageAppendExecutor.scala
core/src/main/scala/kafka/server/storage/BrokerStorageFetchExecutor.scala
core/src/main/scala/kafka/server/storage/BrokerStorageDrainReason.scala
core/src/main/scala/kafka/server/storage/BrokerStorageRuntime.scala
core/src/main/scala/kafka/server/storage/BrokerStorageRuntimeContext.scala
core/src/main/scala/kafka/server/storage/BrokerStorageRuntimeFactory.scala
core/src/main/scala/kafka/server/storage/ControllerStorageRuntime.scala
core/src/main/scala/kafka/server/storage/ControllerStorageRuntimeContext.scala
core/src/main/scala/kafka/server/storage/ControllerStorageRuntimeFactory.scala
core/src/test/java/kafka/log/nereus/NereusKafkaExceptionMapperTest.java
core/src/test/java/kafka/log/nereus/NereusCanonicalLogStateTest.java
core/src/test/java/kafka/log/nereus/NereusKafkaRecoveryStateCodecTest.java
core/src/test/java/kafka/log/nereus/NereusListOffsetsBridgeTest.java
core/src/test/java/kafka/log/nereus/NereusProducerStateManagerTest.java
core/src/test/java/kafka/log/nereus/NereusRecordTimestampInspectorTest.java
core/src/test/java/kafka/server/nereus/NereusKafkaContextAdaptersTest.java
core/src/test/java/kafka/server/nereus/NereusControllerStorageRuntimeTest.java
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
core/src/test/scala/unit/kafka/server/nereus/NereusBrokerStorageAppendExecutorTest.scala
core/src/test/scala/unit/kafka/server/nereus/NereusBrokerStorageFetchExecutorTest.scala
core/src/test/scala/unit/kafka/server/nereus/NereusBrokerStorageRuntimeTest.scala
core/src/test/scala/unit/kafka/server/nereus/NereusKafkaOwnedPartitionSourceBridgeTest.scala
core/src/test/scala/unit/kafka/server/nereus/NereusKafkaTest.scala
core/src/test/scala/unit/kafka/server/storage/BrokerStorageRuntimeFactoryTest.scala
core/src/test/scala/unit/kafka/tools/StorageToolTest.scala
metadata/src/main/java/org/apache/kafka/controller/ConfigurationControlManager.java
metadata/src/main/java/org/apache/kafka/controller/FeatureControlManager.java
metadata/src/main/java/org/apache/kafka/controller/QuorumFeatures.java
metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java
metadata/src/main/java/org/apache/kafka/image/FeaturesImage.java
metadata/src/test/java/org/apache/kafka/controller/ConfigurationControlManagerTest.java
metadata/src/test/java/org/apache/kafka/controller/FeatureControlManagerTest.java
metadata/src/test/java/org/apache/kafka/controller/QuorumFeaturesTest.java
metadata/src/test/java/org/apache/kafka/controller/ReplicationControlManagerTest.java
server-common/src/main/java/org/apache/kafka/server/common/Feature.java
server-common/src/main/java/org/apache/kafka/server/common/NereusStorageVersion.java
server-common/src/main/java/org/apache/kafka/server/util/KafkaScheduler.java
server-common/src/test/java/org/apache/kafka/server/common/FeatureTest.java
server/src/main/java/org/apache/kafka/server/BrokerFeatures.java
server/src/main/java/org/apache/kafka/server/config/AbstractKafkaConfig.java
server/src/main/java/org/apache/kafka/server/config/NereusKafkaConfigs.java
server/src/main/java/org/apache/kafka/server/config/NereusKafkaStorageConfig.java
server/src/test/java/org/apache/kafka/server/BrokerFeaturesTest.java
server/src/test/java/org/apache/kafka/server/config/NereusKafkaStorageConfigTest.java
server/src/test/java/org/apache/kafka/server/util/SchedulerTest.java
storage/src/main/java/org/apache/kafka/storage/internals/log/LeaderEpochAwareOffsetLookup.java
storage/src/main/java/org/apache/kafka/storage/internals/log/LeaderEpochAwareRecoveryState.java
storage/src/main/java/org/apache/kafka/storage/internals/log/BrokerStorageManagedLog.java
storage/src/main/java/org/apache/kafka/storage/internals/log/PartitionLeaderAuthority.java
storage/src/main/java/org/apache/kafka/storage/internals/log/ProducerStateEntry.java
storage/src/main/java/org/apache/kafka/storage/internals/log/RequiredAcksAwareAppend.java
storage/src/test/java/org/apache/kafka/storage/internals/log/ProducerStateManagerTest.java
FILES
)"
[[ "$actual_changes" == "$expected_changes" ]] \
    || fail "fork change set differs from the reviewed one-hundred-eighteen-file log-IO/bridge/recovery/metadata-lifecycle/configuration/runtime-composition/retention/compaction/controller/launcher/feature-control slice"

while read -r expected path; do
    [[ -n "$expected" ]] || continue
    actual="$(git -C "$kafka_checkout" hash-object "$path")"
    [[ "$actual" == "$expected" ]] \
        || fail "fork source drifted: $path expected $expected, got $actual"
done <<'LOCKS'
f5e0be83cf17defd199e750f07aab49bf8c3be58 bin/nereus-kafka-server-start.sh
94dbb912600e483c7fd43cb04aa56d9742270ae0 build.gradle
5804a298e97c745997a73874789fb79f200e0b9d checkstyle/import-control-core.xml
6f430993a3a5d1cbb3904a0395de75a470c3e43d core/src/main/java/kafka/log/nereus/NereusCanonicalLogState.java
60dbfb45a00f3c007c624ea31c1aca32ea49a8b2 core/src/main/java/kafka/log/nereus/NereusKafkaExceptionMapper.java
2631dc94da9a4e1a40de31f3119cb92e10c196ea core/src/main/java/kafka/log/nereus/NereusKafkaRecoveredState.java
f96b416a12290443c800bd082b0592130ec71db5 core/src/main/java/kafka/log/nereus/NereusKafkaRecoveryStateCodec.java
47eca0ad9a439e952794b2030d46c5b48714a839 core/src/main/java/kafka/log/nereus/NereusListOffsetsBridge.java
6f1e5f76fb4ed51f786e7f07a22c3fc3f46cf9ae core/src/main/java/kafka/log/nereus/NereusListOffsetsScanConfig.java
f05e82ed60782d0d135d75711bac1c807f7f2e62 core/src/main/java/kafka/log/nereus/NereusLocalLog.java
c80c745f3bc4adb6ac462883ea89efdf0cbc4a7b core/src/main/java/kafka/log/nereus/NereusLogSegment.java
2205c54ff7d07b9009caa822f4cb425a6bff7386 core/src/main/java/kafka/log/nereus/NereusProducerStateManager.java
aadcc658a9e74de9798b06d674ecb784947c8762 core/src/main/java/kafka/log/nereus/NereusRecordTimestampInspector.java
4b941cc2ab3326b41d0e7186c6b65ae9f3f5cf70 core/src/main/java/kafka/log/nereus/NereusTransactionIndex.java
7ea7807874c39b2ce383f9472fca019633602b1d core/src/main/java/kafka/log/nereus/NereusUnifiedLog.java
df74856a75146e0e35aaf5431b1ecb35531ec054 core/src/main/java/kafka/server/builders/LogManagerBuilder.java
0984006b925982dea46544d6459a5b5510e2a634 core/src/main/java/kafka/server/builders/ReplicaManagerBuilder.java
5a2db8b4237f3bf968824437155bde8f3840410c core/src/main/java/kafka/server/nereus/NereusControllerStorageRuntime.java
6521c6972def23a62c1fa1e8cc81a284f3b5c502 core/src/main/java/kafka/server/nereus/NereusKafkaControllerActivation.java
3c61509e24531a47edeef62800a1ba0eb625240d core/src/main/java/kafka/server/nereus/NereusKafkaControllerActivationCreator.java
44fe428c0b70ac64e4b8d1a5709ccde3c70d6f69 core/src/main/java/kafka/server/nereus/NereusKafkaControllerRuntimeConfiguration.java
5e2061bbb1655ab63a2796a3c0f12d34d7346ea7 core/src/main/java/kafka/server/nereus/NereusKafkaClock.java
2a72f47dd161052374b47e7a1eaee64a4d5f0dde core/src/main/java/kafka/server/nereus/NereusKafkaDeferredRuntime.java
d522cfe11ff3c4c3745be971c7a4613f1fc1502a core/src/main/java/kafka/server/nereus/NereusKafkaForkRuntimeBridges.java
4377b992347ae118b0b879ada2a9aad1d593b4ad core/src/main/java/kafka/server/nereus/NereusKafkaMappedRuntimeConfiguration.java
af9f5237d973c31eec101b2f3c966d836dc35353 core/src/main/java/kafka/server/nereus/NereusKafkaProductRuntimeCreator.java
bf0b8ed32d850cda69ba3e05b5ba64fe288bf452 core/src/main/java/kafka/server/nereus/NereusKafkaRecoveryStateFactory.java
b903540487b6553d4a1944b5f36e9567fc9262ba core/src/main/java/kafka/server/nereus/NereusKafkaRecoveryStateFactoryBridge.java
0d9a8133d57b8e3aaeb88865108b48c431e1a735 core/src/main/java/kafka/server/nereus/NereusKafkaRuntimeConfigurationMapper.java
1970537a48ac13fd77c6bc32fd2bf1e99fb31670 core/src/main/java/kafka/server/nereus/NereusKafkaStorageClusterSnapshotProvider.java
ae2387ee9d6b318eaf37f62840d8ca3ac44f4e9b core/src/main/scala/kafka/Kafka.scala
d477c7485376ae62f82abcb8393c9582be8794df core/src/main/scala/kafka/cluster/Partition.scala
69b47dc0c8441ec0e22408b6a7a4ea42e56a9d2b core/src/main/scala/kafka/log/LogManager.scala
27cf63cf77c51cd5d1cdc9599629e8e6b644e93e core/src/main/scala/kafka/log/UnifiedLogFactory.scala
5d48cd669ee816cd3215f93a4db0c9fc8b4e9a2f core/src/main/scala/kafka/log/nereus/NereusListOffsetsLifecycle.scala
21441c7e0e06556ff072f38b5c58e90514176748 core/src/main/scala/kafka/log/nereus/NereusTopicDeltaLifecycle.scala
418831074feeec2d16d75b7400e2108c3ec1f378 core/src/main/scala/kafka/log/nereus/NereusUnifiedLogFactory.scala
9a956b643165d6e0a04a506f7fc8378299e834e8 core/src/main/scala/kafka/server/BrokerServer.scala
e22088276dc750fe4ab5698f58959bb68dbd5cdd core/src/main/scala/kafka/server/ConfigHandler.scala
cfaf49737e0f7608560eedee4a496d9aae331b27 core/src/main/scala/kafka/server/ControllerServer.scala
457e08ad6714dd972abdb92d9f7471bb258469b7 core/src/main/scala/kafka/server/KafkaConfig.scala
d2a01927593f183e272995bef5177a314b959276 core/src/main/scala/kafka/server/KafkaRaftServer.scala
3101bfd5f7f93db61c9da3cbe75af034e71d5455 core/src/main/scala/kafka/server/NereusKafkaConfigValidator.scala
647af758ca065d8944bf4e7e03172028827db98e core/src/main/scala/kafka/server/ReplicaManager.scala
02ac193dbf5028903804126f73be86a1e87dc34b core/src/main/scala/kafka/tools/StorageTool.scala
7a3674d0cb71daa8830ea1ef89273181733ba661 core/src/main/scala/kafka/server/metadata/AsyncTopicDeltaLifecycle.scala
7c4da64c61aff4cefe9769764a9ff05306e5de73 core/src/main/scala/kafka/server/metadata/BrokerMetadataPublisher.scala
a6b0ad3e0effee4b673d6f831862d2750e190c0a core/src/main/scala/kafka/server/metadata/DynamicConfigPublisher.scala
6c24857341618113e7c7d3e1646112b3c469b75f core/src/main/scala/kafka/server/nereus/NereusBrokerStorageAppendExecutor.scala
b5d51ebc62ffa3e99cccff99030060fb8c59e262 core/src/main/scala/kafka/server/nereus/NereusBrokerStorageFetchExecutor.scala
e5c3a9fac1939d1c1eb485267cfec959985d38bf core/src/main/scala/kafka/server/nereus/NereusBrokerStorageRuntime.scala
fb91f4e6f99b70c7c26470ca2115583eb6ee4dd6 core/src/main/scala/kafka/server/nereus/NereusBrokerStorageRuntimeFactory.scala
04380389df34bc0facb10aa5e0d8371fbb1005c5 core/src/main/scala/kafka/server/nereus/NereusControllerStorageRuntimeFactory.scala
f566a6ab54024f37e8d5f4cfaa43be781d92e7e1 core/src/main/scala/kafka/server/nereus/NereusKafka.scala
431486f57cd3aefd9dc8ed019607ed193e98fb43 core/src/main/scala/kafka/server/nereus/NereusKafkaOwnedPartitionSourceBridge.scala
315a6959c87bc2f86466148ffb9630ab9eeedbeb core/src/main/scala/kafka/server/storage/BrokerStorageAppendExecutor.scala
1b2984caa4062c995c10ffcc91710c3b9b4ea42c core/src/main/scala/kafka/server/storage/BrokerStorageFetchExecutor.scala
876bde2298de1e772d6bcd4eee2e38bb0817bbde core/src/main/scala/kafka/server/storage/BrokerStorageDrainReason.scala
f798e41b5dd028eb6880aedad7dd427690ebae64 core/src/main/scala/kafka/server/storage/BrokerStorageRuntime.scala
b2d6eccbc8169932d4104c6f494d945476becfd1 core/src/main/scala/kafka/server/storage/BrokerStorageRuntimeContext.scala
ce68275cd8367da3cbb3a8d043ac6234163dd032 core/src/main/scala/kafka/server/storage/BrokerStorageRuntimeFactory.scala
d73e74019d8769f2ac04175c36eb77430e052cfd core/src/main/scala/kafka/server/storage/ControllerStorageRuntime.scala
bcc5ec81f637a1a473d33e2f03267cf15ef85c60 core/src/main/scala/kafka/server/storage/ControllerStorageRuntimeContext.scala
21200a7a039f5bcf495547ad1b94d8976702ce86 core/src/main/scala/kafka/server/storage/ControllerStorageRuntimeFactory.scala
9685d6627eaaebdf9de5a8e71c5f1b789372375f core/src/test/java/kafka/log/nereus/NereusCanonicalLogStateTest.java
f81ec4137daa9e9fff7b7262733ded7998c86eba core/src/test/java/kafka/log/nereus/NereusKafkaExceptionMapperTest.java
ab0cd6d890b40a9309d56c55db596b19ad0ebb96 core/src/test/java/kafka/log/nereus/NereusKafkaRecoveryStateCodecTest.java
c2bd8e03152a23547044a42f439b33698ace4251 core/src/test/java/kafka/log/nereus/NereusListOffsetsBridgeTest.java
59293bda207a9617016cab39c94bcd5bfb6f894f core/src/test/java/kafka/log/nereus/NereusProducerStateManagerTest.java
205989c5d3adf68127d71be28c6ff9f521abcbf1 core/src/test/java/kafka/log/nereus/NereusRecordTimestampInspectorTest.java
7f36f601ae68ccb353878327bd9bdb0219b90186 core/src/test/java/kafka/server/nereus/NereusKafkaContextAdaptersTest.java
36112d19c97b6a49a5f61c210e0df0790a75869d core/src/test/java/kafka/server/nereus/NereusControllerStorageRuntimeTest.java
d7f0b8cca7dec9cfa4de9a542c8eb1b3c3c9cfe5 core/src/test/java/kafka/server/nereus/NereusKafkaDeferredRuntimeTest.java
ec32f2b8e23e9548a7a8b4e8bdb717a7949dc788 core/src/test/java/kafka/server/nereus/NereusKafkaRecoveryStateFactoryBridgeTest.java
0dad9ef15898372476787e354ce96ac2415a8a3c core/src/test/java/kafka/server/nereus/NereusKafkaRecoveryStateFactoryTest.java
e4b1bc88c777fa45cd56165fb0a37334c90b4237 core/src/test/java/kafka/server/nereus/NereusKafkaRuntimeConfigurationMapperTest.java
e06ff96da5853e2ab0afc1cbc3e4153b981f7b7d core/src/test/scala/unit/kafka/cluster/PartitionTest.scala
c28a29d488b51c0630cb1197b95b30bc6bf43a68 core/src/test/scala/unit/kafka/log/nereus/NereusListOffsetsLifecycleTest.scala
ba0bcb6a45f1715683ac23611873dcb83ce5a474 core/src/test/scala/unit/kafka/log/nereus/NereusTopicDeltaLifecycleTest.scala
7c58b46e3d2746d61b5c673257ef75b57a6ce1f2 core/src/test/scala/unit/kafka/log/nereus/NereusUnifiedLogFactoryTest.scala
14358b2d91ae9a25ea683946509cd3fd1657b6ca core/src/test/scala/unit/kafka/server/KafkaConfigTest.scala
dda8c0b06459e6df5fd05b0537a540f21a143a51 core/src/test/scala/unit/kafka/server/NereusKafkaConfigValidatorTest.scala
6bfc0f2d51334dbe9213a98b21ab7124416a666b core/src/test/scala/unit/kafka/server/ReplicaManagerTest.scala
98e32cef17bc011b1f6f13f6865fd56d87cfdc27 core/src/test/scala/unit/kafka/server/metadata/BrokerMetadataPublisherTest.scala
97aec0dfd4972d124a6456b5dc09777563c0b0c1 core/src/test/scala/unit/kafka/server/nereus/NereusBrokerStorageAppendExecutorTest.scala
3c9281f1c48872b3645de485aaa98bcc2ac431ae core/src/test/scala/unit/kafka/server/nereus/NereusBrokerStorageFetchExecutorTest.scala
49c96ae84cb30876cfd79afd39e93ad52aa92618 core/src/test/scala/unit/kafka/server/nereus/NereusBrokerStorageRuntimeTest.scala
d2a9265f60ae82e93bb152832c4dd36f69c46126 core/src/test/scala/unit/kafka/server/nereus/NereusKafkaOwnedPartitionSourceBridgeTest.scala
e09fa0c9643d3982af69a6679438f1baf8230606 core/src/test/scala/unit/kafka/server/nereus/NereusKafkaTest.scala
733c4d4815cbec6a7335f9a337053e980e87883d core/src/test/scala/unit/kafka/server/storage/BrokerStorageRuntimeFactoryTest.scala
30e87b42350e3f88161dcd14ffee41cb0eb3da81 core/src/test/scala/unit/kafka/tools/StorageToolTest.scala
cd06ee5e4709b7d70c19cdd82c241db5d44377bb metadata/src/main/java/org/apache/kafka/controller/ConfigurationControlManager.java
e7bc734e74e6e35346f6cb5a621b34ca6e20b1ac metadata/src/main/java/org/apache/kafka/controller/FeatureControlManager.java
6cd7b89e51a474c98844431503a2c31808f0f3b5 metadata/src/main/java/org/apache/kafka/controller/QuorumFeatures.java
4ba7b1b76a03e1750730e1df12630058d06eed12 metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java
153a1668c2e3db8b850e0d739ec49374efc375d2 metadata/src/main/java/org/apache/kafka/image/FeaturesImage.java
20b59845539d419251efc3e5174b8d602f0a4450 metadata/src/test/java/org/apache/kafka/controller/ConfigurationControlManagerTest.java
d096d6b28cf4efaddb4fac5b4ffdf5e122df08e0 metadata/src/test/java/org/apache/kafka/controller/FeatureControlManagerTest.java
0932a4f8edc114d5525f770d186e8747668e67be metadata/src/test/java/org/apache/kafka/controller/QuorumFeaturesTest.java
da062ff1ddde7348d830eb850128d86d8621040f metadata/src/test/java/org/apache/kafka/controller/ReplicationControlManagerTest.java
e4cfd9a94228abfd06c80d84982431d4029eac1b server-common/src/main/java/org/apache/kafka/server/common/Feature.java
6d430f14bf450811f3e72cfc1215ecd4a7b9abd7 server-common/src/main/java/org/apache/kafka/server/common/NereusStorageVersion.java
1fbf9180a68bca9a5d45e38f9862841ea486f739 server-common/src/main/java/org/apache/kafka/server/util/KafkaScheduler.java
cc4e0b9ab0f32fb83c0f7ab0c0fd8d5e3bbd41f2 server-common/src/test/java/org/apache/kafka/server/common/FeatureTest.java
3b6f063deb7be47b2676a0a44300eab9173278e9 server/src/main/java/org/apache/kafka/server/BrokerFeatures.java
3036df4e77ad23fabb6533d1dc173458356ea6b3 server/src/main/java/org/apache/kafka/server/config/AbstractKafkaConfig.java
159b5b49316f9284df524b855409837fae0641b1 server/src/main/java/org/apache/kafka/server/config/NereusKafkaConfigs.java
bcf3d34104255dba08937f27b9642ee20f40de5d server/src/main/java/org/apache/kafka/server/config/NereusKafkaStorageConfig.java
1c979a97392a3d7b42769864dbcdf4d4211fa677 server/src/test/java/org/apache/kafka/server/BrokerFeaturesTest.java
cb1fc8b5fca7a7c97ec0a5c383474d8eab9f23ec server/src/test/java/org/apache/kafka/server/config/NereusKafkaStorageConfigTest.java
168371ca93e4cc0aa8e7168f82c880396dd723a2 server/src/test/java/org/apache/kafka/server/util/SchedulerTest.java
6a9a43c81b0b60e69fb95099a76d80e7894ba453 storage/src/main/java/org/apache/kafka/storage/internals/log/LeaderEpochAwareOffsetLookup.java
9920d51f0f7740f1db62064868ac6224a0db18b0 storage/src/main/java/org/apache/kafka/storage/internals/log/LeaderEpochAwareRecoveryState.java
968d752e86b19798fdb5bff349186185d8d2e183 storage/src/main/java/org/apache/kafka/storage/internals/log/BrokerStorageManagedLog.java
d3486819a1a5a79c45b71d73f2634bd84b317f63 storage/src/main/java/org/apache/kafka/storage/internals/log/PartitionLeaderAuthority.java
4882c9804a4b00119511aef25f2e9e02533f55cd storage/src/main/java/org/apache/kafka/storage/internals/log/ProducerStateEntry.java
a53537ae18237fe48296b5bed516a3d63bd11c91 storage/src/main/java/org/apache/kafka/storage/internals/log/RequiredAcksAwareAppend.java
ed1563406b0e711961f74411fe19861dcac41c4e storage/src/test/java/org/apache/kafka/storage/internals/log/ProducerStateManagerTest.java
LOCKS

marker_start="$(grep -h -F -c 'Nereus inject start:' \
    "$kafka_checkout/build.gradle" "$kafka_checkout/checkstyle/import-control-core.xml" \
    "$kafka_checkout/core/src/main/java/kafka/server/builders/LogManagerBuilder.java" \
    "$kafka_checkout/core/src/main/scala/kafka/cluster/Partition.scala" \
    "$kafka_checkout/core/src/main/scala/kafka/log/LogManager.scala" \
    "$kafka_checkout/core/src/main/scala/kafka/server/BrokerServer.scala" \
    "$kafka_checkout/core/src/main/scala/kafka/server/ControllerServer.scala" \
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
    "$kafka_checkout/core/src/main/scala/kafka/server/ControllerServer.scala" \
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
grep -F -q 'implements RequiredAcksAwareAppend' "$nereus_unified_log" \
    || fail "Nereus UnifiedLog lost the exact required-acks append boundary"
grep -F -q 'exactStorage.append(records.buffer().duplicate(), context)' "$nereus_unified_log" \
    || fail "Nereus UnifiedLog lost stable append delegation"
grep -F -q 'invocation.markStable(exactStorage, records);' "$nereus_unified_log" \
    || fail "Nereus UnifiedLog lost post-stable stock-state capture"
grep -F -q 'observeCommittedAppend(invocation);' "$nereus_unified_log" \
    || fail "Nereus UnifiedLog lost post-stable canonical/producer-state publication"
grep -F -q 'fenceUnknownAppend(invocation.committedStorage);' "$nereus_unified_log" \
    || fail "Nereus UnifiedLog lost post-stable failure fencing"
grep -F -q 'exactStorage.read(request)' "$nereus_unified_log" \
    || fail "Nereus UnifiedLog lost bounded Fetch delegation"
grep -F -q 'MemoryRecords.readableRecords(assembly.recordsBuffer())' "$nereus_unified_log" \
    || fail "Nereus UnifiedLog lost exact Kafka Fetch assembly"

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
grep -F -q 'case requiredAcksAware: RequiredAcksAwareAppend =>' "$partition" \
    || fail "Partition lost authoritative required-acks routing"
grep -F -q 'requiredAcks.toShort)' "$partition" \
    || fail "Partition no longer preserves the exact required-acks value"
grep -F -q 'new PartitionLeaderAuthority {' "$partition" \
    || fail "Partition lost the stock-compatible maintenance authority"
grep -F -q 'override def capture[T](' "$partition" \
    || fail "Partition lost generic capture under leader authority"
grep -F -q 'override def publish(' "$partition" \
    || fail "Partition lost publication under leader authority"

managed_log_seam="$kafka_checkout/storage/src/main/java/org/apache/kafka/storage/internals/log/BrokerStorageManagedLog.java"
grep -F -q 'public interface BrokerStorageManagedLog' "$managed_log_seam" \
    || fail "stock managed-log seam is missing"
grep -F -q 'PartitionLeaderAuthority authority' "$managed_log_seam" \
    || fail "stock managed-log seam lost partition authority"

partition_authority_seam="$kafka_checkout/storage/src/main/java/org/apache/kafka/storage/internals/log/PartitionLeaderAuthority.java"
grep -F -q '<T> T capture(int expectedLeaderEpoch, Supplier<T> action);' "$partition_authority_seam" \
    || fail "stock partition authority lost generic capture"
grep -F -q 'void publish(int expectedLeaderEpoch, Runnable action);' "$partition_authority_seam" \
    || fail "stock partition authority lost durable publication"

required_acks_seam="$kafka_checkout/storage/src/main/java/org/apache/kafka/storage/internals/log/RequiredAcksAwareAppend.java"
grep -F -q 'public interface RequiredAcksAwareAppend' "$required_acks_seam" \
    || fail "stock required-acks append seam is missing"
grep -F -q 'short requiredAcks);' "$required_acks_seam" \
    || fail "stock required-acks append seam lost the protocol fact"

replica_manager="$kafka_checkout/core/src/main/scala/kafka/server/ReplicaManager.scala"
grep -F -q 'delayedRemoteListOffsetsPurgatory.checkAndComplete' "$replica_manager" \
    || fail "ReplicaManager lost async ListOffsets wakeup"
grep -F -q 'onLeaderStatePublished: (Partition, Uuid, Int) => Unit' "$replica_manager" \
    || fail "ReplicaManager lost synchronous new-leader preparation callback"
grep -F -q 'storageAppendExecutor match {' "$replica_manager" \
    || fail "ReplicaManager lost optional storage append handoff routing"
grep -F -q 'executor.validateRequest(entriesPerPartition.values)' "$replica_manager" \
    || fail "ReplicaManager lost request-wide validation before partition submit"
grep -F -q 'RequestLocal.noCaching' "$replica_manager" \
    || fail "ReplicaManager resumed borrowing request-thread state on append workers"
grep -F -q 'defaultActionQueue.tryCompleteActions()' "$replica_manager" \
    || fail "ReplicaManager lost post-worker delayed-action completion"
grep -F -q 'storageFetchExecutor match {' "$replica_manager" \
    || fail "ReplicaManager lost optional whole-request Fetch handoff routing"
grep -F -q 'readFromPurgatory = !initialWave' "$replica_manager" \
    || fail "ReplicaManager lost initial-versus-delayed Fetch wave semantics"
grep -F -q 'completeStorageFetch(params, fetchInfos, responseCallback, exactResults)' "$replica_manager" \
    || fail "ReplicaManager lost callback-once storage Fetch completion"

append_executor_seam="$kafka_checkout/core/src/main/scala/kafka/server/storage/BrokerStorageAppendExecutor.scala"
grep -F -q 'trait BrokerStorageAppendExecutor extends AutoCloseable' "$append_executor_seam" \
    || fail "stock-compatible bounded append executor seam is missing"
grep -F -q 'def validateRequest(entries: Iterable[MemoryRecords]): Unit' "$append_executor_seam" \
    || fail "bounded append seam lost request-wide prevalidation"
grep -F -q 'def drained: CompletionStage[Void]' "$append_executor_seam" \
    || fail "bounded append seam lost drain completion"

fetch_executor_seam="$kafka_checkout/core/src/main/scala/kafka/server/storage/BrokerStorageFetchExecutor.scala"
grep -F -q 'trait BrokerStorageFetchExecutor extends AutoCloseable' "$fetch_executor_seam" \
    || fail "stock-compatible whole-request Fetch executor seam is missing"
grep -F -q 'read: Boolean => Seq[(TopicIdPartition, LogReadResult)]' "$fetch_executor_seam" \
    || fail "whole-request Fetch seam lost stock read-wave ownership"
grep -F -q 'def drained: CompletionStage[Void]' "$fetch_executor_seam" \
    || fail "whole-request Fetch seam lost drain completion"

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
grep -F -q 'storageAppendExecutor.close()' "$adapter_runtime" \
    || fail "adapter-backed broker runtime lost synchronous Produce admission drain"
grep -F -q 'storageAppendExecutor.drained.toCompletableFuture' "$adapter_runtime" \
    || fail "adapter-backed broker runtime no longer waits for admitted Produce work"
grep -F -q 'storageFetchExecutor.close()' "$adapter_runtime" \
    || fail "adapter-backed broker runtime lost synchronous Fetch admission drain"
grep -F -q 'storageFetchExecutor.drained.toCompletableFuture' "$adapter_runtime" \
    || fail "adapter-backed broker runtime no longer waits for admitted Fetch work"

adapter_append_executor="$kafka_checkout/core/src/main/scala/kafka/server/nereus/NereusBrokerStorageAppendExecutor.scala"
grep -F -q 'new KafkaBoundedAppendExecutor(' "$adapter_append_executor" \
    || fail "adapter-backed append handoff lost the product bounded executor"
grep -F -q 'records.buffer().duplicate()' "$adapter_append_executor" \
    || fail "adapter-backed append handoff lost exact Produce buffer capture"
grep -F -q 'NereusKafkaExceptionMapper.map(failure)' "$adapter_append_executor" \
    || fail "adapter-backed append handoff lost Kafka error mapping"

adapter_fetch_executor="$kafka_checkout/core/src/main/scala/kafka/server/nereus/NereusBrokerStorageFetchExecutor.scala"
grep -F -q 'new KafkaFetchWaveOperation' "$adapter_fetch_executor" \
    || fail "adapter-backed Fetch handoff lost the product wave operation"
grep -F -q 'operations.size >= maxOutstanding' "$adapter_fetch_executor" \
    || fail "adapter-backed Fetch handoff lost logical operation capacity"
grep -F -q 'config.executorThreads(),' "$adapter_fetch_executor" \
    || fail "adapter-backed Fetch handoff lost bounded worker sizing"
grep -F -q 'maxOutstanding,' "$adapter_fetch_executor" \
    || fail "adapter-backed Fetch handoff can reject wakeups for admitted logical operations"
grep -F -q 'storageManager.current(identity).ifPresent' "$adapter_fetch_executor" \
    || fail "adapter-backed Fetch handoff lost exact current-storage event subscription"

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
grep -F -q 'storageAppendExecutor = brokerStorageRuntime.appendExecutor' "$broker_server" \
    || fail "BrokerServer lost exact bounded append executor injection"
grep -F -q 'storageFetchExecutor = brokerStorageRuntime.fetchExecutor' "$broker_server" \
    || fail "BrokerServer lost exact whole-request Fetch executor injection"
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
grep -F -q 'controllerStorageRuntimeFactory: ControllerStorageRuntimeFactory = ControllerStorageRuntimeFactory.Disabled' "$kafka_raft_server" \
    || fail "KafkaRaftServer lost explicit stock-default controller runtime injection"
grep -F -q 'controllerStorageRuntimeFactory,' "$kafka_raft_server" \
    || fail "KafkaRaftServer lost ControllerServer runtime factory injection"

controller_server="$kafka_checkout/core/src/main/scala/kafka/server/ControllerServer.scala"
grep -F -q 'controllerStorageRuntimeFactory.create(' "$controller_server" \
    || fail "ControllerServer lost explicit storage runtime creation"
grep -F -q 'controllerStorageRuntime.start().toCompletableFuture' "$controller_server" \
    || fail "ControllerServer lost bounded startup resource creation"
grep -F -q 'metadataPublishers.add(controllerStorageRuntime)' "$controller_server" \
    || fail "ControllerServer lost metadata/leadership callback registration"
grep -F -q 'Utils.closeQuietly(controllerStorageRuntime, "controller storage runtime")' "$controller_server" \
    || fail "ControllerServer lost pre-publisher-removal activation shutdown"

kafka_main="$kafka_checkout/core/src/main/scala/kafka/Kafka.scala"
grep -F -q 'BrokerStorageRuntimeFactory.Disabled,' "$kafka_main" \
    || fail "stock Kafka launcher no longer selects the disabled broker runtime"
grep -F -q 'ControllerStorageRuntimeFactory.Disabled)' "$kafka_main" \
    || fail "stock Kafka launcher no longer selects the disabled controller runtime"
grep -F -q 'brokerStorageRuntimeFactory,' "$kafka_main" \
    || fail "shared Kafka lifecycle lost explicit KafkaRaftServer broker factory injection"
grep -F -q 'controllerStorageRuntimeFactory)' "$kafka_main" \
    || fail "shared Kafka lifecycle lost explicit KafkaRaftServer controller factory injection"

nereus_launcher="$kafka_checkout/core/src/main/scala/kafka/server/nereus/NereusKafka.scala"
grep -F -q 'productionBrokerFactory,' "$nereus_launcher" \
    || fail "Nereus launcher no longer reuses the stock Kafka lifecycle"
grep -F -q 'NereusBrokerStorageRuntimeFactory.production()' "$nereus_launcher" \
    || fail "Nereus launcher lost static broker production factory selection"
grep -F -q 'NereusControllerStorageRuntimeFactory.production()' "$nereus_launcher" \
    || fail "Nereus launcher lost static controller production factory selection"

nereus_start_script="$kafka_checkout/bin/nereus-kafka-server-start.sh"
grep -F -q 'kafka.server.nereus.NereusKafka' "$nereus_start_script" \
    || fail "Nereus server-start script lost the explicit launcher"

for stock_source in \
        "$partition" \
        "$kafka_checkout/core/src/main/scala/kafka/log/LogManager.scala" \
        "$replica_manager" \
        "$controller_server" \
        "$kafka_raft_server" \
        "$kafka_main"; do
    if grep -E -q 'com\.nereusstream|kafka\.(log|server)\.nereus' "$stock_source"; then
        fail "stock source directly links an artifact-only Nereus class: $stock_source"
    fi
done

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
grep -F -q 'new NereusKafkaCompactionRuntimeConfiguration(' "$runtime_mapper" \
    || fail "runtime mapper lost typed compaction configuration"
grep -F -q 'new KafkaCompactionTwoPassExecutor.Limits(' "$runtime_mapper" \
    || fail "runtime mapper lost bounded two-pass compaction limits"
grep -F -q 'public NereusKafkaControllerRuntimeConfiguration mapController(' "$runtime_mapper" \
    || fail "runtime mapper lost provider-neutral controller configuration"
grep -F -q 'new KafkaStorageActivationPolicy(' "$runtime_mapper" \
    || fail "runtime mapper lost exact controller activation policy"

controller_runtime_seam="$kafka_checkout/core/src/main/scala/kafka/server/storage/ControllerStorageRuntime.scala"
grep -F -q 'trait ControllerStorageRuntime extends MetadataPublisher' "$controller_runtime_seam" \
    || fail "stock controller runtime lost MetadataLoader callback ownership"
grep -F -q 'def start(): CompletionStage[Void]' "$controller_runtime_seam" \
    || fail "stock controller runtime lost explicit resource startup"

controller_factory_seam="$kafka_checkout/core/src/main/scala/kafka/server/storage/ControllerStorageRuntimeFactory.scala"
grep -F -q 'val Disabled: ControllerStorageRuntimeFactory' "$controller_factory_seam" \
    || fail "controller storage runtime lost its stock-disabled factory"
grep -F -q 'requires an explicitly installed ControllerStorageRuntimeFactory' "$controller_factory_seam" \
    || fail "enabled controller storage no longer fails closed without an explicit runtime factory"

controller_adapter_factory="$kafka_checkout/core/src/main/scala/kafka/server/nereus/NereusControllerStorageRuntimeFactory.scala"
grep -F -q 'mapper.mapController(' "$controller_adapter_factory" \
    || fail "adapter controller factory lost pure controller mapping"
grep -F -q '() => activationCreator.create(mapped, clusterSnapshots, clock)' "$controller_adapter_factory" \
    || fail "adapter controller factory no longer defers Oxia creation to start"

controller_activation_creator="$kafka_checkout/core/src/main/java/kafka/server/nereus/NereusKafkaControllerActivationCreator.java"
grep -F -q 'SharedOxiaClientRuntime.connect(exact.oxia(), exactClock)' "$controller_activation_creator" \
    || fail "controller activation creator lost its owned Oxia runtime"
grep -F -q 'new KafkaStorageBindingAwareClusterSnapshotProvider(' "$controller_activation_creator" \
    || fail "controller activation creator lost the durable binding scan"
grep -F -q 'new KafkaStorageFirstActivationCoordinator(' "$controller_activation_creator" \
    || fail "controller activation creator lost product coordinator composition"

controller_adapter_runtime="$kafka_checkout/core/src/main/java/kafka/server/nereus/NereusControllerStorageRuntime.java"
grep -F -q 'localController = exact.isLeader(nodeId);' "$controller_adapter_runtime" \
    || fail "controller runtime lost current-controller scheduling"
grep -F -q 'if (inFlight != null || scheduled != null)' "$controller_adapter_runtime" \
    || fail "controller runtime lost single-attempt coalescing"
grep -F -q 'nereus.retriable()' "$controller_adapter_runtime" \
    || fail "controller runtime lost typed retry classification"
grep -F -q 'terminalFailure = true;' "$controller_adapter_runtime" \
    || fail "controller runtime lost per-epoch terminal failure suppression"
grep -F -q 'cancelScheduled();' "$controller_adapter_runtime" \
    || fail "controller runtime lost leadership-loss/shutdown cancellation"

product_runtime_creator="$kafka_checkout/core/src/main/java/kafka/server/nereus/NereusKafkaProductRuntimeCreator.java"
grep -F -q 'NereusKafkaObjectWalRuntimeFactory.createActivated(' "$product_runtime_creator" \
    || fail "product runtime creator lost activation-backed Object-WAL construction"
grep -F -q 'scheduler, "scheduler").scheduledExecutorService()' "$product_runtime_creator" \
    || fail "product runtime creator lost the borrowed Kafka scheduler boundary"
grep -F -q 'new NereusKafkaStorageClusterSnapshotProvider(' "$product_runtime_creator" \
    || fail "product runtime creator lost the KRaft/local-log activation snapshot"
grep -F -q 'exactBridges.ownedPartitions().configureCompaction(storage, nereusBuild);' "$product_runtime_creator" \
    || fail "product runtime creator lost one-time fork compaction configuration"
grep -F -q 'Optional.of(new NereusKafkaCompactionContext(' "$product_runtime_creator" \
    || fail "product runtime creator lost compaction runtime composition"

owned_partition_source="$kafka_checkout/core/src/main/scala/kafka/server/nereus/NereusKafkaOwnedPartitionSourceBridge.scala"
grep -F -q 'with KafkaCompactionRuntime.OwnedPartitionSource' "$owned_partition_source" \
    || fail "owned-partition bridge lost the compaction source contract"
grep -F -q 'new KafkaCompactionRuntime.OwnedPartition(' "$owned_partition_source" \
    || fail "owned-partition bridge lost leader-only compaction registration"
grep -F -q 'KafkaCompactionRuntime.WorkClass.INTERNAL' "$owned_partition_source" \
    || fail "owned-partition bridge lost internal-topic work classification"
grep -F -q 'log.compactionCaptureProvider(' "$owned_partition_source" \
    || fail "owned-partition bridge lost exact leader capture-provider registration"

grep -F -q 'public KafkaCompactionPartitionPass.CaptureProvider compactionCaptureProvider(' "$nereus_unified_log" \
    || fail "Nereus UnifiedLog lost the product compaction capture provider"
grep -F -q 'return maintenance.captureCompaction(hooks);' "$nereus_unified_log" \
    || fail "Nereus UnifiedLog lost product-side authority capture"
grep -F -q 'CleanedTransactionMetadata cleaned = new CleanedTransactionMetadata();' "$nereus_unified_log" \
    || fail "Nereus UnifiedLog lost the stock transaction-cleaner oracle"
grep -F -q 'captureCompactionTransactions(' "$nereus_unified_log" \
    || fail "Nereus UnifiedLog lost partition-locked transaction pre-scan"

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
grep -F -q 'producerStateManager.replayBatch(batch);' "$recovered_state" \
    || fail "fork recovery state lost exact stock producer/transaction replay"
grep -F -q 'producerStateManager.freezeCanonical(expectedStableEndOffset)' "$recovered_state" \
    || fail "fork recovery state lost canonical NKC1 producer/transaction freeze"
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

echo "F9 Kafka fork development source lock: published $actual_remote_head from Apache $expected_base; cached organization trunk $actual_remote_trunk; thirty-one commits, one hundred eighteen log-IO/bridge/recovery/metadata-lifecycle/configuration/runtime-composition/retention/compaction/controller/launcher/feature-control blobs and markers match"
