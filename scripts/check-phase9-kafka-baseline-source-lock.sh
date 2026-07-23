#!/usr/bin/env bash
set -euo pipefail

kafka_checkout="${1:?usage: check-phase9-kafka-baseline-source-lock.sh KAFKA_CHECKOUT EXPECTED_HEAD EXPECTED_VERSION}"
expected_head="${2:?missing expected Kafka HEAD}"
expected_version="${3:?missing expected Kafka version}"

fail() {
    echo "F9 Kafka baseline source lock: $*" >&2
    exit 1
}

[[ -d "$kafka_checkout/.git" ]] || fail "missing Kafka checkout: $kafka_checkout"
actual_head="$(git -C "$kafka_checkout" rev-parse HEAD)"
[[ "$actual_head" == "$expected_head" ]] \
    || fail "Kafka HEAD drifted: expected $expected_head, got $actual_head"

actual_version="$(git -C "$kafka_checkout" show HEAD:gradle.properties \
    | sed -n 's/^version=//p' | head -n 1)"
[[ "$actual_version" == "$expected_version" ]] \
    || fail "Kafka version drifted: expected $expected_version, got $actual_version"

[[ -z "$(git -C "$kafka_checkout" status --porcelain)" ]] \
    || fail "Kafka baseline checkout has uncommitted changes"

origin_url="$(git -C "$kafka_checkout" remote get-url origin)"
[[ "$origin_url" == "https://github.com/apache/kafka" \
        || "$origin_url" == "https://github.com/apache/kafka.git" \
        || "$origin_url" == "git@github.com:apache/kafka.git" ]] \
    || fail "Kafka baseline origin is not apache/kafka: $origin_url"

while read -r expected path; do
    [[ -n "$expected" ]] || continue
    [[ -f "$kafka_checkout/$path" ]] || fail "missing locked Kafka source: $path"
    actual="$(git -C "$kafka_checkout" hash-object "$path")"
    [[ "$actual" == "$expected" ]] \
        || fail "Kafka source drifted: $path expected $expected, got $actual"
done <<'LOCKS'
d6e9cc6bd7fbb0189889c332486443bc0751d879 clients/src/main/java/org/apache/kafka/common/record/DefaultRecordBatch.java
f806cce692073b3ab25c0d8b895a16f6c6edbc75 clients/src/main/java/org/apache/kafka/common/record/MemoryRecords.java
467bee5e12013f659ac7529623ea6fda730a42ee storage/src/main/java/org/apache/kafka/storage/internals/log/UnifiedLog.java
2d9b35973b8138fc9ab56c0edea964a2f4a03e09 storage/src/main/java/org/apache/kafka/storage/internals/log/LocalLog.java
76a4b247eddb4a9aa22b1d11cfb2e4f8023e5118 core/src/main/scala/kafka/cluster/Partition.scala
4ee24f2e414282328562b87dd851354e8a7e3c70 core/src/main/scala/kafka/server/ReplicaManager.scala
c173a9aad04ef53e32956bd0dfd95a65588d7e2c core/src/main/scala/kafka/server/BrokerServer.scala
bfee35061f82f491f77ee792211cb24283d8ef96 core/src/main/scala/kafka/log/LogManager.scala
35c44b9524d76ac55b3dd7fb2099f0b680c07fdb core/src/main/scala/kafka/server/metadata/BrokerMetadataPublisher.scala
a1e93b3f10ff6cd49c06de749108eb8e40da6426 metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java
LOCKS

echo "F9 Kafka baseline source lock: Apache Kafka $actual_head / $actual_version and 10 source blobs match"
