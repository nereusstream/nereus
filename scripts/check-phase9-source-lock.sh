#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
automq_checkout="${1:?usage: check-phase9-source-lock.sh AUTOMQ_CHECKOUT EXPECTED_HEAD EXPECTED_VERSION}"
expected_automq_head="${2:?missing expected AutoMQ HEAD}"
expected_automq_version="${3:?missing expected AutoMQ version}"

fail() {
    echo "F9 source lock: $*" >&2
    exit 1
}

[[ -d "$automq_checkout/.git" ]] || fail "missing AutoMQ checkout: $automq_checkout"
actual_automq_head="$(git -C "$automq_checkout" rev-parse HEAD)"
[[ "$actual_automq_head" == "$expected_automq_head" ]] \
    || fail "AutoMQ HEAD drifted: expected $expected_automq_head, got $actual_automq_head"
actual_automq_version="$(git -C "$automq_checkout" show HEAD:gradle.properties \
    | sed -n 's/^version=//p' | head -n 1)"
[[ "$actual_automq_version" == "$expected_automq_version" ]] \
    || fail "AutoMQ version drifted: expected $expected_automq_version, got $actual_automq_version"

while read -r expected path; do
    [[ -n "$expected" ]] || continue
    [[ -f "$repo_root/$path" ]] || fail "missing locked Nereus source: $path"
    actual="$(git -C "$repo_root" hash-object "$path")"
    [[ "$actual" == "$expected" ]] \
        || fail "Nereus source drifted: $path expected $expected, got $actual"
done <<'LOCKS'
c6bac3e4efcb0e597c3441071a638ebf8489934a nereus-api/src/main/java/com/nereusstream/api/AppendBatch.java
ffa36d92a0e5bbe225358cde8ee9cdd1c829e6ac nereus-api/src/main/java/com/nereusstream/api/AppendEntry.java
eb0d309b33676c4786b5134ded3840ce761386bd nereus-api/src/main/java/com/nereusstream/api/StreamStorage.java
a642b6488cf6f12c488e68949b8ec09a32d28451 nereus-api/src/main/java/com/nereusstream/api/StableStreamHeadSnapshot.java
41fc221d99a1ec612a121739a2d01ef59af0758c nereus-api/src/main/java/com/nereusstream/api/StreamCommitAnchor.java
799caf45554ffaab72927bf18e869cce9ba02d2a nereus-api/src/main/java/com/nereusstream/api/AppendPrecondition.java
e43bd0bb99f1762cc4893e20da18450c9e94b76f nereus-api/src/main/java/com/nereusstream/api/ReadRequest.java
3b2605616150ffb9efada7287df90fa557b92a34 nereus-api/src/main/java/com/nereusstream/api/SemanticReadResult.java
2b6aa2510c3a110839988fd0b849200f00b67523 nereus-core/src/main/java/com/nereusstream/core/DefaultStreamStorage.java
68c0457cddf7db051da12f676c8c0dc599458be7 nereus-core/src/main/java/com/nereusstream/core/append/AppendCoordinator.java
336d08fa1078248b31925f5a6391932f177999ec nereus-core/src/main/java/com/nereusstream/core/append/AppendResultValidator.java
293b5bacd47c2908e226cadec32a6f67b01bdfb5 nereus-core/src/main/java/com/nereusstream/core/read/ReadCoordinator.java
11294d4965e76c94cf34a5d66d455c7ace25dad4 nereus-core/src/main/java/com/nereusstream/core/read/ParquetV2CompactedTargetReader.java
e92e604b377ea8a481d5bece22600073c6fc0235 nereus-core/src/main/java/com/nereusstream/core/capability/PhysicalFormatCapabilityRegistry.java
6ee7fb5ffcc0b1ff6c9d669ab16b69403b45e9f4 nereus-object-store/src/main/java/com/nereusstream/objectstore/wal/DefaultWalObjectReader.java
c9334acbdd8e01c3a4505cf3b12c6c9cdfbe755e nereus-object-store/src/main/java/com/nereusstream/objectstore/compacted/CompactedObjectFormatV2.java
327e835599992f579373c342487af77c26b1ed1a nereus-object-store/src/main/java/com/nereusstream/objectstore/compacted/ParquetRangedCompactedObjectWriter.java
99636d5b397088ed9f59ff359964901664c8f3f7 nereus-object-store/src/main/java/com/nereusstream/objectstore/compacted/ParquetRangedCompactedObjectReader.java
187cfd5b7210c106e39601d0636837b191a32014 nereus-object-store/src/main/java/com/nereusstream/objectstore/compacted/ParquetKafkaTopicCompactedWriter.java
a6e9a0ffe576c88bebd67765864509df5d6680d5 nereus-object-store/src/main/java/com/nereusstream/objectstore/compacted/ParquetKafkaTopicCompactedReader.java
92871c04d0e767f5252038b142aeab32f1fac18d nereus-bookkeeper/src/main/java/com/nereusstream/bookkeeper/BookKeeperPrimaryWalReader.java
dd6233c9a084b4473ba9bc9b1c77fbbf85511c75 nereus-bookkeeper/src/main/java/com/nereusstream/bookkeeper/BookKeeperRangedEntryCodecV1.java
f4e35b83ffeb0fb630a9b45893f2f9630608dc3e nereus-materialization/src/main/java/com/nereusstream/materialization/RangedLosslessMaterializationRowPublisher.java
LOCKS

echo "F9 source lock: AutoMQ and 23 Nereus ranged/head-foundation sources match"
