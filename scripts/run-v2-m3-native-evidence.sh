#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

fail() {
    echo "V2 M3 native evidence: $*" >&2
    exit 1
}

[[ $# -eq 4 ]] || fail \
    "usage: $0 <tested-commit> <kafka-worktree> <pulsar-worktree> <fresh-output-directory>"

tested_commit="$1"
kafka_worktree="$2"
pulsar_worktree="$3"
output_directory="$4"

[[ "$tested_commit" =~ ^[0-9a-f]{40}$ ]] || fail "tested commit is not canonical"
[[ -d "$kafka_worktree" && -f "$kafka_worktree/.git" && ! -L "$kafka_worktree/.git" ]] ||
    fail "Kafka input must be a dedicated linked worktree"
[[ -d "$pulsar_worktree" && -f "$pulsar_worktree/.git" && ! -L "$pulsar_worktree/.git" ]] ||
    fail "Pulsar input must be a dedicated linked worktree"

cd "$repo_root"
[[ "$(git rev-parse --show-toplevel)" == "$repo_root" ]] || fail "repository root differs"
[[ "$(git rev-parse HEAD)" == "$tested_commit" ]] || fail "HEAD differs from tested commit"
[[ "$(git rev-parse origin/main)" == "$tested_commit" ]] || fail "origin/main differs from tested commit"
[[ "$(git ls-remote --heads origin refs/heads/main | awk 'NR == 1 { print $1 }')" == "$tested_commit" ]] ||
    fail "remote main differs from tested commit"
[[ -z "$(git status --porcelain=v1 --untracked-files=all)" ]] || fail "Nereus source tree is not clean"

lock_decision="$(jq -er '.m3NativeEvidenceBindings.decision' docs/v2/source-locks.json)"
lock_result="$(jq -er '.m3NativeEvidenceBindings.result' docs/v2/source-locks.json)"
lock_status="$(jq -er '.m3NativeEvidenceBindings.evidenceStatus' docs/v2/source-locks.json)"
lock_promotable="$(jq -er '.m3NativeEvidenceBindings.promotionEligible' docs/v2/source-locks.json)"
[[ "$lock_decision" == "docs/decisions/0099-v2-m3-native-and-allocator-source-lock-separation.md" ]] ||
    fail "M3 native decision lock differs"
[[ "$lock_result" == "ACCEPTED_M3_NATIVE_EVIDENCE_INPUT_ONLY" ]] || fail "M3 native result lock differs"
[[ "$lock_status" == "ACCEPTED_INPUT_ONLY" && "$lock_promotable" == "false" ]] ||
    fail "M3 native input-only boundary differs"

verify_external_worktree() {
    local name="$1"
    local worktree="$2"
    local expected_repository expected_branch expected_commit
    expected_repository="$(jq -er ".m3NativeEvidenceBindings.${name}.repository" docs/v2/source-locks.json)"
    expected_branch="$(jq -er ".m3NativeEvidenceBindings.${name}.branch" docs/v2/source-locks.json)"
    expected_commit="$(jq -er ".m3NativeEvidenceBindings.${name}.sourceCommit" docs/v2/source-locks.json)"

    [[ "$(git -C "$worktree" rev-parse --show-toplevel)" == "$(cd "$worktree" && pwd)" ]] ||
        fail "$name worktree root differs"
    [[ "$(git -C "$worktree" branch --show-current)" == "$expected_branch" ]] ||
        fail "$name worktree branch differs"
    [[ "$(git -C "$worktree" rev-parse HEAD)" == "$expected_commit" ]] ||
        fail "$name worktree commit differs"
    [[ "$(git -C "$worktree" remote get-url origin)" == "$expected_repository" ]] ||
        fail "$name origin differs"
    [[ -z "$(git -C "$worktree" status --porcelain=v1 --untracked-files=all)" ]] ||
        fail "$name worktree is not clean"
    [[ "$(git -C "$worktree" rev-parse "refs/remotes/origin/$expected_branch")" == "$expected_commit" ]] ||
        fail "$name tracking ref differs"
    local remote_commit
    remote_commit="$(
        git -C "$worktree" ls-remote --heads origin "refs/heads/$expected_branch" |
            awk 'NR == 1 { print $1 }'
    )"
    [[ "$remote_commit" == "$expected_commit" ]] || fail "$name remote branch differs"
}

verify_external_worktree kafka "$kafka_worktree"
verify_external_worktree pulsar "$pulsar_worktree"

output_parent="$(dirname "$output_directory")"
[[ -d "$output_parent" && ! -L "$output_parent" ]] || fail "output parent is absent or a symlink"
output_parent="$(cd "$output_parent" && pwd -P)"
output_directory="$output_parent/$(basename "$output_directory")"
case "$output_directory" in
    "$repo_root" | "$repo_root"/*) fail "native evidence output must be outside the repository" ;;
esac
[[ ! -e "$output_directory" && ! -L "$output_directory" ]] || fail "fresh output directory already exists"
mkdir "$output_directory"

gradle_common=(
    ./gradlew
    --no-daemon
    --no-configuration-cache
    --no-parallel
    --console=plain
    "-PpulsarCheckout=$pulsar_worktree"
)

run_component() {
    local name="$1"
    local module="$2"
    local child_kind="$3"
    local logical_repository="$4"
    local source_commit="$5"
    local emit_task="$6"
    local check_task="$7"
    shift 7
    local native_paths=("$@")
    local raw="$output_directory/$name-raw.json"
    local sealed_native="$output_directory/$name-native-sealed.json"
    local sealed_junit="$output_directory/$name-junit-sealed.json"

    "${gradle_common[@]}" ":$module:cleanTest"
    local started_at finished_at
    started_at="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    "${gradle_common[@]}" --rerun-tasks \
        ":$module:test" ":$module:spotlessCheck" ":$module:checkstyleMain" ":$module:checkstyleTest"
    finished_at="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"

    local property_prefix
    if [[ "$name" == "kafka" ]]; then
        property_prefix="v2M3Kafka"
    else
        property_prefix="v2M3Pulsar"
    fi
    "${gradle_common[@]}" ":$module:$emit_task" \
        "-P${property_prefix}NativeReceiptOutput=$raw" \
        "-P${property_prefix}TestedSourceCommit=$tested_commit" \
        "-P${property_prefix}SourceRepository=$logical_repository" \
        "-P${property_prefix}SourceCommit=$source_commit" \
        "-P${property_prefix}TestStartedAtUtc=$started_at" \
        "-P${property_prefix}TestFinishedAtUtc=$finished_at"
    "${gradle_common[@]}" ":$module:$check_task" "-P${property_prefix}NativeReceiptInput=$raw"

    local native_args=()
    local path
    for path in "${native_paths[@]}"; do
        native_args+=(--native-junit-xml "$path=$repo_root/$path")
    done
    python3 scripts/publish-v2-m3-child.py \
        --repo-root "$repo_root" \
        --tested-commit "$tested_commit" \
        --seal-native-kind "$child_kind" \
        --raw-evidence "$raw" \
        "${native_args[@]}" \
        --sealed-output "$sealed_native"

    local sorted_paths=()
    while IFS= read -r path; do
        sorted_paths+=("$path")
    done < <(printf '%s\n' "${native_paths[@]}" | LC_ALL=C sort)
    local junit_args=()
    for path in "${sorted_paths[@]}"; do
        junit_args+=(--child-junit-xml "$path=$repo_root/$path")
    done
    python3 scripts/publish-v2-m3-child.py \
        --repo-root "$repo_root" \
        --tested-commit "$tested_commit" \
        --seal-junit-kind "$child_kind" \
        "${junit_args[@]}" \
        --sealed-output "$sealed_junit"

    for path in "$raw" "$sealed_native" "$sealed_junit"; do
        [[ -s "$path" && ! -L "$path" ]] || fail "$name output is absent, empty, or a symlink: $path"
        echo "V2 M3 native evidence output: $(basename "$path") bytes=$(stat -f '%z' "$path") sha256=$(shasum -a 256 "$path" | awk '{print $1}')"
    done
}

kafka_logical_repository="$(jq -er '.m3NativeEvidenceBindings.kafka.logicalRepository' docs/v2/source-locks.json)"
kafka_source_commit="$(jq -er '.m3NativeEvidenceBindings.kafka.sourceCommit' docs/v2/source-locks.json)"
pulsar_logical_repository="$(jq -er '.m3NativeEvidenceBindings.pulsar.logicalRepository' docs/v2/source-locks.json)"
pulsar_source_commit="$(jq -er '.m3NativeEvidenceBindings.pulsar.sourceCommit' docs/v2/source-locks.json)"

run_component \
    kafka \
    nereus-kafka-bookkeeper \
    U_KAFKA_OBJECT_WAL \
    "$kafka_logical_repository" \
    "$kafka_source_commit" \
    v2M3KafkaNativeReceiptEmit \
    v2M3KafkaNativeReceiptCheck \
    nereus-kafka-bookkeeper/build/test-results/test/TEST-com.nereusstream.kafka.bookkeeper.object.nwkcp1.Nwkcp1CodecV1Test.xml \
    nereus-kafka-bookkeeper/build/test-results/test/TEST-com.nereusstream.kafka.bookkeeper.object.nwkcp1.ObjectKafkaProtocolCheckpointStoreV1Test.xml \
    nereus-kafka-bookkeeper/build/test-results/test/TEST-com.nereusstream.kafka.bookkeeper.object.nwkcp1.StorageObjectNwkcp1BackendV1Test.xml \
    nereus-kafka-bookkeeper/build/test-results/test/TEST-com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectPublicationBridgeV1Test.xml \
    nereus-kafka-bookkeeper/build/test-results/test/TEST-com.nereusstream.kafka.bookkeeper.object.evidence.KafkaObjectWalNativeResultV1Test.xml

run_component \
    pulsar \
    nereus-pulsar-offload \
    P_PULSAR_OBJECT_WAL \
    "$pulsar_logical_repository" \
    "$pulsar_source_commit" \
    v2M3PulsarNativeReceiptEmit \
    v2M3PulsarNativeReceiptCheck \
    nereus-pulsar-offload/build/test-results/test/TEST-com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1Test.xml \
    nereus-pulsar-offload/build/test-results/test/TEST-com.nereusstream.pulsar.offload.objectwal.PulsarVirtualLedgerChainControllerV1Test.xml

verify_external_worktree kafka "$kafka_worktree"
verify_external_worktree pulsar "$pulsar_worktree"
[[ "$(git rev-parse HEAD)" == "$tested_commit" && "$(git rev-parse origin/main)" == "$tested_commit" ]] ||
    fail "Nereus source changed during native evidence execution"
[[ -z "$(git status --porcelain=v1 --untracked-files=all)" ]] ||
    fail "Nereus source tree changed during native evidence execution"

echo "V2 M3 native evidence PASS: tested=$tested_commit output=$output_directory"
