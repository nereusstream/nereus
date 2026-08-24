#!/usr/bin/env bash
set -euo pipefail
export LC_ALL=C
export LANG=C

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pulsar_checkout="${1:-/Users/liusinan/apps/ideaproject/nereusstream/pulsar-worktrees/nereus-v2-m3}"
oxia_server_checkout="${2:-/Users/liusinan/apps/ideaproject/nereusstream/oxia-worktrees/nereus-v2-m3}"
oxia_client_checkout="${3:-/Users/liusinan/apps/ideaproject/nereusstream/oxia-client-java-worktrees/nereus-v2-m3}"
output="${NEREUS_M3_ALLOCATOR_PREFLIGHT_OUTPUT:-$repo_root/nereus-metadata-oxia/build/m3-allocator-evidence/preflight.json}"
allow_dirty="${NEREUS_M3_ALLOCATOR_PREFLIGHT_ALLOW_DIRTY:-false}"
oxia_image="${NEREUS_M3_ALLOCATOR_OXIA_IMAGE:-nereus/oxia-m3-allocator:37a17bef1720}"
expected_nereus="${NEREUS_M3_ALLOCATOR_EXPECTED_NEREUS_COMMIT:-}"

expected_pulsar="7ff908330809f2e9bc5c69ead87bb85c566bc0a9"
expected_oxia_server="37a17bef17202d5fd6e23282da5fd26d94865484"
expected_oxia_client="091a42c2780d92da56e9ec1f02ce1c3d988adc16"
expected_oxia_server_published_ref="refs/heads/main"
expected_oxia_client_published_ref="refs/heads/nereus/v2-m1.1a-o1-notification-continuity"
expected_oxia_client_jar_sha="0ca719e6d11bd2ee2c2e7e94b42c6843e60f776bea12f7b5814cff9928e2e4c5"
expected_oxia_image="nereus/oxia-m3-allocator:37a17bef1720"
expected_oxia_image_id="sha256:7eef9af2cdc897fbf418bf7616da1387aca87ce860b8205395cdf88b867df4da"
expected_oxia_image_recipe="NEREUS_V2_M3_ALLOCATOR_OXIA_IMAGE_V1"
oxia_image_dockerfile="$repo_root/scripts/containers/oxia-v2-m3-allocator.Dockerfile"
client_jar="$repo_root/gradle/locked-artifacts/oxia-client-java/$expected_oxia_client/m2/io/github/oxia-db/oxia-client/0.9.4/oxia-client-0.9.4.jar"
docker_settings="$HOME/Library/Group Containers/group.com.docker/settings.json"

test ! -e "$output"
test -f "$repo_root/docs/v2/source-locks.json"
test -f "$pulsar_checkout/settings.gradle.kts"
test -f "$oxia_server_checkout/Dockerfile"
test -f "$oxia_image_dockerfile"
test -f "$client_jar"
command -v docker >/dev/null
command -v git >/dev/null
command -v jq >/dev/null

nereus_commit="$(git -C "$repo_root" rev-parse HEAD)"
nereus_branch="$(git -C "$repo_root" branch --show-current)"
nereus_origin_main="$(git -C "$repo_root" rev-parse refs/remotes/origin/main)"
nereus_remote_main="$(git -C "$repo_root" ls-remote --heads origin refs/heads/main | awk 'NR == 1 { print $1 }')"
pulsar_commit="$(git -C "$pulsar_checkout" rev-parse HEAD)"
pulsar_branch="$(git -C "$pulsar_checkout" branch --show-current)"
pulsar_origin_branch="$(git -C "$pulsar_checkout" rev-parse refs/remotes/origin/nereus/v2-m3-object-wal-evidence)"
pulsar_remote_branch="$(git -C "$pulsar_checkout" ls-remote --heads origin \
  refs/heads/nereus/v2-m3-object-wal-evidence | awk 'NR == 1 { print $1 }')"
oxia_server_commit="$(git -C "$oxia_server_checkout" rev-parse HEAD)"
oxia_server_branch="$(git -C "$oxia_server_checkout" branch --show-current)"
oxia_server_origin_ref="$(git -C "$oxia_server_checkout" rev-parse refs/remotes/origin/main)"
oxia_server_remote_ref="$(git -C "$oxia_server_checkout" ls-remote --heads origin \
  refs/heads/main | awk 'NR == 1 { print $1 }')"
oxia_client_commit="$(git -C "$oxia_client_checkout" rev-parse HEAD)"
oxia_client_branch="$(git -C "$oxia_client_checkout" branch --show-current)"
oxia_client_origin_ref="$(git -C "$oxia_client_checkout" \
  rev-parse refs/remotes/origin/nereus/v2-m1.1a-o1-notification-continuity)"
oxia_client_remote_ref="$(git -C "$oxia_client_checkout" ls-remote --heads origin \
  refs/heads/nereus/v2-m1.1a-o1-notification-continuity | awk 'NR == 1 { print $1 }')"
nereus_dirty="$(git -C "$repo_root" status --porcelain --untracked-files=all)"
pulsar_dirty="$(git -C "$pulsar_checkout" status --porcelain --untracked-files=all)"
oxia_server_dirty="$(git -C "$oxia_server_checkout" status --porcelain --untracked-files=all)"
oxia_client_dirty="$(git -C "$oxia_client_checkout" status --porcelain --untracked-files=all)"

# Formal admission rejects source drift before inspecting/running any image or starting Gradle.
if test "$allow_dirty" != true; then
  test -n "$expected_nereus"
  test "$nereus_commit" = "$expected_nereus"
  test "$nereus_branch" = main
  test "$nereus_origin_main" = "$expected_nereus"
  test "$nereus_remote_main" = "$expected_nereus"
  test -z "$nereus_dirty"
  test "$pulsar_commit" = "$expected_pulsar"
  test "$pulsar_branch" = nereus/v2-m3-object-wal-evidence
  test "$pulsar_origin_branch" = "$expected_pulsar"
  test "$pulsar_remote_branch" = "$expected_pulsar"
  test -z "$pulsar_dirty"
  test "$oxia_server_commit" = "$expected_oxia_server"
  test "$oxia_server_branch" = nereus/v2-m3-object-wal-evidence
  test "$oxia_server_origin_ref" = "$expected_oxia_server"
  test "$oxia_server_remote_ref" = "$expected_oxia_server"
  test -z "$oxia_server_dirty"
  test "$oxia_client_commit" = "$expected_oxia_client"
  test "$oxia_client_branch" = nereus/v2-m3-object-wal-evidence
  test "$oxia_client_origin_ref" = "$expected_oxia_client"
  test "$oxia_client_remote_ref" = "$expected_oxia_client"
  test -z "$oxia_client_dirty"
fi

source_locks_sha="$(shasum -a 256 "$repo_root/docs/v2/source-locks.json" | awk '{print $1}')"
client_jar_sha="$(shasum -a 256 "$client_jar" | awk '{print $1}')"
dockerfile_sha="$(shasum -a 256 "$oxia_image_dockerfile" | awk '{print $1}')"
build_script_sha="$(shasum -a 256 "$repo_root/scripts/build-v2-m3-allocator-oxia-image.sh" | awk '{print $1}')"
image_id="$(docker image inspect "$oxia_image" --format '{{.Id}}')"
image_revision="$(docker image inspect "$oxia_image" --format '{{index .Config.Labels "org.opencontainers.image.revision"}}')"
image_source="$(docker image inspect "$oxia_image" --format '{{index .Config.Labels "org.opencontainers.image.source"}}')"
image_recipe="$(docker image inspect "$oxia_image" \
  --format '{{index .Config.Labels "com.nereusstream.evidence.recipe"}}')"
image_platform="$(docker image inspect "$oxia_image" --format '{{.Os}}/{{.Architecture}}')"
image_size="$(docker image inspect "$oxia_image" --format '{{.Size}}')"
image_repo_digests="$(docker image inspect "$oxia_image" --format '{{json .RepoDigests}}')"
oxia_binary_version="$(docker run --rm "$oxia_image" /oxia/bin/oxia --version | tr -d '\r')"

docker_cpus="$(docker info --format '{{.NCPU}}')"
docker_memory_bytes="$(docker info --format '{{.MemTotal}}')"
docker_server_version="$(docker info --format '{{.ServerVersion}}')"
docker_os_arch="$(docker info --format '{{.OSType}}/{{.Architecture}}')"
configured_docker_cpus="$docker_cpus"
configured_docker_memory_mib="$((docker_memory_bytes / 1024 / 1024))"
if test -f "$docker_settings"; then
  configured_docker_cpus="$(jq -r '.cpus' "$docker_settings")"
  configured_docker_memory_mib="$(jq -r '.memoryMiB' "$docker_settings")"
fi

jdk_runtime="$(java -version 2>&1 | tr '\n' ' ' | sed 's/[[:space:]]*$//')"
host_os="$(uname -srv)"
host_arch="$(uname -m)"
host_cpu_model="$(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -p)"
host_cpu_frequency_hz="$(sysctl -n hw.cpufrequency 2>/dev/null || true)"
if test -z "$host_cpu_frequency_hz"; then
  host_cpu_frequency_hz="UNAVAILABLE_APPLE_SILICON"
fi
storage_model="$(diskutil info / 2>/dev/null | awk -F: '/Device \/ Media Name|Solid State|Protocol/ {gsub(/^[[:space:]]+/, "", $2); printf "%s=%s;", $1, $2}')"
if test -z "$storage_model"; then
  storage_model="UNAVAILABLE"
fi

blockers=()
if test -z "$expected_nereus"; then
  blockers+=("EXPECTED_NEREUS_COMMIT_UNSET")
elif test "$nereus_commit" != "$expected_nereus"; then
  blockers+=("NEREUS_COMMIT_MISMATCH")
fi
test "$nereus_branch" = main || blockers+=("NEREUS_BRANCH_NOT_MAIN")
test "$nereus_origin_main" = "$nereus_commit" || blockers+=("NEREUS_ORIGIN_MAIN_MISMATCH")
test "$nereus_remote_main" = "$nereus_commit" || blockers+=("NEREUS_REMOTE_MAIN_MISMATCH")
test "$pulsar_commit" = "$expected_pulsar" || blockers+=("PULSAR_COMMIT_MISMATCH")
test "$pulsar_branch" = nereus/v2-m3-object-wal-evidence || blockers+=("PULSAR_BRANCH_MISMATCH")
test "$pulsar_origin_branch" = "$pulsar_commit" || blockers+=("PULSAR_ORIGIN_BRANCH_MISMATCH")
test "$pulsar_remote_branch" = "$pulsar_commit" || blockers+=("PULSAR_REMOTE_BRANCH_MISMATCH")
test "$oxia_server_commit" = "$expected_oxia_server" || blockers+=("OXIA_SERVER_COMMIT_MISMATCH")
test "$oxia_server_branch" = nereus/v2-m3-object-wal-evidence || blockers+=("OXIA_SERVER_BRANCH_MISMATCH")
test "$oxia_server_origin_ref" = "$oxia_server_commit" || blockers+=("OXIA_SERVER_ORIGIN_REF_MISMATCH")
test "$oxia_server_remote_ref" = "$oxia_server_commit" || blockers+=("OXIA_SERVER_REMOTE_REF_MISMATCH")
test "$oxia_client_commit" = "$expected_oxia_client" || blockers+=("OXIA_CLIENT_COMMIT_MISMATCH")
test "$oxia_client_branch" = nereus/v2-m3-object-wal-evidence || blockers+=("OXIA_CLIENT_BRANCH_MISMATCH")
test "$oxia_client_origin_ref" = "$oxia_client_commit" || blockers+=("OXIA_CLIENT_ORIGIN_REF_MISMATCH")
test "$oxia_client_remote_ref" = "$oxia_client_commit" || blockers+=("OXIA_CLIENT_REMOTE_REF_MISMATCH")
git -C "$oxia_client_checkout" cat-file -e "$expected_oxia_client^{commit}" \
  || blockers+=("LOCKED_OXIA_CLIENT_COMMIT_UNAVAILABLE")
test "$client_jar_sha" = "$expected_oxia_client_jar_sha" || blockers+=("OXIA_CLIENT_JAR_SHA_MISMATCH")
test -z "$pulsar_dirty" || blockers+=("PULSAR_TREE_DIRTY")
test -z "$oxia_server_dirty" || blockers+=("OXIA_SERVER_TREE_DIRTY")
test -z "$oxia_client_dirty" || blockers+=("OXIA_CLIENT_TREE_DIRTY")
test "$image_revision" = "$expected_oxia_server" || blockers+=("OXIA_IMAGE_REVISION_LABEL_MISMATCH")
test "$image_source" = "https://github.com/oxia-db/oxia.git" || blockers+=("OXIA_IMAGE_SOURCE_LABEL_MISMATCH")
test "$image_recipe" = "$expected_oxia_image_recipe" || blockers+=("OXIA_IMAGE_RECIPE_LABEL_MISMATCH")
test "$oxia_image" = "$expected_oxia_image" || blockers+=("OXIA_IMAGE_REFERENCE_MISMATCH")
test "$image_id" = "$expected_oxia_image_id" || blockers+=("OXIA_IMAGE_CONFIG_DIGEST_MISMATCH")
test "$image_platform" = "linux/arm64" || blockers+=("OXIA_IMAGE_PLATFORM_MISMATCH")
test "$oxia_binary_version" = "oxia version 0.16.3-167-g37a17bef" || blockers+=("OXIA_BINARY_VERSION_MISMATCH")
test "$configured_docker_cpus" -ge 8 || blockers+=("DOCKER_CPU_ENVELOPE_BELOW_8")
test "$configured_docker_memory_mib" -ge 16384 || blockers+=("DOCKER_MEMORY_ENVELOPE_BELOW_16_GIB")
if test -n "$nereus_dirty"; then
  blockers+=("NEREUS_TREE_DIRTY")
fi
jq -e --arg commit "$expected_pulsar" '.m3AllocatorEvidenceBinding.pulsarSourceCommit == $commit' \
  "$repo_root/docs/v2/source-locks.json" >/dev/null || blockers+=("PULSAR_SOURCE_LOCK_MISMATCH")
jq -e --arg commit "$expected_oxia_client" '.m3AllocatorEvidenceBinding.oxiaClientSourceCommit == $commit' \
  "$repo_root/docs/v2/source-locks.json" >/dev/null || blockers+=("OXIA_CLIENT_SOURCE_LOCK_MISMATCH")
jq -e --arg commit "$expected_oxia_server" '.m3AllocatorEvidenceBinding.oxiaServerSourceCommit == $commit' \
  "$repo_root/docs/v2/source-locks.json" >/dev/null || blockers+=("OXIA_SERVER_SOURCE_LOCK_MISMATCH")
jq -e --arg ref "$expected_oxia_client_published_ref" \
  '.m3AllocatorEvidenceBinding.oxiaClientPublishedRef == $ref' \
  "$repo_root/docs/v2/source-locks.json" >/dev/null || blockers+=("OXIA_CLIENT_PUBLISHED_REF_LOCK_MISMATCH")
jq -e --arg ref "$expected_oxia_server_published_ref" \
  '.m3AllocatorEvidenceBinding.oxiaServerPublishedRef == $ref' \
  "$repo_root/docs/v2/source-locks.json" >/dev/null || blockers+=("OXIA_SERVER_PUBLISHED_REF_LOCK_MISMATCH")
jq -e --arg reference "$expected_oxia_image" '.m3AllocatorEvidenceBinding.oxiaServerImageReference == $reference' \
  "$repo_root/docs/v2/source-locks.json" >/dev/null || blockers+=("OXIA_IMAGE_REFERENCE_SOURCE_LOCK_MISMATCH")
jq -e --arg digest "$expected_oxia_image_id" '.m3AllocatorEvidenceBinding.oxiaServerImageDigest == $digest' \
  "$repo_root/docs/v2/source-locks.json" >/dev/null || blockers+=("OXIA_IMAGE_DIGEST_SOURCE_LOCK_MISMATCH")
jq -e --arg digest "$dockerfile_sha" '.m3AllocatorEvidenceBinding.oxiaImageRecipeSha256 == $digest' \
  "$repo_root/docs/v2/source-locks.json" >/dev/null || blockers+=("OXIA_IMAGE_RECIPE_SOURCE_LOCK_MISMATCH")
jq -e --arg digest "$build_script_sha" '.m3AllocatorEvidenceBinding.oxiaImageBuildScriptSha256 == $digest' \
  "$repo_root/docs/v2/source-locks.json" >/dev/null || blockers+=("OXIA_IMAGE_BUILD_SCRIPT_SOURCE_LOCK_MISMATCH")

blockers_json="$(printf '%s\n' "${blockers[@]:-}" | jq -Rsc 'split("\n") | map(select(length > 0))')"
admitted=true
if test "$(jq 'length' <<<"$blockers_json")" -ne 0; then
  admitted=false
fi
status="ADMITTED_FORMAL_PRE_RUN"
if test "$admitted" != true; then
  status="NON_PROMOTABLE_PRE_RUN"
fi

mkdir -p "$(dirname "$output")"
jq -n \
  --arg schema "NEREUS_V2_M3_ALLOCATOR_PREFLIGHT_V1" \
  --arg status "$status" \
  --argjson admitted "$admitted" \
  --arg nereusCommit "$nereus_commit" \
  --arg nereusBranch "$nereus_branch" \
  --arg nereusOriginMain "$nereus_origin_main" \
  --arg nereusRemoteMain "$nereus_remote_main" \
  --arg nereusTreeState "$(test -z "$nereus_dirty" && printf CLEAN || printf DIRTY)" \
  --arg pulsarCommit "$pulsar_commit" \
  --arg pulsarBranch "$pulsar_branch" \
  --arg pulsarOriginBranch "$pulsar_origin_branch" \
  --arg pulsarRemoteBranch "$pulsar_remote_branch" \
  --arg pulsarTreeState "$(test -z "$pulsar_dirty" && printf CLEAN || printf DIRTY)" \
  --arg oxiaClientCommit "$expected_oxia_client" \
  --arg oxiaClientCheckoutObservedCommit "$oxia_client_commit" \
  --arg oxiaClientCheckoutBranch "$oxia_client_branch" \
  --arg oxiaClientOriginPublishedRef "$oxia_client_origin_ref" \
  --arg oxiaClientRemotePublishedRef "$oxia_client_remote_ref" \
  --arg oxiaClientCheckoutTreeState "$(test -z "$oxia_client_dirty" && printf CLEAN || printf DIRTY)" \
  --arg oxiaClientJarSha256 "$client_jar_sha" \
  --arg oxiaServerCommit "$oxia_server_commit" \
  --arg oxiaServerBranch "$oxia_server_branch" \
  --arg oxiaServerOriginPublishedRef "$oxia_server_origin_ref" \
  --arg oxiaServerRemotePublishedRef "$oxia_server_remote_ref" \
  --arg oxiaServerTreeState "$(test -z "$oxia_server_dirty" && printf CLEAN || printf DIRTY)" \
  --arg sourceLocksSha256 "$source_locks_sha" \
  --arg imageReference "$oxia_image" \
  --arg imageId "$image_id" \
  --arg configDigest "$image_id" \
  --argjson repoDigests "$image_repo_digests" \
  --arg platform "$image_platform" \
  --arg revisionLabel "$image_revision" \
  --arg sourceLabel "$image_source" \
  --arg recipeLabel "$image_recipe" \
  --arg binaryVersion "$oxia_binary_version" \
  --arg dockerfileSha256 "$dockerfile_sha" \
  --arg buildScriptSha256 "$build_script_sha" \
  --arg buildCommand "scripts/build-v2-m3-allocator-oxia-image.sh <dedicated-oxia-worktree>" \
  --argjson imageSizeBytes "$image_size" \
  --arg jdkRuntime "$jdk_runtime" \
  --arg hostOs "$host_os" \
  --arg hostArch "$host_arch" \
  --arg hostCpuModel "$host_cpu_model" \
  --arg hostCpuFrequencyHz "$host_cpu_frequency_hz" \
  --arg storageModel "$storage_model" \
  --arg dockerServerVersion "$docker_server_version" \
  --arg dockerOsArch "$docker_os_arch" \
  --argjson dockerConfiguredCpus "$configured_docker_cpus" \
  --argjson dockerConfiguredMemoryMiB "$configured_docker_memory_mib" \
  --argjson dockerVisibleCpus "$docker_cpus" \
  --argjson dockerVisibleMemoryBytes "$docker_memory_bytes" \
  --argjson blockers "$blockers_json" \
  '{
    schema: $schema,
    status: $status,
    admitted: $admitted,
    blockers: $blockers,
    source: {
      nereusCommit: $nereusCommit,
      nereusBranch: $nereusBranch,
      nereusOriginMain: $nereusOriginMain,
      nereusRemoteMain: $nereusRemoteMain,
      nereusTreeState: $nereusTreeState,
      pulsarCommit: $pulsarCommit,
      pulsarBranch: $pulsarBranch,
      pulsarOriginBranch: $pulsarOriginBranch,
      pulsarRemoteBranch: $pulsarRemoteBranch,
      pulsarTreeState: $pulsarTreeState,
      oxiaClientCommit: $oxiaClientCommit,
      oxiaClientCheckoutObservedCommit: $oxiaClientCheckoutObservedCommit,
      oxiaClientCheckoutBranch: $oxiaClientCheckoutBranch,
      oxiaClientPublishedRef: "refs/heads/nereus/v2-m1.1a-o1-notification-continuity",
      oxiaClientOriginPublishedCommit: $oxiaClientOriginPublishedRef,
      oxiaClientRemotePublishedCommit: $oxiaClientRemotePublishedRef,
      oxiaClientCheckoutTreeState: $oxiaClientCheckoutTreeState,
      oxiaClientJarSha256: $oxiaClientJarSha256,
      oxiaServerCommit: $oxiaServerCommit,
      oxiaServerBranch: $oxiaServerBranch,
      oxiaServerPublishedRef: "refs/heads/main",
      oxiaServerOriginPublishedCommit: $oxiaServerOriginPublishedRef,
      oxiaServerRemotePublishedCommit: $oxiaServerRemotePublishedRef,
      oxiaServerTreeState: $oxiaServerTreeState,
      sourceLocksSha256: $sourceLocksSha256
    },
    oxiaImage: {
      reference: $imageReference,
      imageId: $imageId,
      configDigest: $configDigest,
      repoDigests: $repoDigests,
      platform: $platform,
      revisionLabel: $revisionLabel,
      sourceLabel: $sourceLabel,
      recipeLabel: $recipeLabel,
      binaryVersion: $binaryVersion,
      dockerfileSha256: $dockerfileSha256,
      buildScriptSha256: $buildScriptSha256,
      buildCommand: $buildCommand,
      imageSizeBytes: $imageSizeBytes,
      historicalM1ImageReplacement: false,
      independentlyVersionedM3EvidenceImage: true
    },
    executor: {
      jdkRuntime: $jdkRuntime,
      allocatorHeapMiB: 6144,
      allocatorWorkerThreads: 96,
      oxiaShards: 4,
      brokerActors: 4,
      hostOs: $hostOs,
      hostArch: $hostArch,
      hostCpuModel: $hostCpuModel,
      hostCpuFrequencyHz: $hostCpuFrequencyHz,
      storageModel: $storageModel,
      dockerServerVersion: $dockerServerVersion,
      dockerOsArch: $dockerOsArch,
      dockerConfiguredCpus: $dockerConfiguredCpus,
      dockerConfiguredMemoryMiB: $dockerConfiguredMemoryMiB,
      dockerVisibleCpus: $dockerVisibleCpus,
      dockerVisibleMemoryBytes: $dockerVisibleMemoryBytes
    }
  }' >"$output"

shasum -a 256 "$output"
if test "$admitted" != true && test "$allow_dirty" != true; then
  exit 1
fi
