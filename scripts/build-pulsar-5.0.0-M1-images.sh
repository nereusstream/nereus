#!/usr/bin/env bash

set -euo pipefail

readonly APACHE_PULSAR_SHA="8dae0236c0a0d405ed7f8303081080520fe91551"
readonly NEREUS_PULSAR_SHA="5ffc2caa0e08dac95bc8c2ea76ed3d32382dfe3e"
readonly NEREUS_SOURCE_SHA="81a1fa83e9aa4275229226cb895c72a6ea20ca87"
readonly NEREUS_RUNTIME_SHA="18cb06e8cd39454d8263b0134da980ad6aef6a31"
readonly NEREUS_ADAPTER_SHA256="7a913651ba3701bf0fce718d4fa7ef98b6767dfc5886cfa0865380756d0f7724"
readonly PULSAR_VERSION="5.0.0-M1"
readonly NEREUS_DEVELOPMENT_VERSION="0.1.0-f2-dev"
readonly PULSAR_CLIENT_PYTHON_VERSION="3.12.0"
readonly SNAPPY_VERSION="1.1.10.8"
readonly IMAGE_JDK_MAJOR_VERSION="21"

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
nereus_repo="$(cd "${script_dir}/.." && pwd)"
pulsar_repo=""
worktree_root=""
output_dir=""
image_repository="nereus-benchmark/pulsar"
containerd_namespace="${CONTAINERD_NAMESPACE:-k8s.io}"
containerd_address="${CONTAINERD_ADDRESS:-}"
nerdctl_bin="${NERDCTL_BIN:-nerdctl}"
target_platform=""
push_images=false
sudo_nerdctl=false

usage() {
    cat <<'EOF'
Build the frozen Apache and Nereus Pulsar 5.0.0-M1 benchmark images with nerdctl.

Usage:
  build-pulsar-5.0.0-M1-images.sh --pulsar-repo PATH [options]

Required:
  --pulsar-repo PATH       Pulsar Git clone containing both frozen commits.

Options:
  --nereus-repo PATH       Nereus v0.1.0 checkout (default: repository containing this script).
  --worktree-root PATH     Pulsar worktree parent (default: PULSAR_REPO/../pulsar-worktrees).
  --output-dir PATH        Generated manifest/inspection files (default: NEREUS_REPO/build/performance-images).
  --image-repository NAME  Repository prefix (default: nereus-benchmark/pulsar).
  --platform PLATFORM      Build platform (default: native linux/amd64 or linux/arm64).
  --namespace NAME         containerd namespace (default: k8s.io).
  --address PATH           containerd socket/address (default: nerdctl default).
  --nerdctl PATH           nerdctl executable (default: nerdctl).
  --sudo-nerdctl           Run only nerdctl commands through sudo.
  --push                   Push both tags after building; log in to the registry first.
  -h, --help               Show this help.

nerdctl build requires a running BuildKit daemon. Images are built directly into the
k8s.io containerd namespace by default, so a single-node Kubernetes installation can
use them with imagePullPolicy: Never. For a multi-node cluster, push to a registry or
save/load the resulting archive on every eligible node.
EOF
}

die() {
    echo "error: $*" >&2
    exit 1
}

require_command() {
    command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

native_platform() {
    case "$(uname -m)" in
        x86_64 | amd64)
            echo "linux/amd64"
            ;;
        aarch64 | arm64)
            echo "linux/arm64"
            ;;
        *)
            die "unsupported native architecture: $(uname -m); pass --platform explicitly"
            ;;
    esac
}

sha256_file() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{print $1}'
    elif command -v shasum >/dev/null 2>&1; then
        shasum -a 256 "$1" | awk '{print $1}'
    else
        die "sha256sum or shasum is required"
    fi
}

run_nerdctl() {
    local -a global_args=(--namespace "${containerd_namespace}")
    if [[ -n "${containerd_address}" ]]; then
        global_args+=(--address "${containerd_address}")
    fi
    if [[ "${sudo_nerdctl}" == true ]]; then
        sudo "${nerdctl_bin}" "${global_args[@]}" "$@"
    else
        "${nerdctl_bin}" "${global_args[@]}" "$@"
    fi
}

verify_commit() {
    local repository="$1"
    local commit="$2"
    local label="$3"

    git -C "${repository}" cat-file -e "${commit}^{commit}" 2>/dev/null \
        || die "${label} commit is unavailable in ${repository}: ${commit}"
}

ensure_worktree() {
    local repository="$1"
    local path="$2"
    local commit="$3"
    local label="$4"
    local actual_head
    local actual_version

    if [[ ! -e "${path}" ]]; then
        git -C "${repository}" worktree add --detach "${path}" "${commit}"
    fi

    [[ -f "${path}/settings.gradle.kts" ]] || die "${label} worktree is invalid: ${path}"
    actual_head="$(git -C "${path}" rev-parse HEAD)"
    [[ "${actual_head}" == "${commit}" ]] \
        || die "${label} worktree HEAD mismatch: expected ${commit}, got ${actual_head}"
    [[ -z "$(git -C "${path}" status --porcelain --untracked-files=no)" ]] \
        || die "${label} worktree has tracked modifications: ${path}"

    actual_version="$(sed -n 's/^version=//p' "${path}/gradle.properties" | head -n 1)"
    [[ "${actual_version}" == "${PULSAR_VERSION}" ]] \
        || die "${label} version mismatch: expected ${PULSAR_VERSION}, got ${actual_version}"

    echo "verified ${label}: ${actual_head} (${actual_version})"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --pulsar-repo)
            [[ $# -ge 2 ]] || die "--pulsar-repo requires a value"
            pulsar_repo="$2"
            shift 2
            ;;
        --nereus-repo)
            [[ $# -ge 2 ]] || die "--nereus-repo requires a value"
            nereus_repo="$2"
            shift 2
            ;;
        --worktree-root)
            [[ $# -ge 2 ]] || die "--worktree-root requires a value"
            worktree_root="$2"
            shift 2
            ;;
        --output-dir)
            [[ $# -ge 2 ]] || die "--output-dir requires a value"
            output_dir="$2"
            shift 2
            ;;
        --image-repository)
            [[ $# -ge 2 ]] || die "--image-repository requires a value"
            image_repository="$2"
            shift 2
            ;;
        --platform)
            [[ $# -ge 2 ]] || die "--platform requires a value"
            target_platform="$2"
            shift 2
            ;;
        --namespace)
            [[ $# -ge 2 ]] || die "--namespace requires a value"
            containerd_namespace="$2"
            shift 2
            ;;
        --address)
            [[ $# -ge 2 ]] || die "--address requires a value"
            containerd_address="$2"
            shift 2
            ;;
        --nerdctl)
            [[ $# -ge 2 ]] || die "--nerdctl requires a value"
            nerdctl_bin="$2"
            shift 2
            ;;
        --sudo-nerdctl)
            sudo_nerdctl=true
            shift
            ;;
        --push)
            push_images=true
            shift
            ;;
        -h | --help)
            usage
            exit 0
            ;;
        *)
            die "unknown argument: $1"
            ;;
    esac
done

require_command git
require_command tar
require_command awk
require_command sed
require_command grep
require_command tr
require_command wc
require_command "${nerdctl_bin}"
if [[ "${sudo_nerdctl}" == true ]]; then
    require_command sudo
fi

[[ -n "${pulsar_repo}" ]] || die "--pulsar-repo is required"
git -C "${pulsar_repo}" rev-parse --is-inside-work-tree >/dev/null 2>&1 \
    || die "not a Pulsar Git clone/worktree: ${pulsar_repo}"
git -C "${nereus_repo}" rev-parse --is-inside-work-tree >/dev/null 2>&1 \
    || die "not a Nereus Git clone/worktree: ${nereus_repo}"
[[ "${image_repository}" != *[[:space:]]* ]] || die "image repository must not contain whitespace"

target_platform="${target_platform:-$(native_platform)}"
case "${target_platform}" in
    linux/amd64 | linux/arm64) ;;
    *) die "supported benchmark platforms are linux/amd64 and linux/arm64: ${target_platform}" ;;
esac
target_arch="${target_platform#linux/}"

worktree_root="${worktree_root:-$(cd "${pulsar_repo}/.." && pwd)/pulsar-worktrees}"
output_dir="${output_dir:-${nereus_repo}/build/performance-images}"
apache_worktree="${worktree_root}/pulsar-5.0.0-M1"
nereus_pulsar_worktree="${worktree_root}/pulsar-5.0.0-M1-nereus"

verify_commit "${pulsar_repo}" "${APACHE_PULSAR_SHA}" "Apache Pulsar"
verify_commit "${pulsar_repo}" "${NEREUS_PULSAR_SHA}" "Nereus Pulsar"
verify_commit "${nereus_repo}" "${NEREUS_SOURCE_SHA}" "Nereus source"

nereus_coordination_sha="$(git -C "${nereus_repo}" rev-parse HEAD)"
[[ -z "$(git -C "${nereus_repo}" status --porcelain --untracked-files=no)" ]] \
    || die "Nereus checkout has tracked modifications: ${nereus_repo}"
git -C "${nereus_repo}" merge-base --is-ancestor "${NEREUS_SOURCE_SHA}" "${nereus_coordination_sha}" \
    || die "Nereus source anchor ${NEREUS_SOURCE_SHA} is not an ancestor of ${nereus_coordination_sha}"
while IFS= read -r changed_path; do
    case "${changed_path}" in
        docs/performance/pulsar-5.0.0-M1-baselines.md \
            | docs/performance/pulsar-5.0.0-M1-images.md \
            | scripts/build-pulsar-5.0.0-M1-images.sh \
            | scripts/containerd-transfer-pulsar-5.0.0-M1-images.sh) ;;
        *) die "runtime-affecting path changed after Nereus source anchor: ${changed_path}" ;;
    esac
done < <(git -C "${nereus_repo}" diff --name-only "${NEREUS_SOURCE_SHA}" "${nereus_coordination_sha}")
grep -Fqx "pulsarExpectedHead=${NEREUS_PULSAR_SHA}" "${nereus_repo}/gradle.properties" \
    || die "gradle.properties does not pin pulsarExpectedHead=${NEREUS_PULSAR_SHA}"

mkdir -p "${worktree_root}" "${output_dir}"
ensure_worktree "${pulsar_repo}" "${apache_worktree}" "${APACHE_PULSAR_SHA}" "Apache Pulsar"
ensure_worktree "${pulsar_repo}" "${nereus_pulsar_worktree}" "${NEREUS_PULSAR_SHA}" "Nereus Pulsar"

echo "publishing Nereus ${NEREUS_DEVELOPMENT_VERSION} artifacts from frozen source ${NEREUS_SOURCE_SHA}"
(
    cd "${nereus_repo}"
    ./gradlew publishPhase2DevelopmentArtifacts --rerun-tasks --no-daemon
)

nereus_maven_repository="${nereus_repo}/build/development-repository"
adapter_jar="${nereus_maven_repository}/com/nereusstream/nereus-pulsar-adapter/${NEREUS_DEVELOPMENT_VERSION}/nereus-pulsar-adapter-${NEREUS_DEVELOPMENT_VERSION}.jar"
[[ -f "${adapter_jar}" ]] || die "published Nereus adapter is missing: ${adapter_jar}"
adapter_sha256="$(sha256_file "${adapter_jar}")"
[[ "${adapter_sha256}" == "${NEREUS_ADAPTER_SHA256}" ]] \
    || die "Nereus adapter checksum mismatch: expected ${NEREUS_ADAPTER_SHA256}, got ${adapter_sha256}"

echo "assembling Apache Pulsar distribution"
(
    cd "${apache_worktree}"
    ./gradlew :docker:pulsar:copyTarball :docker:pulsar:copyOffloaderTarball --no-daemon
)

echo "assembling Nereus Pulsar distribution"
(
    cd "${nereus_pulsar_worktree}"
    ./gradlew :docker:pulsar:copyTarball :docker:pulsar:copyOffloaderTarball --no-daemon \
        "-PnereusDevelopmentRepository=${nereus_maven_repository}"
)

apache_context="${apache_worktree}/docker/pulsar"
nereus_context="${nereus_pulsar_worktree}/docker/pulsar"
server_tarball="build/target/apache-pulsar-${PULSAR_VERSION}-bin.tar.gz"
offloader_tarball="build/target/apache-pulsar-offloaders-${PULSAR_VERSION}-bin.tar.gz"

for context in "${apache_context}" "${nereus_context}"; do
    [[ -f "${context}/${server_tarball}" ]] || die "server tarball is missing: ${context}/${server_tarball}"
    [[ -f "${context}/${offloader_tarball}" ]] || die "offloader tarball is missing: ${context}/${offloader_tarball}"
done

apache_tag="${image_repository}:5.0.0-m1-apache-p${APACHE_PULSAR_SHA:0:8}-${target_arch}"
nereus_tag="${image_repository}:5.0.0-m1-nereus-p${NEREUS_PULSAR_SHA:0:8}-n${NEREUS_SOURCE_SHA:0:8}-${target_arch}"
apache_iid_file="${output_dir}/apache-pulsar-5.0.0-M1-${target_arch}.iid"
nereus_iid_file="${output_dir}/nereus-pulsar-5.0.0-M1-${target_arch}.iid"

echo "building ${apache_tag} in containerd namespace ${containerd_namespace}"
run_nerdctl build \
    --progress plain \
    --platform "${target_platform}" \
    --iidfile "${apache_iid_file}" \
    --file "${apache_context}/Dockerfile" \
    --tag "${apache_tag}" \
    --label "org.opencontainers.image.title=Apache-Pulsar" \
    --label "org.opencontainers.image.version=${PULSAR_VERSION}" \
    --label "org.opencontainers.image.revision=${APACHE_PULSAR_SHA}" \
    --label "org.opencontainers.image.source=https://github.com/apache/pulsar" \
    --label "com.nereusstream.benchmark.variant=apache" \
    --label "com.nereusstream.benchmark.platform=${target_platform}" \
    --build-arg "PULSAR_TARBALL=${server_tarball}" \
    --build-arg "PULSAR_OFFLOADER_TARBALL=${offloader_tarball}" \
    --build-arg "PULSAR_CLIENT_PYTHON_VERSION=${PULSAR_CLIENT_PYTHON_VERSION}" \
    --build-arg "SNAPPY_VERSION=${SNAPPY_VERSION}" \
    --build-arg "IMAGE_JDK_MAJOR_VERSION=${IMAGE_JDK_MAJOR_VERSION}" \
    "${apache_context}"

echo "building ${nereus_tag} in containerd namespace ${containerd_namespace}"
run_nerdctl build \
    --progress plain \
    --platform "${target_platform}" \
    --iidfile "${nereus_iid_file}" \
    --file "${nereus_context}/Dockerfile" \
    --tag "${nereus_tag}" \
    --label "org.opencontainers.image.title=Nereus-Pulsar" \
    --label "org.opencontainers.image.version=${PULSAR_VERSION}" \
    --label "org.opencontainers.image.revision=${NEREUS_PULSAR_SHA}" \
    --label "org.opencontainers.image.source=https://github.com/nereusstream/pulsar" \
    --label "com.nereusstream.benchmark.variant=nereus" \
    --label "com.nereusstream.benchmark.platform=${target_platform}" \
    --label "com.nereusstream.pulsar.revision=${NEREUS_PULSAR_SHA}" \
    --label "com.nereusstream.nereus.revision=${NEREUS_SOURCE_SHA}" \
    --label "com.nereusstream.nereus.runtime-revision=${NEREUS_RUNTIME_SHA}" \
    --label "com.nereusstream.benchmark.coordination-revision=${nereus_coordination_sha}" \
    --label "com.nereusstream.nereus.adapter.sha256=${adapter_sha256}" \
    --build-arg "PULSAR_TARBALL=${server_tarball}" \
    --build-arg "PULSAR_OFFLOADER_TARBALL=${offloader_tarball}" \
    --build-arg "PULSAR_CLIENT_PYTHON_VERSION=${PULSAR_CLIENT_PYTHON_VERSION}" \
    --build-arg "SNAPPY_VERSION=${SNAPPY_VERSION}" \
    --build-arg "IMAGE_JDK_MAJOR_VERSION=${IMAGE_JDK_MAJOR_VERSION}" \
    "${nereus_context}"

if [[ "${push_images}" == true ]]; then
    echo "pushing ${apache_tag}"
    run_nerdctl push "${apache_tag}"
    echo "pushing ${nereus_tag}"
    run_nerdctl push "${nereus_tag}"
fi

echo "running image smoke checks"
run_nerdctl run --rm --net none --pull never --platform "${target_platform}" "${apache_tag}" \
    sh -ec 'bin/pulsar version | grep -F "5.0.0-M1"
if find lib -maxdepth 1 -name "com.nereusstream-*.jar" | grep -q .; then
    echo "Apache image unexpectedly contains Nereus jars" >&2
    exit 1
fi'
run_nerdctl run --rm --net none --pull never --platform "${target_platform}" "${nereus_tag}" \
    sh -ec 'bin/pulsar version | grep -F "5.0.0-M1"
jar_count="$(find lib -maxdepth 1 -name "com.nereusstream-*.jar" | wc -l | tr -d " ")"
test "${jar_count}" -eq 8
actual_sha="$(sha256sum "lib/com.nereusstream-nereus-pulsar-adapter-0.1.0-f2-dev.jar" | awk "{print \$1}")"
test "${actual_sha}" = "$1"' sh "${adapter_sha256}"

run_nerdctl image inspect --mode native "${apache_tag}" \
    >"${output_dir}/apache-pulsar-5.0.0-M1-${target_arch}.inspect.json"
run_nerdctl image inspect --mode native "${nereus_tag}" \
    >"${output_dir}/nereus-pulsar-5.0.0-M1-${target_arch}.inspect.json"
run_nerdctl images --digests --no-trunc "${apache_tag}" \
    >"${output_dir}/apache-pulsar-5.0.0-M1-${target_arch}.digests.txt"
run_nerdctl images --digests --no-trunc "${nereus_tag}" \
    >"${output_dir}/nereus-pulsar-5.0.0-M1-${target_arch}.digests.txt"

apache_image_id="$(tr -d '[:space:]' <"${apache_iid_file}")"
nereus_image_id="$(tr -d '[:space:]' <"${nereus_iid_file}")"
manifest_file="${output_dir}/pulsar-5.0.0-M1-${target_arch}.env"
{
    printf 'PULSAR_VERSION=%s\n' "${PULSAR_VERSION}"
    printf 'TARGET_PLATFORM=%s\n' "${target_platform}"
    printf 'CONTAINERD_NAMESPACE=%s\n' "${containerd_namespace}"
    printf 'CONTAINERD_ADDRESS=%s\n' "${containerd_address}"
    printf 'APACHE_PULSAR_SHA=%s\n' "${APACHE_PULSAR_SHA}"
    printf 'NEREUS_PULSAR_SHA=%s\n' "${NEREUS_PULSAR_SHA}"
    printf 'NEREUS_SHA=%s\n' "${NEREUS_SOURCE_SHA}"
    printf 'NEREUS_COORDINATION_SHA=%s\n' "${nereus_coordination_sha}"
    printf 'NEREUS_RUNTIME_SHA=%s\n' "${NEREUS_RUNTIME_SHA}"
    printf 'NEREUS_ADAPTER_SHA256=%s\n' "${adapter_sha256}"
    printf 'APACHE_IMAGE=%s\n' "${apache_tag}"
    printf 'APACHE_IMAGE_ID=%s\n' "${apache_image_id}"
    printf 'NEREUS_IMAGE=%s\n' "${nereus_tag}"
    printf 'NEREUS_IMAGE_ID=%s\n' "${nereus_image_id}"
    printf 'BUILT_AT_UTC=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
} >"${manifest_file}"

echo "build complete"
echo "Apache image: ${apache_tag} (${apache_image_id})"
echo "Nereus image: ${nereus_tag} (${nereus_image_id})"
echo "Manifest: ${manifest_file}"
echo "Digest listings: ${output_dir}/*.digests.txt"
