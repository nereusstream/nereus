#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -euo pipefail

readonly APACHE_PULSAR_REF_DEFAULT="8dae0236c0a0d405ed7f8303081080520fe91551"
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
pulsar_image_repository="nereus-benchmark/pulsar"
admin_image_repository="nereus-benchmark/nereus-admin"
apache_pulsar_ref="${APACHE_PULSAR_REF_DEFAULT}"
nereus_pulsar_ref="5.0.0-M1-nereus"
nereus_source_ref="HEAD"
admin_base_image="${NEREUS_ADMIN_BASE_IMAGE:-}"
containerd_namespace="${CONTAINERD_NAMESPACE:-k8s.io}"
containerd_address="${CONTAINERD_ADDRESS:-}"
nerdctl_bin="${NERDCTL_BIN:-nerdctl}"
target_platform=""
push_images=false
sudo_nerdctl=false
proxy_build_args=()

usage() {
  cat <<'EOF'
Build commit-qualified Apache Pulsar, Nereus Pulsar, and Nereus admin images
directly in containerd with nerdctl.

Usage:
  build-pulsar-5.0.0-M1-images.sh --pulsar-repo PATH [options]

Required:
  --pulsar-repo PATH          Pulsar clone containing Apache and Nereus refs.

Source selection:
  --apache-pulsar-ref REF     Apache baseline (default: v5.0.0-M1 commit).
  --nereus-pulsar-ref REF     Final Nereus Pulsar commit/branch
                              (default: 5.0.0-M1-nereus).
  --nereus-repo PATH          Final Nereus v0.1.0 checkout.
  --nereus-source-ref REF     Nereus commit to build (default: HEAD).

Output and image options:
  --worktree-root PATH        Pulsar worktree parent.
  --output-dir PATH           Manifest/inspection output directory.
  --image-repository NAME     Pulsar repository prefix.
  --admin-image-repository NAME
                              Nereus admin repository prefix.
  --admin-base-image IMAGE    Required JRE 21 base pinned as
                              IMAGE@sha256:<64-lowercase-hex>.
  --platform PLATFORM         linux/amd64 or linux/arm64.
  --namespace NAME            containerd namespace (default: k8s.io).
  --address PATH              containerd socket/address.
  --nerdctl PATH              nerdctl executable.
  --sudo-nerdctl              Elevate only nerdctl commands.
  --push                      Push all three images after building.
  HTTP_PROXY, HTTPS_PROXY,
  NO_PROXY (and lowercase)    Forward set proxy variables to Docker build
                              stages, including native dependency downloads.
  -h, --help                  Show this help.

Immutable-image safety:
  The selected Nereus and Nereus-Pulsar refs must resolve to commits, and both
  source checkouts must be clean, including untracked files. Uncommitted changes
  are intentionally rejected because they cannot be represented by an image tag
  or reconstructed from the evidence manifest.
EOF
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "required command is unavailable: $1"
}

native_platform() {
  case "$(uname -m)" in
    x86_64|amd64) echo "linux/amd64" ;;
    aarch64|arm64) echo "linux/arm64" ;;
    *) die "unsupported native architecture: $(uname -m)" ;;
  esac
}

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

run_nerdctl() {
  local -a global_args=(--namespace "${containerd_namespace}")
  if [[ -n "${containerd_address}" ]]; then
    global_args+=(--address "${containerd_address}")
  fi
  if [[ "${sudo_nerdctl}" == "true" ]]; then
    sudo "${nerdctl_bin}" "${global_args[@]}" "$@"
  else
    "${nerdctl_bin}" "${global_args[@]}" "$@"
  fi
}

append_proxy_build_arg() {
  local variable_name="$1"
  local variable_value="${!variable_name-}"
  if [[ -n "${variable_value}" ]]; then
    proxy_build_args+=(--build-arg "${variable_name}=${variable_value}")
  fi
}

for proxy_variable in HTTP_PROXY HTTPS_PROXY NO_PROXY http_proxy https_proxy no_proxy; do
  append_proxy_build_arg "${proxy_variable}"
done

resolve_commit() {
  local repository="$1"
  local ref="$2"
  local label="$3"
  local commit
  commit="$(git -C "${repository}" rev-parse --verify "${ref}^{commit}" 2>/dev/null)" \
    || die "${label} ref is not a local commit: ${ref}"
  printf '%s\n' "${commit}"
}

require_clean_checkout() {
  local repository="$1"
  local label="$2"
  local status
  status="$(git -C "${repository}" status --porcelain --untracked-files=all)"
  if [[ -n "${status}" ]]; then
    echo "${status}" >&2
    die "${label} checkout is dirty; commit or remove every listed path before building"
  fi
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
  [[ -f "${path}/settings.gradle.kts" ]] \
    || die "${label} worktree is invalid: ${path}"
  actual_head="$(git -C "${path}" rev-parse HEAD)"
  [[ "${actual_head}" == "${commit}" ]] \
    || die "${label} worktree HEAD mismatch: expected ${commit}, got ${actual_head}"
  [[ -z "$(git -C "${path}" status --porcelain --untracked-files=all)" ]] \
    || die "${label} worktree is dirty: ${path}"
  actual_version="$(sed -n 's/^version=//p' \
    "${path}/gradle.properties" | head -n 1)"
  [[ "${actual_version}" == "${PULSAR_VERSION}" ]] \
    || die "${label} version mismatch: expected ${PULSAR_VERSION}, got ${actual_version}"
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
    --apache-pulsar-ref)
      [[ $# -ge 2 ]] || die "--apache-pulsar-ref requires a value"
      apache_pulsar_ref="$2"
      shift 2
      ;;
    --nereus-pulsar-ref)
      [[ $# -ge 2 ]] || die "--nereus-pulsar-ref requires a value"
      nereus_pulsar_ref="$2"
      shift 2
      ;;
    --nereus-source-ref)
      [[ $# -ge 2 ]] || die "--nereus-source-ref requires a value"
      nereus_source_ref="$2"
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
      pulsar_image_repository="$2"
      shift 2
      ;;
    --admin-image-repository)
      [[ $# -ge 2 ]] || die "--admin-image-repository requires a value"
      admin_image_repository="$2"
      shift 2
      ;;
    --admin-base-image)
      [[ $# -ge 2 ]] || die "--admin-base-image requires a value"
      admin_base_image="$2"
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
    -h|--help)
      usage
      exit 0
      ;;
    *) die "unknown argument: $1" ;;
  esac
done

for command_name in git tar awk sed grep tr wc; do
  require_command "${command_name}"
done
require_command "${nerdctl_bin}"
if [[ "${sudo_nerdctl}" == "true" ]]; then
  require_command sudo
fi

[[ -n "${pulsar_repo}" ]] || die "--pulsar-repo is required"
git -C "${pulsar_repo}" rev-parse --is-inside-work-tree >/dev/null 2>&1 \
  || die "not a Pulsar Git checkout: ${pulsar_repo}"
git -C "${nereus_repo}" rev-parse --is-inside-work-tree >/dev/null 2>&1 \
  || die "not a Nereus Git checkout: ${nereus_repo}"
require_clean_checkout "${pulsar_repo}" "Pulsar"
require_clean_checkout "${nereus_repo}" "Nereus"

apache_pulsar_sha="$(resolve_commit \
  "${pulsar_repo}" "${apache_pulsar_ref}" "Apache Pulsar")"
nereus_pulsar_sha="$(resolve_commit \
  "${pulsar_repo}" "${nereus_pulsar_ref}" "Nereus Pulsar")"
nereus_source_sha="$(resolve_commit \
  "${nereus_repo}" "${nereus_source_ref}" "Nereus")"
[[ "$(git -C "${nereus_repo}" rev-parse HEAD)" == "${nereus_source_sha}" ]] \
  || die "Nereus checkout HEAD must equal selected source ref ${nereus_source_sha}"
pulsar_expected_head="$(sed -n 's/^pulsarExpectedHead=//p' \
  "${nereus_repo}/gradle.properties" | head -n 1)"
[[ "${pulsar_expected_head}" == "${nereus_pulsar_sha}" ]] \
  || die "Nereus pulsarExpectedHead must equal selected Nereus Pulsar commit: expected ${nereus_pulsar_sha}, got ${pulsar_expected_head}"
case "$(git -C "${nereus_repo}" branch --show-current)" in
  v0.1.0|release/v0.1.0) ;;
  *) die "Nereus image must be built from branch v0.1.0 or release/v0.1.0" ;;
esac

target_platform="${target_platform:-$(native_platform)}"
case "${target_platform}" in
  linux/amd64|linux/arm64) ;;
  *) die "unsupported benchmark platform: ${target_platform}" ;;
esac
[[ -n "${admin_base_image}" ]] \
  || die "--admin-base-image is required"
admin_base_digest="${admin_base_image##*@sha256:}"
[[ "${admin_base_image}" == *"@sha256:"*
    && "${admin_base_digest}" =~ ^[0-9a-f]{64}$ ]] \
  || die "--admin-base-image must be pinned as IMAGE@sha256:<64-lowercase-hex>"
target_arch="${target_platform#linux/}"
worktree_root="${worktree_root:-$(cd "${pulsar_repo}/.." && pwd)/pulsar-worktrees}"
output_dir="${output_dir:-${nereus_repo}/build/performance-images}"
apache_worktree="${worktree_root}/pulsar-5.0.0-M1-apache-${apache_pulsar_sha:0:8}"
nereus_pulsar_worktree="${worktree_root}/pulsar-5.0.0-M1-nereus-${nereus_pulsar_sha:0:8}"
mkdir -p "${worktree_root}" "${output_dir}"

ensure_worktree \
  "${pulsar_repo}" "${apache_worktree}" "${apache_pulsar_sha}" "Apache Pulsar"
ensure_worktree \
  "${pulsar_repo}" "${nereus_pulsar_worktree}" "${nereus_pulsar_sha}" "Nereus Pulsar"

echo "publishing Nereus artifacts from ${nereus_source_sha}"
(
  cd "${nereus_repo}"
  ./gradlew publishPhase2DevelopmentArtifacts \
    :nereus-admin:installDist \
    --rerun-tasks \
    --no-daemon \
    "-PpulsarCheckout=${nereus_pulsar_worktree}"
)

nereus_maven_repository="${nereus_repo}/build/development-repository"
adapter_jar="${nereus_maven_repository}/com/nereusstream/nereus-pulsar-adapter/${NEREUS_DEVELOPMENT_VERSION}/nereus-pulsar-adapter-${NEREUS_DEVELOPMENT_VERSION}.jar"
[[ -f "${adapter_jar}" ]] \
  || die "published Nereus adapter is missing: ${adapter_jar}"
adapter_sha256="$(sha256_file "${adapter_jar}")"

echo "assembling Apache Pulsar distribution"
(
  cd "${apache_worktree}"
  ./gradlew \
    :docker:pulsar:copyTarball \
    :docker:pulsar:copyOffloaderTarball \
    --no-daemon
)

echo "assembling Nereus Pulsar distribution"
(
  cd "${nereus_pulsar_worktree}"
  ./gradlew \
    :docker:pulsar:copyTarball \
    :docker:pulsar:copyOffloaderTarball \
    --no-daemon \
    "-PnereusDevelopmentRepository=${nereus_maven_repository}"
)

apache_context="${apache_worktree}/docker/pulsar"
nereus_context="${nereus_pulsar_worktree}/docker/pulsar"
apache_dockerfile_sha256="$(sha256_file "${apache_context}/Dockerfile")"
nereus_pulsar_dockerfile_sha256="$(sha256_file "${nereus_context}/Dockerfile")"
nereus_admin_dockerfile_sha256="$(
  sha256_file "${nereus_repo}/docker/nereus-admin/Dockerfile"
)"
server_tarball="build/target/apache-pulsar-${PULSAR_VERSION}-bin.tar.gz"
offloader_tarball="build/target/apache-pulsar-offloaders-${PULSAR_VERSION}-bin.tar.gz"
for context in "${apache_context}" "${nereus_context}"; do
  [[ -f "${context}/${server_tarball}" ]] \
    || die "server tarball is missing: ${context}/${server_tarball}"
  [[ -f "${context}/${offloader_tarball}" ]] \
    || die "offloader tarball is missing: ${context}/${offloader_tarball}"
done

apache_tag="${pulsar_image_repository}:5.0.0-m1-apache-p${apache_pulsar_sha:0:8}-${target_arch}"
nereus_tag="${pulsar_image_repository}:5.0.0-m1-nereus-p${nereus_pulsar_sha:0:8}-n${nereus_source_sha:0:8}-${target_arch}"
admin_tag="${admin_image_repository}:v0.1.0-n${nereus_source_sha:0:8}-${target_arch}"
apache_iid_file="${output_dir}/apache-pulsar-5.0.0-M1-${target_arch}.iid"
nereus_iid_file="${output_dir}/nereus-pulsar-5.0.0-M1-${target_arch}.iid"
admin_iid_file="${output_dir}/nereus-admin-v0.1.0-${target_arch}.iid"

echo "building ${apache_tag}"
run_nerdctl build \
  --progress plain \
  --platform "${target_platform}" \
  --iidfile "${apache_iid_file}" \
  --file "${apache_context}/Dockerfile" \
  --tag "${apache_tag}" \
  --label "org.opencontainers.image.title=Apache-Pulsar" \
  --label "org.opencontainers.image.version=${PULSAR_VERSION}" \
  --label "org.opencontainers.image.revision=${apache_pulsar_sha}" \
  --label "com.nereusstream.benchmark.variant=apache" \
  --label "com.nereusstream.benchmark.platform=${target_platform}" \
  --build-arg "PULSAR_TARBALL=${server_tarball}" \
  --build-arg "PULSAR_OFFLOADER_TARBALL=${offloader_tarball}" \
  --build-arg "PULSAR_CLIENT_PYTHON_VERSION=${PULSAR_CLIENT_PYTHON_VERSION}" \
  --build-arg "SNAPPY_VERSION=${SNAPPY_VERSION}" \
  --build-arg "IMAGE_JDK_MAJOR_VERSION=${IMAGE_JDK_MAJOR_VERSION}" \
  "${proxy_build_args[@]}" \
  "${apache_context}"

echo "building ${nereus_tag}"
run_nerdctl build \
  --progress plain \
  --platform "${target_platform}" \
  --iidfile "${nereus_iid_file}" \
  --file "${nereus_context}/Dockerfile" \
  --tag "${nereus_tag}" \
  --label "org.opencontainers.image.title=Nereus-Pulsar" \
  --label "org.opencontainers.image.version=${PULSAR_VERSION}" \
  --label "org.opencontainers.image.revision=${nereus_pulsar_sha}" \
  --label "com.nereusstream.benchmark.variant=nereus" \
  --label "com.nereusstream.benchmark.platform=${target_platform}" \
  --label "com.nereusstream.pulsar.revision=${nereus_pulsar_sha}" \
  --label "com.nereusstream.nereus.revision=${nereus_source_sha}" \
  --label "com.nereusstream.nereus.adapter.sha256=${adapter_sha256}" \
  --build-arg "PULSAR_TARBALL=${server_tarball}" \
  --build-arg "PULSAR_OFFLOADER_TARBALL=${offloader_tarball}" \
  --build-arg "PULSAR_CLIENT_PYTHON_VERSION=${PULSAR_CLIENT_PYTHON_VERSION}" \
  --build-arg "SNAPPY_VERSION=${SNAPPY_VERSION}" \
  --build-arg "IMAGE_JDK_MAJOR_VERSION=${IMAGE_JDK_MAJOR_VERSION}" \
  "${proxy_build_args[@]}" \
  "${nereus_context}"

echo "building ${admin_tag}"
run_nerdctl build \
  --progress plain \
  --platform "${target_platform}" \
  --iidfile "${admin_iid_file}" \
  --file "${nereus_repo}/docker/nereus-admin/Dockerfile" \
  --tag "${admin_tag}" \
  --build-arg "NEREUS_ADMIN_BASE_IMAGE=${admin_base_image}" \
  --build-arg "NEREUS_COMMIT=${nereus_source_sha}" \
  "${proxy_build_args[@]}" \
  --label "org.opencontainers.image.title=Nereus-Admin" \
  --label "org.opencontainers.image.version=v0.1.0" \
  --label "org.opencontainers.image.revision=${nereus_source_sha}" \
  --label "com.nereusstream.benchmark.platform=${target_platform}" \
  "${nereus_repo}"

echo "running image smoke checks"
run_nerdctl run --rm --net none --pull never \
  --platform "${target_platform}" "${apache_tag}" \
  sh -ec 'bin/pulsar version | grep -F "5.0.0-M1"
if find lib -maxdepth 1 -name "com.nereusstream-*.jar" | grep -q .; then
  echo "Apache image unexpectedly contains Nereus jars" >&2
  exit 1
fi'
run_nerdctl run --rm --net none --pull never \
  --platform "${target_platform}" "${nereus_tag}" \
  sh -ec 'bin/pulsar version | grep -F "5.0.0-M1"
jar_count="$(find lib -maxdepth 1 -name "com.nereusstream-*.jar" | wc -l | tr -d " ")"
test "${jar_count}" -eq 8
actual_sha="$(sha256sum "lib/com.nereusstream-nereus-pulsar-adapter-0.1.0-f2-dev.jar" | awk "{print \$1}")"
test "${actual_sha}" = "$1"' sh "${adapter_sha256}"
run_nerdctl run --rm --net none --pull never \
  --platform "${target_platform}" \
  --entrypoint /bin/sh \
  "${admin_tag}" \
  -ec 'test -x /opt/nereus-admin/bin/nereus-admin
java -version 2>&1 | grep -F "21."'

if [[ "${push_images}" == "true" ]]; then
  for image in "${apache_tag}" "${nereus_tag}" "${admin_tag}"; do
    echo "pushing verified image ${image}"
    run_nerdctl push "${image}"
  done
fi

for image_kind in apache-pulsar-5.0.0-M1 nereus-pulsar-5.0.0-M1 nereus-admin-v0.1.0; do
  case "${image_kind}" in
    apache-pulsar-5.0.0-M1) image="${apache_tag}" ;;
    nereus-pulsar-5.0.0-M1) image="${nereus_tag}" ;;
    nereus-admin-v0.1.0) image="${admin_tag}" ;;
  esac
  run_nerdctl image inspect --mode native "${image}" \
    > "${output_dir}/${image_kind}-${target_arch}.inspect.json"
  run_nerdctl images --digests --no-trunc "${image}" \
    > "${output_dir}/${image_kind}-${target_arch}.digests.txt"
done

apache_image_id="$(tr -d '[:space:]' < "${apache_iid_file}")"
nereus_image_id="$(tr -d '[:space:]' < "${nereus_iid_file}")"
admin_image_id="$(tr -d '[:space:]' < "${admin_iid_file}")"
manifest_file="${output_dir}/pulsar-5.0.0-M1-${target_arch}.env"
{
  printf 'PULSAR_VERSION=%s\n' "${PULSAR_VERSION}"
  printf 'TARGET_PLATFORM=%s\n' "${target_platform}"
  printf 'CONTAINERD_NAMESPACE=%s\n' "${containerd_namespace}"
  printf 'CONTAINERD_ADDRESS=%s\n' "${containerd_address}"
  printf 'APACHE_PULSAR_SHA=%s\n' "${apache_pulsar_sha}"
  printf 'NEREUS_PULSAR_SHA=%s\n' "${nereus_pulsar_sha}"
  printf 'NEREUS_SHA=%s\n' "${nereus_source_sha}"
  printf 'NEREUS_ADAPTER_SHA256=%s\n' "${adapter_sha256}"
  printf 'NEREUS_ADMIN_BASE_IMAGE=%s\n' "${admin_base_image}"
  printf 'APACHE_PULSAR_DOCKERFILE_SHA256=%s\n' "${apache_dockerfile_sha256}"
  printf 'NEREUS_PULSAR_DOCKERFILE_SHA256=%s\n' "${nereus_pulsar_dockerfile_sha256}"
  printf 'NEREUS_ADMIN_DOCKERFILE_SHA256=%s\n' "${nereus_admin_dockerfile_sha256}"
  printf 'APACHE_IMAGE=%s\n' "${apache_tag}"
  printf 'APACHE_IMAGE_ID=%s\n' "${apache_image_id}"
  printf 'NEREUS_IMAGE=%s\n' "${nereus_tag}"
  printf 'NEREUS_IMAGE_ID=%s\n' "${nereus_image_id}"
  printf 'NEREUS_ADMIN_IMAGE=%s\n' "${admin_tag}"
  printf 'NEREUS_ADMIN_IMAGE_ID=%s\n' "${admin_image_id}"
  printf 'BUILT_AT_UTC=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
} > "${manifest_file}"
printf '%s  %s\n' "$(sha256_file "${manifest_file}")" \
  "$(basename "${manifest_file}")" > "${manifest_file}.sha256"

echo "build complete"
echo "Apache image: ${apache_tag} (${apache_image_id})"
echo "Nereus image: ${nereus_tag} (${nereus_image_id})"
echo "Admin image: ${admin_tag} (${admin_image_id})"
echo "Manifest: ${manifest_file}"
