#!/usr/bin/env bash
set -euo pipefail
export LC_ALL=C
export LANG=C

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pulsar_checkout="${1:-/Users/liusinan/apps/ideaproject/nereusstream/pulsar-worktrees/nereus-v2-m3}"
expected_commit="${NEREUS_M3_ALLOCATOR_EXPECTED_NEREUS_COMMIT:?set exact clean origin/main commit}"
oxia_image="${NEREUS_M3_ALLOCATOR_OXIA_IMAGE:-nereus/oxia-m3-allocator:37a17bef1720}"
output="${NEREUS_M3_ALLOCATOR_V2_DIAGNOSTIC_OUTPUT:-$repo_root/nereus-metadata-oxia/build/m3-allocator-evidence/diagnostic-v2/$expected_commit.nadv}"
container_name="nereus-m3-allocator-v2-diagnostic-$$"
plan_file="$(mktemp "${TMPDIR:-/tmp}/nereus-m3-allocator-v2-plan.XXXXXX")"

cleanup() {
  docker rm -f "$container_name" >/dev/null 2>&1 || true
  rm -f "$plan_file"
}
trap cleanup EXIT INT TERM

assert_source_state() {
  test "$(git -C "$repo_root" rev-parse HEAD)" = "$expected_commit"
  test "$(git -C "$repo_root" rev-parse refs/remotes/origin/main)" = "$expected_commit"
  test "$(git -C "$repo_root" branch --show-current)" = main
  test "$(git -C "$repo_root" status --porcelain --untracked-files=all)" = ""
  test -f "$pulsar_checkout/settings.gradle.kts"
  test "$(git -C "$pulsar_checkout" branch --show-current)" = nereus/v2-m3-object-wal-evidence
  test "$(git -C "$pulsar_checkout" status --porcelain --untracked-files=all)" = ""
}
assert_source_state
test ! -e "$output"
mkdir -p "$(dirname "$output")"

"$repo_root/scripts/run-v2-m3-real-allocator-evidence.sh" --plan-only >"$plan_file"
workload_digest="$(shasum -a 256 "$plan_file" | awk '{print $1}')"
dependency_lock_digest="$(shasum -a 256 "$repo_root/docs/v2/source-locks.json" | awk '{print $1}')"
oxia_image_id="$(docker image inspect "$oxia_image" --format '{{.Id}}')"
oxia_image_digest="${oxia_image_id#sha256:}"
test "${#oxia_image_digest}" = 64
test "$(docker image inspect "$oxia_image" --format '{{index .Config.Labels "org.opencontainers.image.revision"}}')" = \
  37a17bef17202d5fd6e23282da5fd26d94865484

"$repo_root/gradlew" :nereus-metadata-oxia:realAllocatorEvidenceArtifactJar \
  "-PpulsarCheckout=$pulsar_checkout" \
  --console=plain \
  --no-configuration-cache
executor_artifact="$(find "$repo_root/nereus-metadata-oxia/build/libs" -maxdepth 1 -type f \
  -name 'nereus-v2-m3-real-allocator-evidence-*.jar' ! -name '*-sources.jar' | sort)"
test "$(printf '%s\n' "$executor_artifact" | awk 'NF { count++ } END { print count + 0 }')" = 1
executor_digest="$(shasum -a 256 "$executor_artifact" | awk '{print $1}')"
assert_source_state

docker run --detach \
  --name "$container_name" \
  --label com.nereusstream.evidence=v2-m3-allocator-v2-diagnostic \
  --publish 127.0.0.1::6648 \
  --publish 127.0.0.1::8080 \
  "$oxia_image" \
  oxia standalone --shards=4 >/dev/null
oxia_service_port="$(docker port "$container_name" 6648/tcp | awk -F: '{print $NF}')"
oxia_metrics_port="$(docker port "$container_name" 8080/tcp | awk -F: '{print $NF}')"
for attempt in $(seq 1 60); do
  if curl --fail --silent --show-error "http://127.0.0.1:$oxia_metrics_port/metrics" >/dev/null; then
    break
  fi
  if test "$attempt" = 60; then
    docker logs "$container_name"
    exit 1
  fi
  sleep 1
done

diagnostic_run_id="short-${expected_commit:0:12}-$$"
"$repo_root/gradlew" :nereus-metadata-oxia:sealRealAllocatorV2Diagnostic \
  "-PpulsarCheckout=$pulsar_checkout" \
  "-Pv2M3AllocatorOxiaServiceAddress=127.0.0.1:$oxia_service_port" \
  "-Pv2M3AllocatorV2DiagnosticRunId=$diagnostic_run_id" \
  "-Pv2M3AllocatorV2DiagnosticOutput=$output" \
  "-Pv2M3AllocatorV2NereusCommit=$expected_commit" \
  "-Pv2M3AllocatorV2OxiaImageDigest=$oxia_image_digest" \
  "-Pv2M3AllocatorV2DependencyLockDigest=$dependency_lock_digest" \
  "-Pv2M3AllocatorV2ExecutorDigest=$executor_digest" \
  "-Pv2M3AllocatorV2WorkloadDigest=$workload_digest" \
  --console=plain \
  --no-configuration-cache

assert_source_state
test "$(shasum -a 256 "$executor_artifact" | awk '{print $1}')" = "$executor_digest"
test "$(shasum -a 256 "$repo_root/docs/v2/source-locks.json" | awk '{print $1}')" = "$dependency_lock_digest"
test "$(shasum -a 256 "$plan_file" | awk '{print $1}')" = "$workload_digest"
test -s "$output"
junit="$repo_root/nereus-metadata-oxia/build/test-results/realAllocatorV2ShortDiagnosticTest/TEST-com.nereusstream.metadata.oxia.v2.allocator.evidence.M3V2RealOxiaDiagnosticTest.xml"
test -s "$junit"
printf 'allocator V2 short diagnostic complete: receiptSha256=%s junitSha256=%s\n' \
  "$(shasum -a 256 "$output" | awk '{print $1}')" \
  "$(shasum -a 256 "$junit" | awk '{print $1}')"
printf 'diagnostic-only: no formal campaign, evaluation, selection, receipt, or scenario PASS was produced\n'
