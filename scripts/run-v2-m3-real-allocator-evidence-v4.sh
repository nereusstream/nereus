#!/usr/bin/env bash
set -euo pipefail
export LC_ALL=C
export LANG=C

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ "${1:-}" == "--plan-only" ]]; then
  if (( $# != 1 )); then
    echo "usage: $0 --plan-only" >&2
    exit 64
  fi
  exec python3 "$repo_root/scripts/v2-m3-allocator-plan-v4.py"
fi

if [[ "${1:-}" == "--pre-campaign-check" ]]; then
  if (( $# != 1 )); then
    echo "usage: $0 --pre-campaign-check" >&2
    exit 64
  fi
  python3 "$repo_root/scripts/v2-m3-allocator-plan-v4.py" >/dev/null
  exec "$repo_root/gradlew" :nereus-metadata-oxia:realAllocatorV4PreCampaignCheck \
    --rerun-tasks \
    --console=plain \
    --no-configuration-cache
fi

if [[ "${1:-}" == "--bounded-adaptive-formal" ]]; then
  if (( $# != 2 )); then
    echo "usage: $0 --bounded-adaptive-formal NEW_EMPTY_OUTPUT_DIRECTORY" >&2
    exit 64
  fi
  formal_authorization="${NEREUS_M3_ALLOCATOR_V4_FORMAL_AUTHORIZATION_SHA:?set the separately authorized exact SHA}"
  formal_plan_sha="${NEREUS_M3_ALLOCATOR_V4_ZERO_DECISION_PLAN_SHA256:?set the reviewed V4 plan-only SHA}"
  formal_profile_sha="${NEREUS_M3_ALLOCATOR_V4_NATIVE_EXECUTION_PROFILE_SHA256:?set the reviewed V4 execution profile SHA}"
  formal_source_locks_sha="${NEREUS_M3_ALLOCATOR_V4_SOURCE_LOCKS_SHA256:?set the reviewed source-lock SHA}"
  formal_dependency_lock_sha="${NEREUS_M3_ALLOCATOR_V4_DEPENDENCY_LOCK_SHA256:?set the reviewed dependency-lock SHA}"
  formal_pulsar_checkout="${NEREUS_M3_ALLOCATOR_PULSAR_CHECKOUT:?set the dedicated Pulsar worktree}"
  formal_oxia_server_checkout="${NEREUS_M3_ALLOCATOR_OXIA_SERVER_CHECKOUT:?set the dedicated Oxia-server worktree}"
  formal_oxia_client_checkout="${NEREUS_M3_ALLOCATOR_OXIA_CLIENT_CHECKOUT:?set the dedicated Oxia-client worktree}"
  formal_pulsar_commit="${NEREUS_M3_ALLOCATOR_V4_PULSAR_COMMIT:?set the reviewed Pulsar commit}"
  formal_oxia_server_commit="${NEREUS_M3_ALLOCATOR_V4_OXIA_SERVER_COMMIT:?set the reviewed Oxia-server commit}"
  formal_oxia_client_commit="${NEREUS_M3_ALLOCATOR_V4_OXIA_CLIENT_COMMIT:?set the reviewed Oxia-client commit}"
  formal_oxia_client_jar="${NEREUS_M3_ALLOCATOR_V4_OXIA_CLIENT_JAR:?set the source-locked Oxia-client JAR}"
  formal_oxia_client_jar_sha="${NEREUS_M3_ALLOCATOR_V4_OXIA_CLIENT_JAR_SHA256:?set the reviewed Oxia-client JAR SHA}"
  formal_oxia_image="${NEREUS_M3_ALLOCATOR_V4_OXIA_IMAGE:?set the reviewed Oxia image tag}"
  formal_oxia_image_digest="${NEREUS_M3_ALLOCATOR_V4_OXIA_IMAGE_DIGEST:?set the reviewed Oxia image digest}"
  formal_diagnostic="${NEREUS_M3_ALLOCATOR_V4_DIAGNOSTIC_PATH:?set the exact-source NADV4 path}"
  formal_diagnostic_sha="${NEREUS_M3_ALLOCATOR_V4_DIAGNOSTIC_SHA256:?set the reviewed NADV4 SHA}"
  formal_diagnostic_junit="${NEREUS_M3_ALLOCATOR_V4_DIAGNOSTIC_JUNIT_DIRECTORY:?set the exact nine-suite V4 diagnostic JUnit directory}"
  formal_output="$2"

  test "$(git -C "$repo_root" rev-parse HEAD)" = "$formal_authorization"
  test "$(git -C "$repo_root" branch --show-current)" = main
  test "$(git -C "$repo_root" rev-parse refs/remotes/origin/main)" = "$formal_authorization"
  test "$(git -C "$repo_root" status --porcelain --untracked-files=all)" = ""
  case "$(printf '%s' "$formal_output" | tr '[:upper:]' '[:lower:]')" in
    *full-matrix*|*diagnostic*|*nare1*|*naea1*|*nars1*)
      echo "bounded-adaptive V4 output aliases an old V1 or diagnostic directory" >&2
      exit 64
      ;;
  esac
  test ! -e "$formal_output"

  formal_plan_json="$(python3 "$repo_root/scripts/v2-m3-allocator-plan-v4.py")"
  test "$(printf '%s' "$formal_plan_json" | jq -r '.schema')" = \
    NEREUS_V2_M3_ALLOCATOR_CAMPAIGN_PLAN_V4
  test "$(printf '%s' "$formal_plan_json" | jq -r '.feasibilityStatus')" = PLAN_FEASIBLE
  test "$(printf '%s' "$formal_plan_json" | jq -r '.terminalCensoringFeasibility.status')" = PLAN_FEASIBLE
  test "$(printf '%s' "$formal_plan_json" | jq -r '.zeroDecisionPlanSha256')" = "$formal_plan_sha"
  test "$(printf '%s' "$formal_plan_json" | jq -r '.nativeExecution.nativeExecutionProfileSha256')" = \
    "$formal_profile_sha"
  test "$(printf '%s' "$formal_plan_json" | jq -r '.exactSourceTuple.nereusCommit')" = "$formal_authorization"
  test "$(printf '%s' "$formal_plan_json" | jq -r '.exactSourceTuple.sourceLocksSha256')" = \
    "$formal_source_locks_sha"
  test "$(printf '%s' "$formal_plan_json" | jq -r '.exactSourceTuple.dependencyLockSha256')" = \
    "$formal_dependency_lock_sha"
  test "$(printf '%s' "$formal_plan_json" | jq -r '.exactSourceTuple.pulsarCommit')" = "$formal_pulsar_commit"
  test "$(printf '%s' "$formal_plan_json" | jq -r '.exactSourceTuple.oxiaServerCommit')" = \
    "$formal_oxia_server_commit"
  test "$(printf '%s' "$formal_plan_json" | jq -r '.exactSourceTuple.oxiaClientCommit')" = \
    "$formal_oxia_client_commit"
  test "$(printf '%s' "$formal_plan_json" | jq -r '.exactSourceTuple.oxiaClientJarSha256')" = \
    "$formal_oxia_client_jar_sha"
  test "$(printf '%s' "$formal_plan_json" | jq -r '.exactSourceTuple.oxiaServerImageDigest')" = \
    "$formal_oxia_image_digest"
  test "$(printf '%s' "$formal_plan_json" | jq -r '.maximumExecutedIntervalCells')" = 328
  test "$(printf '%s' "$formal_plan_json" | jq -r '.maximumExecutedFaultActions')" = 360
  test "$(printf '%s' "$formal_plan_json" | jq -r '.maximumExecutedScaleActions')" = 32
  test "$(printf '%s' "$formal_plan_json" | jq -r '.maximumTotalExecutedActions')" = 720
  test "$(printf '%s' "$formal_plan_json" | jq -r '.campaignWallClockCapSeconds')" = 48000

  for checkout_tuple in \
    "$formal_pulsar_checkout:$formal_pulsar_commit" \
    "$formal_oxia_server_checkout:$formal_oxia_server_commit" \
    "$formal_oxia_client_checkout:$formal_oxia_client_commit"; do
    formal_checkout="${checkout_tuple%:*}"
    formal_commit="${checkout_tuple##*:}"
    test "$(git -C "$formal_checkout" rev-parse HEAD)" = "$formal_commit"
    test "$(git -C "$formal_checkout" status --porcelain --untracked-files=all)" = ""
  done
  test "$(shasum -a 256 "$formal_oxia_client_jar" | awk '{print $1}')" = "$formal_oxia_client_jar_sha"
  test "$(shasum -a 256 "$formal_diagnostic" | awk '{print $1}')" = "$formal_diagnostic_sha"
  test -d "$formal_diagnostic_junit"
  test ! -L "$formal_diagnostic_junit"
  test "$(docker image inspect "$formal_oxia_image" --format '{{.Id}}')" = "$formal_oxia_image_digest"
  test "$(docker image inspect "$formal_oxia_image" \
    --format '{{index .Config.Labels "org.opencontainers.image.revision"}}')" = "$formal_oxia_server_commit"

  "$repo_root/gradlew" :nereus-metadata-oxia:realAllocatorEvidenceArtifactJar \
    "-PpulsarCheckout=$formal_pulsar_checkout" \
    --console=plain \
    --no-configuration-cache
  formal_executor_jar="$(find "$repo_root/nereus-metadata-oxia/build/libs" -maxdepth 1 -type f \
    -name 'nereus-v2-m3-real-allocator-evidence-*.jar' -print)"
  test "$(printf '%s\n' "$formal_executor_jar" | awk 'NF { count++ } END { print count + 0 }')" = 1
  formal_executor_sha="$(shasum -a 256 "$formal_executor_jar" | awk '{print $1}')"
  test "$(git -C "$repo_root" status --porcelain --untracked-files=all)" = ""

  "$repo_root/gradlew" :nereus-metadata-oxia:realAllocatorV4PreCampaignCheck \
    "-PpulsarCheckout=$formal_pulsar_checkout" \
    --rerun-tasks \
    --console=plain \
    --no-configuration-cache
  test ! -e "$formal_output"

  "$repo_root/gradlew" :nereus-metadata-oxia:validateExistingRealAllocatorV4Diagnostic \
    "-PpulsarCheckout=$formal_pulsar_checkout" \
    "-Pv2M3AllocatorV4DiagnosticPath=$formal_diagnostic" \
    "-Pv2M3AllocatorV4DiagnosticJUnitDirectory=$formal_diagnostic_junit" \
    "-Pv2M3AllocatorV4NereusCommit=$formal_authorization" \
    "-Pv2M3AllocatorV4OxiaImageDigest=${formal_oxia_image_digest#sha256:}" \
    "-Pv2M3AllocatorV4DependencyLockDigest=$formal_dependency_lock_sha" \
    "-Pv2M3AllocatorV4ExecutorDigest=$formal_executor_sha" \
    "-Pv2M3AllocatorV4WorkloadDigest=$formal_plan_sha" \
    --console=plain \
    --no-configuration-cache
  test "$(shasum -a 256 "$formal_diagnostic" | awk '{print $1}')" = "$formal_diagnostic_sha"
  test ! -e "$formal_output"

  formal_container="nereus-m3-allocator-v4-formal-$$"
  cleanup_formal_container() {
    docker rm -f "$formal_container" >/dev/null 2>&1 || true
  }
  trap cleanup_formal_container EXIT INT TERM
  docker run --detach \
    --name "$formal_container" \
    --label com.nereusstream.evidence=v4-m3-bounded-adaptive-formal \
    --publish 127.0.0.1::6648 \
    --publish 127.0.0.1::8080 \
    "$formal_oxia_image" \
    oxia standalone --shards=4 >/dev/null
  formal_oxia_port="$(docker port "$formal_container" 6648/tcp | awk -F: '{print $NF}')"
  formal_metrics_port="$(docker port "$formal_container" 8080/tcp | awk -F: '{print $NF}')"
  for attempt in $(seq 1 60); do
    if curl --fail --silent --show-error "http://127.0.0.1:$formal_metrics_port/metrics" >/dev/null; then
      break
    fi
    test "$attempt" != 60
    sleep 1
  done

  mkdir -p "$formal_output"
  python3 "$repo_root/scripts/run-v2-m3-with-hard-deadline.py" \
    --hard-deadline-seconds 48000 \
    --termination-grace-seconds 30 \
    -- \
    "$repo_root/gradlew" :nereus-metadata-oxia:realAllocatorV4BoundedAdaptiveFormalCampaign \
    "-PpulsarCheckout=$formal_pulsar_checkout" \
    "-Pv2M3AllocatorV4FormalAuthorizationSha=$formal_authorization" \
    "-Pv2M3AllocatorV4ZeroDecisionPlanSha256=$formal_plan_sha" \
    "-Pv2M3AllocatorV4NativeExecutionProfileSha256=$formal_profile_sha" \
    "-Pv2M3AllocatorV4SourceLocksSha256=$formal_source_locks_sha" \
    "-Pv2M3AllocatorV4DependencyLockSha256=$formal_dependency_lock_sha" \
    "-Pv2M3AllocatorV4PulsarCheckout=$formal_pulsar_checkout" \
    "-Pv2M3AllocatorV4PulsarCommit=$formal_pulsar_commit" \
    "-Pv2M3AllocatorV4OxiaServerCheckout=$formal_oxia_server_checkout" \
    "-Pv2M3AllocatorV4OxiaServerCommit=$formal_oxia_server_commit" \
    "-Pv2M3AllocatorV4OxiaClientCheckout=$formal_oxia_client_checkout" \
    "-Pv2M3AllocatorV4OxiaClientCommit=$formal_oxia_client_commit" \
    "-Pv2M3AllocatorV4OxiaClientJarPath=$formal_oxia_client_jar" \
    "-Pv2M3AllocatorV4OxiaClientJarSha256=$formal_oxia_client_jar_sha" \
    "-Pv2M3AllocatorV4OxiaImageDigest=$formal_oxia_image_digest" \
    "-Pv2M3AllocatorV4OxiaContainerName=$formal_container" \
    "-Pv2M3AllocatorV4ExecutorSha256=$formal_executor_sha" \
    "-Pv2M3AllocatorV4DiagnosticPath=$formal_diagnostic" \
    "-Pv2M3AllocatorV4DiagnosticSha256=$formal_diagnostic_sha" \
    "-Pv2M3AllocatorV4DiagnosticJUnitDirectory=$formal_diagnostic_junit" \
    "-Pv2M3AllocatorV4OxiaServiceAddress=127.0.0.1:$formal_oxia_port" \
    "-Pv2M3AllocatorV4FormalOutputDirectory=$formal_output" \
    --console=plain \
    --no-configuration-cache
  exit 0
fi

echo "usage: $0 --plan-only | --pre-campaign-check | --bounded-adaptive-formal NEW_OUTPUT_DIRECTORY" >&2
exit 64
