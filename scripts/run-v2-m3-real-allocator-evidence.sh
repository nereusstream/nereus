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
  exec python3 "$repo_root/scripts/v2-m3-allocator-plan.py"
  exit 0
fi

if [[ "${1:-}" == "--bounded-adaptive-formal" ]]; then
  if (( $# != 2 )); then
    echo "usage: $0 --bounded-adaptive-formal NEW_EMPTY_OUTPUT_DIRECTORY" >&2
    exit 64
  fi
  formal_authorization="${NEREUS_M3_ALLOCATOR_V2_FORMAL_AUTHORIZATION_SHA:?set the separately authorized exact SHA}"
  formal_plan_sha="${NEREUS_M3_ALLOCATOR_V2_ZERO_DECISION_PLAN_SHA256:?set the reviewed plan-only SHA}"
  formal_source_locks_sha="${NEREUS_M3_ALLOCATOR_V2_SOURCE_LOCKS_SHA256:?set the reviewed source-lock SHA}"
  formal_dependency_lock_sha="${NEREUS_M3_ALLOCATOR_V2_DEPENDENCY_LOCK_SHA256:?set the reviewed dependency-lock SHA}"
  formal_pulsar_checkout="${NEREUS_M3_ALLOCATOR_PULSAR_CHECKOUT:?set the dedicated Pulsar worktree}"
  formal_oxia_server_checkout="${NEREUS_M3_ALLOCATOR_OXIA_SERVER_CHECKOUT:?set the dedicated Oxia-server worktree}"
  formal_oxia_client_checkout="${NEREUS_M3_ALLOCATOR_OXIA_CLIENT_CHECKOUT:?set the dedicated Oxia-client worktree}"
  formal_pulsar_commit="${NEREUS_M3_ALLOCATOR_V2_PULSAR_COMMIT:?set the reviewed Pulsar commit}"
  formal_oxia_server_commit="${NEREUS_M3_ALLOCATOR_V2_OXIA_SERVER_COMMIT:?set the reviewed Oxia-server commit}"
  formal_oxia_client_commit="${NEREUS_M3_ALLOCATOR_V2_OXIA_CLIENT_COMMIT:?set the reviewed Oxia-client commit}"
  formal_oxia_client_jar="${NEREUS_M3_ALLOCATOR_V2_OXIA_CLIENT_JAR:?set the source-locked Oxia-client JAR}"
  formal_oxia_client_jar_sha="${NEREUS_M3_ALLOCATOR_V2_OXIA_CLIENT_JAR_SHA256:?set the reviewed Oxia-client JAR SHA}"
  formal_oxia_image="${NEREUS_M3_ALLOCATOR_V2_OXIA_IMAGE:?set the reviewed Oxia image tag}"
  formal_oxia_image_digest="${NEREUS_M3_ALLOCATOR_V2_OXIA_IMAGE_DIGEST:?set the reviewed Oxia image digest}"
  formal_output="$2"

  test "$(git -C "$repo_root" rev-parse HEAD)" = "$formal_authorization"
  test "$(git -C "$repo_root" branch --show-current)" = main
  test "$(git -C "$repo_root" rev-parse refs/remotes/origin/main)" = "$formal_authorization"
  test "$(git -C "$repo_root" status --porcelain --untracked-files=all)" = ""
  case "$(printf '%s' "$formal_output" | tr '[:upper:]' '[:lower:]')" in
    *full-matrix*|*diagnostic*|*nare1*|*naea1*|*nars1*)
      echo "bounded-adaptive output aliases an old V1 or diagnostic directory" >&2
      exit 64
      ;;
  esac
  if test -e "$formal_output"; then
    test -d "$formal_output"
    test ! -L "$formal_output"
    test -z "$(find "$formal_output" -mindepth 1 -maxdepth 1 -print -quit)"
  fi

  formal_plan_json="$(python3 "$repo_root/scripts/v2-m3-allocator-plan.py")"
  test "$(printf '%s' "$formal_plan_json" | jq -r '.zeroDecisionPlanSha256')" = "$formal_plan_sha"
  test "$(printf '%s' "$formal_plan_json" | jq -r '.exactSourceTuple.nereusCommit')" = "$formal_authorization"
  test "$(printf '%s' "$formal_plan_json" | jq -r '.exactSourceTuple.sourceLocksSha256')" = "$formal_source_locks_sha"
  test "$(printf '%s' "$formal_plan_json" | jq -r '.exactSourceTuple.dependencyLockSha256')" = "$formal_dependency_lock_sha"
  test "$(printf '%s' "$formal_plan_json" | jq -r '.exactSourceTuple.pulsarCommit')" = "$formal_pulsar_commit"
  test "$(printf '%s' "$formal_plan_json" | jq -r '.exactSourceTuple.oxiaServerCommit')" = "$formal_oxia_server_commit"
  test "$(printf '%s' "$formal_plan_json" | jq -r '.exactSourceTuple.oxiaClientCommit')" = "$formal_oxia_client_commit"
  test "$(printf '%s' "$formal_plan_json" | jq -r '.exactSourceTuple.oxiaClientJarSha256')" = "$formal_oxia_client_jar_sha"
  test "$(printf '%s' "$formal_plan_json" | jq -r '.exactSourceTuple.oxiaServerImageDigest')" = "$formal_oxia_image_digest"

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
  test "$(docker image inspect "$formal_oxia_image" --format '{{.Id}}')" = "$formal_oxia_image_digest"
  test "$(docker image inspect "$formal_oxia_image" --format '{{index .Config.Labels \"org.opencontainers.image.revision\"}}')" = \
    "$formal_oxia_server_commit"

  "$repo_root/gradlew" :nereus-metadata-oxia:realAllocatorEvidenceArtifactJar \
    "-PpulsarCheckout=$formal_pulsar_checkout" \
    --console=plain \
    --no-configuration-cache
  formal_executor_jar="$(find "$repo_root/nereus-metadata-oxia/build/libs" -maxdepth 1 -type f \
    -name 'nereus-v2-m3-real-allocator-evidence-*.jar' -print)"
  test "$(printf '%s\n' "$formal_executor_jar" | awk 'NF { count++ } END { print count + 0 }')" = 1
  formal_executor_sha="$(shasum -a 256 "$formal_executor_jar" | awk '{print $1}')"
  test "$(git -C "$repo_root" status --porcelain --untracked-files=all)" = ""

  formal_container="nereus-m3-allocator-v2-formal-$$"
  cleanup_formal_container() {
    docker rm -f "$formal_container" >/dev/null 2>&1 || true
  }
  trap cleanup_formal_container EXIT INT TERM
  docker run --detach \
    --name "$formal_container" \
    --label com.nereusstream.evidence=v2-m3-bounded-adaptive-formal \
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
  "$repo_root/gradlew" :nereus-metadata-oxia:realAllocatorV2BoundedAdaptiveFormalCampaign \
    "-PpulsarCheckout=$formal_pulsar_checkout" \
    "-Pv2M3AllocatorV2FormalAuthorizationSha=$formal_authorization" \
    "-Pv2M3AllocatorV2ZeroDecisionPlanSha256=$formal_plan_sha" \
    "-Pv2M3AllocatorV2SourceLocksSha256=$formal_source_locks_sha" \
    "-Pv2M3AllocatorV2DependencyLockSha256=$formal_dependency_lock_sha" \
    "-Pv2M3AllocatorV2PulsarCheckout=$formal_pulsar_checkout" \
    "-Pv2M3AllocatorV2PulsarCommit=$formal_pulsar_commit" \
    "-Pv2M3AllocatorV2OxiaServerCheckout=$formal_oxia_server_checkout" \
    "-Pv2M3AllocatorV2OxiaServerCommit=$formal_oxia_server_commit" \
    "-Pv2M3AllocatorV2OxiaClientCheckout=$formal_oxia_client_checkout" \
    "-Pv2M3AllocatorV2OxiaClientCommit=$formal_oxia_client_commit" \
    "-Pv2M3AllocatorV2OxiaClientJarPath=$formal_oxia_client_jar" \
    "-Pv2M3AllocatorV2OxiaClientJarSha256=$formal_oxia_client_jar_sha" \
    "-Pv2M3AllocatorV2OxiaImageDigest=$formal_oxia_image_digest" \
    "-Pv2M3AllocatorV2OxiaContainerName=$formal_container" \
    "-Pv2M3AllocatorV2ExecutorSha256=$formal_executor_sha" \
    "-Pv2M3AllocatorV2OxiaServiceAddress=127.0.0.1:$formal_oxia_port" \
    "-Pv2M3AllocatorV2FormalOutputDirectory=$formal_output" \
    --console=plain \
    --no-configuration-cache
  exit 0
fi

protocol_pulsar_checkout="${NEREUS_M3_ALLOCATOR_PULSAR_CHECKOUT:-/Users/liusinan/apps/ideaproject/nereusstream/pulsar-worktrees/nereus-v2-m3}"
protocol_commit="${NEREUS_M3_ALLOCATOR_EXPECTED_NEREUS_COMMIT:-}"
protocol_oxia_image_digest="${NEREUS_M3_ALLOCATOR_V2_OXIA_IMAGE_DIGEST:-}"
protocol_dependency_lock_digest="${NEREUS_M3_ALLOCATOR_V2_DEPENDENCY_LOCK_DIGEST:-}"
protocol_executor_digest="${NEREUS_M3_ALLOCATOR_V2_EXECUTOR_DIGEST:-}"
protocol_workload_digest="${NEREUS_M3_ALLOCATOR_V2_WORKLOAD_DIGEST:-}"

require_protocol_binding() {
  test -n "$protocol_commit"
  test -n "$protocol_oxia_image_digest"
  test -n "$protocol_dependency_lock_digest"
  test -n "$protocol_executor_digest"
  test -n "$protocol_workload_digest"
  test "$(git -C "$repo_root" rev-parse HEAD)" = "$protocol_commit"
  test "$(git -C "$repo_root" rev-parse refs/remotes/origin/main)" = "$protocol_commit"
}

protocol_gradle_properties=(
  "-PpulsarCheckout=$protocol_pulsar_checkout"
  "-Pv2M3AllocatorV2NereusCommit=$protocol_commit"
  "-Pv2M3AllocatorV2OxiaImageDigest=$protocol_oxia_image_digest"
  "-Pv2M3AllocatorV2DependencyLockDigest=$protocol_dependency_lock_digest"
  "-Pv2M3AllocatorV2ExecutorDigest=$protocol_executor_digest"
  "-Pv2M3AllocatorV2WorkloadDigest=$protocol_workload_digest"
)

if [[ "${1:-}" == "--pre-campaign-check" ]]; then
  if (( $# != 1 )); then
    echo "usage: $0 --pre-campaign-check" >&2
    exit 64
  fi
  "$repo_root/gradlew" :nereus-metadata-oxia:realAllocatorV2PreCampaignCheck \
    "-PpulsarCheckout=$protocol_pulsar_checkout" \
    --rerun-tasks \
    --console=plain \
    --no-configuration-cache
  exit 0
fi

if [[ "${1:-}" == "--validate-checkpoint" ]]; then
  if (( $# != 2 )); then
    echo "usage: $0 --validate-checkpoint checkpoint.nacp" >&2
    exit 64
  fi
  require_protocol_binding
  "$repo_root/gradlew" :nereus-metadata-oxia:validateRealAllocatorV2Checkpoint \
    "${protocol_gradle_properties[@]}" \
    "-Pv2M3AllocatorV2CheckpointPath=$2" \
    --console=plain \
    --no-configuration-cache
  exit 0
fi

if [[ "${1:-}" == "--seal-evaluation" ]]; then
  if (( $# != 3 )); then
    echo "usage: $0 --seal-evaluation checkpoint.nacp evaluation.naev" >&2
    exit 64
  fi
  require_protocol_binding
  "$repo_root/gradlew" :nereus-metadata-oxia:sealRealAllocatorV2Evaluation \
    "${protocol_gradle_properties[@]}" \
    "-Pv2M3AllocatorV2CheckpointPath=$2" \
    "-Pv2M3AllocatorV2EvaluationOutput=$3" \
    --console=plain \
    --no-configuration-cache
  exit 0
fi

if [[ "${1:-}" == "--promotion-check" ]]; then
  if (( $# != 8 )); then
    echo "usage: $0 --promotion-check evaluation.naev checkpoint.nacp diagnostic.nadv diagnostic-junit.xml formal-junit.xml attachments-dir decision.json" >&2
    exit 64
  fi
  require_protocol_binding
  "$repo_root/gradlew" :nereus-metadata-oxia:realAllocatorV2PromotionCheck \
    "${protocol_gradle_properties[@]}" \
    "-Pv2M3AllocatorV2EvaluationPath=$2" \
    "-Pv2M3AllocatorV2CheckpointPath=$3" \
    "-Pv2M3AllocatorV2DiagnosticPath=$4" \
    "-Pv2M3AllocatorV2DiagnosticJUnitPath=$5" \
    "-Pv2M3AllocatorV2FormalJUnitPath=$6" \
    "-Pv2M3AllocatorV2AttachmentDirectory=$7" \
    "-Pv2M3AllocatorV2PromotionOutput=$8" \
    --console=plain \
    --no-configuration-cache
  exit 0
fi

echo "full allocator execution is disabled: complete ADR-0104 V2 pre-campaign and short real-Oxia gates first" >&2
echo "the immutable V1 runner below is diagnostic compatibility code and cannot be resumed or promoted" >&2
exit 64

pulsar_checkout="${1:-/Users/liusinan/apps/ideaproject/nereusstream/pulsar-worktrees/nereus-v2-m3}"
oxia_server_checkout="${2:-/Users/liusinan/apps/ideaproject/nereusstream/oxia-worktrees/nereus-v2-m3}"
oxia_client_checkout="${3:-/Users/liusinan/apps/ideaproject/nereusstream/oxia-client-java-worktrees/nereus-v2-m3}"
oxia_image="${NEREUS_M3_ALLOCATOR_OXIA_IMAGE:-nereus/oxia-m3-allocator:37a17bef1720}"
expected_nereus="${NEREUS_M3_ALLOCATOR_EXPECTED_NEREUS_COMMIT:?set exact clean origin/main commit}"
output_directory="${NEREUS_M3_ALLOCATOR_OUTPUT_DIRECTORY:-$repo_root/nereus-metadata-oxia/build/m3-allocator-evidence/formal/$expected_nereus}"
preflight_output="${NEREUS_M3_ALLOCATOR_PREFLIGHT_OUTPUT:-$repo_root/nereus-metadata-oxia/build/m3-allocator-evidence/preflight/$expected_nereus.json}"
executor_manifest="${NEREUS_M3_ALLOCATOR_EXECUTOR_MANIFEST:-$repo_root/nereus-metadata-oxia/build/m3-allocator-evidence/executor/$expected_nereus.json}"

expected_oxia_server="37a17bef17202d5fd6e23282da5fd26d94865484"
expected_oxia_client="091a42c2780d92da56e9ec1f02ce1c3d988adc16"
expected_oxia_image_id="sha256:7eef9af2cdc897fbf418bf7616da1387aca87ce860b8205395cdf88b867df4da"
client_jar="$repo_root/gradle/locked-artifacts/oxia-client-java/$expected_oxia_client/m2/io/github/oxia-db/oxia-client/0.9.4/oxia-client-0.9.4.jar"

test ! -e "$output_directory"
test ! -e "$preflight_output"
test ! -e "$executor_manifest"

"$repo_root/scripts/build-v2-m3-allocator-oxia-image.sh" "$oxia_server_checkout"

NEREUS_M3_ALLOCATOR_EXPECTED_NEREUS_COMMIT="$expected_nereus" \
NEREUS_M3_ALLOCATOR_PREFLIGHT_OUTPUT="$preflight_output" \
  "$repo_root/scripts/run-v2-m3-real-allocator-evidence-preflight.sh" \
  "$pulsar_checkout" \
  "$oxia_server_checkout" \
  "$oxia_client_checkout"
jq -e '.admitted == true and .status == "ADMITTED_FORMAL_PRE_RUN" and (.blockers | length) == 0' \
  "$preflight_output" >/dev/null

test -f "$pulsar_checkout/settings.gradle.kts"
test -f "$oxia_server_checkout/Dockerfile"
test -f "$client_jar"

pulsar_commit="$(git -C "$pulsar_checkout" rev-parse HEAD)"
oxia_server_commit="$(git -C "$oxia_server_checkout" rev-parse HEAD)"

assert_source_state() {
  test "$(git -C "$repo_root" rev-parse HEAD)" = "$expected_nereus"
  test "$(git -C "$repo_root" branch --show-current)" = main
  test "$(git -C "$repo_root" rev-parse refs/remotes/origin/main)" = "$expected_nereus"
  test "$(git -C "$repo_root" status --porcelain --untracked-files=all)" = ""
  test "$(git -C "$pulsar_checkout" rev-parse HEAD)" = "$pulsar_commit"
  test "$(git -C "$pulsar_checkout" branch --show-current)" = nereus/v2-m3-object-wal-evidence
  test "$(git -C "$pulsar_checkout" rev-parse refs/remotes/origin/nereus/v2-m3-object-wal-evidence)" = "$pulsar_commit"
  test "$(git -C "$pulsar_checkout" status --porcelain --untracked-files=all)" = ""
  test "$(git -C "$oxia_server_checkout" rev-parse HEAD)" = "$expected_oxia_server"
  test "$(git -C "$oxia_server_checkout" branch --show-current)" = nereus/v2-m3-object-wal-evidence
  test "$(git -C "$oxia_server_checkout" rev-parse refs/remotes/origin/main)" = "$expected_oxia_server"
  test "$(git -C "$oxia_server_checkout" status --porcelain --untracked-files=all)" = ""
  test "$(git -C "$oxia_client_checkout" rev-parse HEAD)" = "$expected_oxia_client"
  test "$(git -C "$oxia_client_checkout" branch --show-current)" = nereus/v2-m3-object-wal-evidence
  test "$(git -C "$oxia_client_checkout" \
    rev-parse refs/remotes/origin/nereus/v2-m1.1a-o1-notification-continuity)" = "$expected_oxia_client"
  test "$(git -C "$oxia_client_checkout" status --porcelain --untracked-files=all)" = ""
}
assert_source_state

image_id="$(docker image inspect "$oxia_image" --format '{{.Id}}')"
image_revision="$(docker image inspect "$oxia_image" --format '{{index .Config.Labels "org.opencontainers.image.revision"}}')"
image_source="$(docker image inspect "$oxia_image" --format '{{index .Config.Labels "org.opencontainers.image.source"}}')"
image_platform="$(docker image inspect "$oxia_image" --format '{{.Os}}/{{.Architecture}}')"
test "$image_revision" = "$expected_oxia_server"
test "$image_id" = "$expected_oxia_image_id"
test "$image_source" = "https://github.com/oxia-db/oxia.git"
test "$image_platform" = "linux/arm64"
docker run --rm "$oxia_image" /oxia/bin/oxia --version | grep -F 'oxia version 0.16.3-167-g37a17bef'

container_name="nereus-m3-allocator-oxia-$$"
cleanup_container() {
  docker rm -f "$container_name" >/dev/null 2>&1 || true
}
trap cleanup_container EXIT INT TERM
docker run --detach \
  --name "$container_name" \
  --label com.nereusstream.evidence=v2-m3-real-allocator \
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
  if test "$attempt" = "60"; then
    docker logs "$container_name"
    exit 1
  fi
  sleep 1
done
oxia_service_address="127.0.0.1:$oxia_service_port"

nereus_commit="$(git -C "$repo_root" rev-parse HEAD)"
source_locks_sha="$(shasum -a 256 "$repo_root/docs/v2/source-locks.json" | awk '{print $1}')"
client_jar_sha="$(shasum -a 256 "$client_jar" | awk '{print $1}')"
test "$client_jar_sha" = "0ca719e6d11bd2ee2c2e7e94b42c6843e60f776bea12f7b5814cff9928e2e4c5"

"$repo_root/gradlew" \
  :nereus-domain:jar \
  :nereus-metadata-spi:jar \
  :nereus-metadata-oxia:jar \
  :nereus-metadata-oxia:realAllocatorEvidenceArtifactJar \
  :nereus-metadata-oxia:writeRealAllocatorEvidenceRuntimeClasspath \
  -PpulsarCheckout="$pulsar_checkout" \
  --console=plain \
  --no-configuration-cache

runtime_classpath="$repo_root/nereus-metadata-oxia/build/m3-allocator-evidence/runtime-classpath.txt"
test -s "$runtime_classpath"
artifact_from_runtime_classpath() {
  local exact_directory="$1"
  local basename_prefix="$2"
  local matches
  local count
  matches="$(awk -v directory="$exact_directory/" -v prefix="$basename_prefix" '
    index($0, directory) == 1 {
      n = split($0, parts, "/")
      if (index(parts[n], prefix) == 1 && parts[n] ~ /\.jar$/ && parts[n] !~ /-sources\.jar$/) print $0
    }
  ' "$runtime_classpath")"
  count="$(printf '%s\n' "$matches" | awk 'NF { count++ } END { print count + 0 }')"
  test "$count" = 1
  printf '%s\n' "$matches"
}

tested_evidence_artifact="$(artifact_from_runtime_classpath \
  "$repo_root/nereus-metadata-oxia/build/libs" \
  "nereus-v2-m3-real-allocator-evidence-")"
runtime_domain_artifact="$(artifact_from_runtime_classpath \
  "$repo_root/nereus-domain/build/libs" \
  "nereus-domain-")"
runtime_metadata_spi_artifact="$(artifact_from_runtime_classpath \
  "$repo_root/nereus-metadata-spi/build/libs" \
  "nereus-metadata-spi-")"
runtime_metadata_oxia_artifact="$(artifact_from_runtime_classpath \
  "$repo_root/nereus-metadata-oxia/build/libs" \
  "nereus-metadata-oxia-")"

tested_evidence_artifact_sha="$(shasum -a 256 "$tested_evidence_artifact" | awk '{print $1}')"
runtime_domain_artifact_sha="$(shasum -a 256 "$runtime_domain_artifact" | awk '{print $1}')"
runtime_metadata_spi_artifact_sha="$(shasum -a 256 "$runtime_metadata_spi_artifact" | awk '{print $1}')"
runtime_metadata_oxia_artifact_sha="$(shasum -a 256 "$runtime_metadata_oxia_artifact" | awk '{print $1}')"
preflight_sha="$(shasum -a 256 "$preflight_output" | awk '{print $1}')"
runtime_classpath_sha="$(shasum -a 256 "$runtime_classpath" | awk '{print $1}')"
classpath_jsonl="$(mktemp "${TMPDIR:-/tmp}/nereus-m3-allocator-classpath.XXXXXX")"
cleanup_classpath_jsonl() {
  rm -f "$classpath_jsonl"
}
trap 'cleanup_classpath_jsonl; cleanup_container' EXIT INT TERM
classpath_ordinal=0
while IFS= read -r entry; do
  test -f "$entry"
  entry_sha="$(shasum -a 256 "$entry" | awk '{print $1}')"
  entry_bytes="$(wc -c < "$entry" | tr -d ' ')"
  entry_basename="${entry##*/}"
  jq -nc \
    --argjson ordinal "$classpath_ordinal" \
    --arg basename "$entry_basename" \
    --argjson bytes "$entry_bytes" \
    --arg sha256 "$entry_sha" \
    '{ordinal: $ordinal, basename: $basename, bytes: $bytes, sha256: $sha256}' >>"$classpath_jsonl"
  classpath_ordinal=$((classpath_ordinal + 1))
done <"$runtime_classpath"
test "$classpath_ordinal" -gt 0
mkdir -p "$(dirname "$executor_manifest")"
jq -s \
  --arg schema "NEREUS_V2_M3_ALLOCATOR_EXECUTOR_MANIFEST_V1" \
  --arg nereusCommit "$nereus_commit" \
  --arg pulsarCommit "$pulsar_commit" \
  --arg preflightBasename "${preflight_output##*/}" \
  --arg preflightSha256 "$preflight_sha" \
  --arg runtimeClasspathSha256 "$runtime_classpath_sha" \
  '{
    schema: $schema,
    nereusCommit: $nereusCommit,
    pulsarCommit: $pulsarCommit,
    preflight: {basename: $preflightBasename, sha256: $preflightSha256},
    runtimeClasspathSha256: $runtimeClasspathSha256,
    orderedRuntimeArtifacts: .
  }' "$classpath_jsonl" >"$executor_manifest"
jq -e --arg nereus "$nereus_commit" --arg pulsar "$pulsar_commit" \
  '.nereusCommit == $nereus
   and .pulsarCommit == $pulsar
   and (.orderedRuntimeArtifacts | length) > 0
   and any(.orderedRuntimeArtifacts[]; .basename | startswith("nereus-v2-m3-real-allocator-evidence-"))
   and any(.orderedRuntimeArtifacts[]; .basename | startswith("nereus-domain-"))
   and any(.orderedRuntimeArtifacts[]; .basename | startswith("nereus-metadata-spi-"))
   and any(.orderedRuntimeArtifacts[]; .basename | startswith("nereus-metadata-oxia-"))
   and any(.orderedRuntimeArtifacts[]; .basename | startswith("managed-ledger-"))
   and any(.orderedRuntimeArtifacts[]; .basename | startswith("testmocks-"))' \
  "$executor_manifest" >/dev/null
executor_manifest_sha="$(shasum -a 256 "$executor_manifest" | awk '{print $1}')"

assert_source_state
"$repo_root/gradlew" \
  :nereus-metadata-oxia:checkstyleRealAllocatorTest \
  :nereus-metadata-oxia:realAllocatorEvidenceFormalCheck \
  -PpulsarCheckout="$pulsar_checkout" \
  -Pv2M3AllocatorOutputDirectory="$output_directory" \
  -Pv2M3AllocatorNereusSourceCommit="$nereus_commit" \
  -Pv2M3AllocatorPulsarSourceCommit="$pulsar_commit" \
  -Pv2M3AllocatorOxiaClientSourceCommit="$expected_oxia_client" \
  -Pv2M3AllocatorOxiaClientJarSha256="$client_jar_sha" \
  -Pv2M3AllocatorOxiaServerSourceCommit="$oxia_server_commit" \
  -Pv2M3AllocatorTestedEvidenceArtifactSha256="$tested_evidence_artifact_sha" \
  -Pv2M3AllocatorRuntimeDomainArtifactSha256="$runtime_domain_artifact_sha" \
  -Pv2M3AllocatorRuntimeMetadataSpiArtifactSha256="$runtime_metadata_spi_artifact_sha" \
  -Pv2M3AllocatorRuntimeMetadataOxiaArtifactSha256="$runtime_metadata_oxia_artifact_sha" \
  -Pv2M3AllocatorOxiaServiceAddress="$oxia_service_address" \
  -Pv2M3AllocatorSourceLocksSha256="$source_locks_sha" \
  -Pv2M3AllocatorExecutorManifestSha256="$executor_manifest_sha" \
  -Pv2M3AllocatorOxiaClientJarPath="$client_jar" \
  -Pv2M3AllocatorExecutorManifestPath="$executor_manifest" \
  --console=plain \
  --no-configuration-cache
assert_source_state

evaluation="$output_directory/evaluation.json"
selection="$output_directory/selection.nars"
raw_verification="$output_directory/raw-verification.json"
jq -e --arg commit "$expected_nereus" --arg pulsar "$pulsar_commit" \
  '.schema == "NEREUS_V2_M3_ALLOCATOR_SEAL_V1"
   and .status == "SEALED_SELECTED"
   and .selectionEligible == true
   and (.selectedMode == "STRICT_SERIALIZED" or .selectedMode == "RANGE_LEASED")
   and .source.nereusCommit == $commit
   and .source.pulsarCommit == $pulsar
   and .junit.tests > 0
   and .junit.failures == 0
   and .junit.errors == 0
   and .junit.skips == 0
   and (.attachments | keys | sort) == (["fault.naea", "native.naea", "scale-10000.naea", "scale-100000.naea", "test.naea"] | sort)' \
  "$evaluation" >/dev/null
test "$(wc -c < "$selection" | tr -d ' ')" = 2328

python3 - "$raw_verification" "$output_directory/raw-verification-payload.json" <<'PY'
import hashlib
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
exact = path.read_bytes()
receipt = json.loads(exact)
assert receipt["schema"] == "NEREUS_V2_M3_ALLOCATOR_SEALED_VERIFICATION_V1"
assert receipt["selfHashRule"] == "SHA256_OF_EXACT_UTF8_WITH_SELF_SHA256_64_ZERO_HEX"
observed = receipt["selfSha256"]
assert len(observed) == 64
zeroed = exact.replace(observed.encode("ascii"), b"0" * 64, 1)
assert hashlib.sha256(zeroed).hexdigest() == observed
raw = receipt["rawVerification"]
raw_exact = pathlib.Path(sys.argv[2]).read_bytes()
assert receipt["rawVerificationBytes"] == len(raw_exact)
assert receipt["rawVerificationSha256"] == hashlib.sha256(raw_exact).hexdigest()
raw_from_file = json.loads(raw_exact)
assert raw_from_file == raw
assert raw["schema"] == "NEREUS_V2_M3_ALLOCATOR_RAW_RECOMPUTATION_V1"
assert raw["selfHashRule"] == "SHA256_OF_EXACT_UTF8_WITH_SELF_SHA256_64_ZERO_HEX"
raw_observed = raw["selfSha256"]
assert len(raw_observed) == 64
raw_zeroed = raw_exact.replace(raw_observed.encode("ascii"), b"0" * 64, 1)
assert hashlib.sha256(raw_zeroed).hexdigest() == raw_observed
assert raw["status"] == "PASS_RAW_RECOMPUTED"
assert raw["derived"] == {"intervals": 288, "faultCutKinds": 9, "selectedRows": 8}
assert raw["junit"]["tests"] > 0
assert raw["junit"]["failures"] == raw["junit"]["errors"] == raw["junit"]["skips"] == 0
assert receipt["verifierJUnit"]["tests"] == 1
assert receipt["verifierJUnit"]["failures"] == 0
assert receipt["verifierJUnit"]["errors"] == 0
assert receipt["verifierJUnit"]["skips"] == 0
PY

for attachment in native.naea fault.naea scale-10000.naea scale-100000.naea test.naea; do
  test -s "$output_directory/$attachment"
  observed_sha="$(shasum -a 256 "$output_directory/$attachment" | awk '{print $1}')"
  test "$observed_sha" = "$(jq -r --arg attachment "$attachment" '.attachments[$attachment]' "$evaluation")"
done

printf '%s\n' "$evaluation"
shasum -a 256 \
  "$selection" \
  "$output_directory"/*.naea \
  "$raw_verification" \
  "$executor_manifest" \
  "$preflight_output"
