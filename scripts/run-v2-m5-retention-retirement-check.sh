#!/usr/bin/env bash
set -euo pipefail
export LC_ALL=C
export LANG=C

m5_repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
m5_oxia_checkout="${1:-/Users/liusinan/apps/ideaproject/nereusstream/oxia-worktrees/nereus-v2-m3}"
m5_image="nereus/oxia-m3-allocator:37a17bef1720"
m5_expected_image_id="sha256:7eef9af2cdc897fbf418bf7616da1387aca87ce860b8205395cdf88b867df4da"
m5_expected_server_source="37a17bef17202d5fd6e23282da5fd26d94865484"
m5_expected_client_jar_sha="0ca719e6d11bd2ee2c2e7e94b42c6843e60f776bea12f7b5814cff9928e2e4c5"
m5_client_jar="$m5_repo_root/gradle/locked-artifacts/oxia-client-java/091a42c2780d92da56e9ec1f02ce1c3d988adc16/m2/io/github/oxia-db/oxia-client/0.9.4/oxia-client-0.9.4.jar"

"$m5_repo_root/scripts/build-v2-m3-allocator-oxia-image.sh" "$m5_oxia_checkout" >/dev/null
test "$(docker image inspect "$m5_image" --format '{{.Id}}')" = "$m5_expected_image_id"
test "$(docker image inspect "$m5_image" --format '{{.Os}}/{{.Architecture}}')" = "linux/arm64"
test "$(docker image inspect "$m5_image" --format '{{index .Config.Labels "org.opencontainers.image.revision"}}')" = \
  "$m5_expected_server_source"
test "$(shasum -a 256 "$m5_client_jar" | awk '{print $1}')" = "$m5_expected_client_jar_sha"
test "$(docker run --rm "$m5_image" /oxia/bin/oxia --version)" = "oxia version 0.16.3-167-g37a17bef"

m5_container="nereus-m5-retention-oxia-$$"
m5_cleanup() {
  docker rm -f "$m5_container" >/dev/null 2>&1 || true
}
trap m5_cleanup EXIT INT TERM

docker run --detach \
  --name "$m5_container" \
  --label com.nereusstream.evidence=v2-m5-retention-implementation \
  --publish 127.0.0.1::6648 \
  --publish 127.0.0.1::8080 \
  "$m5_image" \
  oxia standalone --shards=4 >/dev/null
m5_service_port="$(docker port "$m5_container" 6648/tcp | awk -F: '{print $NF}')"
m5_metrics_port="$(docker port "$m5_container" 8080/tcp | awk -F: '{print $NF}')"

for m5_attempt in $(seq 1 60); do
  if curl --fail --silent "http://127.0.0.1:$m5_metrics_port/metrics" >/dev/null; then
    break
  fi
  if test "$m5_attempt" = 60; then
    docker logs "$m5_container"
    exit 1
  fi
  sleep 1
done

"$m5_repo_root/gradlew" \
  --no-daemon \
  --no-configuration-cache \
  --rerun-tasks \
  "-Pv2M5RetentionOxiaServiceAddress=127.0.0.1:$m5_service_port" \
  v2M5RetentionRetirementCheck \
  --console=plain

printf 'PASS_V2_M5_RETENTION_RETIREMENT_REAL_OXIA implementation-only image=%s id=%s\n' \
  "$m5_image" \
  "$m5_expected_image_id"
