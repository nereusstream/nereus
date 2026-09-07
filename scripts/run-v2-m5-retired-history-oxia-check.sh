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

if ! docker image inspect "$m5_image" >/dev/null 2>&1; then
  "$m5_repo_root/scripts/build-v2-m3-allocator-oxia-image.sh" "$m5_oxia_checkout" >/dev/null
fi
test "$(docker image inspect "$m5_image" --format '{{.Id}}')" = "$m5_expected_image_id"
test "$(docker image inspect "$m5_image" --format '{{.Os}}/{{.Architecture}}')" = "linux/arm64"
test "$(docker image inspect "$m5_image" --format '{{index .Config.Labels "org.opencontainers.image.revision"}}')" = \
  "$m5_expected_server_source"
test "$(shasum -a 256 "$m5_client_jar" | awk '{print $1}')" = "$m5_expected_client_jar_sha"
test "$(docker run --rm "$m5_image" /oxia/bin/oxia --version)" = "oxia version 0.16.3-167-g37a17bef"

m5_container="nereus-m5-history-oxia-$$"
m5_output="$m5_repo_root/build/m5-retired-history-oxia/$m5_container"
mkdir -p "$m5_output"
m5_cleanup() {
  docker rm -f "$m5_container" >/dev/null 2>&1 || true
}
trap m5_cleanup EXIT INT TERM

docker run --detach \
  --name "$m5_container" \
  --label com.nereusstream.evidence=v2-m5-history-implementation \
  --publish 127.0.0.1::6648 \
  --publish 127.0.0.1::8080 \
  "$m5_image" \
  oxia standalone --shards=4 >/dev/null
m5_container_id="$(docker inspect --format '{{.Id}}' "$m5_container")"
test "$(docker inspect --format '{{.Image}}' "$m5_container")" = "$m5_expected_image_id"
m5_started_before="$(docker inspect --format '{{.State.StartedAt}}' "$m5_container")"
m5_service_port="$(docker port "$m5_container" 6648/tcp | awk -F: '{print $NF}')"
m5_metrics_port="$(docker port "$m5_container" 8080/tcp | awk -F: '{print $NF}')"
m5_wait_ready() {
  for m5_attempt in $(seq 1 60); do
    if curl --fail --silent "http://127.0.0.1:$m5_metrics_port/metrics" > "$m5_output/metrics.txt"; then
      return
    fi
    if test "$m5_attempt" = 60; then
      docker logs "$m5_container"
      return 1
    fi
    sleep 1
  done
}
m5_wait_ready

python3 - "$m5_repo_root" "$m5_output" <<'PY_SOURCE'
import hashlib
import json
from pathlib import Path
import sys
root, output = map(Path, sys.argv[1:])
paths = []
for module in ("nereus-domain", "nereus-storage-api", "nereus-metadata-spi", "nereus-storage-object", "nereus-metadata-oxia"):
    paths.extend((root / module / "src/main/java").rglob("*.java"))
paths.extend((root / "nereus-metadata-oxia/src/oxiaIntegrationTest/java/com/nereusstream/metadata/oxia/v2/retention").glob("*.java"))
source = {str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(paths)}
(output / "tested-java-inputs.json").write_text(json.dumps(source, indent=2) + "\n")
PY_SOURCE

"$m5_repo_root/gradlew" \
  --no-daemon --no-configuration-cache --rerun-tasks \
  "-Pv2M5RetentionOxiaServiceAddress=127.0.0.1:$m5_service_port" \
  "-Pv2M5HistoryOxiaRestartCheckpoint=$m5_output/restart-checkpoint.txt" \
  v2M5RetiredHistoryOxiaCheck \
  :nereus-metadata-oxia:v2M5RetiredHistoryOxiaRestartWriteTest \
  --console=plain

docker restart "$m5_container" >/dev/null
test "$(docker inspect --format '{{.Id}}' "$m5_container")" = "$m5_container_id"
test "$(docker inspect --format '{{.Image}}' "$m5_container")" = "$m5_expected_image_id"
m5_started_after="$(docker inspect --format '{{.State.StartedAt}}' "$m5_container")"
test "$m5_started_before" != "$m5_started_after"
m5_service_port="$(docker port "$m5_container" 6648/tcp | awk -F: '{print $NF}')"
m5_metrics_port="$(docker port "$m5_container" 8080/tcp | awk -F: '{print $NF}')"
m5_wait_ready

"$m5_repo_root/gradlew" \
  --no-daemon --no-configuration-cache \
  "-Pv2M5RetentionOxiaServiceAddress=127.0.0.1:$m5_service_port" \
  "-Pv2M5HistoryOxiaRestartCheckpoint=$m5_output/restart-checkpoint.txt" \
  :nereus-metadata-oxia:v2M5RetiredHistoryOxiaRestartReadTest \
  --console=plain

docker logs "$m5_container" > "$m5_output/server.log" 2>&1
python3 - "$m5_repo_root" "$m5_output" "$m5_container_id" "$m5_started_before" "$m5_started_after" <<'PY'
import hashlib
import json
from pathlib import Path
import sys
import xml.etree.ElementTree as ET
root, output = map(Path, sys.argv[1:3])
inputs = (output / "tested-java-inputs.json").read_bytes()
for path, expected in json.loads(inputs).items():
    if hashlib.sha256((root / path).read_bytes()).hexdigest() != expected:
        raise SystemExit("native Java input changed during real history execution: " + path)
results = []
for task, name, count in (
    ("v2M5RetiredHistoryRealOxiaTest", "M5RetiredHistoryOxiaIntegrationTest", 5),
    ("v2M5RetiredHistoryOxiaRestartWriteTest", "M5RetiredHistoryOxiaRestartTest", 1),
    ("v2M5RetiredHistoryOxiaRestartReadTest", "M5RetiredHistoryOxiaRestartTest", 1),
    ("v2M5RetentionRealOxiaTest", "M5RetentionOxiaIntegrationTest", 2),
):
    path = root / f"nereus-metadata-oxia/build/test-results/{task}/TEST-com.nereusstream.metadata.oxia.v2.retention.{name}.xml"
    data = path.read_bytes()
    suite = ET.fromstring(data)
    if {key: suite.attrib.get(key) for key in ("tests", "failures", "errors", "skipped")} != {
        "tests": str(count), "failures": "0", "errors": "0", "skipped": "0"
    }:
        raise SystemExit(f"real Oxia JUnit summary differs: {task}")
    (output / f"{task}.xml").write_bytes(data)
    results.append({"task": task, "tests": count, "xmlSha256": hashlib.sha256(data).hexdigest()})
summary = {
    "schema": "NEREUS_V2_M5_RETIRED_HISTORY_OXIA_RUN_V1",
    "containerId": sys.argv[3], "startedBefore": sys.argv[4], "startedAfter": sys.argv[5],
    "sameServerContainerRestarted": True,
    "testedJavaInputManifestSha256": hashlib.sha256(inputs).hexdigest(),
    "nativeJavaInputsUnchangedDuringRun": True,
    "serverImageId": "sha256:7eef9af2cdc897fbf418bf7616da1387aca87ce860b8205395cdf88b867df4da",
    "serverSource": "37a17bef17202d5fd6e23282da5fd26d94865484",
    "clientJarSha256": "0ca719e6d11bd2ee2c2e7e94b42c6843e60f776bea12f7b5814cff9928e2e4c5",
    "sourceAndReferenceAuthority": "SYNTHETIC_PROOF_FACTS_WITH_REAL_NATIVE_METADATA",
    "responseFaults": "POST_NATIVE_MUTATION_CLIENT_DELIVERY_INJECTION",
    "continuousRetirements": 1026, "activeHoles": 1, "selectorBytesStrictlyBelow": 2048,
    "suites": results, "m5Receipt": False, "m5FinalAuthority": False, "productionAuthority": False,
}
(output / "run-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
PY
printf 'PASS_V2_M5_RETIRED_HISTORY_REAL_OXIA_NON_PROMOTABLE image=%s id=%s output=%s\n' \
  "$m5_image" "$m5_expected_image_id" "$m5_output"
