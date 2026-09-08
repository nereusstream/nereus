#!/usr/bin/env bash
set -euo pipefail
export LC_ALL=C
export LANG=C

m5_repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
m5_image="nereus/oxia-m3-allocator:37a17bef1720"
m5_expected_image_id="sha256:7eef9af2cdc897fbf418bf7616da1387aca87ce860b8205395cdf88b867df4da"
m5_expected_server_source="37a17bef17202d5fd6e23282da5fd26d94865484"
m5_expected_client_jar_sha="0ca719e6d11bd2ee2c2e7e94b42c6843e60f776bea12f7b5814cff9928e2e4c5"
m5_client_jar="$m5_repo_root/gradle/locked-artifacts/oxia-client-java/091a42c2780d92da56e9ec1f02ce1c3d988adc16/m2/io/github/oxia-db/oxia-client/0.9.4/oxia-client-0.9.4.jar"

test "$(docker image inspect "$m5_image" --format '{{.Id}}')" = "$m5_expected_image_id"
test "$(docker image inspect "$m5_image" --format '{{.Os}}/{{.Architecture}}')" = "linux/arm64"
test "$(docker image inspect "$m5_image" --format '{{index .Config.Labels "org.opencontainers.image.revision"}}')" = \
  "$m5_expected_server_source"
test "$(shasum -a 256 "$m5_client_jar" | awk '{print $1}')" = "$m5_expected_client_jar_sha"
test "$(docker run --rm "$m5_image" /oxia/bin/oxia --version)" = "oxia version 0.16.3-167-g37a17bef"

m5_container="nereus-m5-read-fenced-oxia-$$"
m5_output="$m5_repo_root/build/m5-read-fenced-oxia/$m5_container"
mkdir -p "$m5_output"
m5_cleanup() {
  docker rm -f "$m5_container" >/dev/null 2>&1 || true
}
trap m5_cleanup EXIT INT TERM

docker run --detach \
  --name "$m5_container" \
  --label com.nereusstream.evidence=v2-m5-read-fenced-implementation \
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
paths.extend((root / "nereus-storage-object/src/test/java/com/nereusstream/storage/object/gc").glob("*.java"))
paths.extend((root / "nereus-metadata-oxia/src/test/java/com/nereusstream/metadata/oxia/v2/retention").glob("*.java"))
paths.extend((root / "scripts").glob("check-v2-m5-*.py"))
paths.append(root / "docs/v2/detailed_design/m5/m5-read-fenced-recovery-projection.json")
paths.append(root / "nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/compaction/KafkaBookKeeperDeleteIdentityReaderV2.java")
paths.extend(root / name for name in ("gradle/libs.versions.toml", "settings.gradle.kts", "build.gradle.kts", "nereus-storage-object/build.gradle.kts",
    "nereus-metadata-oxia/build.gradle.kts", "scripts/run-v2-m5-read-fenced-oxia-check.sh"))
source = {str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(paths)}
(output / "tested-inputs.json").write_text(json.dumps(source, indent=2) + "\n")
PY_SOURCE

"$m5_repo_root/gradlew" \
  --no-daemon --no-configuration-cache --no-parallel --max-workers=2 --rerun-tasks \
  "-Pv2M5RetentionOxiaServiceAddress=127.0.0.1:$m5_service_port" \
  "-Pv2M5ReadFencedOxiaRestartCheckpoint=$m5_output/restart-checkpoint.txt" \
  v2M5ReadFencedOxiaCheck \
  :nereus-metadata-oxia:v2M5ReadFencedOxiaRestartWriteTest \
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
  --no-daemon --no-configuration-cache --no-parallel --max-workers=2 \
  "-Pv2M5RetentionOxiaServiceAddress=127.0.0.1:$m5_service_port" \
  "-Pv2M5ReadFencedOxiaRestartCheckpoint=$m5_output/restart-checkpoint.txt" \
  :nereus-metadata-oxia:v2M5ReadFencedOxiaRestartReadTest \
  --console=plain

docker logs "$m5_container" > "$m5_output/server.log" 2>&1
python3 - "$m5_repo_root" "$m5_output" "$m5_container_id" "$m5_started_before" "$m5_started_after" <<'PY'
import hashlib
import json
from pathlib import Path
import sys
import xml.etree.ElementTree as ET
root, output = map(Path, sys.argv[1:3])
inputs = (output / "tested-inputs.json").read_bytes()
for path, expected in json.loads(inputs).items():
    if hashlib.sha256((root / path).read_bytes()).hexdigest() != expected:
        raise SystemExit("native Java input changed during real history execution: " + path)
results = []
for module, task, name, count in (
    ("nereus-storage-object", "v2M5TargetDeleteAuthorityCoordinatorTest", "com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCoordinatorV1Test", 35),
    ("nereus-storage-object", "v2M5DeleteEligibilityTest", "com.nereusstream.storage.object.gc.M5DeleteEligibilityV2Test", 10),
    ("nereus-metadata-oxia", "v2M5TargetDeleteRouteTest", "com.nereusstream.metadata.oxia.v2.retention.OxiaTargetDeleteAuthorityStoreV2Test", 4),
    ("nereus-metadata-oxia", "v2M5ReadFencedRealOxiaTest", "com.nereusstream.metadata.oxia.v2.retention.M5ReadFencedOxiaIntegrationTest", 6),
    ("nereus-metadata-oxia", "v2M5ReadFencedOxiaRestartWriteTest", "com.nereusstream.metadata.oxia.v2.retention.M5ReadFencedOxiaRestartTest", 1),
    ("nereus-metadata-oxia", "v2M5ReadFencedOxiaRestartReadTest", "com.nereusstream.metadata.oxia.v2.retention.M5ReadFencedOxiaRestartTest", 1),
):
    path = root / f"{module}/build/test-results/{task}/TEST-{name}.xml"
    data = path.read_bytes()
    suite = ET.fromstring(data)
    if {key: suite.attrib.get(key) for key in ("tests", "failures", "errors", "skipped")} != {
        "tests": str(count), "failures": "0", "errors": "0", "skipped": "0"
    }:
        raise SystemExit(f"real Oxia JUnit summary differs: {task}")
    (output / f"{task}.xml").write_bytes(data)
    results.append({"task": task, "tests": count, "xmlSha256": hashlib.sha256(data).hexdigest()})
summary = {
    "schema": "NEREUS_V2_M5_READ_FENCED_OXIA_RUN_V2",
    "containerId": sys.argv[3], "startedBefore": sys.argv[4], "startedAfter": sys.argv[5],
    "sameServerContainerRestarted": True,
    "testedInputManifestSha256": hashlib.sha256(inputs).hexdigest(),
    "capturedInputsUnchangedDuringRun": True,
    "serverImageId": "sha256:7eef9af2cdc897fbf418bf7616da1387aca87ce860b8205395cdf88b867df4da",
    "serverSource": "37a17bef17202d5fd6e23282da5fd26d94865484",
    "clientJarSha256": "0ca719e6d11bd2ee2c2e7e94b42c6843e60f776bea12f7b5814cff9928e2e4c5",
    "sourceAndReferenceAuthority": "SYNTHETIC_PROOF_FACTS_WITH_REAL_NATIVE_METADATA",
    "responseFaults": "POST_NATIVE_MUTATION_DELIVERY_LOSS_AND_PRE_CAS_CALLBACK_HOLD",
    "factKeysVersionsAndHashesAreNative": True,
    "appliedRefreshDeliveryLossReconciled": True,
    "heldOldRefreshLosesNativeCas": True,
    "sameBytesNewFactVersionVetoesIntent": True,
    "missingNativeOwnerVerifierRejectsRecovery": True,
    "readFenceSurvivedServerRestart": True,
    "durableRecoveryVetoSurvivedServerRestart": True,
    "qualifiedRefreshClearsPersistedVeto": True,
    "lateRejectedValidationCannotOverwriteWinningRefresh": True,
    "ordinaryV4AuthorityBytesPreserved": True,
    "vetoWireVersion": 5,
    "typedIntentWireVersion": 6,
    "typedIntentSurvivedServerRestart": True,
    "typedIntentLateReadCannotOverwriteWinner": True,
    "typedIntentPostReadFactChangeRejected": True,
    "maximumVetoEncodedBytes": 73,
    "checkpointContainsAuthorityBodies": False,
    "nativeProtocolOwnerAdaptersIntegrated": False,
    "externalFullIdentityReaderIntegrated": False,
    "actualProviderOrBookKeeperDeletionVerified": False,
    "physicalDeleteAuthority": False,
    "suites": results, "m5Receipt": False, "m5FinalAuthority": False, "productionAuthority": False,
}
(output / "run-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
PY
printf 'PASS_V2_M5_READ_FENCED_REAL_OXIA_NON_PROMOTABLE image=%s id=%s output=%s\n' \
  "$m5_image" "$m5_expected_image_id" "$m5_output"
