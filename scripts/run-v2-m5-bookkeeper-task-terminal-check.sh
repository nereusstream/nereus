#!/usr/bin/env bash
set -euo pipefail
export LC_ALL=C
export LANG=C
m5_repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
m5_compose="$m5_repo_root/config/v2/m2/kafka/k9/bookkeeper-conformance.compose.yml"
m5_project="nereus-v2-m5-bk-task-terminal-$$"
m5_container="$m5_project-oxia"
m5_output="$m5_repo_root/build/m5-bookkeeper-task-terminal/$m5_project"
m5_bk_image="apache/bookkeeper@sha256:c0a128931c402d6bf6a6f973ba2f305b9be261659e30754ab95a29510a33bc0d"
m5_bk_id="sha256:d0e78aaf987ac2feb526507ffb7d4c5137d58c0530f2a8cab4a9595abc89d605"
m5_oxia_image="nereus/oxia-m3-allocator:37a17bef1720"
m5_oxia_id="sha256:7eef9af2cdc897fbf418bf7616da1387aca87ce860b8205395cdf88b867df4da"
mkdir -p "$m5_output"
# Reuse only exact verified locked images. Missing image provisioning remains the existing source-locked workflow.
test "$(docker image inspect "$m5_bk_image" --format '{{.Id}}')" = "$m5_bk_id"
test "$(docker image inspect "$m5_bk_image" --format '{{.Os}}/{{.Architecture}}')" = "linux/amd64"
test "$(docker image inspect "$m5_oxia_image" --format '{{.Id}}')" = "$m5_oxia_id"
test "$(docker image inspect "$m5_oxia_image" --format '{{.Os}}/{{.Architecture}}')" = "linux/arm64"
test "$(docker image inspect "$m5_oxia_image" --format '{{index .Config.Labels "org.opencontainers.image.revision"}}')" = \
  "37a17bef17202d5fd6e23282da5fd26d94865484"
test "$(docker run --rm "$m5_oxia_image" /oxia/bin/oxia --version)" = "oxia version 0.16.3-167-g37a17bef"
m5_oxia_owned_id=""
m5_cleanup() {
  if test -n "$m5_oxia_owned_id"; then docker rm -f "$m5_oxia_owned_id" >/dev/null 2>&1 || true; fi
  docker compose -p "$m5_project" -f "$m5_compose" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap m5_cleanup EXIT INT TERM

docker compose -p "$m5_project" -f "$m5_compose" up -d --wait
for m5_service in metadata-service bookie-0 bookie-1 bookie-2; do
  m5_id="$(docker compose -p "$m5_project" -f "$m5_compose" ps -q "$m5_service")"
  test -n "$m5_id"
  test "$(docker inspect --format '{{.Config.Image}}' "$m5_id")" = "$m5_bk_image"
  test "$(docker inspect --format '{{.Image}}' "$m5_id")" = "$m5_bk_id"
done
m5_oxia_owned_id="$(docker run --detach --name "$m5_container" --label com.nereusstream.evidence=v2-m5-bk-task-terminal \
  --publish 127.0.0.1::6648 --publish 127.0.0.1::8080 "$m5_oxia_image" oxia standalone --shards=4)"
test "$(docker inspect --format '{{.Image}}' "$m5_oxia_owned_id")" = "$m5_oxia_id"
m5_started_before="$(docker inspect --format '{{.State.StartedAt}}' "$m5_oxia_owned_id")"
m5_wait_ready() {
  m5_port="$(docker port "$m5_oxia_owned_id" 6648/tcp | awk -F: '{print $NF}')"
  m5_metrics_port="$(docker port "$m5_oxia_owned_id" 8080/tcp | awk -F: '{print $NF}')"
  for m5_attempt in $(seq 1 60); do
    if curl --fail --silent "http://127.0.0.1:$m5_metrics_port/metrics" > "$m5_output/metrics.txt"; then return; fi
    if test "$m5_attempt" = 60; then docker logs "$m5_oxia_owned_id"; return 1; fi
    sleep 1
  done
}
m5_wait_ready
python3 - "$m5_repo_root" "$m5_output" <<'PY'
import hashlib,json,sys
from pathlib import Path
root,out=map(Path,sys.argv[1:]); paths=[]
for module in ('nereus-domain','nereus-storage-api','nereus-metadata-spi','nereus-storage-object',
               'nereus-storage-bookkeeper','nereus-kafka-bookkeeper','nereus-metadata-oxia'):
    paths.extend((root/module/'src/main/java').rglob('*.java'))
for source in ('test','realBookKeeperTest'):
    paths.extend((root/f'nereus-kafka-bookkeeper/src/{source}/java/com/nereusstream/kafka/bookkeeper/compaction').glob('*.java'))
paths.extend((root/'nereus-metadata-oxia/src/test/java/com/nereusstream/metadata/oxia/v2/compaction').glob('*.java'))
for source in ('test','realBookKeeperTest'):
    paths.extend((root/f'nereus-storage-bookkeeper/src/{source}/java').rglob('*.java'))
paths.extend((root/'nereus-storage-object/src/test/java/com/nereusstream/storage/object/retention').glob('*.java'))
paths.append(root/'nereus-storage-object/src/test/java/com/nereusstream/storage/object/materialization/M5MaterializationV1Test.java')
paths.extend((root/'nereus-metadata-oxia/src/test/java/com/nereusstream/metadata/oxia/v2/retention').glob('*.java'))
paths.extend([root/'nereus-storage-object/build.gradle.kts',root/'nereus-storage-bookkeeper/build.gradle.kts',
              root/'config/v2/m2/kafka/k9/bookkeeper-conformance.compose.yml',root/'build.gradle.kts',root/'nereus-kafka-bookkeeper/build.gradle.kts',
              root/'nereus-metadata-oxia/build.gradle.kts',root/'scripts/run-v2-m5-bookkeeper-task-terminal-check.sh'])
paths.extend([root/'scripts/check-v2-m5-bookkeeper-descriptor.py',
              root/'scripts/check-v2-m5-bookkeeper-descriptor-tests.py'])
manifest={str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(paths)}
(out/'tested-inputs.json').write_text(json.dumps(manifest,indent=2)+'\n')
PY
"$m5_repo_root/gradlew" --no-daemon --no-configuration-cache --rerun-tasks \
  "-Pv2M2BookKeeperMetadataServiceUri=zk://127.0.0.1:2181/ledgers" \
  "-Pv2M5RetentionOxiaServiceAddress=127.0.0.1:$m5_port" \
  "-Pv2M5BookKeeperOxiaRestartCheckpoint=$m5_output/restart-checkpoint.txt" \
  v2M5BookKeeperTaskTerminationCheck :nereus-kafka-bookkeeper:v2M5BookKeeperOxiaRestartWriteTest \
  :nereus-kafka-bookkeeper:v2M5BookKeeperNativeCreateRestartWriteTest \
  :nereus-kafka-bookkeeper:v2M5BookKeeperTaskTerminationRestartWriteTest --console=plain

docker compose -p "$m5_project" -f "$m5_compose" ps -q > "$m5_output/bookkeeper-container-ids.txt"
while IFS= read -r m5_id; do
  docker inspect --format '{{.Id}} {{.Image}} {{.State.StartedAt}}' "$m5_id"
done < "$m5_output/bookkeeper-container-ids.txt" > "$m5_output/bookkeeper-before-restart.txt"
docker compose -p "$m5_project" -f "$m5_compose" stop bookie-0 bookie-1 bookie-2
docker compose -p "$m5_project" -f "$m5_compose" restart metadata-service
docker compose -p "$m5_project" -f "$m5_compose" up -d --wait
while IFS= read -r m5_id; do
  docker inspect --format '{{.Id}} {{.Image}} {{.State.StartedAt}}' "$m5_id"
done < "$m5_output/bookkeeper-container-ids.txt" > "$m5_output/bookkeeper-after-restart.txt"
docker restart "$m5_oxia_owned_id" >/dev/null
test "$(docker inspect --format '{{.Id}}' "$m5_container")" = "$m5_oxia_owned_id"
test "$(docker inspect --format '{{.Image}}' "$m5_oxia_owned_id")" = "$m5_oxia_id"
m5_started_after="$(docker inspect --format '{{.State.StartedAt}}' "$m5_oxia_owned_id")"
test "$m5_started_before" != "$m5_started_after"
m5_wait_ready
"$m5_repo_root/gradlew" --no-daemon --no-configuration-cache \
  "-Pv2M2BookKeeperMetadataServiceUri=zk://127.0.0.1:2181/ledgers" \
  "-Pv2M5RetentionOxiaServiceAddress=127.0.0.1:$m5_port" \
  "-Pv2M5BookKeeperOxiaRestartCheckpoint=$m5_output/restart-checkpoint.txt" \
  :nereus-kafka-bookkeeper:v2M5BookKeeperOxiaRestartReadTest \
  :nereus-kafka-bookkeeper:v2M5BookKeeperNativeCreateRestartReadTest \
  :nereus-kafka-bookkeeper:v2M5BookKeeperTaskTerminationRestartReadTest --console=plain

docker logs "$m5_oxia_owned_id" > "$m5_output/oxia-server.log" 2>&1
docker compose -p "$m5_project" -f "$m5_compose" logs > "$m5_output/bookkeeper.log" 2>&1
python3 - "$m5_repo_root" "$m5_output" "$m5_oxia_owned_id" "$m5_started_before" "$m5_started_after" <<'PY'
import hashlib,json,sys,xml.etree.ElementTree as ET
from pathlib import Path
root,out=map(Path,sys.argv[1:3]); inputs=(out/'tested-inputs.json').read_bytes()
for path,sha in json.loads(inputs).items():
    if hashlib.sha256((root/path).read_bytes()).hexdigest()!=sha:
        raise SystemExit('Input changed during native execution: '+path)
before=[line.split() for line in (out/'bookkeeper-before-restart.txt').read_text().splitlines()]
after=[line.split() for line in (out/'bookkeeper-after-restart.txt').read_text().splitlines()]
if len(before)!=4 or len(after)!=4 or any(a[:2]!=b[:2] or a[2]==b[2] for a,b in zip(before,after)):
    raise SystemExit('Native BK restart did not retain all four exact containers and change their start times')
suites=[]
for module,task,name,count in (
    ('nereus-storage-object','v2M5TaskSelectionTest','M5TaskSelectionCoordinatorV2Test',7),
    ('nereus-storage-object','v2M5RetiredHistoryTest','M5RetiredBatchHistoryV2Test',15),
    ('nereus-metadata-oxia','v2M5TaskSelectionRouteTest','OxiaTaskSelectionRouteV2Test',3),
    ('nereus-kafka-bookkeeper','v2M5BookKeeperTaskTerminalTest','KafkaBookKeeperTaskTerminalV2Test',4),
    ('nereus-kafka-bookkeeper','v2M5BookKeeperTaskTerminationRealTest','KafkaBookKeeperTaskTerminationV2RealTest',5),
    ('nereus-kafka-bookkeeper','v2M5BookKeeperTaskTerminationRestartWriteTest','KafkaBookKeeperTaskTerminationV2RestartTest',1),
    ('nereus-kafka-bookkeeper','v2M5BookKeeperTaskTerminationRestartReadTest','KafkaBookKeeperTaskTerminationV2RestartTest',1),
    ('nereus-storage-bookkeeper','v2M5NativeCreateTest','M5BookKeeperNativeCreateSpecV2Test',4),
    ('nereus-storage-bookkeeper','v2M5NativeCreateRealTest','M5BookKeeperNativeCreateV2RealTest',8),
    ('nereus-kafka-bookkeeper','v2M5BookKeeperNativeCreateRealTest','KafkaBookKeeperNativeCreateV2RealTest',3),
    ('nereus-kafka-bookkeeper','v2M5BookKeeperNativeCreateRestartWriteTest','KafkaBookKeeperNativeCreateV2RestartTest',1),
    ('nereus-kafka-bookkeeper','v2M5BookKeeperNativeCreateRestartReadTest','KafkaBookKeeperNativeCreateV2RestartTest',1),
    ('nereus-kafka-bookkeeper','v2M5BookKeeperOxiaControlRealTest','KafkaBookKeeperOxiaControlV2RealTest',5),
    ('nereus-kafka-bookkeeper','v2M5BookKeeperOxiaRestartWriteTest','KafkaBookKeeperOxiaControlV2RestartTest',1),
    ('nereus-kafka-bookkeeper','v2M5BookKeeperOxiaRestartReadTest','KafkaBookKeeperOxiaControlV2RestartTest',1),
    ('nereus-kafka-bookkeeper','v2M5BookKeeperControlStoreTest','KafkaBookKeeperControlMetadataStoreV2Test',6),
    ('nereus-metadata-oxia','v2M5BookKeeperRecordStoreTest','OxiaKafkaBookKeeperRecordStoreV2Test',5),
):
    matches=list((root/module/f'build/test-results/{task}').glob(f'TEST-*.{name}.xml'))
    if len(matches)!=1: raise SystemExit('Missing or ambiguous native test XML: '+task)
    data=matches[0].read_bytes(); suite=ET.fromstring(data)
    if {k:suite.attrib.get(k) for k in ('tests','failures','errors','skipped')}!={
        'tests':str(count),'failures':'0','errors':'0','skipped':'0'}:
        raise SystemExit('Native suite did not pass without skips: '+task)
    (out/f'{task}.xml').write_bytes(data)
    suites.append({'task':task,'tests':count,'xmlSha256':hashlib.sha256(data).hexdigest()})
summary={'schema':'NEREUS_V2_M5_BK_TASK_TERMINAL_RUN_V2','suites':suites,
    'oxiaContainerId':sys.argv[3],'startedBefore':sys.argv[4],'startedAfter':sys.argv[5],
    'sameOxiaServerContainerRestarted':True,'bookKeeperClusterRetainedAcrossOxiaRestart':True,
    'sameZooKeeperAndBookieContainersRestarted':True,
    'bookKeeperBeforeRestart':before,'bookKeeperAfterRestart':after,
    'guardedTaskNativeInstanceIdAndCreateFenceSurvivedRestart':True,
    'testedInputManifestSha256':hashlib.sha256(inputs).hexdigest(),'capturedInputsUnchangedDuringRun':True,
    'bookKeeperImageId':'sha256:d0e78aaf987ac2feb526507ffb7d4c5137d58c0530f2a8cab4a9595abc89d605',
    'oxiaImageId':'sha256:7eef9af2cdc897fbf418bf7616da1387aca87ce860b8205395cdf88b867df4da',
    'sourceAndProtocolAuthority':'SYNTHETIC_FACTS_WITH_REAL_OXIA_AND_BK',
    'nativeTaskCreateFenceCompleteWithinGuardedProfile':True,
    'sameSelectorTaskDecisionsAndNativeDrainedTerminalWithinGuardedProfile':True,
    'latePublicationRejectedAfterCancellationArchiveAndNextTaskSelection':True,
    'taskTerminalAndExactNativeSealsSurvivedRestart':True,
    'allTaskTerminalCasesComplete':False,
    'taskPublicationTerminalComplete':False,'nativeSourceWriterAdmissionComplete':False,'nativeUniqueNamespaceAdmissionComplete':False,
    'internalTopicLifecycleComplete':False,'nativeQuotaComplete':False,
    'm5Receipt':False,'m5FinalAuthority':False,'physicalDeleteAuthority':False,'productionAuthority':False}
(out/'run-summary.json').write_text(json.dumps(summary,indent=2)+'\n')
PY
printf 'PASS_V2_M5_BOOKKEEPER_TASK_TERMINAL_NON_PROMOTABLE output=%s\n' "$m5_output"
