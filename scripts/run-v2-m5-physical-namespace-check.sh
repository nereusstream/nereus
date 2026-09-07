#!/usr/bin/env bash
set -euo pipefail
export LC_ALL=C
export LANG=C
m5_repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
m5_compose="$m5_repo_root/config/v2/m2/kafka/k9/bookkeeper-conformance.compose.yml"
m5_project="nereus-v2-m5-physical-namespace-$$"
m5_container="$m5_project-oxia"
m5_foreign_container="$m5_project-oxia-foreign"
m5_output="$m5_repo_root/build/m5-physical-namespace/$m5_project"
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
m5_foreign_owned_id=""
m5_cleanup() {
  if test -n "$m5_oxia_owned_id"; then docker rm -f "$m5_oxia_owned_id" >/dev/null 2>&1 || true; fi
  if test -n "$m5_foreign_owned_id"; then docker rm -f "$m5_foreign_owned_id" >/dev/null 2>&1 || true; fi
  docker compose -p "$m5_project" -f "$m5_compose" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap m5_cleanup EXIT INT TERM

# Run legacy unbound creation/terminal/recovery regressions on their own retained-data cluster first.
# Namespace binding is permanent, so the bound-profile phase must not reset that cluster to run legacy clients.
m5_legacy_output="${NEREUS_M5_VERIFIED_LEGACY_OUTPUT:-}"
if test -z "$m5_legacy_output"; then
  bash "$m5_repo_root/scripts/run-v2-m5-bookkeeper-task-terminal-check.sh" > "$m5_output/legacy-regression.log" 2>&1
  m5_legacy_output="$(sed -n 's/^PASS_V2_M5_BOOKKEEPER_TASK_TERMINAL_NON_PROMOTABLE output=//p' "$m5_output/legacy-regression.log")"
else
  printf 'Rechecking existing native legacy run: %s\n' "$m5_legacy_output" > "$m5_output/legacy-regression.log"
fi
python3 - "$m5_repo_root" "$m5_legacy_output" "$m5_output" <<'PY_LEGACY'
import hashlib,json,sys,xml.etree.ElementTree as ET
from pathlib import Path
root,previous,out=map(Path,sys.argv[1:])
previous=previous.resolve()
if not previous.is_relative_to((root/'build/m5-bookkeeper-task-terminal').resolve()):
    raise SystemExit('Legacy run is outside the local native regression directory')
summary_bytes=(previous/'run-summary.json').read_bytes(); summary=json.loads(summary_bytes)
inputs=(previous/'tested-inputs.json').read_bytes()
if summary['schema']!='NEREUS_V2_M5_BK_TASK_TERMINAL_RUN_V2' or hashlib.sha256(inputs).hexdigest()!=summary['testedInputManifestSha256']:
    raise SystemExit('Legacy summary or captured source manifest differs')
for key in ('capturedInputsUnchangedDuringRun','sameOxiaServerContainerRestarted','sameZooKeeperAndBookieContainersRestarted'):
    if summary[key] is not True: raise SystemExit('Legacy native execution incomplete: '+key)
for key in ('m5Receipt','m5FinalAuthority','physicalDeleteAuthority','productionAuthority'):
    if summary[key] is not False: raise SystemExit('Legacy run exceeds focused authority: '+key)
for path,sha in json.loads(inputs).items():
    candidate=(root/path).resolve()
    if not candidate.is_relative_to(root.resolve()) or hashlib.sha256(candidate.read_bytes()).hexdigest()!=sha:
        raise SystemExit('Legacy tested source changed: '+path)
if sum(suite['tests'] for suite in summary['suites'])!=71:
    raise SystemExit('Legacy focused suite count differs')
for suite in summary['suites']:
    data=(previous/(suite['task']+'.xml')).read_bytes(); actual=ET.fromstring(data)
    if hashlib.sha256(data).hexdigest()!=suite['xmlSha256'] or any(actual.attrib[key]!='0' for key in ('failures','errors','skipped')) or actual.attrib['tests']!=str(suite['tests']):
        raise SystemExit('Legacy native suite archive differs: '+suite['task'])
(out/'legacy-regression-summary.json').write_bytes(summary_bytes)
(out/'legacy-tested-inputs.json').write_bytes(inputs)
(out/'legacy-run-path.txt').write_text(str(previous)+'\n')
PY_LEGACY

docker compose -p "$m5_project" -f "$m5_compose" up -d --wait
for m5_service in metadata-service bookie-0 bookie-1 bookie-2; do
  m5_id="$(docker compose -p "$m5_project" -f "$m5_compose" ps -q "$m5_service")"
  test -n "$m5_id"
  test "$(docker inspect --format '{{.Config.Image}}' "$m5_id")" = "$m5_bk_image"
  test "$(docker inspect --format '{{.Image}}' "$m5_id")" = "$m5_bk_id"
done
read -r m5_primary_host_port m5_alias_host_port < <(python3 - <<'PY_PORTS'
import socket
with socket.socket() as first, socket.socket() as second:
    first.bind(('127.0.0.1',0)); second.bind(('127.0.0.1',0))
    print(first.getsockname()[1],second.getsockname()[1])
PY_PORTS
)
m5_oxia_owned_id="$(docker create --name "$m5_container" --label com.nereusstream.evidence=v2-m5-physical-namespace \
  --publish "127.0.0.1:$m5_primary_host_port:6648" --publish "127.0.0.1:$m5_alias_host_port:6648" \
  --publish 127.0.0.1::8080 "$m5_oxia_image" oxia standalone --shards=4)"
docker start "$m5_oxia_owned_id" >/dev/null
test "$(docker inspect --format '{{.Image}}' "$m5_oxia_owned_id")" = "$m5_oxia_id"
m5_started_before="$(docker inspect --format '{{.State.StartedAt}}' "$m5_oxia_owned_id")"
m5_foreign_owned_id="$(docker create --name "$m5_foreign_container" \
  --label com.nereusstream.evidence=v2-m5-physical-namespace-foreign \
  --publish 127.0.0.1::6648 --publish 127.0.0.1::8080 "$m5_oxia_image" oxia standalone --shards=4)"
docker start "$m5_foreign_owned_id" >/dev/null
test "$(docker inspect --format '{{.Image}}' "$m5_foreign_owned_id")" = "$m5_oxia_id"
m5_foreign_before="$(docker inspect --format '{{.State.StartedAt}}' "$m5_foreign_owned_id")"

m5_wait_ready() {
  m5_port="$(docker port "$m5_oxia_owned_id" 6648/tcp | awk -F: 'NR == 1 {print $NF}')"
  m5_alias_port="$(docker port "$m5_oxia_owned_id" 6648/tcp | awk -F: 'NR == 2 {print $NF}')"
  test -n "$m5_alias_port"
  test "$m5_port" != "$m5_alias_port"
  m5_foreign_port="$(docker port "$m5_foreign_owned_id" 6648/tcp | awk -F: '{print $NF}')"
  m5_foreign_metrics="$(docker port "$m5_foreign_owned_id" 8080/tcp | awk -F: '{print $NF}')"
  m5_metrics_port="$(docker port "$m5_oxia_owned_id" 8080/tcp | awk -F: '{print $NF}')"
  for m5_attempt in $(seq 1 60); do
    if curl --fail --silent "http://127.0.0.1:$m5_metrics_port/metrics" > "$m5_output/metrics.txt" &&
       curl --fail --silent "http://127.0.0.1:$m5_foreign_metrics/metrics" > "$m5_output/foreign-metrics.txt"; then return; fi
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
paths.extend((root/'nereus-metadata-oxia/src/test/java/com/nereusstream/metadata/oxia/v2/retention').glob('*.java'))
paths.extend([root/'nereus-storage-object/build.gradle.kts',root/'nereus-storage-bookkeeper/build.gradle.kts',
              root/'config/v2/m2/kafka/k9/bookkeeper-conformance.compose.yml',root/'build.gradle.kts',root/'nereus-kafka-bookkeeper/build.gradle.kts',
              root/'nereus-metadata-oxia/build.gradle.kts',root/'nereus-storage-api/build.gradle.kts',
              root/'scripts/run-v2-m5-bookkeeper-task-terminal-check.sh',
              root/'scripts/run-v2-m5-physical-namespace-check.sh'])
paths.extend((root/'nereus-storage-api/src/test/java/com/nereusstream/storage/api/lifecycle').glob('*.java'))
paths.extend((root/'nereus-storage-object/src/test/java/com/nereusstream/storage/object/gc').glob('*.java'))
paths.extend((root/'nereus-metadata-oxia/src/oxiaIntegrationTest/java/com/nereusstream/metadata/oxia/v2/retention').glob('*.java'))
manifest={str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(paths)}
(out/'tested-inputs.json').write_text(json.dumps(manifest,indent=2)+'\n')
PY
"$m5_repo_root/gradlew" --no-daemon --no-configuration-cache --rerun-tasks \
  "-Pv2M2BookKeeperMetadataServiceUri=zk://127.0.0.1:2181/ledgers" \
  "-Pv2M5RetentionOxiaServiceAddress=127.0.0.1:$m5_port" \
  "-Pv2M5NamespaceOxiaAliasAddress=127.0.0.1:$m5_alias_port" \
  "-Pv2M5NamespaceForeignOxiaAddress=127.0.0.1:$m5_foreign_port" \
  "-Pv2M5NamespaceRestartCheckpoint=$m5_output/restart-checkpoint.txt" \
  v2M5PhysicalNamespaceCheck --console=plain

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
docker restart "$m5_foreign_owned_id" >/dev/null
test "$(docker inspect --format '{{.Id}}' "$m5_foreign_container")" = "$m5_foreign_owned_id"
test "$(docker inspect --format '{{.Image}}' "$m5_foreign_owned_id")" = "$m5_oxia_id"
m5_foreign_after="$(docker inspect --format '{{.State.StartedAt}}' "$m5_foreign_owned_id")"
test "$m5_foreign_before" != "$m5_foreign_after"

m5_wait_ready
"$m5_repo_root/gradlew" --no-daemon --no-configuration-cache \
  "-Pv2M2BookKeeperMetadataServiceUri=zk://127.0.0.1:2181/ledgers" \
  "-Pv2M5RetentionOxiaServiceAddress=127.0.0.1:$m5_port" \
  "-Pv2M5NamespaceOxiaAliasAddress=127.0.0.1:$m5_alias_port" \
  "-Pv2M5NamespaceForeignOxiaAddress=127.0.0.1:$m5_foreign_port" \
  "-Pv2M5NamespaceRestartCheckpoint=$m5_output/restart-checkpoint.txt" \
  :nereus-storage-bookkeeper:v2M5PhysicalNamespaceRestartTest --console=plain

docker logs "$m5_oxia_owned_id" > "$m5_output/oxia-server.log" 2>&1
docker compose -p "$m5_project" -f "$m5_compose" logs > "$m5_output/bookkeeper.log" 2>&1
python3 - "$m5_repo_root" "$m5_output" "$m5_oxia_owned_id" "$m5_started_before" "$m5_started_after" "$m5_foreign_owned_id" "$m5_foreign_before" "$m5_foreign_after" <<'PY'
import hashlib,json,sys,xml.etree.ElementTree as ET
from pathlib import Path
root,out=map(Path,sys.argv[1:3]); inputs=(out/'tested-inputs.json').read_bytes()
for path,sha in (json.loads(inputs) | json.loads((out/'legacy-tested-inputs.json').read_bytes())).items():
    if hashlib.sha256((root/path).read_bytes()).hexdigest()!=sha:
        raise SystemExit('Input changed during native execution: '+path)
before=[line.split() for line in (out/'bookkeeper-before-restart.txt').read_text().splitlines()]
after=[line.split() for line in (out/'bookkeeper-after-restart.txt').read_text().splitlines()]
if len(before)!=4 or len(after)!=4 or any(a[:2]!=b[:2] or a[2]==b[2] for a,b in zip(before,after)):
    raise SystemExit('Native BK restart did not retain all four exact containers and change their start times')
suites=[]
for module,task,name,count in (
    ('nereus-storage-api','v2M5NamespaceIdentityTest','PhysicalNamespaceAuthorityBindingV2Test',3),
    ('nereus-storage-bookkeeper','v2M5NativeCreateTest','M5BookKeeperNativeCreateSpecV2Test',4),
    ('nereus-storage-bookkeeper','v2M5PhysicalNamespaceRealTest','M5PhysicalNamespaceOxiaBookKeeperRealTest',1),
    ('nereus-storage-bookkeeper','v2M5PhysicalNamespaceRestartTest','M5PhysicalNamespaceOxiaBookKeeperRealTest',1),
    ('nereus-storage-object','v2M5GcQuotaTest','M5GcQuotaV2Test',9),
    ('nereus-metadata-oxia','v2M5GcQuotaRealOxiaTest','M5GcQuotaOxiaIntegrationTest',8),
):
    matches=list((root/module/f'build/test-results/{task}').glob(f'TEST-*.{name}.xml'))
    if len(matches)!=1: raise SystemExit('Missing or ambiguous native test XML: '+task)
    data=matches[0].read_bytes(); suite=ET.fromstring(data)
    if {k:suite.attrib.get(k) for k in ('tests','failures','errors','skipped')}!={
        'tests':str(count),'failures':'0','errors':'0','skipped':'0'}:
        raise SystemExit('Native suite did not pass without skips: '+task)
    (out/f'{task}.xml').write_bytes(data)
    suites.append({'task':task,'tests':count,'xmlSha256':hashlib.sha256(data).hexdigest()})
summary={'schema':'NEREUS_V2_M5_PHYSICAL_NAMESPACE_RUN_V2','suites':suites,
    'oxiaContainerId':sys.argv[3],'startedBefore':sys.argv[4],'startedAfter':sys.argv[5],
    'foreignOxiaContainerId':sys.argv[6],'foreignStartedBefore':sys.argv[7],'foreignStartedAfter':sys.argv[8],
    'sameTwoOxiaServerContainersRestarted':True,'sameZooKeeperAndBookieContainersRestarted':True,
    'bookKeeperBeforeRestart':before,'bookKeeperAfterRestart':after,
    'testedInputManifestSha256':hashlib.sha256(inputs).hexdigest(),'capturedInputsUnchangedDuringRun':True,
    'bookKeeperImageId':'sha256:d0e78aaf987ac2feb526507ffb7d4c5137d58c0530f2a8cab4a9595abc89d605',
    'oxiaImageId':'sha256:7eef9af2cdc897fbf418bf7616da1387aca87ce860b8205395cdf88b867df4da',
    'nativeBookKeeperNamespaceAssignmentVerified':True,'metadataBackendMismatchRejected':True,
    'endpointAliasesShareActualNamespaceAndAuthority':True,'heldUnboundNativeLedgerCreateRejected':True,
    'lostAppliedNativeBindingResponseReconciled':True,'boundCreateAndSealedReadAfterRestartVerified':True,
    'namespaceBindingAuthorityAndQuotaSurvivedRestart':True,
    'sourceAndProtocolAuthority':'SYNTHETIC_OPEN_ELIGIBILITY_WITH_REAL_OXIA_AND_BK',
    'objectNamespaceAssignmentComplete':False,'allConcreteWritersAdmitted':False,
    'crossCellResourceOwnershipComplete':False,'offlineOldAuthorityCompatibilityComplete':False,
    'physicalDeleteVerified':False,'m5Receipt':False,'m5FinalAuthority':False,'physicalDeleteAuthority':False,
    'productionAuthority':False,
    'legacyRegressionSummarySha256':hashlib.sha256((out/'legacy-regression-summary.json').read_bytes()).hexdigest()}
(out/'run-summary.json').write_text(json.dumps(summary,indent=2)+'\n')
PY
printf 'PASS_V2_M5_PHYSICAL_NAMESPACE_NON_PROMOTABLE output=%s\n' "$m5_output"
