#!/usr/bin/env bash
set -euo pipefail
export LC_ALL=C
export LANG=C
export GRADLE_OPTS="${GRADLE_OPTS:-} -Dorg.gradle.parallel=false -Dorg.gradle.workers.max=2"
m5_repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
m5_compose="$m5_repo_root/config/v2/m2/kafka/k9/bookkeeper-conformance.compose.yml"
m5_project="nereus-v2-m5-kafka-run-source-$$"
m5_container="$m5_project-oxia"
m5_output="$m5_repo_root/build/m5-kafka-run-source/$m5_project"
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

# This non-deleting GC profile retains its native owner reservation permanently. It must not consume the
# separate two-resource M4 bound-delete fixture's capacity; both profiles remain mandatory and source-bound.
python3 "$m5_repo_root/scripts/run-v2-m5-epoch-grace-check.py" > "$m5_output/epoch-grace-dependency.log" 2>&1
m5_epoch_output="$(sed -n 's/^PASS_V2_M5_EPOCH_GRACE_DEPENDENCY_NON_PROMOTABLE output=//p' "$m5_output/epoch-grace-dependency.log")"
test -n "$m5_epoch_output"
printf '%s\n' "$m5_epoch_output" > "$m5_output/epoch-grace-run-path.txt"

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
if sum(suite['tests'] for suite in summary['suites'])!=116:
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
m5_oxia_owned_id="$(docker run --detach --name "$m5_container" --label com.nereusstream.evidence=v2-m5-kafka-run-source \
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
paths.extend((root/'nereus-kafka-bookkeeper/src/test/java/com/nereusstream/kafka/bookkeeper/broker').glob('*.java'))
paths.extend((root/'nereus-kafka-bookkeeper/src/test/java/com/nereusstream/kafka/bookkeeper/recovery').glob('*.java'))
paths.extend((root/'nereus-metadata-oxia/src/test/java/com/nereusstream/metadata/oxia/v2/compaction').glob('*.java'))
for source in ('test','realBookKeeperTest'):
    paths.extend((root/f'nereus-storage-bookkeeper/src/{source}/java').rglob('*.java'))
paths.extend((root/'nereus-storage-object/src/test/java/com/nereusstream/storage/object/retention').glob('*.java'))
paths.append(root/'nereus-storage-object/src/test/java/com/nereusstream/storage/object/materialization/M5MaterializationV1Test.java')
paths.extend((root/'nereus-metadata-oxia/src/test/java/com/nereusstream/metadata/oxia/v2/retention').glob('*.java'))
paths.extend([root/'nereus-storage-object/build.gradle.kts',root/'nereus-storage-bookkeeper/build.gradle.kts',
              root/'config/v2/m2/kafka/k9/bookkeeper-conformance.compose.yml',root/'build.gradle.kts',root/'nereus-kafka-bookkeeper/build.gradle.kts',
              root/'nereus-metadata-oxia/build.gradle.kts',root/'scripts/run-v2-m5-publication-tickets-check.sh',
              root/'scripts/run-v2-m5-kafka-run-roots-check.sh',root/'scripts/run-v2-m5-kafka-run-source-check.sh',
              root/'scripts/run-v2-m5-epoch-grace-check.py',
              root/'scripts/check-v2-m5-bookkeeper-descriptor.py',root/'scripts/check-v2-m5-bookkeeper-descriptor-tests.py',
              root/'scripts/run-v2-m5-bookkeeper-task-terminal-check.sh',root/'nereus-storage-api/build.gradle.kts'])
paths.extend((root/'nereus-storage-object/src/test/java/com/nereusstream/storage/object/gc').glob('*.java'))
paths.extend((root/'nereus-metadata-oxia/src/oxiaIntegrationTest/java/com/nereusstream/metadata/oxia/v2/retention').glob('*.java'))
paths.extend((root/'nereus-storage-api/src/test/java/com/nereusstream/storage/api/lifecycle').glob('*.java'))
paths.extend(root/'scripts'/name for name in (
    'check-v2-m5-delete-eligibility.py', 'check-v2-m5-delete-eligibility-tests.py',
    'check-v2-m5-retention-retirement.py', 'check-v2-m5-retention-retirement-tests.py'))
manifest={str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(paths)}
(out/'tested-inputs.json').write_text(json.dumps(manifest,indent=2)+'\n')
PY
"$m5_repo_root/gradlew" --no-daemon --no-configuration-cache --no-parallel --max-workers=2 --rerun-tasks \
  "-Pv2M2BookKeeperMetadataServiceUri=zk://127.0.0.1:2181/ledgers" \
  "-Pv2M5RetentionOxiaServiceAddress=127.0.0.1:$m5_port" \
  "-Pv2M5PublicationTicketsRestartCheckpoint=$m5_output/restart-checkpoint.txt" \
  "-Pv2M5KafkaRunRootsRestartCheckpoint=$m5_output/run-root-checkpoint" \
  "-Pv2M5KafkaRunSourceRestartCheckpoint=$m5_output/run-source-checkpoint" \
  "-Pv2M5BoundDeleteRestartCheckpoint=$m5_output/bound-delete-checkpoint" \
  "-Pv2M5NativeDeleteCellRestartCheckpoint=$m5_output/native-cell-checkpoint" \
  :nereus-storage-bookkeeper:v2M5NativeDeleteQuotaRealTest \
  v2M5KafkaRunSourceCheck :nereus-kafka-bookkeeper:test \
  :nereus-kafka-bookkeeper:v2M5PublicationTicketsRestartWriteTest \
  :nereus-kafka-bookkeeper:v2M5KafkaRunRootsRestartWriteTest \
  :nereus-kafka-bookkeeper:v2M5KafkaRunSourceRestartWriteTest \
  :nereus-kafka-bookkeeper:v2M5BoundDeleteRealTest :nereus-kafka-bookkeeper:v2M5BoundDeleteRestartWriteTest \
  :nereus-domain:jar :nereus-metadata-spi:jar :nereus-storage-api:jar \
  :nereus-storage-object:jar :nereus-storage-bookkeeper:jar \
  :nereus-metadata-oxia:jar :nereus-kafka-bookkeeper:jar --console=plain

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
"$m5_repo_root/gradlew" --no-daemon --no-configuration-cache --no-parallel --max-workers=2 \
  "-Pv2M2BookKeeperMetadataServiceUri=zk://127.0.0.1:2181/ledgers" \
  "-Pv2M5RetentionOxiaServiceAddress=127.0.0.1:$m5_port" \
  "-Pv2M5PublicationTicketsRestartCheckpoint=$m5_output/restart-checkpoint.txt" \
  "-Pv2M5KafkaRunRootsRestartCheckpoint=$m5_output/run-root-checkpoint" \
  "-Pv2M5KafkaRunSourceRestartCheckpoint=$m5_output/run-source-checkpoint" \
  "-Pv2M5BoundDeleteRestartCheckpoint=$m5_output/bound-delete-checkpoint" \
  "-Pv2M5NativeDeleteCellRestartCheckpoint=$m5_output/native-cell-checkpoint" \
  :nereus-kafka-bookkeeper:v2M5PublicationTicketsRestartReadTest \
  :nereus-kafka-bookkeeper:v2M5KafkaRunRootsRestartReadTest \
  :nereus-kafka-bookkeeper:v2M5KafkaRunSourceRestartReadTest \
  :nereus-kafka-bookkeeper:v2M5BoundDeleteRestartReadTest \
  :nereus-storage-bookkeeper:v2M5NativeDeleteCellBudgetRealTest \
  :nereus-storage-bookkeeper:v2M5NativeDeleteCellRestartWriteTest --console=plain

# A second actual service restart begins with both ACTIVE and terminal-UNKNOWN native Cell heads occupied.
m5_cell_started_before="$(docker inspect --format '{{.State.StartedAt}}' "$m5_oxia_owned_id")"
while IFS= read -r m5_id; do
  docker inspect --format '{{.Id}} {{.Image}} {{.State.StartedAt}}' "$m5_id"
done < "$m5_output/bookkeeper-container-ids.txt" > "$m5_output/bookkeeper-before-cell-restart.txt"
docker compose -p "$m5_project" -f "$m5_compose" stop bookie-0 bookie-1 bookie-2
docker compose -p "$m5_project" -f "$m5_compose" restart metadata-service
docker compose -p "$m5_project" -f "$m5_compose" up -d --wait
while IFS= read -r m5_id; do
  docker inspect --format '{{.Id}} {{.Image}} {{.State.StartedAt}}' "$m5_id"
done < "$m5_output/bookkeeper-container-ids.txt" > "$m5_output/bookkeeper-after-cell-restart.txt"
docker restart "$m5_oxia_owned_id" >/dev/null
test "$(docker inspect --format '{{.Id}}' "$m5_container")" = "$m5_oxia_owned_id"
test "$(docker inspect --format '{{.Image}}' "$m5_oxia_owned_id")" = "$m5_oxia_id"
m5_cell_started_after="$(docker inspect --format '{{.State.StartedAt}}' "$m5_oxia_owned_id")"
test "$m5_cell_started_before" != "$m5_cell_started_after"
m5_wait_ready
"$m5_repo_root/gradlew" --no-daemon --no-configuration-cache --no-parallel --max-workers=2 \
  "-Pv2M2BookKeeperMetadataServiceUri=zk://127.0.0.1:2181/ledgers" \
  "-Pv2M5NativeDeleteCellRestartCheckpoint=$m5_output/native-cell-checkpoint" \
  :nereus-storage-bookkeeper:v2M5NativeDeleteCellRestartReadTest --console=plain

"$m5_repo_root/gradlew" --no-daemon --no-configuration-cache --no-parallel --max-workers=2 \
  "-Pv2M2BookKeeperMetadataServiceUri=zk://127.0.0.1:2181/ledgers" \
  "-Pv2M5NativeDeleteCellRestartCheckpoint=$m5_output/native-cell-checkpoint" \
  :nereus-storage-bookkeeper:v2M5NativeDeleteCellRestartRecoverTest --console=plain

docker logs "$m5_oxia_owned_id" > "$m5_output/oxia-server.log" 2>&1
docker compose -p "$m5_project" -f "$m5_compose" logs > "$m5_output/bookkeeper.log" 2>&1
python3 - "$m5_repo_root" "$m5_output" "$m5_oxia_owned_id" "$m5_started_before" "$m5_started_after" "$m5_cell_started_before" "$m5_cell_started_after" <<'PY'
import hashlib,json,sys,xml.etree.ElementTree as ET
from pathlib import Path
root,out=map(Path,sys.argv[1:3]); inputs=(out/'tested-inputs.json').read_bytes()
for manifest in (json.loads(inputs), json.loads((out/'legacy-tested-inputs.json').read_bytes())):
    for path,sha in manifest.items():
        if hashlib.sha256((root/path).read_bytes()).hexdigest()!=sha:
            raise SystemExit('Input changed during native execution: '+path)
before=[line.split() for line in (out/'bookkeeper-before-restart.txt').read_text().splitlines()]
after=[line.split() for line in (out/'bookkeeper-after-restart.txt').read_text().splitlines()]
if len(before)!=4 or len(after)!=4 or any(a[:2]!=b[:2] or a[2]==b[2] for a,b in zip(before,after)):
    raise SystemExit('Native BK restart did not retain all four exact containers and change their start times')
cell_before=[line.split() for line in (out/'bookkeeper-before-cell-restart.txt').read_text().splitlines()]
cell_after=[line.split() for line in (out/'bookkeeper-after-cell-restart.txt').read_text().splitlines()]
if cell_before!=after or len(cell_after)!=4 or any(a[:2]!=b[:2] or a[2]==b[2] for a,b in zip(cell_before,cell_after)):
    raise SystemExit('Occupied Cell restart did not preserve all exact native containers with new start times')
if sys.argv[6]!=sys.argv[5] or sys.argv[6]==sys.argv[7]:
    raise SystemExit('Occupied Cell restart did not retain the same Oxia service and advance its start time')
cell_checkpoints={name:hashlib.sha256((out/name).read_bytes()).hexdigest()
    for name in ('native-cell-checkpoint-active','native-cell-checkpoint-unknown',
                 'native-cell-checkpoint-unknown-absent')}
bound_unknown=out/'bound-delete-checkpoint-unknown'
if len(bound_unknown.read_text().splitlines())!=8:
    raise SystemExit('Bound Oxia/BK UNKNOWN restart checkpoint is incomplete')
bound_unknown_sha=hashlib.sha256(bound_unknown.read_bytes()).hexdigest()
epoch_out=Path((out/'epoch-grace-run-path.txt').read_text().strip()).resolve()
if not epoch_out.is_relative_to((root/'build/m5-epoch-grace').resolve()):
    raise SystemExit('Epoch grace dependency is outside its owned profile directory')
epoch_result=json.loads((epoch_out/'result.json').read_bytes())
epoch_cleanup=json.loads((epoch_out/'cleanup.json').read_bytes())
if epoch_result['actualGradleExit']!=0 or not epoch_result['freshXml'] or not epoch_result['capturedInputsUnchangedDuringRun'] or epoch_result['counts']!={'tests':1,'failures':0,'errors':0,'skipped':0}:
    raise SystemExit('Native epoch grace dependency did not pass')
if not epoch_cleanup['originalContainersUnchanged'] or not epoch_cleanup['ownedContainersAbsent']:
    raise SystemExit('Native epoch grace dependency cleanup is incomplete')
for path,expected in json.loads((epoch_out/'tested-inputs.json').read_bytes()).items():
    source=(root/path).resolve()
    archived=epoch_out/'source-archive/sources'/path
    if not source.is_relative_to(root.resolve()) or hashlib.sha256(source.read_bytes()).hexdigest()!=expected or hashlib.sha256(archived.read_bytes()).hexdigest()!=expected:
        raise SystemExit('Native epoch grace dependency source changed: '+path)
epoch_xml=(epoch_out/'native-grace.xml').read_bytes()
epoch_suite=ET.fromstring(epoch_xml)
if {k:epoch_suite.attrib.get(k) for k in ('tests','failures','errors','skipped')}!={'tests':'1','failures':'0','errors':'0','skipped':'0'}:
    raise SystemExit('Native epoch grace dependency XML differs')
(out/'v2M5EpochGraceProfileDependency.xml').write_bytes(epoch_xml)
suites=[{'task':'v2M5EpochGraceProfileDependency','tests':1,'xmlSha256':hashlib.sha256(epoch_xml).hexdigest()}]
for module,task,name,count in (
    ('nereus-storage-object','v2M5MaterializationTest','M5MaterializationV1Test',18),
    ('nereus-kafka-bookkeeper','v2M5KafkaCompactionTest','KafkaSemanticCompactorV1Test',22),
    ('nereus-kafka-bookkeeper','v2M5KafkaRecordBatchBudgetTest','KafkaRecordBatchBudgetV2Test',3),
    ('nereus-kafka-bookkeeper','v2M5KafkaReadOwnerTest','KafkaBookKeeperReadOwnerV2Test',8),
    ('nereus-kafka-bookkeeper','v2M5BookKeeperM4RecoveryTest','KafkaBookKeeperM4RecoveryV2Test',7),
    ('nereus-kafka-bookkeeper','v2M5KafkaSelectedSourceTest','KafkaBookKeeperSelectedSourceV2Test',3),
    ('nereus-kafka-bookkeeper','v2M5KafkaRunSourceRealTest','KafkaBookKeeperRunSourceV2RealTest',14),
    ('nereus-kafka-bookkeeper','v2M5KafkaRunSourceRestartWriteTest','KafkaBookKeeperRunSourceV2RealTest',1),
    ('nereus-kafka-bookkeeper','v2M5KafkaRunSourceRestartReadTest','KafkaBookKeeperRunSourceV2RealTest',1),
    ('nereus-storage-bookkeeper','v2M5NativeDeleteQuotaRealTest','M5BookKeeperNativeDeleteQuotaV2RealTest',2),
    ('nereus-storage-bookkeeper','v2M5NativeDeleteCellBudgetRealTest','M5BookKeeperDeleteCellBudgetV2RealTest',3),
    ('nereus-storage-bookkeeper','v2M5NativeDeleteCellRestartWriteTest','M5BookKeeperDeleteCellBudgetV2RestartTest',1),
    ('nereus-storage-bookkeeper','v2M5NativeDeleteCellRestartReadTest','M5BookKeeperDeleteCellBudgetV2RestartTest',1),
    ('nereus-storage-bookkeeper','v2M5NativeDeleteCellRestartRecoverTest','M5BookKeeperDeleteCellBudgetV2RestartTest',1),
    ('nereus-kafka-bookkeeper','v2M5BoundDeleteRealTest','KafkaBookKeeperBoundDeleteV2RealTest',1),
    ('nereus-kafka-bookkeeper','v2M5BoundDeleteRestartWriteTest','KafkaBookKeeperBoundDeleteV2RealTest',1),
    ('nereus-kafka-bookkeeper','v2M5BoundDeleteRestartReadTest','KafkaBookKeeperBoundDeleteV2RealTest',1),
    ('nereus-metadata-oxia','v2M5KafkaRunRootTest','OxiaKafkaRunRootAuthorityV2Test',20),
    ('nereus-kafka-bookkeeper','v2M5KafkaRunRootsRealTest','KafkaBookKeeperRunRootsV2RealTest',16),
    ('nereus-kafka-bookkeeper','v2M5KafkaRunRootsRestartWriteTest','KafkaBookKeeperRunRootsV2RealTest',1),
    ('nereus-kafka-bookkeeper','v2M5KafkaRunRootsRestartReadTest','KafkaBookKeeperRunRootsV2RealTest',1),
    ('nereus-storage-object','v2M5MultiWriterTicketTest','M5TargetDeleteMultiWriterGuardV2Test',10),
    ('nereus-kafka-bookkeeper','v2M5BookKeeperDescriptorTest','KafkaSealedBookKeeperDescriptorV2Test',13),
    ('nereus-kafka-bookkeeper','v2M5PublicationTicketsRealTest','KafkaBookKeeperPublicationTicketsV2RealTest',9),
    ('nereus-kafka-bookkeeper','v2M5PublicationTicketsRestartWriteTest','KafkaBookKeeperPublicationTicketsV2RealTest',1),
    ('nereus-kafka-bookkeeper','v2M5PublicationTicketsRestartReadTest','KafkaBookKeeperPublicationTicketsV2RealTest',1),
):
    matches=list((root/module/f'build/test-results/{task}').glob(f'TEST-*.{name}.xml'))
    if len(matches)!=1: raise SystemExit('Missing or ambiguous native test XML: '+task)
    data=matches[0].read_bytes(); suite=ET.fromstring(data)
    if {k:suite.attrib.get(k) for k in ('tests','failures','errors','skipped')}!={
        'tests':str(count),'failures':'0','errors':'0','skipped':'0'}:
        raise SystemExit('Native suite did not pass without skips: '+task)
    if task=='v2M5KafkaRunRootsRealTest' and not any(
        case.attrib.get('name')=='blockedOxiaSelectorCasDoesNotHoldPartitionFenceOrOldBkReadCompletion()'
        for case in suite.findall('testcase')):
        raise SystemExit('Native M4 held-selector-CAS real callback case is absent')
    if task=='v2M5KafkaRunRootTest' and not any(
        case.attrib.get('name')=='qualifiedRetiredAnchorPreservesDoneAndOriginalChildTicketsThroughLostReply()'
        for case in suite.findall('testcase')):
        raise SystemExit('Retired-anchor qualification and original-ticket cleanup regression is absent')
    if task=='v2M5KafkaRunSourceRealTest' and not any(
        case.attrib.get('name')=='nativePhysicalMembersRetainOriginalUnknownTicketAndEveryBinding()'
        for case in suite.findall('testcase')):
        raise SystemExit('Native member registration, UNKNOWN ticket and exact-version regression is absent')
    (out/f'{task}.xml').write_bytes(data)
    suites.append({'task':task,'tests':count,'xmlSha256':hashlib.sha256(data).hexdigest()})
cache_budget_xml=(root/'nereus-kafka-bookkeeper/build/test-results/test/'
    'TEST-com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperControlMetadataStoreV2Test.xml')
cache_budget_bytes=cache_budget_xml.read_bytes()
cache_budget_suite=ET.fromstring(cache_budget_bytes)
if {k:cache_budget_suite.attrib.get(k) for k in ('tests','failures','errors','skipped')}!={
    'tests':'9','failures':'0','errors':'0','skipped':'0'} or not any(
    case.attrib.get('name')=='selectedCacheBudgetKeepsActualTaskBoundsAndRequiresIndependentOwnerIdentity()'
    for case in cache_budget_suite.findall('testcase')):
    raise SystemExit('Selected cache budget and native Task metadata unit suite did not pass without skips')
(out/'v2M5KafkaSelectedCacheBudgetTest.xml').write_bytes(cache_budget_bytes)
suites.append({'task':'v2M5KafkaSelectedCacheBudgetTest','tests':9,
               'xmlSha256':hashlib.sha256(cache_budget_bytes).hexdigest()})
broker_read_owner_xml=(root/'nereus-kafka-bookkeeper/build/test-results/test/'
    'TEST-com.nereusstream.kafka.bookkeeper.broker.KafkaBookKeeperM4BrokerReadOwnerV2Test.xml')
broker_read_owner_bytes=broker_read_owner_xml.read_bytes()
broker_read_owner_suite=ET.fromstring(broker_read_owner_bytes)
if {k:broker_read_owner_suite.attrib.get(k) for k in ('tests','failures','errors','skipped')}!={
    'tests':'5','failures':'0','errors':'0','skipped':'0'}:
    raise SystemExit('Native M4 Broker read-owner unit suite did not pass without skips')
(out/'v2M5KafkaM4BrokerReadOwnerTest.xml').write_bytes(broker_read_owner_bytes)
suites.append({'task':'v2M5KafkaM4BrokerReadOwnerTest','tests':5,
               'xmlSha256':hashlib.sha256(broker_read_owner_bytes).hexdigest()})
recovery_artifact_xml=(root/'nereus-kafka-bookkeeper/build/test-results/test/'
    'TEST-com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperRecoveryArtifactV3Test.xml')
recovery_artifact_bytes=recovery_artifact_xml.read_bytes()
recovery_artifact_suite=ET.fromstring(recovery_artifact_bytes)
if {k:recovery_artifact_suite.attrib.get(k) for k in ('tests','failures','errors','skipped')}!={
    'tests':'9','failures':'0','errors':'0','skipped':'0'}:
    raise SystemExit('Independent BK protocol recovery artifact unit suite did not pass without skips')
for name in (
    'replacementComparisonBindsIndependentBodiesRecoveryIndexesAndOriginalMembership()',
    'unchangedCheckpointProvenanceCannotHideChangedStateOrForeignPhysicalNamespace()',
    'retiredRawPrefixKeepsItsClosedLineageAndIndependentSelectedProtocolState()',
    'selectedRecoveryRejectsChangedPhysicalMembersBoundaryAndOwnerClosureBeforeSdkWork()',
    'maximumCoveredRunCatalogDoesNotGrowTheRecoveryCallbackStack()',
):
    if not any(case.attrib.get('name') == name for case in recovery_artifact_suite.findall('testcase')):
        raise SystemExit('Selected raw-prefix recovery unit case is absent: '+name)
(out/'v2M5KafkaRecoveryArtifactUnitTest.xml').write_bytes(recovery_artifact_bytes)
suites.append({'task':'v2M5KafkaRecoveryArtifactUnitTest','tests':9,
               'xmlSha256':hashlib.sha256(recovery_artifact_bytes).hexdigest()})
read_classes={
    'nativePartition':'nereus-kafka-bookkeeper/com/nereusstream/kafka/bookkeeper/broker/KafkaBookKeeperPartitionV1',
    'nativeReadOwner':'nereus-kafka-bookkeeper/com/nereusstream/kafka/bookkeeper/broker/KafkaBookKeeperM4BrokerReadOwnerV2',
    'readAsyncExecutor':'nereus-storage-object/com/nereusstream/storage/object/read/BindingReadAsyncExecutorV1',
    'selectorRuntime':'nereus-storage-object/com/nereusstream/storage/object/read/control/BindingReadSelectorRuntimeV1',
    'readControl':'nereus-storage-object/com/nereusstream/storage/object/read/control/M4ReadControlCoordinatorV1',
    'proofCleanupPlanner':'nereus-storage-object/com/nereusstream/storage/object/read/control/M4ProofCleanupPlannerV1',
    'oxiaReadRoute':'nereus-metadata-oxia/com/nereusstream/metadata/oxia/v2/retention/OxiaKafkaBookKeeperM4ReadRouteV2',
}
read_class_sha={}
for name,location in read_classes.items():
    module,qualified=location.split('/',1)
    compiled=root/module/'build/classes/java/main'/f'{qualified}.class'
    read_class_sha[name]=hashlib.sha256(compiled.read_bytes()).hexdigest()
runtime_artifact_sha={}
for module in ('nereus-storage-api','nereus-metadata-spi',
               'nereus-storage-object','nereus-storage-bookkeeper',
               'nereus-kafka-bookkeeper','nereus-metadata-oxia'):
    artifact=root/module/'build/libs'/f'{module}-0.2.0-SNAPSHOT.jar'
    runtime_artifact_sha[module]=hashlib.sha256(artifact.read_bytes()).hexdigest()
oxia_lock=root/'gradle/locked-artifacts/oxia-client-java/091a42c2780d92da56e9ec1f02ce1c3d988adc16/m2/io/github/oxia-db'
oxia_artifact_sha={name:hashlib.sha256((oxia_lock/name/'0.9.4'/f'{name}-0.9.4.jar').read_bytes()).hexdigest()
    for name in ('oxia-client','oxia-client-api')}
summary={'rawCaptureRequiresExpectedBinding':True,
    'rawCaptureReservesBeforeRootCatalogRead':True,
    'rejectedRawCaptureDoesNotReadRootCatalog':True,
    'wrongAdmittedBindingCannotCaptureAnotherNativeRun':True,
    'unconfirmedRawReadRetainsShareAndTicketUntilCallback':True,
    'unconfirmedRawCloseRetainsShareAndTicket':True,
    'healthySameCellRawBindingReadsDuringSiblingQuarantine':True,
    'cancelledRawObserverCannotClearQuarantine':True,
    'confirmedRawSessionExposesExactTicketCleanupRetry':True,
    'rawTicketCleanupRetryIsMetadataOnly':True,
    'rawSourceRequiresSharedCellReadBudget':True,
    'wholeRawResolutionRetainsChargeBetweenNativeSessions':True,
    'cancelledRawResolutionKeepsChargeUntilActualCompletion':True,
    'rawResolutionBlocksSameBindingSelectedCaptureAtCapacity':True,
    'selectedGenerationCaptureUsesScopedM4ReadOwner':True,
    'selectedCaptureAndScopedReadersShareCellCapacityNativeVerified':True,
    'selectedCaptureTicketCleanupPreservesLiveSameContextSibling':True,
    'confirmedNativeReadDrainExposesExactOperationTicketRetry':True,
    'nativeReadTicketRetryPreservesLiveSameContextSibling':True,'unknownNativeCloseExposesTicketRetry':False,
    'nativeBkReadOwnerReservesFixedBindingShares':True,'cancelledNativeReadRetainsBindingShareUntilSessionTermination':True,
    'healthyBindingReadsInSameConfiguredCellWhileSiblingHeld':True,'globalProcessReadBudgetAuthority':False,
    'occupiedNativeCellHeadsSurvivedServiceRestart':True,'freshJvmDidNotRedispatchOrReleaseActiveNativeCell':True,
    'healthyConfiguredCellDeletesAfterOccupiedHeadsRestart':True,'qualifiedActiveTransportDrain':False,
    'successorNativeEpochReleasesFencedActiveCellCapacity':True,
    'cellRestartStartedBefore':sys.argv[6],'cellRestartStartedAfter':sys.argv[7],
    'bookKeeperBeforeCellRestart':cell_before,'bookKeeperAfterCellRestart':cell_after,
    'nativeCellRestartCheckpointSha256':cell_checkpoints,
    'nativeCellDispatchAndUnknownSlotsReservedBeforeDelete':True,
    'observerCancellationRetainsNativeCellReservation':True,'distinctNativeCellBudgetHeadsAreIndependent':True,
    'terminalUnknownRequiresReadOnlyNativeAbsenceBeforeRelease':True,'fullCellAndPerBindingDispatchAdmission':False,
    'nativeIntentEligibilityFactsRereadBeforeAndAfterBinding':True,'nativeGcCanonicalCapacityAtomicallyReserved':True,
    'nativeGcCapacitySurvivedRestartAndDone':True,'existingNativeIntentContinuesAtCapacity':True,
    'nativeGcUsesUniqueAuthorityRoute':True,'nativeGcRequiresActiveQuotaAuthority':True,
    'boundGcEpochIntentAndM5AuthoritySurvivedRestart':True,
    'boundCellUnknownAbsenceRecoveredAfterRestart':True,
    'boundUnknownCheckpointSha256':bound_unknown_sha,
    'nativeEpochMonotonicGracePrerequisite':True,
    'nativeEpochGraceUsesIndependentRetainedQuotaProfile':True,
    'nativeEpochGraceDependencyResultSha256':hashlib.sha256((epoch_out/'result.json').read_bytes()).hexdigest(),
    'nativeEpochGraceDependencyInputSha256':hashlib.sha256((epoch_out/'tested-inputs.json').read_bytes()).hexdigest(),
    'graceAndDispatchCapacityAdmitted':False,'schema':'NEREUS_V2_M5_KAFKA_RUN_SOURCE_RUN_V2','suites':suites,
    'oxiaContainerId':sys.argv[3],'startedBefore':sys.argv[4],'startedAfter':sys.argv[5],
    'sameOxiaServerContainerRestarted':True,'bookKeeperClusterRetainedAcrossOxiaRestart':True,
    'sameZooKeeperAndBookieContainersRestarted':True,
    'bookKeeperBeforeRestart':before,'bookKeeperAfterRestart':after,
    'guardedTaskNativeInstanceIdAndCreateFenceSurvivedRestart':True,
    'testedInputManifestSha256':hashlib.sha256(inputs).hexdigest(),'capturedInputsUnchangedDuringRun':True,
    'everyCapturedSourceManifestCheckedIndependently':True,
    'bookKeeperImageId':'sha256:d0e78aaf987ac2feb526507ffb7d4c5137d58c0530f2a8cab4a9595abc89d605',
    'oxiaImageId':'sha256:7eef9af2cdc897fbf418bf7616da1387aca87ce860b8205395cdf88b867df4da',
    'sourceAndProtocolAuthority':'SYNTHETIC_FACTS_WITH_REAL_OXIA_AND_BK',
    'nativeM4HeldSelectorCasReadAndFenceReal':True,
    'currentLoadedM4ReadClassSha256':read_class_sha,
    'currentM4RuntimeArtifactSha256':runtime_artifact_sha,
    'lockedOxiaClientArtifactSha256':oxia_artifact_sha,
    'nativeNamespaceBoundToActualOxia':True,
    'nativeSourceAndOutputLedgerBytesVerified':True,
    'inputAndOutputTicketsBeforeNativeSelectorCas':True,
    'concurrentWinnerReconcilesOlderHeldInvocationTickets':True,
    'completePhysicalTargetSetBoundIntoTicketContext':True,
    'terminalRecoveryAtMost256TicketRemovalsPerPass':True,
    'fenceFirstPreventsPublicationAndReleasesAcquiredTickets':True,
    'nativeLostSelectorResponseReconciled':True,
    'observerCancellationRetainsTicketsUntilNativeCompletion':True,
    'nativeTaskCancellationReconcilesUnpublishedTickets':True,
    'missingInputMembershipRejected':True,
    'sameServerDataRestartedWithUnresolvedPublicationTickets':True,
    'freshJvmReconcilesTicketsFromNativeSelectionWithoutRepublishing':True,
    'sourceOwnershipAndEligibilityAuthority':'SYNTHETIC_LOGICAL_MEMBERSHIP_AND_ELIGIBILITY',
    'nativeKafkaRunRootsPersisted':True,
    'nativeRunHeaderAndClosedFooterVerified':True,
    'pendingUnselectedRootsInvisible':True,
    'nativeParentSelectsOneSuccessor':True,
    'bothInternalTopicRunBytesVerified':True,
    'freshJvmRecoversSelectedPendingRootsWithoutRepublishing':True,
    'runRootGcAdmissionAndProtocolOwnerAuthority':'SYNTHETIC_FIXTURES',
    'runRootMetadataQuotaComplete':False,'nativeReadFencedRecoveryComplete':False,
    'nativeRunDataProducesSourceExtentsAndInputBatches':True,
    'nativeRunMembershipChecksCompleteFrozenExtent':True,
    'nativePlanInputListComparedBeforePublication':True,
    'omittedReorderedRelocatedAndChangedInputBatchesRejected':True,
    'omittedNativeInputPreventsCandidateAndSelectorWrites':True,
    'readTicketHeldUntilOwnedSessionClosed':True,
    'nativeGroupAndIndexCorruptionRejected':True,
    'sourceBudgetSharedAcrossWholeResolution':True,
    'decodedKafkaRecordCountAndBytesBounded':True,
    'nativeSourceAndSelectedOutputReverifiedAfterRestart':True,
    'nativeSelectedGenerationSourceAndAllPhysicalMembersVerified':True,
    'indexOnlyGenerationCanReenterSemanticCompilerWithNoInputBatches':True,
    'selectedGenerationSourceIdentityRederivedAfterRestart':True,
    'secondNativePublicationAfterExactM4FallbackClosure':True,
    'newFallbackProtectionStartsAtIntroducedEpoch':True,
    'scopedReadOwnerTicketsPrecedeNativeSessionCreation':True,
    'scopedReadOwnerClosesAdmissionBeforeExactM4Closure':True,
    'scopedReadOwnerDrainWaitsForActualReadsAndSessionClose':True,
    'scopedReadOwnerCancellationCannotReleaseTicketsEarly':True,
    'scopedReadOwnerCreatesM4TerminalOrReleasedRecord':False,
    'newFallbackRejectsPreviousPreferredOnlyAndFutureEpochs':True,
    'existingFallbackFirstEpochInherited':True,
    'syntheticM4ProofNeedsNoPreferredOnlyEpoch':True,
    'secondSelectedGenerationReverifiedAfterRestart':True,
    'secondGenerationTaskReadFromNativeControlAfterRestart':True,
    'secondGenerationRestartUsesNoCopiedDescriptorOrTaskBody':True,
    'secondGenerationRestartCoversUserInternalAndIndexOnlyRoutes':True,
    'secondGenerationRestartCreatesNoRecordsAndDoesNotRepublish':True,
    'oldFallbackProtectionsRemainProtectedAfterSecondPublication':True,
    'selectedGenerationRepublicationAndFallbackRetirementComplete':False,
    'realProtocolSemanticAndBirthAdmissionComplete':False,
    'm5EComplete':False,
    'allConcreteWriterRowsComplete':False,'nativeInputCatalogAdmissionComplete':False,
    'physicalDeleteVerified':False,'all17AcceptanceObligationsComplete':False,
    'legacyRegressionSummarySha256':hashlib.sha256((out/'legacy-regression-summary.json').read_bytes()).hexdigest(),
    'm5Receipt':False,'m5FinalAuthority':False,'physicalDeleteAuthority':False,'productionAuthority':False}
(out/'run-summary.json').write_text(json.dumps(summary,indent=2)+'\n')
PY
printf 'PASS_V2_M5_KAFKA_RUN_SOURCE_NON_PROMOTABLE output=%s\n' "$m5_output"
