#!/usr/bin/env python3
"""Execute the fixed Kafka57 and agreed GC/internal-topic closure without resetting its backend."""

import argparse
import importlib.util
import hashlib
import json
import os
import re
from pathlib import Path
import shutil
import subprocess
import sys
import time
import urllib.request
import xml.etree.ElementTree as ET


ROOT = Path(__file__).resolve().parent.parent
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--output', type=Path, required=True)
parser.add_argument('--input-closure', type=Path, required=True)
parser.add_argument('--source-run', type=Path, required=True)
parser.add_argument('--class-logs', type=Path, required=True)
args = parser.parse_args()
spec = importlib.util.spec_from_file_location('m5_native_kafka_inputs', ROOT / 'scripts/m5-candidate-inputs.py')
INPUTS = importlib.util.module_from_spec(spec)
spec.loader.exec_module(INPUTS)
COHORT = INPUTS.verify(args.input_closure.resolve(strict=True))
assert ROOT == (args.input_closure.resolve() / 'workspace' / INPUTS.LAYOUT['nereus']).resolve()
KAFKA = (args.input_closure.resolve() / 'workspace' / INPUTS.LAYOUT['kafka']).resolve()
SOURCE = args.source_run.resolve(strict=True)
assert SOURCE.is_relative_to(ROOT / 'build/m5-kafka-run-source')
LEGACY = Path((SOURCE / 'legacy-run-path.txt').read_text().strip())
METHODS = ('nativeM5MaterializesAndDrainsItsActualTaskScope', 'nativeM5PublishesItsActualTaskAndFetchesSelectedBytes', 'nativeM5RecompactionDrainsActualOldReadsAndTransfersSelectedCache')
OUT = args.output.resolve()
PROJECT = 'nereus-m5-native-authority-facts-broker-' + str(os.getpid())
COMPOSE = ROOT / 'config/v2/m2/kafka/k9/bookkeeper-conformance.compose.yml'
OXIA_IMAGE = 'nereus/oxia-m3-allocator:37a17bef1720'
OXIA_ID = 'sha256:7eef9af2cdc897fbf418bf7616da1387aca87ce860b8205395cdf88b867df4da'
REPOSITORY = ROOT / 'build/m5-evidence-development-repository'


def output(args, cwd=None):
    return subprocess.check_output(args, cwd=cwd, text=True).strip()


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write(name, value):
    (OUT / name).write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')


def containers():
    ids = output(['docker', 'ps', '-aq']).split()
    return {} if not ids else {
        x['Id']: (x['Name'], x['State']['Status'])
        for x in json.loads(output(['docker', 'inspect', *ids]))
    }


assert not OUT.exists()
OUT.mkdir()
for directory in (SOURCE, LEGACY):
    manifest = json.loads((directory / 'tested-inputs.json').read_text())
    assert all(sha(ROOT / path) == expected for path, expected in manifest.items())
    for path, expected in manifest.items():
        # Standalone source runners verify their manifest before/after execution. The complete
        # campaign also verifies the full frozen workspace; materialize the legacy native-reader
        # archive from those exact bytes rather than depending on an earlier hand-built archive.
        record = COHORT['repositories']['nereus']['files'][path]
        assert record['sha256'] == expected
        target = directory / 'source-archive/sources' / path
        if not target.exists():
            target.parent.mkdir(parents=True, exist_ok=True)
            with target.open('xb') as destination:
                destination.write((ROOT / path).read_bytes())
    assert all(sha(directory / 'source-archive/sources' / path) == expected
               for path, expected in manifest.items())
summary = json.loads((SOURCE / 'run-summary.json').read_text())
assert summary['suites'] and all(x['tests'] > 0 for x in summary['suites'])
modules = tuple(summary['currentM4RuntimeArtifactSha256'])
preparation = INPUTS.load(INPUTS.regular(ROOT / 'build/m5-native-build-preparation.json'))
assert preparation['schema'] == 'NEREUS_V2_M5_NATIVE_BUILD_PREPARATION_V1'
assert preparation['executionEvidence'] is False and preparation['promotionEligible'] is False
assert preparation['workspace'] == str(args.input_closure.resolve())
assert preparation['sourceInventorySha256'] == INPUTS.digest(INPUTS.canonical(COHORT['repositories']))
assert preparation['rootRuntimeArtifacts'] == summary['currentM4RuntimeArtifactSha256']
for name, tool in preparation['toolchain'].items():
    executable = Path(os.environ['NEREUS_M5_KAFKA_JAVA_HOME']).resolve() / 'bin' / name
    assert str(executable) == tool['path'] and executable.stat().st_size == tool['bytes'] and sha(executable) == tool['sha256']
# Publication and dependency resolution completed before the final input freeze. This phase only
# consumes that exact repository; runtime input checks remain active before and after the real tests.
for module, expected in summary['currentM4RuntimeArtifactSha256'].items():
    artifact = module + '-0.2.0-SNAPSHOT.jar'
    assert sha(ROOT / module / 'build/libs' / artifact) == expected
    published = REPOSITORY / 'com/nereusstream' / module / '0.2.0-SNAPSHOT'
    versions = [x.findtext('value') for x in ET.parse(published / 'maven-metadata.xml').findall('.//snapshotVersion')
                if x.findtext('extension') == 'jar' and x.find('classifier') is None]
    assert len(versions) == 1
    assert sha(published / (module + '-' + versions[0] + '.jar')) == expected
assert output(['git', 'rev-parse', 'HEAD'], KAFKA) == COHORT['repositories']['kafka']['baseCommit']
before_git = output(['git', 'status', '--porcelain'], KAFKA)
(OUT / 'kafka-status-before.txt').write_text(before_git + '\n')
test_source = (KAFKA / 'core/src/test/java/kafka/server/nsip/KafkaBookKeeperM4AdmissionRealTest.java').read_text()
method_cases = {name: 1 for name in re.findall(r'public void (\w+)\(', test_source)}
for values in re.finditer(r'@ValueSource\(strings\s*=\s*\{(.*?)\}\)', test_source, re.S):
    name = re.search(r'public void (\w+)\(', test_source[values.end():]).group(1)
    method_cases[name] = len(re.findall(r'"[^"\n]*"', values.group(1)))
expected_cases = sum(method_cases[name] for name in METHODS)
assert expected_cases == 57
write('declared-case-counts.json', method_cases)
paths = set()
for directory in ('core/src/main/java/kafka/server/nereus', 'core/src/main/java/kafka/log/nereus',
                  'core/src/test/java/kafka/server/nereus', 'core/src/main/scala/kafka/server/nereus',
                  'core/src/main/scala/kafka/log/nereus'):
    paths.update(x for x in (KAFKA / directory).rglob('*') if x.is_file())
paths.update(KAFKA / x for x in output(['git', 'diff', '--name-only'], KAFKA).splitlines()
             if (KAFKA / x).is_file())
paths.update(KAFKA / x for x in ('build.gradle', 'gradle.properties', 'checkstyle/import-control-core.xml',
    'core/src/test/java/kafka/server/nsip/KafkaBookKeeperM4AdmissionRealTest.java'))
manifest = {str(p.relative_to(KAFKA)): sha(p) for p in sorted(paths)}
write('kafka-inputs.json', manifest)
for path in manifest:
    destination = OUT / 'kafka-source' / path
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(KAFKA / path, destination)
write('inputs.json', {'sourceSummarySha256': sha(SOURCE / 'run-summary.json'),
    'sourceManifestSha256': sha(SOURCE / 'tested-inputs.json'),
    'legacySummarySha256': sha(LEGACY / 'run-summary.json'),
    'legacyManifestSha256': sha(LEGACY / 'tested-inputs.json'),
    'kafkaManifestSha256': sha(OUT / 'kafka-inputs.json'),
    'runtimeJars': summary['currentM4RuntimeArtifactSha256'], 'runnerSha256': sha(Path(__file__)),
    'selection': list(METHODS), 'expectedCases': expected_cases})
shutil.copy2(Path(__file__), OUT / 'runner-source.py')
assert output(['docker', 'image', 'inspect', OXIA_IMAGE, '--format', '{{.Id}}']) == OXIA_ID
original = containers()
compose = ['docker', 'compose', '-p', PROJECT, '-f', str(COMPOSE)]
oxia = None
exit_code = None
try:
    subprocess.run(compose + ['up', '-d', '--wait'], check=True)
    oxia = output(['docker', 'run', '--detach', '--name', PROJECT + '-oxia',
        '--label', 'com.nereusstream.evidence=m5-native-authority-facts-broker',
        '--publish', '127.0.0.1::6648', '--publish', '127.0.0.1::8080',
        OXIA_IMAGE, 'oxia', 'standalone', '--shards=4'])
    port = output(['docker', 'port', oxia, '6648/tcp']).rsplit(':', 1)[1]
    metrics = output(['docker', 'port', oxia, '8080/tcp']).rsplit(':', 1)[1]
    for attempt in range(60):
        try:
            with urllib.request.urlopen('http://127.0.0.1:' + metrics + '/metrics', timeout=2) as response:
                (OUT / 'metrics.txt').write_bytes(response.read())
            break
        except Exception:
            if attempt == 59:
                raise
            time.sleep(1)
    write('docker-inspect.json', json.loads(output(['docker', 'inspect', *output(compose + ['ps', '-q']).split(), oxia])))
    write('backend.json', {'project': PROJECT, 'oxiaId': oxia, 'oxiaAddress': '127.0.0.1:' + port,
        'source': str(SOURCE), 'kafkaHead': output(['git', 'rev-parse', 'HEAD'], KAFKA)})
    gradle_args = ['./gradlew', '--offline', '--no-daemon', '--no-configuration-cache', '--max-workers=2',
        '--init-script', str(ROOT / 'config/v2/m5/evidence-native.init.gradle'),
        '-Dnereus.m5.evidenceClassLogDirectory=' + str(args.class_logs.resolve()),
        '-PmaxParallelForks=1', '-Pv2M5KafkaSealedGcProfile=true', '-PnereusDevelopmentRepository=' + str(REPOSITORY),
        '-PnereusDevelopmentVersion=0.2.0-SNAPSHOT',
        '-PnereusLockedOxiaRepository=' + str(ROOT / 'gradle/locked-artifacts/oxia-client-java/091a42c2780d92da56e9ec1f02ce1c3d988adc16/m2'),
        '-Pv2M2BookKeeperMetadataServiceUri=zk://127.0.0.1:2181/ledgers',
        '-Pv2M5RetentionOxiaServiceAddress=127.0.0.1:' + port,
        '-Pv2M5KafkaRunSourceRoot=' + str(ROOT), '-Pv2M5KafkaRunSummaryFile=' + str(SOURCE / 'run-summary.json'),
        ':core:nereusBookKeeperM4AdmissionRealTest']
    gradle_args += [value for method in METHODS for value in ('--tests', 'kafka.server.nsip.KafkaBookKeeperM4AdmissionRealTest.' + method)]
    env = dict(os.environ, JAVA_HOME=os.environ['NEREUS_M5_KAFKA_JAVA_HOME'])
    env['PATH'] = env['JAVA_HOME'] + '/bin:' + env['PATH']
    started_native = time.time_ns()
    with (OUT / 'gradle.log').open('w') as log:
        exit_code = subprocess.run(gradle_args, cwd=KAFKA, env=env, stdout=log, stderr=subprocess.STDOUT).returncode
    xml = KAFKA / 'core/build/test-results/nereusBookKeeperM4AdmissionRealTest/TEST-kafka.server.nsip.KafkaBookKeeperM4AdmissionRealTest.xml'
    counts = None
    if xml.exists() and xml.stat().st_mtime_ns >= started_native:
        shutil.copy2(xml, OUT / 'native-broker.xml')
        counts = {k: int(ET.parse(xml).getroot().attrib[k]) for k in ('tests', 'failures', 'errors', 'skipped')}
    assert exit_code == 0 and counts == {'tests': 57, 'failures': 0, 'errors': 0, 'skipped': 0}
    rows = [{'task': ':core:nereusBookKeeperM4AdmissionRealTest', 'path': 'native-broker.xml',
             'producedAtMicros': xml.stat().st_mtime_ns // 1000}]
    # Keep the original 57-case arrays and backend intact. Run the already agreed GC/internal-topic
    # closure afterward on the same fixed-capacity server; there is no data or quota reset.
    additional = ('nativeM5CancelledTaskReclaimsSettlesAndReadmits', 'nativeM5GcTotalSwitchVeto',
                  'nativeM5BookKeeperOnlyInternalTopicsCompactReclaimAndRecover',
                  'nativeM5ReplacedSourceResumesAfterDeleteBeforeDone', 'nativeM5ReplacedSourceReclaimsAndSettles')
    base = gradle_args[:gradle_args.index(':core:nereusBookKeeperM4AdmissionRealTest') + 1]
    started_extra = time.time_ns()
    with (OUT / 'additional-gradle.log').open('w') as log:
        exit_code = subprocess.run(base + [value for method in additional for value in
            ('--tests', 'kafka.server.nsip.KafkaBookKeeperM4AdmissionRealTest.' + method)], cwd=KAFKA,
            env=env, stdout=log, stderr=subprocess.STDOUT).returncode
    assert xml.is_file() and xml.stat().st_mtime_ns >= started_extra
    extra_counts = {k: int(ET.parse(xml).getroot().attrib[k]) for k in ('tests', 'failures', 'errors', 'skipped')}
    shutil.copy2(xml, OUT / 'native-gc-internal.xml')
    rows.append({'task': ':core:nereusBookKeeperM4AdmissionRealTest', 'path': 'native-gc-internal.xml',
                 'producedAtMicros': xml.stat().st_mtime_ns // 1000})
    assert exit_code == 0 and extra_counts == {'tests': 6, 'failures': 0, 'errors': 0, 'skipped': 0}
    write('stage-result.json', {'junit': rows, 'exitCode': exit_code})
    unchanged = all(sha(KAFKA / path) == expected for path, expected in manifest.items())
    write('result.json', {'actualGradleExit': exit_code, 'counts': counts, 'kafkaInputsUnchanged': unchanged,
        'wholeM5Authority': False, 'physicalDeleteAuthority': False, 'freshBrokerXml': counts is not None})
    assert exit_code == 0 and counts == {'tests': expected_cases, 'failures': 0, 'errors': 0, 'skipped': 0}
    assert unchanged and output(['git', 'status', '--porcelain'], KAFKA) == before_git
    assert INPUTS.verify(args.input_closure.resolve()) == COHORT
finally:
    if oxia:
        with (OUT / 'oxia.log').open('w') as log:
            subprocess.run(['docker', 'logs', oxia], stdout=log, stderr=subprocess.STDOUT)
        subprocess.run(['docker', 'rm', '-f', oxia], stdout=subprocess.DEVNULL, check=True)
    with (OUT / 'bookkeeper.log').open('w') as log:
        subprocess.run(compose + ['logs'], stdout=log, stderr=subprocess.STDOUT)
    subprocess.run(compose + ['down', '-v', '--remove-orphans'], check=True)
    current = containers()
    write('cleanup.json', {'originalContainersUnchanged': all(current.get(k) == v for k, v in original.items()),
        'ownedContainersAbsent': not any(PROJECT in v[0] for v in current.values()),
        'actualGradleExit': exit_code})
