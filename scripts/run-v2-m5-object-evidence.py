#!/usr/bin/env python3
"""Run current Object retirement, isolation and native provider contracts on owned pinned backends."""

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import time
import urllib.request
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parent.parent
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--output', type=Path, required=True)
parser.add_argument('--input-closure', type=Path, required=True)
parser.add_argument('--class-logs', type=Path, required=True)
args = parser.parse_args()
OUT = args.output.resolve()
PROJECT = 'nereus-m5-object-cell-' + str(os.getpid())
MINIO = 'quay.io/minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e'
MINIO_ID = 'sha256:8f08aee614800a237906bd48114d733e5ac5bfac4ccdf731f141b0e880d7a253'
OXIA = 'nereus/oxia-m3-allocator:37a17bef1720'
OXIA_ID = 'sha256:7eef9af2cdc897fbf418bf7616da1387aca87ce860b8205395cdf88b867df4da'
TASK = 'v2M5ObjectRunRecoveryRetirementRealTest'


def output(args):
    return subprocess.check_output(args, text=True).strip()


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write(name, value):
    (OUT / name).write_text(json.dumps(value, sort_keys=True, indent=2) + '\n')


def inventory():
    ids = output(['docker', 'ps', '-aq']).split()
    return {} if not ids else {row['Id']: (row['Name'], row['State']['Status'])
        for row in json.loads(output(['docker', 'inspect', *ids]))}


def ready(url):
    for attempt in range(60):
        try:
            with urllib.request.urlopen(url, timeout=2) as response:
                if response.status == 200:
                    return
        except Exception:
            pass
        time.sleep(1)
    raise RuntimeError('owned backend readiness expired')


assert not OUT.exists()
OUT.mkdir()
spec = importlib.util.spec_from_file_location('m5_cell_source_closure', ROOT / 'scripts/m5-candidate-inputs.py')
source = importlib.util.module_from_spec(spec)
spec.loader.exec_module(source)
cohort = source.verify(args.input_closure.resolve(strict=True))
assert ROOT == (args.input_closure.resolve() / 'workspace' / source.LAYOUT['nereus']).resolve()
for image, identity in [(MINIO, MINIO_ID), (OXIA, OXIA_ID)]:
    row = json.loads(output(['docker', 'image', 'inspect', image]))[0]
    assert row['Id'] == identity and row['Os'] == 'linux' and row['Architecture'] == 'arm64'
original = inventory()
write('original-containers.json', original)
owned = []
code = None
try:
    minio = output(['docker', 'run', '-d', '--name', PROJECT + '-minio', '--platform', 'linux/arm64',
        '--publish', '127.0.0.1::9000', '--env', 'MINIO_ROOT_USER=nereus-m3-evidence',
        '--env', 'MINIO_ROOT_PASSWORD=nereus-m3-evidence-secret', MINIO, 'server', '/data'])
    owned.append(minio)
    assert output(['docker', 'inspect', minio, '--format', '{{.Image}}']) == MINIO_ID
    endpoint = 'http://127.0.0.1:' + output(['docker', 'port', minio, '9000/tcp']).rsplit(':', 1)[1]
    ready(endpoint + '/minio/health/ready')
    oxia = output(['docker', 'run', '-d', '--name', PROJECT + '-oxia', '--platform', 'linux/arm64',
        '--publish', '127.0.0.1::6648', '--publish', '127.0.0.1::8080', OXIA, 'oxia', 'standalone', '--shards=4'])
    owned.append(oxia)
    assert output(['docker', 'inspect', oxia, '--format', '{{.Image}}']) == OXIA_ID
    address = '127.0.0.1:' + output(['docker', 'port', oxia, '6648/tcp']).rsplit(':', 1)[1]
    ready('http://127.0.0.1:' + output(['docker', 'port', oxia, '8080/tcp']).rsplit(':', 1)[1] + '/metrics')
    write('docker-inspect.json', json.loads(output(['docker', 'inspect', *owned])))
    write('backend.json', {'project': PROJECT, 'minioImage': MINIO, 'minioImageId': MINIO_ID,
        'oxiaImage': OXIA, 'oxiaImageId': OXIA_ID, 'endpoint': endpoint, 'oxiaAddress': address,
        'realBoundaries': ['OBJECT', 'OXIA'], 'kmsTransport': 'ADMITTED_M2_TEST_TRANSPORT',
        'nativeProcessActivation': False})
    started = time.time_ns()
    with (OUT / 'gradle.log').open('w') as log:
        code = subprocess.run(['./gradlew', '--offline', '--no-daemon', '--no-configuration-cache', '--max-workers=2',
            '--init-script', str(ROOT / 'config/v2/m5/evidence-native.init.gradle'),
            '-Dnereus.m5.evidenceClassLogDirectory=' + str(args.class_logs.resolve()),
            '-Dnereus.m5.evidenceMethodNames=true',
            '-Pnsip1ObjectEndpoint=' + endpoint, '-Pnsip1ObjectOxia=' + address,
            ':nereus-kafka-bookkeeper:' + TASK, ':nereus-kafka-bookkeeper:nsip1ObjectAuthorizationRealTest', ':nereus-kafka-bookkeeper:v2M5ObjectCellIsolationRealTest', ':nereus-kafka-bookkeeper:checkstyleRealBookKeeperTest',
            '-Pv2M5ObjectOxiaAddress=' + address,
            ':nereus-storage-object-s3:v2M5ObjectOxiaNamespaceRealTest',
            ':nereus-storage-object-s3:v2M5VersionMatchDeleteRealProviderTest',
            ':nereus-storage-object-s3:v2M5MultipartCleanupRealProviderTest',
            ':nereus-kafka-bookkeeper:checkstyleMain', ':nereus-kafka-bookkeeper:checkstyleTest',
            ':nereus-kafka-bookkeeper:spotlessCheck', ':nereus-storage-object:checkstyleMain',
            ':nereus-storage-object:spotlessCheck'], cwd=ROOT,
            env=os.environ.copy(),
            stdout=log, stderr=subprocess.STDOUT).returncode
    xml = ROOT / 'nereus-kafka-bookkeeper/build/test-results' / TASK / 'TEST-com.nereusstream.kafka.bookkeeper.object.nwkcp1.M5ObjectRunRecoveryRetirementRealTest.xml'
    counts = None
    if xml.exists() and xml.stat().st_mtime_ns >= started:
        shutil.copy2(xml, OUT / 'native-object.xml')
        tree = ET.parse(xml).getroot()
        counts = {key: int(tree.attrib[key]) for key in ['tests', 'failures', 'errors', 'skipped']}
        lines = '\n'.join(node.text or '' for node in tree.findall('system-out')).splitlines()
        samples = [json.loads(line.split('M5_CELL_PUT_SAMPLE ', 1)[1]) for line in lines if line.startswith('M5_CELL_PUT_SAMPLE ')]
        write('binding-samples.json', samples)
    for task, class_name, expected_tests, destination in [
        ('nsip1ObjectAuthorizationRealTest', 'KafkaObjectAuthorizationRealTest', 28, 'authorization.xml'),
        ('v2M5ObjectCellIsolationRealTest', 'M5ObjectCellIsolationRealTest', 2, 'cell-isolation.xml')]:
        sibling = ROOT / 'nereus-kafka-bookkeeper/build/test-results' / task / ('TEST-com.nereusstream.kafka.bookkeeper.object.nwkcp1.' + class_name + '.xml')
        assert sibling.is_file() and sibling.stat().st_mtime_ns >= started
        shutil.copy2(sibling, OUT / destination)
        sibling_root = ET.parse(sibling).getroot()
        sibling_counts = {key: int(sibling_root.attrib[key]) for key in ['tests', 'failures', 'errors', 'skipped']}
        write(destination + '-counts.json', sibling_counts)
        assert sibling_counts == {'tests': expected_tests, 'failures': 0, 'errors': 0, 'skipped': 0}
    unchanged = source.verify(args.input_closure.resolve()) == cohort
    write('result.json', {'actualGradleExit': code, 'counts': counts, 'capturedInputsUnchanged': unchanged,
        'inputRootSha256': cohort['inputRootSha256'], 'wholeM5Authority': False, 'm5Receipt': False})
    assert code == 0 and counts and counts['tests'] == 5 and all(counts[key] == 0 for key in ['failures', 'errors', 'skipped']) and unchanged

finally:
    for identity in owned:
        name = output(['docker', 'inspect', identity, '--format', '{{.Name}}']).lstrip('/')
        with (OUT / (name + '.log')).open('w') as log:
            subprocess.run(['docker', 'logs', identity], stdout=log, stderr=subprocess.STDOUT)
        subprocess.run(['docker', 'rm', '-f', identity], check=True, stdout=subprocess.DEVNULL)
    after = inventory()
    write('cleanup.json', {'originalContainersUnchanged': all(after.get(key) == value for key, value in original.items()),
        'ownedContainersAbsent': not any(PROJECT in value[0] for value in after.values()), 'actualGradleExit': code})

rows = []
for module in ('nereus-kafka-bookkeeper', 'nereus-storage-object-s3'):
    for xml in sorted((ROOT / module / 'build/test-results').glob('*/TEST-*.xml')):
        if xml.stat().st_mtime_ns < started:
            continue
        task = ':' + module + ':' + xml.parent.name
        target = OUT / 'junit' / module / xml.parent.name / xml.name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(xml, target)
        rows.append({'task': task, 'path': str(target.relative_to(OUT)), 'producedAtMicros': xml.stat().st_mtime_ns // 1000})
assert rows
write('stage-result.json', {'junit': rows, 'exitCode': code})
