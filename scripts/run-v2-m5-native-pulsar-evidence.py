#!/usr/bin/env python3
"""Execute the existing complete native Pulsar M5 recipe against owned pinned backends."""

import argparse
import importlib.util
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import time
import urllib.request
import xml.etree.ElementTree as ET


ROOT = Path(__file__).resolve().parent.parent
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--output', type=Path, required=True)
parser.add_argument('--input-closure', type=Path, required=True)
parser.add_argument('--maven-repository', type=Path, required=True)
parser.add_argument('--class-logs', type=Path, required=True)
args = parser.parse_args()
SPEC = importlib.util.spec_from_file_location('m5_native_pulsar_inputs', ROOT / 'scripts/m5-candidate-inputs.py')
INPUTS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(INPUTS)
PREPARED = args.input_closure.resolve(strict=True)
COHORT = INPUTS.verify(PREPARED)
assert ROOT == (PREPARED / 'workspace' / INPUTS.LAYOUT['nereus']).resolve()
PULSAR = (PREPARED / 'workspace' / INPUTS.LAYOUT['pulsar']).resolve()
PHASE = 'green-complete'
OWNER_TESTS = []
args.output.mkdir(parents=True, exist_ok=False)
PREFIX = args.output.resolve() / 'native-pulsar' 
PROJECT = 'nereus-m5-native-pulsar-' + PHASE + '-' + str(os.getpid())
BK_IMAGE = 'apache/bookkeeper@sha256:c0a128931c402d6bf6a6f973ba2f305b9be261659e30754ab95a29510a33bc0d'
BK_ID = 'sha256:d0e78aaf987ac2feb526507ffb7d4c5137d58c0530f2a8cab4a9595abc89d605'
MINIO_IMAGE = 'quay.io/minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e'
MINIO_ID = 'sha256:8f08aee614800a237906bd48114d733e5ac5bfac4ccdf731f141b0e880d7a253'
OXIA_IMAGE = 'nereus/oxia-m3-allocator:37a17bef1720'
OXIA_ID = 'sha256:7eef9af2cdc897fbf418bf7616da1387aca87ce860b8205395cdf88b867df4da'
JDK = os.environ['JAVA_HOME']


def output(args):
    return subprocess.check_output(args, cwd=ROOT, text=True).strip()


def inspect(ids):
    if not ids:
        return []
    return json.loads(output(['docker', 'inspect', *ids]))


def containers():
    return {item['Id']: (item['Name'], item['State']['Status'])
            for item in inspect(output(['docker', 'ps', '-aq']).split())}


def write(suffix, data):
    path = Path(str(PREFIX) + suffix)
    assert not path.exists(), path
    path.write_text(json.dumps(data, sort_keys=True, indent=2) + '\n')


def write_capsule(capsule, runtime, suites):
    assert runtime.exists() and not capsule.exists()
    lines = ['schema=NEREUS_NATIVE_PULSAR_M4_SDK_DRAIN_V1',
             'authority=ACTUAL_NATIVE_PULSAR_BK_OBJECT_OXIA', 'expiryAuthority=false',
             'wholeM5Authority=false', 'physicalDeleteAuthority=false',
             'source.count=' + str(len(inputs)), 'suite.count=' + str(len(suites)),
             'bookKeeperImageId=' + BK_ID, 'minioImageId=' + MINIO_ID, 'oxiaImageId=' + OXIA_ID]
    for index, (name, digest) in enumerate(sorted(inputs.items())):
        lines += [f'source.{index}.path={name}', f'source.{index}.sha256={digest}']
    for index, (name, result) in enumerate(sorted(suites.items())):
        path = Path(str(PREFIX) + '-junit') / name
        lines += [f'suite.{index}.path={path.resolve()}', f'suite.{index}.sha256={result["sha256"]}']
    capsule.write_text('\n'.join(lines) + '\n' + runtime.read_text())
    return hashlib.sha256(capsule.read_bytes()).hexdigest()


NATIVE_OWNED = {
    'managed-ledger/src/main/java/org/apache/bookkeeper/mledger/ManagedLedgerConfig.java',
    'managed-ledger/src/main/java/org/apache/bookkeeper/mledger/impl/MetaStore.java',
    'managed-ledger/src/main/java/org/apache/bookkeeper/mledger/impl/MetaStoreImpl.java',
    'managed-ledger/src/main/java/org/apache/bookkeeper/mledger/impl/EntryImpl.java',
    'managed-ledger/src/test/java/org/apache/bookkeeper/mledger/impl/EntryImplTest.java',
    'managed-ledger/src/test/java/org/apache/bookkeeper/mledger/impl/OffloadLedgerDeleteTest.java',
    'managed-ledger/src/main/java/org/apache/bookkeeper/mledger/SourceSafeLedgerOffloader.java',
    'managed-ledger/src/main/java/org/apache/bookkeeper/mledger/impl/ManagedLedgerImpl.java',
    'managed-ledger/src/test/java/org/apache/bookkeeper/mledger/impl/ManagedLedgerTest.java',
    'managed-ledger/src/test/java/org/apache/bookkeeper/mledger/impl/ManagedCursorTest.java',
    'managed-ledger/src/main/java/org/apache/bookkeeper/mledger/impl/ManagedCursorImpl.java',
    'managed-ledger/src/test/java/org/apache/bookkeeper/mledger/impl/DualSourceReadHandleTest.java',
    'managed-ledger/src/main/java/org/apache/bookkeeper/mledger/impl/DualSourceReadHandle.java',
}


def assert_native_checkout():
    assert output(['git', '-C', str(PULSAR), 'rev-parse', 'HEAD']) == COHORT['repositories']['pulsar']['baseCommit']
    observed = INPUTS.repository(PULSAR)
    assert observed['files'] == COHORT['repositories']['pulsar']['files']


def captured_path(original):
    original = Path(original)
    for name, repository in COHORT['repositories'].items():
        base = Path(repository['sourceRoot'])
        if original.is_relative_to(base):
            return PREPARED / 'workspace' / INPUTS.LAYOUT[name] / original.relative_to(base)
    record = COHORT['extraInputs'][str(original)]
    return PREPARED / 'objects' / record['sha256']


original = containers()
compose = Path(str(PREFIX) + '-compose.yml')
assert not compose.exists()
shutil.copyfile(ROOT / 'config/v2/m2/kafka/k9/bookkeeper-conformance.compose.yml', compose)
compose_command = ['docker', 'compose', '-p', PROJECT, '-f', str(compose)]
minio_id = None
oxia_id = None
actual_exit = None
for image, expected, platform in [(BK_IMAGE, BK_ID, 'linux/amd64'),
                                  (MINIO_IMAGE, MINIO_ID, 'linux/arm64'),
                                  (OXIA_IMAGE, OXIA_ID, 'linux/arm64')]:
    actual = json.loads(output(['docker', 'image', 'inspect', image]))[0]
    assert actual['Id'] == expected
    assert actual['Os'] + '/' + actual['Architecture'] == platform
for port in (2181, 3181, 3182, 3183):
    with socket.socket() as probe:
        assert probe.connect_ex(('127.0.0.1', port)) != 0, ('occupied', port)
assert_native_checkout()
heads = {str(ROOT): output(['git', 'rev-parse', 'HEAD']),
         str(PULSAR): output(['git', '-C', str(PULSAR), 'rev-parse', 'HEAD'])}
paths = list((ROOT / 'nereus-pulsar-offload/src/main/java').rglob('*.java'))
paths += list((ROOT / 'nereus-pulsar-offload/src/test/java').rglob('*.java'))
paths.append(Path(__file__).resolve())
paths.append(ROOT / 'settings.gradle.kts')
paths.append(ROOT / 'config/v2/m5/evidence-native.init.gradle')
for repository in (args.maven_repository.resolve(), ROOT / 'build/m5-native-pulsar-owner-local-dependencies'):
    paths.extend(path for path in repository.rglob('*') if path.is_file())
paths += [ROOT / 'nereus-pulsar-offload/src/p6ProviderTest/java/com/nereusstream/pulsar/offload/M5NativePulsarOffloadRealTest.java',
          ROOT / 'nereus-pulsar-offload/src/p6ProviderTest/java/com/nereusstream/pulsar/offload/M5S3PulsarObjectStoreDrainTest.java',
          ROOT / 'nereus-pulsar-offload/src/p6ProviderTest/java/com/nereusstream/pulsar/offload/PulsarP6CandidateEvidenceTest.java',
          ROOT / 'nereus-pulsar-offload/build.gradle.kts', ROOT / 'gradle/libs.versions.toml', ROOT / 'build.gradle.kts',
          ROOT / 'config/v2/m2/kafka/k9/bookkeeper-conformance.compose.yml']
for relative in ['managed-ledger/src/main/java/org/apache/bookkeeper/mledger/impl/MetaStore.java',
                 'managed-ledger/src/main/java/org/apache/bookkeeper/mledger/impl/EntryImpl.java',
                 'managed-ledger/src/test/java/org/apache/bookkeeper/mledger/impl/EntryImplTest.java',
                 'managed-ledger/src/main/java/org/apache/bookkeeper/mledger/impl/ManagedLedgerImpl.java',
                 'managed-ledger/src/main/java/org/apache/bookkeeper/mledger/impl/ManagedLedgerFactoryImpl.java',
                 'managed-ledger/src/main/java/org/apache/bookkeeper/mledger/impl/ManagedCursorImpl.java',
                 'managed-ledger/src/test/java/org/apache/bookkeeper/mledger/impl/ManagedCursorTest.java',
                 'managed-ledger/src/main/java/org/apache/bookkeeper/mledger/ManagedLedgerConfig.java',
                 'managed-ledger/src/main/java/org/apache/bookkeeper/mledger/ManagedLedgerFactoryConfig.java',
                 'managed-ledger/src/main/java/org/apache/bookkeeper/mledger/impl/EntryImpl.java',
    'managed-ledger/src/test/java/org/apache/bookkeeper/mledger/impl/EntryImplTest.java',
    'managed-ledger/src/main/java/org/apache/bookkeeper/mledger/SourceSafeLedgerOffloader.java',
                 'managed-ledger/src/main/java/org/apache/bookkeeper/mledger/LedgerOffloader.java',
                 'managed-ledger/src/main/java/org/apache/bookkeeper/mledger/impl/MetaStoreImpl.java',
                 'managed-ledger/src/main/proto/MLDataFormats.proto',
                 'managed-ledger/src/main/java/org/apache/bookkeeper/mledger/impl/LedgerMetadataUtils.java',
                 'managed-ledger/src/main/java/org/apache/bookkeeper/mledger/offload/OffloadUtils.java',
                 'managed-ledger/src/main/java/org/apache/bookkeeper/mledger/impl/DualSourceReadHandle.java',
                 'managed-ledger/src/main/java/org/apache/bookkeeper/mledger/impl/LedgerReadSourcePinRegistry.java',
                 'managed-ledger/src/main/java/org/apache/bookkeeper/mledger/impl/cache/RangeEntryCacheImpl.java',
                 'managed-ledger/src/main/java/org/apache/bookkeeper/mledger/util/Futures.java',
                 'managed-ledger/src/test/java/org/apache/bookkeeper/mledger/impl/ManagedLedgerTest.java',
                 'managed-ledger/src/test/java/org/apache/bookkeeper/mledger/impl/MetaStoreImplTest.java',
                 'managed-ledger/src/test/java/org/apache/bookkeeper/mledger/impl/DualSourceReadHandleTest.java',
                 'managed-ledger/src/test/java/org/apache/bookkeeper/mledger/impl/OffloadLedgerDeleteTest.java',
                 'managed-ledger/src/test/java/org/apache/bookkeeper/mledger/impl/OffloadPrefixTest.java',
                 'managed-ledger/build.gradle.kts', 'settings.gradle.kts']:
    paths.append(PULSAR / relative)
for module in ['nereus-domain', 'nereus-metadata-spi', 'nereus-metadata-oxia', 'nereus-storage-api', 'nereus-storage-object', 'nereus-storage-object-s3', 'nereus-storage-bookkeeper']:
    paths += list((ROOT / module / 'src/main/java').rglob('*.java'))
    paths.append(ROOT / module / 'build.gradle.kts')
paths += list((ROOT / 'nereus-storage-bookkeeper/src/test/java').rglob('*.java'))
inputs = {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(paths)}
write('-inputs.json', inputs)
owned = []
try:
    with Path(str(PREFIX) + '-bk-start.log').open('w') as log:
        subprocess.run(compose_command + ['up', '-d', '--wait', '--wait-timeout', '180', '--pull', 'never'],
                       cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, check=True)
    bk_ids = output(compose_command + ['ps', '-q']).split()
    assert len(bk_ids) == 4
    for item in inspect(bk_ids):
        assert item['Image'] == BK_ID and item['Config']['Image'] == BK_IMAGE
        owned.append({'id': item['Id'], 'name': item['Name'], 'imageId': item['Image'],
                      'startedAt': item['State']['StartedAt']})
    minio_id = output(['docker', 'run', '--detach', '--name', PROJECT + '-minio', '--platform', 'linux/arm64',
                       '--publish', '127.0.0.1::9000', '--env', 'MINIO_ROOT_USER=nereus-m5-drain',
                       '--env', 'MINIO_ROOT_PASSWORD=nereus-m5-drain-secret', MINIO_IMAGE, 'server', '/data'])
    item = inspect([minio_id])[0]
    assert item['Image'] == MINIO_ID
    owned.append({'id': item['Id'], 'name': item['Name'], 'imageId': item['Image'],
                  'startedAt': item['State']['StartedAt']})
    port = int(output(['docker', 'port', minio_id, '9000/tcp']).rsplit(':', 1)[1])
    endpoint = 'http://127.0.0.1:' + str(port)
    for attempt in range(60):
        try:
            with urllib.request.urlopen(endpoint + '/minio/health/ready', timeout=2) as response:
                if response.status == 200:
                    break
        except Exception:
            pass
        time.sleep(1)
    else:
        raise RuntimeError('owned MinIO never became ready')
    oxia_id = output(['docker', 'run', '--detach', '--name', PROJECT + '-oxia', '--platform', 'linux/arm64',
                      '--publish', '127.0.0.1::6648', '--publish', '127.0.0.1::8080', OXIA_IMAGE,
                      'oxia', 'standalone', '--shards=4'])
    item = inspect([oxia_id])[0]
    assert item['Image'] == OXIA_ID
    assert item['Config']['Labels']['org.opencontainers.image.revision'] == '37a17bef17202d5fd6e23282da5fd26d94865484'
    owned.append({'id': item['Id'], 'name': item['Name'], 'imageId': item['Image'], 'startedAt': item['State']['StartedAt']})
    oxia_port = int(output(['docker', 'port', oxia_id, '6648/tcp']).rsplit(':', 1)[1])
    health_port = int(output(['docker', 'port', oxia_id, '8080/tcp']).rsplit(':', 1)[1])
    for attempt in range(60):
        with socket.socket() as probe:
            if probe.connect_ex(('127.0.0.1', oxia_port)) == 0:
                break
        time.sleep(1)
    else:
        raise RuntimeError('owned Oxia never became ready')
    oxia_address = '127.0.0.1:' + str(oxia_port)
    write('-docker-inspect.json', inspect(bk_ids + [minio_id, oxia_id]))
    write('-backend.json', {'heads': heads, 'originalContainers': original, 'ownedContainers': owned,
                            'bookKeeperImage': BK_IMAGE, 'minioImage': MINIO_IMAGE, 'endpoint': endpoint,
                            'zk': '127.0.0.1:2181', 'oxiaAddress': oxia_address, 'oxiaImage': OXIA_IMAGE, 'nativeOwnedPaths': sorted(NATIVE_OWNED)})
    tasks = [':nereus-pulsar-offload:v2M5NativeOffloadRealTest']
    if PHASE.startswith('green'):
        tasks += [':nereus-pulsar-offload:v2M5S3ProviderDrainRealTest', ':nereus-pulsar-offload:test',
                  ':nereus-storage-bookkeeper:test', ':nereus-storage-bookkeeper:checkstyleMain',
                  ':nereus-storage-bookkeeper:checkstyleTest', ':nereus-storage-bookkeeper:spotlessCheck',
                  ':nereus-pulsar-offload:checkstyleMain', ':nereus-pulsar-offload:checkstyleTest',
                  ':nereus-pulsar-offload:checkstyleP6ProviderTest', ':nereus-pulsar-offload:spotlessCheck',
                  ':pulsar:managed-ledger:test',
                  '--tests', 'org.apache.bookkeeper.mledger.impl.ManagedLedgerTest.testCloseWaitsForReadHandleAcknowledgement',
                  '--tests', 'org.apache.bookkeeper.mledger.impl.ManagedLedgerTest.testCloseWaitsForCancelledReadHandleOpen',
                  '--tests', 'org.apache.bookkeeper.mledger.impl.ManagedLedgerTest.testInvalidatedReadErrorKeepsReplacementHandle',
                  '--tests', 'org.apache.bookkeeper.mledger.impl.ManagedLedgerTest.testCloseWaitsForNativeOffload*',
                  '--tests', 'org.apache.bookkeeper.mledger.impl.OffloadPrefixTest',
                  '--tests', 'org.apache.bookkeeper.mledger.impl.ManagedCursorTest',
                  '--tests', 'org.apache.bookkeeper.mledger.impl.ManagedLedgerTest',
                  '--tests', 'org.apache.bookkeeper.mledger.impl.EntryImplTest',
                  '--tests', 'org.apache.bookkeeper.mledger.impl.MetaStoreImplTest',
                  '--tests', 'org.apache.bookkeeper.mledger.impl.ManagedLedgerTest.closeAndReopen',
                  '--tests', 'org.apache.bookkeeper.mledger.impl.ManagedLedgerTest.asyncCloseWithoutError',
                  '--tests', 'org.apache.bookkeeper.mledger.impl.ManagedLedgerTest.closeLedgerWithError',
                  '--tests', 'org.apache.bookkeeper.mledger.impl.DualSourceReadHandleTest',
                  '--tests', 'org.apache.bookkeeper.mledger.impl.OffloadLedgerDeleteTest.testSourceSafe*',
                  '--tests', 'org.apache.bookkeeper.mledger.impl.OffloadLedgerDeleteTest.testNativeDeleteMetadata*',
                  ':pulsar:managed-ledger:checkstyleMain', ':pulsar:managed-ledger:checkstyleTest',
                  ':pulsar:managed-ledger:spotlessCheck']
        if OWNER_TESTS:
            # SDK prerequisites remain real/fresh. Broad unchanged Native suites run at the fixed candidate.
            tasks = tasks[:tasks.index(':pulsar:managed-ledger:test')]
    env = os.environ.copy()
    env['JAVA_HOME'] = JDK
    env['PATH'] = JDK + '/bin:' + env['PATH']
    started = time.time_ns()
    print('owned_backend_ready', PROJECT, flush=True)
    with Path(str(PREFIX) + '.log').open('w') as log:
        actual_exit = subprocess.run(['./gradlew', '--offline', '--no-daemon', '--no-configuration-cache',
                                     '--no-parallel', '--max-workers=2', '-PtestFailFast=false',
                                     '--init-script', str(ROOT / 'config/v2/m5/evidence-native.init.gradle'),
                                     '-PpulsarCheckout=' + str(PULSAR),
                                     '-Dnereus.m5.evidenceMavenRepository=' + str(args.maven_repository.resolve()),
                                     '-Dnereus.m5.evidenceSdkTestingRepository=' + str(ROOT / 'build/m5-native-pulsar-owner-local-dependencies'),
                                     '-Dnereus.m5.evidenceClassLogDirectory=' + str(args.class_logs.resolve()),
                                     '-Dnereus.m5.evidenceMethodNames=true',
                                     '-Pv2M5NativeOffloadEndpoint=' + endpoint,
                                     '-Pv2M5NativeOffloadZk=127.0.0.1:2181',
                                     '-Pv2M5NativeOffloadOxia=' + oxia_address,
                                     '-Pv2M5S3DrainEndpoint=' + endpoint,
                                     '-Pv2M5NativeM4RuntimeOutput=' + str(PREFIX) + '-runtime.properties', *tasks],
                                    cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT).returncode
    assert all(hashlib.sha256(Path(p).read_bytes()).hexdigest() == sha for p, sha in inputs.items())
    junit = {}
    for task in ['v2M5NativeOffloadRealTest'] + (['v2M5S3ProviderDrainRealTest', 'test'] if PHASE.startswith('green') else []):
        target = Path(str(PREFIX) + '-junit') / task
        for path in sorted((ROOT / 'nereus-pulsar-offload/build/test-results' / task).glob('TEST-*.xml')):
            if path.stat().st_mtime_ns < started:
                continue
            target.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, target / path.name)
            root = ET.parse(path).getroot()
            junit[task + '/' + path.name] = {'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
                                           **{k: int(root.get(k, '0')) for k in ['tests', 'failures', 'errors', 'skipped']}}
    if PHASE.startswith('green'):
        task = 'native-managed-ledger-test'
        target = Path(str(PREFIX) + '-junit') / task
        for native_xml in sorted((PULSAR / 'managed-ledger/build/test-results/test').glob('TEST-*.xml')):
            if native_xml.stat().st_mtime_ns < started:
                continue
            target.mkdir(parents=True, exist_ok=True)
            shutil.copy2(native_xml, target / native_xml.name)
            native_root = ET.parse(native_xml).getroot()
            junit[task + '/' + native_xml.name] = {
                'sha256': hashlib.sha256(native_xml.read_bytes()).hexdigest(),
                **{k: int(native_root.get(k, '0')) for k in ['tests', 'failures', 'errors', 'skipped']}}
    if PHASE.startswith('green'):
        task = 'bookkeeper-module-test'
        target = Path(str(PREFIX) + '-junit') / task
        for unit_xml in sorted((ROOT / 'nereus-storage-bookkeeper/build/test-results/test').glob('TEST-*.xml')):
            if unit_xml.stat().st_mtime_ns < started:
                continue
            target.mkdir(parents=True, exist_ok=True)
            shutil.copy2(unit_xml, target / unit_xml.name)
            unit_root = ET.parse(unit_xml).getroot()
            junit[task + '/' + unit_xml.name] = {
                'sha256': hashlib.sha256(unit_xml.read_bytes()).hexdigest(),
                **{k: int(unit_root.get(k, '0')) for k in ['tests', 'failures', 'errors', 'skipped']}}
    delete_sdk_task = 'v2M5NativeBkDeleteSdkDrainRealTest'
    delete_runtime = Path(str(PREFIX) + '-bk-delete-runtime.properties')
    if actual_exit == 0 and PHASE.startswith('green'):
        started_delete_sdk = time.time_ns()
        with Path(str(PREFIX) + '-bk-delete-sdk-drain.log').open('w') as log:
            actual_exit = subprocess.run(['./gradlew', '--offline', '--no-daemon', '--no-configuration-cache',
                                         '--no-parallel', '--max-workers=2', '-PtestFailFast=false',
                                         '--init-script', str(ROOT / 'config/v2/m5/evidence-native.init.gradle'),
                                         '-PpulsarCheckout=' + str(PULSAR),
                                         '-Dnereus.m5.evidenceMavenRepository=' + str(args.maven_repository.resolve()),
                                         '-Dnereus.m5.evidenceSdkTestingRepository=' + str(ROOT / 'build/m5-native-pulsar-owner-local-dependencies'),
                                         '-Dnereus.m5.evidenceClassLogDirectory=' + str(args.class_logs.resolve()),
                                         '-Dnereus.m5.evidenceMethodNames=true',
                                         '-Pv2M5NativeOffloadEndpoint=' + endpoint,
                                         '-Pv2M5NativeOffloadZk=127.0.0.1:2181',
                                         '-Pv2M5NativeOffloadOxia=' + oxia_address,
                                         '-Pv2M5NativeM4RuntimeOutput=' + str(delete_runtime),
                                         ':nereus-pulsar-offload:' + delete_sdk_task],
                                        cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT).returncode
        assert all(hashlib.sha256(Path(p).read_bytes()).hexdigest() == sha for p, sha in inputs.items())
        target = Path(str(PREFIX) + '-junit') / delete_sdk_task
        for path in sorted((ROOT / 'nereus-pulsar-offload/build/test-results' / delete_sdk_task).glob('TEST-*.xml')):
            assert path.stat().st_mtime_ns >= started_delete_sdk
            target.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, target / path.name)
            suite = ET.parse(path).getroot()
            junit[delete_sdk_task + '/' + path.name] = {'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
                                                      **{k: int(suite.get(k, '0')) for k in ['tests', 'failures', 'errors', 'skipped']}}
    if actual_exit == 0 and PHASE.startswith('green'):
        capsule = Path(str(PREFIX) + '-sdk-drain-admission.properties')
        runtime = Path(str(PREFIX) + '-runtime.properties')
        configurations = [[line.split('=', 1)[1] for line in path.read_text().splitlines()
                           if line.startswith('configuration=')] for path in (runtime, delete_runtime)]
        assert all(len(values) == 1 for values in configurations)
        assert configurations[0] != configurations[1], 'distinct RETAIN/DELETE SDK configurations were not executed'
        retained_suites = {name: result for name, result in junit.items() if not name.startswith(delete_sdk_task + '/')}
        capsule_digest = write_capsule(capsule, runtime, retained_suites)
        delete_capsule = Path(str(PREFIX) + '-bk-delete-sdk-drain-admission.properties')
        delete_suites = {name: result for name, result in junit.items()
                         if name.split('/', 1)[0] in {delete_sdk_task, 'v2M5S3ProviderDrainRealTest'}}
        delete_capsule_digest = write_capsule(delete_capsule, delete_runtime, delete_suites)
        started_owner = time.time_ns()
        print('current_native_sdk_drain_capsule', capsule_digest, flush=True)
        print('current_native_bk_delete_sdk_drain_capsule', delete_capsule_digest, flush=True)
        with Path(str(PREFIX) + '-owner.log').open('w') as log:
            actual_exit = subprocess.run(['./gradlew', '--offline', '--no-daemon', '--no-configuration-cache',
                                         '--no-parallel', '--max-workers=2', '-PtestFailFast=false',
                                         '--init-script', str(ROOT / 'config/v2/m5/evidence-native.init.gradle'),
                                         '-PpulsarCheckout=' + str(PULSAR),
                                     '-Dnereus.m5.evidenceMavenRepository=' + str(args.maven_repository.resolve()),
                                     '-Dnereus.m5.evidenceSdkTestingRepository=' + str(ROOT / 'build/m5-native-pulsar-owner-local-dependencies'),
                                     '-Dnereus.m5.evidenceClassLogDirectory=' + str(args.class_logs.resolve()),
                                     '-Dnereus.m5.evidenceMethodNames=true',
                                         '-Pv2M5NativeOffloadEndpoint=' + endpoint,
                                         '-Pv2M5NativeOffloadZk=127.0.0.1:2181',
                                         '-Pv2M5NativeOffloadOxia=' + oxia_address,
                                         '-Pv2M5NativeM4AdmissionFile=' + str(capsule),
                                         '-Pv2M5NativeM4AdmissionSha256=' + capsule_digest,
                                         '-Pv2M5NativeBkGcAdmissionFile=' + str(delete_capsule),
                                         '-Pv2M5NativeBkGcAdmissionSha256=' + delete_capsule_digest,
                                         ':nereus-pulsar-offload:v2M5NativeM4OwnerRealTest',
                                         *[value for name in OWNER_TESTS for value in ('--tests', name)]],
                                        cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT).returncode
        assert all(hashlib.sha256(Path(p).read_bytes()).hexdigest() == sha for p, sha in inputs.items())
        task = 'v2M5NativeM4OwnerRealTest'
        target = Path(str(PREFIX) + '-junit') / task
        for path in sorted((ROOT / 'nereus-pulsar-offload/build/test-results' / task).glob('TEST-*.xml')):
            assert path.stat().st_mtime_ns >= started_owner
            target.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, target / path.name)
            suite = ET.parse(path).getroot()
            junit[task + '/' + path.name] = {'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
                                           **{k: int(suite.get(k, '0')) for k in ['tests', 'failures', 'errors', 'skipped']}}
    if actual_exit == 0 and PHASE.startswith('green'):
        started_negative = time.time_ns()
        with Path(str(PREFIX) + '-namespace-negative.log').open('w') as log:
            actual_exit = subprocess.run(['./gradlew', '--offline', '--no-daemon', '--no-configuration-cache',
                                         '--no-parallel', '--max-workers=2', '-PtestFailFast=false',
                                         '--init-script', str(ROOT / 'config/v2/m5/evidence-native.init.gradle'),
                                         '-PpulsarCheckout=' + str(PULSAR),
                                     '-Dnereus.m5.evidenceMavenRepository=' + str(args.maven_repository.resolve()),
                                     '-Dnereus.m5.evidenceSdkTestingRepository=' + str(ROOT / 'build/m5-native-pulsar-owner-local-dependencies'),
                                     '-Dnereus.m5.evidenceClassLogDirectory=' + str(args.class_logs.resolve()),
                                     '-Dnereus.m5.evidenceMethodNames=true',
                                         '-Pv2M5NativeOffloadEndpoint=' + endpoint,
                                         '-Pv2M5NativeOffloadZk=127.0.0.1:2181',
                                         '-Pv2M5NativeOffloadOxia=' + oxia_address,
                                         ':nereus-pulsar-offload:v2M5NativeOffloadRealTest',
                                         '--tests', '*.nativeBindingCapturesExactP1OwnerAndObjectNamespace'],
                                        cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT).returncode
        assert all(hashlib.sha256(Path(p).read_bytes()).hexdigest() == sha for p, sha in inputs.items())
        task = 'native-namespace-negative'
        target = Path(str(PREFIX) + '-junit') / task
        for path in sorted((ROOT / 'nereus-pulsar-offload/build/test-results/v2M5NativeOffloadRealTest').glob('TEST-*.xml')):
            assert path.stat().st_mtime_ns >= started_negative
            target.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, target / path.name)
            suite = ET.parse(path).getroot()
            junit[task + '/' + path.name] = {'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
                                           **{k: int(suite.get(k, '0')) for k in ['tests', 'failures', 'errors', 'skipped']}}
    write('-result.json', {'actualGradleExit': actual_exit, 'inputsUnchanged': True, 'junit': junit,
                           'nativePulsarM5LifecycleAuthority': False, 'wholeM5Acceptance': False})
    print('actual_gradle_exit', actual_exit, 'suites', len(junit), flush=True)
finally:
    with Path(str(PREFIX) + '-bk.log').open('w') as log:
        subprocess.run(compose_command + ['logs'], cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
    if oxia_id:
        with Path(str(PREFIX) + '-oxia.log').open('w') as log:
            subprocess.run(['docker', 'logs', oxia_id], stdout=log, stderr=subprocess.STDOUT)
        subprocess.run(['docker', 'rm', '-f', '-v', oxia_id], stdout=subprocess.DEVNULL, check=True)
    if minio_id:
        with Path(str(PREFIX) + '-minio.log').open('w') as log:
            subprocess.run(['docker', 'logs', minio_id], stdout=log, stderr=subprocess.STDOUT)
        subprocess.run(['docker', 'rm', '-f', '-v', minio_id], stdout=subprocess.DEVNULL, check=True)
    subprocess.run(compose_command + ['down', '-v', '--remove-orphans'], cwd=ROOT,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
    current = containers()
    assert all(current.get(identity) == state for identity, state in original.items()), (original, current)
    assert all(item['id'] not in current for item in owned), (owned, current)
    external = {identity: state for identity, state in current.items() if identity not in original}
    assert not output(['docker', 'volume', 'ls', '-q', '--filter', 'label=com.docker.compose.project=' + PROJECT])
    assert not output(['docker', 'network', 'ls', '-q', '--filter', 'label=com.docker.compose.project=' + PROJECT])
    assert_native_checkout()
    write('-cleanup.json', {'actualGradleExit': actual_exit, 'ownedContainersNetworksVolumesAbsent': True,
                            'originalContainersUnchanged': True, 'externalContainersObservedUnmodified': external,
                            'pulsarOwnedPathsUnchanged': True, 'pulsarCheckoutClean': not bool(output(['git', '-C', str(PULSAR), 'status', '--porcelain']))})
    print('owned_cleanup_verified', PROJECT, flush=True)
if actual_exit == 0:
    rows = []
    for name in junit:
        xml = Path(str(PREFIX) + '-junit') / name
        label = name.split('/', 1)[0]
        if label == 'native-managed-ledger-test':
            task = ':managed-ledger:test'
        elif label == 'bookkeeper-module-test':
            task = ':nereus-storage-bookkeeper:test'
        elif label == 'native-namespace-negative':
            task = ':nereus-pulsar-offload:v2M5NativeOffloadRealTest'
        else:
            task = ':nereus-pulsar-offload:' + label
        rows.append({'task': task, 'path': str(xml.relative_to(args.output.resolve())),
                     'producedAtMicros': xml.stat().st_mtime_ns // 1000})
    assert INPUTS.verify(PREPARED) == COHORT
    (args.output / 'stage-result.json').write_text(json.dumps({'junit': rows, 'exitCode': actual_exit},
                                                            sort_keys=True, indent=2) + '\n')
sys.exit(actual_exit if actual_exit is not None else 1)
