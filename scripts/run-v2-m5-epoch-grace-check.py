import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import urllib.request
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / 'build/m5-epoch-grace' / ('owned-' + str(os.getpid()))
assert not sys.argv[1:] and not OUT.exists()
OUT.mkdir(parents=True)
PROJECT = 'nereus-m5-epoch-grace-' + str(os.getpid())
COMPOSE = ['docker', 'compose', '-p', PROJECT, '-f', str(ROOT / 'config/v2/m2/kafka/k9/bookkeeper-conformance.compose.yml')]
OXIA = 'nereus/oxia-m3-allocator:37a17bef1720'

def output(args):
    return subprocess.check_output(args, text=True).strip()

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def write(name, value):
    (OUT / name).write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')

def containers():
    ids = output(['docker', 'ps', '-aq']).split()
    return {} if not ids else {x['Id']: (x['Name'], x['State']['Status']) for x in json.loads(output(['docker', 'inspect', *ids]))}

assert output(['docker', 'image', 'inspect', OXIA, '--format', '{{.Id}}']) == 'sha256:7eef9af2cdc897fbf418bf7616da1387aca87ce860b8205395cdf88b867df4da'
assert output(['docker', 'image', 'inspect', 'apache/bookkeeper@sha256:c0a128931c402d6bf6a6f973ba2f305b9be261659e30754ab95a29510a33bc0d', '--format', '{{.Id}}']) == 'sha256:d0e78aaf987ac2feb526507ffb7d4c5137d58c0530f2a8cab4a9595abc89d605'
paths = set()
for module in ('nereus-domain', 'nereus-storage-api', 'nereus-metadata-spi', 'nereus-storage-object',
               'nereus-storage-bookkeeper', 'nereus-kafka-bookkeeper', 'nereus-metadata-oxia'):
    paths.update(str(p.relative_to(ROOT)) for p in (ROOT / module / 'src/main/java').rglob('*.java'))
    paths.add(module + '/build.gradle.kts')
for directory in ('nereus-kafka-bookkeeper/src/realBookKeeperTest/java',
                  'nereus-storage-object/src/test/java/com/nereusstream/storage/object/gc'):
    paths.update(str(p.relative_to(ROOT)) for p in (ROOT / directory).rglob('*.java'))
paths.update(('build.gradle.kts', 'settings.gradle.kts', 'gradle/libs.versions.toml',
              'config/v2/m2/kafka/k9/bookkeeper-conformance.compose.yml',
              str(Path(__file__).relative_to(ROOT))))
manifest = {p: sha(ROOT / p) for p in sorted(paths)}
write('tested-inputs.json', manifest)
for p in manifest:
    dest = OUT / 'source-archive/sources' / p
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(ROOT / p, dest)
original = containers()
write('original-containers.json', original)
oxia_id = None
code = None
try:
    subprocess.run(COMPOSE + ['up', '-d', '--wait'], check=True)
    oxia_id = output(['docker', 'run', '-d', '--name', PROJECT + '-oxia', '--publish', '127.0.0.1::6648', '--publish', '127.0.0.1::8080', OXIA, 'oxia', 'standalone', '--shards=4'])
    port = output(['docker', 'port', oxia_id, '6648/tcp']).rsplit(':', 1)[1]
    metrics = output(['docker', 'port', oxia_id, '8080/tcp']).rsplit(':', 1)[1]
    for attempt in range(60):
        try:
            with urllib.request.urlopen('http://127.0.0.1:' + metrics + '/metrics', timeout=2) as response:
                (OUT / 'metrics.txt').write_bytes(response.read())
            break
        except Exception:
            if attempt == 59: raise
            time.sleep(1)
    write('backend.json', {'project': PROJECT, 'oxiaId': oxia_id, 'oxiaAddress': '127.0.0.1:' + port})
    args = ['./gradlew', '--offline', '--no-daemon', '--no-configuration-cache', '--max-workers=2', '-Pv2M2BookKeeperMetadataServiceUri=zk://127.0.0.1:2181/ledgers', '-Pv2M5RetentionOxiaServiceAddress=127.0.0.1:' + port, ':nereus-storage-object:checkstyleMain', ':nereus-storage-object:checkstyleTest', ':nereus-kafka-bookkeeper:checkstyleMain', ':nereus-kafka-bookkeeper:checkstyleRealBookKeeperTest', ':nereus-kafka-bookkeeper:v2M5EpochGraceRealTest']
    started = time.time_ns()
    with (OUT / 'gradle.log').open('w') as log:
        code = subprocess.run(args, cwd=ROOT, env=dict(os.environ), stdout=log, stderr=subprocess.STDOUT).returncode
    xml = ROOT / 'nereus-kafka-bookkeeper/build/test-results/v2M5EpochGraceRealTest/TEST-com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperBoundDeleteV2RealTest.xml'
    counts = None
    if xml.exists() and xml.stat().st_mtime_ns >= started:
        shutil.copy2(xml, OUT / 'native-grace.xml')
        counts = {k: int(ET.parse(xml).getroot().attrib[k]) for k in ('tests', 'failures', 'errors', 'skipped')}
    unchanged = all(sha(ROOT / p) == expected for p, expected in manifest.items())
    write('result.json', {'actualGradleExit': code, 'counts': counts, 'freshXml': counts is not None, 'capturedInputsUnchangedDuringRun': unchanged, 'elapsedDependencyOnly': True, 'fullReferenceGraceAssembly': False, 'physicalDeleteAuthority': False, 'wholeM5Authority': False})
    assert code == 0 and counts == {'tests': 1, 'failures': 0, 'errors': 0, 'skipped': 0} and unchanged
finally:
    if oxia_id:
        with (OUT / 'oxia.log').open('w') as log:
            subprocess.run(['docker', 'logs', oxia_id], stdout=log, stderr=subprocess.STDOUT)
        subprocess.run(['docker', 'rm', '-f', oxia_id], check=True, stdout=subprocess.DEVNULL)
    with (OUT / 'bookkeeper.log').open('w') as log:
        subprocess.run(COMPOSE + ['logs'], stdout=log, stderr=subprocess.STDOUT)
    subprocess.run(COMPOSE + ['down', '-v', '--remove-orphans'], check=True)
    current = containers()
    write('cleanup.json', {'originalContainersUnchanged': all(current.get(k) == v for k, v in original.items()), 'ownedContainersAbsent': not any(PROJECT in v[0] for v in current.values()), 'actualGradleExit': code})
    assert all(current.get(k) == v for k, v in original.items())
    assert not any(PROJECT in v[0] for v in current.values())

print('PASS_V2_M5_EPOCH_GRACE_DEPENDENCY_NON_PROMOTABLE output=' + str(OUT), flush=True)
